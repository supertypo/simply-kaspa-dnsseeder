use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use kaspa_p2p_lib::{Adaptor, Hub};
use kaspa_utils_tower::counters::TowerConnectionCounters;
use log::warn;
use tokio::sync::oneshot;

use crate::error::ProbeError;
use crate::model::ProbeResult;
use crate::probe_initializer::{PendingMap, ProbeInitializer, ProbeInitializerConfig};

// Bound on the post-probe peer shutdown so a hung peer can't stall the caller.
const TERMINATE_GRACE: Duration = Duration::from_secs(2);

// Hub event-loop poll: avoids a race where Adaptor::connect_peer returns before
// HubEvent::NewPeer has been processed, which would make Hub::terminate a no-op.
const TERMINATE_HUB_POLL_INTERVAL: Duration = Duration::from_millis(50);
const TERMINATE_HUB_POLL_ATTEMPTS: u32 = 20;

#[async_trait]
pub trait Probe: Send + Sync {
    async fn probe(&self, addr: SocketAddr) -> Result<ProbeResult, ProbeError>;
}

pub struct KaspadProbe {
    adaptor: Arc<Adaptor>,
    pending: PendingMap,
    probe_timeout: Duration,
}

impl KaspadProbe {
    #[must_use]
    pub fn new(config: ProbeInitializerConfig) -> Self {
        let probe_timeout = config.probe_timeout;
        let pending: PendingMap = Arc::new(DashMap::new());
        let initializer = Arc::new(ProbeInitializer::new(config, pending.clone()));
        let adaptor = Adaptor::client_only(Hub::new(), initializer, Arc::new(TowerConnectionCounters::default()));
        Self { adaptor, pending, probe_timeout }
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

#[async_trait]
impl Probe for KaspadProbe {
    async fn probe(&self, addr: SocketAddr) -> Result<ProbeResult, ProbeError> {
        let deadline = Instant::now() + self.probe_timeout;
        let (tx, rx) = oneshot::channel();
        self.pending.insert(addr, tx);

        let connect_budget = deadline.saturating_duration_since(Instant::now());
        let peer_key = match tokio::time::timeout(connect_budget, self.adaptor.connect_peer(addr.to_string())).await {
            Ok(Ok(k)) => k,
            Ok(Err(err)) => {
                self.pending.remove(&addr);
                return Err(ProbeError::Connection(err.to_string()));
            }
            Err(_) => {
                self.pending.remove(&addr);
                warn!("probe {addr}: connect timeout");
                return Err(ProbeError::Timeout);
            }
        };

        let remaining = deadline.saturating_duration_since(Instant::now());
        let outcome = tokio::time::timeout(remaining, rx).await;

        // Wait for the Hub event loop to register the peer before terminating;
        // otherwise `Hub::terminate` finds nothing and the router leaks in `Hub.peers`.
        for _ in 0..TERMINATE_HUB_POLL_ATTEMPTS {
            if self.adaptor.has_peer(peer_key) {
                break;
            }
            tokio::time::sleep(TERMINATE_HUB_POLL_INTERVAL).await;
        }

        // Always terminate the peer connection, even on timeout. Bounded so a
        // hung remote can't keep us inside this call indefinitely.
        let _ = tokio::time::timeout(TERMINATE_GRACE, self.adaptor.terminate(peer_key)).await;

        match outcome {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => {
                self.pending.remove(&addr);
                Err(ProbeError::Handshake("probe outcome channel dropped".into()))
            }
            Err(_) => {
                self.pending.remove(&addr);
                warn!("probe {addr}: overall timeout, dropping pending entry");
                Err(ProbeError::Timeout)
            }
        }
    }
}
