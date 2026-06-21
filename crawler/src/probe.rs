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
use crate::probe::initializer::{PendingMap, ProbeInitializer, ProbeInitializerConfig};

pub mod initializer;
pub mod runner;

#[cfg(test)]
mod initializer_tests;
#[cfg(test)]
mod runner_tests;
#[cfg(test)]
mod tests;

// Bound on the post-probe peer shutdown so a hung peer can't stall the caller.
const TERMINATE_GRACE: Duration = Duration::from_secs(2);

#[async_trait]
pub trait Probe: Send + Sync {
    async fn probe(&self, addr: SocketAddr) -> Result<ProbeResult, ProbeError>;
    /// Number of peers currently tracked by the underlying Hub. Returns 0 for mock impls.
    fn active_peers_len(&self) -> usize {
        0
    }
    /// Terminate all in-flight peer connections owned by the probe. Default is a no-op.
    async fn close(&self) {}
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
        Self {
            adaptor,
            pending,
            probe_timeout,
        }
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
                warn!("crawler: probe {addr}: connect timeout");
                return Err(ProbeError::Timeout);
            }
        };

        let remaining = deadline.saturating_duration_since(Instant::now());
        let outcome = tokio::time::timeout(remaining, rx).await;

        // `connect_peer` returns only after `HubEvent::NewPeer` is queued, so
        // `terminate` here pushes `PeerClosing` after `NewPeer` and the router
        // is correctly removed from `Hub.peers`.
        let adaptor = self.adaptor.clone();
        let terminate_task = tokio::spawn(async move {
            adaptor.terminate(peer_key).await;
        });
        if tokio::time::timeout(TERMINATE_GRACE, terminate_task).await.is_err() {
            warn!("crawler: probe {addr}: terminate exceeded {TERMINATE_GRACE:?}, detaching close task");
        }

        match outcome {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => {
                self.pending.remove(&addr);
                Err(ProbeError::Handshake("probe outcome channel dropped".into()))
            }
            Err(_) => {
                self.pending.remove(&addr);
                warn!("crawler: probe {addr}: overall timeout, dropping pending entry");
                Err(ProbeError::Timeout)
            }
        }
    }

    fn active_peers_len(&self) -> usize {
        self.adaptor.active_peers_len()
    }

    async fn close(&self) {
        self.adaptor.close().await;
    }
}
