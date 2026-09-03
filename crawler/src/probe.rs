use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use kaspa_p2p_lib::{Adaptor, Hub};
use kaspa_utils_tower::counters::TowerConnectionCounters;
use log::{debug, warn};
use tokio::sync::oneshot;

use crate::error::ProbeError;
use crate::model::ProbeResult;
use crate::probe::initializer::{PendingMap, ProbeInitializer, ProbeInitializerConfig, ProbeOutcome};

pub mod initializer;
pub mod runner;

#[cfg(test)]
mod fake_peer;
#[cfg(test)]
mod initializer_tests;
#[cfg(test)]
mod runner_tests;
#[cfg(test)]
mod tests;

// Bound on the post-probe router close so a saturated Hub channel can't stall the caller.
const CLOSE_GRACE: Duration = Duration::from_secs(2);

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
}

impl KaspadProbe {
    #[must_use]
    pub fn new(config: ProbeInitializerConfig) -> Self {
        let pending: PendingMap = Arc::new(DashMap::new());
        let initializer = Arc::new(ProbeInitializer::new(config, pending.clone()));
        let adaptor = Adaptor::client_only(Hub::new(), initializer, Arc::new(TowerConnectionCounters::default()));
        Self { adaptor, pending }
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

struct PendingGuard<'a> {
    pending: &'a PendingMap,
    addr: SocketAddr,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.pending.remove(&self.addr);
    }
}

#[async_trait]
impl Probe for KaspadProbe {
    async fn probe(&self, addr: SocketAddr) -> Result<ProbeResult, ProbeError> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert(addr, tx);
        // Clears the entry on every exit path, including this future being cancelled
        // mid-handshake (the initializer normally removes it, making this a no-op).
        let _pending_guard = PendingGuard {
            pending: &self.pending,
            addr,
        };

        if let Err(err) = self.adaptor.connect_peer(addr.to_string()).await {
            // The connection handler already closed the router on this path.
            return Err(ProbeError::Connection(err.to_string()));
        }
        let Ok(ProbeOutcome { router, result }) = rx.await else {
            return Err(ProbeError::Handshake("probe outcome channel dropped".into()));
        };

        // Close the router directly rather than via `Adaptor::terminate`: the
        // latter only closes routers already inserted into `Hub.peers`, and the
        // Hub event loop has usually not processed `NewPeer` yet at this point,
        // making it a silent no-op that leaves the connection open forever.
        // `connect_peer` returns only after `NewPeer` is queued, so the
        // `PeerClosing` sent by `close()` is ordered after it and the Hub entry
        // is removed. `close()` tears down the routes synchronously and only
        // awaits on the Hub channel, so a detached close still frees the socket.
        let close_task = tokio::spawn(async move {
            if !router.close().await {
                debug!("crawler: probe {}: router was already closed by the peer", router.net_address());
            }
        });
        if tokio::time::timeout(CLOSE_GRACE, close_task).await.is_err() {
            warn!("crawler: probe {addr}: router close exceeded {CLOSE_GRACE:?}, detaching close task");
        }

        result
    }

    fn active_peers_len(&self) -> usize {
        self.adaptor.active_peers_len()
    }

    async fn close(&self) {
        self.adaptor.close().await;
    }
}
