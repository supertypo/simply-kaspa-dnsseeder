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

#[cfg(test)]
mod tests {
    use super::*;
    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use tokio::net::TcpListener;

    /// Bind a listener that accepts but never reads/writes. The OS completes
    /// the TCP handshake from the listen backlog, so `connect_peer` succeeds
    /// at the socket level and then the gRPC client hangs waiting for h2
    /// SETTINGS — long enough for `probe_timeout` to fire.
    async fn black_hole_listener() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Hold the listener forever so the OS keeps accepting.
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout_returns_within_budget_and_cleans_pending() {
        let cfg = ProbeInitializerConfig::new(NetworkId::new(NetworkType::Mainnet), Duration::from_millis(300));
        let probe = KaspadProbe::new(cfg);
        let addr = black_hole_listener().await;

        let start = Instant::now();
        let res = probe.probe(addr).await;
        let elapsed = start.elapsed();

        // Any error is acceptable — Connection failures from the gRPC layer or
        // an outright Timeout — what matters is that we did not silently leak
        // and we returned bounded by probe_timeout + TERMINATE_GRACE.
        assert!(res.is_err(), "expected error against a black-hole listener, got {res:?}");
        assert!(
            elapsed < Duration::from_secs(4),
            "probe took {elapsed:?}; must return within probe_timeout + TERMINATE_GRACE + slack",
        );

        // Cleanup invariant: regardless of which branch we exit through, the
        // pending entry for `addr` must be gone.
        assert_eq!(probe.pending_len(), 0, "pending map leaked entries");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connection_refused_cleans_pending() {
        // Bind, immediately drop → the kernel sends RST on connect.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let cfg = ProbeInitializerConfig::new(NetworkId::new(NetworkType::Mainnet), Duration::from_secs(2));
        let probe = KaspadProbe::new(cfg);

        let res = probe.probe(addr).await;
        assert!(res.is_err());
        assert_eq!(probe.pending_len(), 0, "pending map leaked entries on connection error");
    }
}