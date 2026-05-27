//! [`Probe`] trait + the real kaspa-p2p-lib backed implementation.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use kaspa_p2p_lib::{Adaptor, Hub};
use kaspa_utils_tower::counters::TowerConnectionCounters;
use log::warn;
use tokio::sync::oneshot;

use crate::error::ProbeError;
use crate::model::ProbeResult;
use crate::probe_initializer::{PendingMap, ProbeInitializer, ProbeInitializerConfig};

/// Stateless single-shot peer probe.
#[async_trait]
pub trait Probe: Send + Sync {
    async fn probe(&self, addr: SocketAddr) -> Result<ProbeResult, ProbeError>;
}

/// Real implementation built on top of [`kaspa_p2p_lib::Adaptor`].
pub struct KaspadProbe {
    adaptor: Arc<Adaptor>,
    pending: PendingMap,
    overall_timeout: Duration,
}

impl KaspadProbe {
    #[must_use]
    pub fn new(config: ProbeInitializerConfig) -> Self {
        let overall_timeout = config.handshake_timeout + config.addresses_timeout + Duration::from_secs(2);
        let pending: PendingMap = Arc::new(DashMap::new());
        let initializer = Arc::new(ProbeInitializer::new(config, pending.clone()));
        let adaptor = Adaptor::client_only(Hub::new(), initializer, Arc::new(TowerConnectionCounters::default()));
        Self { adaptor, pending, overall_timeout }
    }
}

#[async_trait]
impl Probe for KaspadProbe {
    async fn probe(&self, addr: SocketAddr) -> Result<ProbeResult, ProbeError> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert(addr, tx);

        let adaptor = self.adaptor.clone();
        let pending = self.pending.clone();
        let work = async move {
            match adaptor.connect_peer(addr.to_string()).await {
                Ok(peer_key) => {
                    let outcome = rx.await.map_err(|_| ProbeError::Handshake("probe outcome channel dropped".into()))?;
                    adaptor.terminate(peer_key).await;
                    outcome
                }
                Err(err) => {
                    pending.remove(&addr);
                    Err(ProbeError::Connection(err.to_string()))
                }
            }
        };

        let res = tokio::time::timeout(self.overall_timeout, work).await;
        match res {
            Ok(r) => r,
            Err(_) => {
                if self.pending.remove(&addr).is_some() {
                    warn!("probe {addr}: overall timeout, dropping pending entry");
                }
                Err(ProbeError::Timeout)
            }
        }
    }
}
