//! Abstraction over the crawler so the HTTP layer can be tested without
//! standing up the whole p2p stack.

use std::net::SocketAddr;

use async_trait::async_trait;
use simply_kaspa_dnsseeder_crawler::{KaspadProbe, Probe, ProbeError, Scheduler};
use simply_kaspa_dnsseeder_store::{PeerRecord, PeerStore};

#[async_trait]
pub trait Prober: Send + Sync + 'static {
    async fn probe(&self, addr: SocketAddr) -> Result<PeerRecord, ProbeError>;
}

/// Default impl that runs the probe via the same code path the scheduler uses
/// for periodic crawls.
pub struct SchedulerProber {
    probe: std::sync::Arc<KaspadProbe>,
    store: PeerStore,
}

impl SchedulerProber {
    #[must_use]
    pub fn new(probe: std::sync::Arc<KaspadProbe>, store: PeerStore) -> Self {
        Self { probe, store }
    }
}

#[async_trait]
impl Prober for SchedulerProber {
    async fn probe(&self, addr: SocketAddr) -> Result<PeerRecord, ProbeError> {
        Scheduler::probe_and_store(self.probe.as_ref() as &dyn Probe, &self.store, addr).await
    }
}
