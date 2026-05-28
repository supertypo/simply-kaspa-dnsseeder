use std::sync::Arc;
use std::time::Instant;

use simply_kaspa_dnsseeder_store::PeerStore;
use sysinfo::System;
use tokio::sync::RwLock;

use crate::config::WebConfig;
use crate::metrics::WebMetrics;
use crate::metrics_source::{MetricsSource, NullMetricsSource};
use crate::peers_cache::PeersCache;
use crate::prober::Prober;
use simply_kaspa_dnsseeder_common::RateLimiter;

/// Shared handler state. Cheap to clone — every field is an `Arc` or
/// otherwise copy-on-write.
#[derive(Clone)]
pub struct AppState {
    pub store: PeerStore,
    pub prober: Arc<dyn Prober>,
    pub config: Arc<WebConfig>,
    pub limiter: Arc<RateLimiter>,
    pub metrics: Arc<WebMetrics>,
    pub metrics_source: Arc<dyn MetricsSource>,
    pub system: Arc<RwLock<System>>,
    pub started: Instant,
    pub peers_cache: Arc<PeersCache>,
}

impl AppState {
    /// Default constructor: spins up fresh metrics and a null metrics source.
    /// Use this from tests and any production path that doesn't aggregate
    /// cross-subsystem metrics.
    #[must_use]
    pub fn new(store: PeerStore, prober: Arc<dyn Prober>, config: WebConfig) -> Self {
        Self::full(store, prober, config, Arc::new(WebMetrics::new()), Arc::new(NullMetricsSource))
    }

    /// Full constructor for the binary, which already owns shared
    /// [`WebMetrics`] and an aggregating [`MetricsSource`].
    #[must_use]
    pub fn full(
        store: PeerStore,
        prober: Arc<dyn Prober>,
        config: WebConfig,
        metrics: Arc<WebMetrics>,
        metrics_source: Arc<dyn MetricsSource>,
    ) -> Self {
        let limiter = Arc::new(RateLimiter::new(config.post_rate_limit, config.rate_limit_window));
        Self {
            store,
            prober,
            config: Arc::new(config),
            limiter,
            metrics,
            metrics_source,
            system: Arc::new(RwLock::new(System::new())),
            started: Instant::now(),
            peers_cache: Arc::new(PeersCache::new(std::time::Duration::from_secs(5))),
        }
    }
}
