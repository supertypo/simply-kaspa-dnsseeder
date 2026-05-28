use std::sync::Arc;
use std::time::Instant;

use simply_kaspa_dnsseeder_store::PeerStore;
use sysinfo::System;
use tokio::sync::RwLock;

use crate::config::WebConfig;
use crate::metrics::WebMetrics;
use crate::metrics_source::{MetricsSource, NullMetricsSource};
use crate::prober::Prober;
use crate::rate_limit::RateLimiter;

/// Shared state passed to every handler. Cheap to clone — every field is
/// either an `Arc` or copy-on-write itself.
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
}

impl AppState {
    #[must_use]
    pub fn new(store: PeerStore, prober: Arc<dyn Prober>, config: WebConfig) -> Self {
        Self::with_metrics(store, prober, config, Arc::new(WebMetrics::new()))
    }

    #[must_use]
    pub fn with_metrics(
        store: PeerStore,
        prober: Arc<dyn Prober>,
        config: WebConfig,
        metrics: Arc<WebMetrics>,
    ) -> Self {
        Self::full(store, prober, config, metrics, Arc::new(NullMetricsSource))
    }

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
        }
    }
}
