use std::sync::Arc;

use simply_kaspa_dnsseeder_store::PeerStore;

use crate::config::WebConfig;
use crate::metrics::WebMetrics;
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
        let limiter = Arc::new(RateLimiter::new(config.post_rate_limit, config.rate_limit_window));
        Self { store, prober, config: Arc::new(config), limiter, metrics }
    }
}
