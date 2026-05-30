//! Shared HTTP handler state.
//!
//! Grouped along responsibilities:
//! * [`RuntimeRefs`] — things handlers *use* (store, prober).
//! * [`ObservabilityCtx`] — things handlers *report* (metrics, system, uptime).
//! * Top-level: configuration and per-request services (rate limiter, cache).

use std::sync::Arc;
use std::time::{Duration, Instant};

use simply_kaspa_dnsseeder_store::PeerStore;
use sysinfo::System;
use tokio::sync::RwLock;

use crate::config::WebConfig;
use crate::metrics::WebMetrics;
use crate::metrics::{MetricsSource, NullMetricsSource};
use crate::runtime::PeersCache;
use crate::runtime::Prober;
use simply_kaspa_dnsseeder_common::RateLimiter;

const PEERS_CACHE_TTL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct RuntimeRefs {
    pub store: PeerStore,
    pub prober: Arc<dyn Prober>,
}

#[derive(Clone)]
pub struct ObservabilityCtx {
    pub metrics: Arc<WebMetrics>,
    pub metrics_source: Arc<dyn MetricsSource>,
    pub system: Arc<RwLock<System>>,
    pub started: Instant,
}

/// Shared handler state. Cheap to clone — every field is an `Arc` or `Copy`.
#[derive(Clone)]
pub struct AppState {
    pub runtime: RuntimeRefs,
    pub obs: ObservabilityCtx,
    pub config: Arc<WebConfig>,
    pub limiter: Arc<RateLimiter>,
    pub peers_cache: Arc<PeersCache>,
}

impl AppState {
    /// Start a builder. Defaults provide fresh `WebMetrics` + a null metrics source,
    /// suitable for tests. The binary overrides both via [`AppStateBuilder`].
    #[must_use]
    pub fn builder(store: PeerStore, prober: Arc<dyn Prober>, config: WebConfig) -> AppStateBuilder {
        AppStateBuilder::new(store, prober, config)
    }
}

/// Builder for [`AppState`] with optional observability overrides.
pub struct AppStateBuilder {
    store: PeerStore,
    prober: Arc<dyn Prober>,
    config: WebConfig,
    metrics: Option<Arc<WebMetrics>>,
    metrics_source: Option<Arc<dyn MetricsSource>>,
}

impl AppStateBuilder {
    fn new(store: PeerStore, prober: Arc<dyn Prober>, config: WebConfig) -> Self {
        Self {
            store,
            prober,
            config,
            metrics: None,
            metrics_source: None,
        }
    }

    #[must_use]
    pub fn metrics(mut self, metrics: Arc<WebMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn metrics_source(mut self, source: Arc<dyn MetricsSource>) -> Self {
        self.metrics_source = Some(source);
        self
    }

    #[must_use]
    pub fn build(self) -> AppState {
        let limiter = Arc::new(RateLimiter::new(self.config.post_rate_limit, self.config.rate_limit_window));
        AppState {
            runtime: RuntimeRefs {
                store: self.store,
                prober: self.prober,
            },
            obs: ObservabilityCtx {
                metrics: self.metrics.unwrap_or_else(|| Arc::new(WebMetrics::new())),
                metrics_source: self.metrics_source.unwrap_or_else(|| Arc::new(NullMetricsSource)),
                system: Arc::new(RwLock::new(System::new())),
                started: Instant::now(),
            },
            config: Arc::new(self.config),
            limiter,
            peers_cache: Arc::new(PeersCache::new(PEERS_CACHE_TTL)),
        }
    }
}
