//! Bridges crawler + dns counters into the web layer's [`MetricsSource`] trait
//! so the `web` crate can include them in `/api/metrics` without depending on
//! the crawler or dns crates directly.

use std::sync::Arc;

use serde_json::{Value, json};
use simply_kaspa_dnsseeder_common::{RateLimiter, now_ms};
use simply_kaspa_dnsseeder_crawler::CrawlerMetrics;
use simply_kaspa_dnsseeder_dns::{DnsMetrics, ServingCache};
use simply_kaspa_dnsseeder_web::MetricsSource;

pub struct SubsystemMetrics {
    pub crawler: Arc<CrawlerMetrics>,
    pub dns: Arc<DnsMetrics>,
    pub dns_limiter: Option<Arc<RateLimiter>>,
    pub serving_cache: Option<Arc<ServingCache>>,
}

impl MetricsSource for SubsystemMetrics {
    fn extra(&self) -> Value {
        let c = self.crawler.snapshot();
        let d = self.dns.snapshot();
        let dns_rate_limiter = match self.dns_limiter.as_ref() {
            Some(rl) => json!({
                "capacity": rl.capacity(),
                "window_ms": u64::try_from(rl.window().as_millis()).unwrap_or(u64::MAX),
                "ops": rl.ops(),
                "tracked_ips": rl.tracked_ips(),
                "denied": d.denied,
            }),
            None => json!({ "denied": d.denied }),
        };
        let mut out = json!({
            "crawler": {
                "ok": c.ok,
                "failed": c.failed,
                "in_flight": c.in_flight,
                "failed_connect": c.failed_connect,
                "failed_handshake": c.failed_handshake,
                "failed_addresses": c.failed_addresses,
                "failed_timeout": c.failed_timeout,
                "failed_too_many_addresses": c.failed_too_many_addresses,
                "probes_skipped_backpressure": c.probes_skipped_backpressure,
            },
            "dns": {
                "answered": d.answered,
                "empty": d.empty,
                "refused": d.refused,
                "a": d.a,
                "aaaa": d.aaaa,
                "rate_limiter": dns_rate_limiter,
            },
        });
        if let Some(cache) = self.serving_cache.as_ref() {
            let snap = cache.load();
            let last = cache.last_refresh_ms();
            let age_ms = if last > 0 { (now_ms() - last).max(0) } else { -1 };
            out["serving_cache"] = json!({
                "v4_size": snap.v4_len(),
                "v6_size": snap.v6_len(),
                "last_refresh_ms": last,
                "last_refresh_age_ms": age_ms,
            });
        }
        out
    }
}
