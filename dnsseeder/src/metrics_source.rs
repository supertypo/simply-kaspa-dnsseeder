//! Bridges crawler + dns counters into the web layer's [`MetricsSource`] trait
//! so the `web` crate can include them in `/api/metrics` without depending on
//! the crawler or dns crates directly.

use std::sync::Arc;

use serde_json::{Value, json};
use simply_kaspa_dnsseeder_crawler::CrawlerMetrics;
use simply_kaspa_dnsseeder_dns::DnsMetrics;
use simply_kaspa_dnsseeder_web::MetricsSource;

pub struct SubsystemMetrics {
    pub crawler: Arc<CrawlerMetrics>,
    pub dns: Arc<DnsMetrics>,
}

impl MetricsSource for SubsystemMetrics {
    fn extra(&self) -> Value {
        let c = self.crawler.snapshot();
        let d = self.dns.snapshot();
        json!({
            "crawler": {
                "ok": c.ok,
                "failed": c.failed,
                "in_flight": c.in_flight,
            },
            "dns": {
                "answered": d.answered,
                "empty": d.empty,
                "refused": d.refused,
                "throttled": d.throttled,
                "a": d.a,
                "aaaa": d.aaaa,
            },
        })
    }
}
