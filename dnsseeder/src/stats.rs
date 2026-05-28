//! Periodic stats dump.
//!
//! Aggregates counters from the crawler / DNS / web subsystems, a one-pass
//! summary of [`PeerStore`], and process metadata into a single formatted
//! info-level log block printed on a configurable interval.
//!
//! Counters are persisted to the store after every dump so cumulative totals
//! survive across restarts (worst case: last interval's increments are lost).

use std::sync::Arc;
use std::time::{Duration, Instant};

use kaspa_consensus_core::network::NetworkId;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use simply_kaspa_dnsseeder_crawler::{CrawlerMetrics, CrawlerSnapshot};
use simply_kaspa_dnsseeder_dns::{DnsMetrics, DnsSnapshot};
use simply_kaspa_dnsseeder_store::PeerStore;
use simply_kaspa_dnsseeder_web::{WebMetrics, WebSnapshot};
use tokio::sync::broadcast;

const METRICS_KEY: &str = "stats/snapshot_v1";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub crawler: CrawlerSnap,
    pub dns: DnsSnap,
    pub web: WebSnap,
    pub persisted_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CrawlerSnap {
    pub ok: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct DnsSnap {
    pub answered: u64,
    pub empty: u64,
    pub refused: u64,
    pub throttled: u64,
    pub a: u64,
    pub aaaa: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct WebSnap {
    pub requests: u64,
    pub accepted: u64,
    pub rejected: u64,
}

pub struct Metrics {
    pub crawler: Arc<CrawlerMetrics>,
    pub dns: Arc<DnsMetrics>,
    pub web: Arc<WebMetrics>,
    started: Instant,
    network: NetworkId,
    version: &'static str,
    stale_good: Duration,
}

impl Metrics {
    #[must_use]
    pub fn new(network: NetworkId, version: &'static str, stale_good: Duration) -> Arc<Self> {
        Arc::new(Self {
            crawler: Arc::new(CrawlerMetrics::new()),
            dns: Arc::new(DnsMetrics::new()),
            web: Arc::new(WebMetrics::new()),
            started: Instant::now(),
            network,
            version,
            stale_good,
        })
    }

    /// Load a persisted snapshot from the store and seed the atomic counters.
    /// Returns `Ok` even when nothing is found or the blob is unreadable; in
    /// those cases the counters start at zero.
    pub fn load_from(&self, store: &PeerStore) {
        let raw = match store.get_blob(METRICS_KEY) {
            Ok(v) => v,
            Err(err) => {
                warn!("stats: failed to read persisted snapshot: {err}");
                return;
            }
        };
        let Some(bytes) = raw else { return };
        let snap: MetricsSnapshot = match bincode::serde::decode_from_slice(&bytes, bincode::config::standard()) {
            Ok((v, _)) => v,
            Err(err) => {
                warn!("stats: persisted snapshot is unreadable, starting fresh: {err}");
                return;
            }
        };
        self.crawler.restore(&CrawlerSnapshot { ok: snap.crawler.ok, failed: snap.crawler.failed, in_flight: 0 });
        self.dns.restore(&DnsSnapshot {
            answered: snap.dns.answered,
            empty: snap.dns.empty,
            refused: snap.dns.refused,
            throttled: snap.dns.throttled,
            a: snap.dns.a,
            aaaa: snap.dns.aaaa,
        });
        self.web.restore(&WebSnapshot {
            requests: snap.web.requests,
            accepted: snap.web.accepted,
            rejected: snap.web.rejected,
        });
        debug!("stats: restored snapshot persisted at {}", snap.persisted_at_ms);
    }

    fn save_to(&self, store: &PeerStore, now_ms: i64) {
        let c = self.crawler.snapshot();
        let d = self.dns.snapshot();
        let w = self.web.snapshot();
        let snap = MetricsSnapshot {
            crawler: CrawlerSnap { ok: c.ok, failed: c.failed },
            dns: DnsSnap {
                answered: d.answered,
                empty: d.empty,
                refused: d.refused,
                throttled: d.throttled,
                a: d.a,
                aaaa: d.aaaa,
            },
            web: WebSnap { requests: w.requests, accepted: w.accepted, rejected: w.rejected },
            persisted_at_ms: now_ms,
        };
        let bytes = match bincode::serde::encode_to_vec(&snap, bincode::config::standard()) {
            Ok(b) => b,
            Err(err) => {
                warn!("stats: failed to encode snapshot: {err}");
                return;
            }
        };
        if let Err(err) = store.put_blob(METRICS_KEY, &bytes) {
            warn!("stats: failed to persist snapshot: {err}");
        }
    }

    /// Emit a single stats block and persist the snapshot.
    pub fn dump(&self, store: &PeerStore) {
        let now = now_ms();
        let stale_good_ms = i64::try_from(self.stale_good.as_millis()).unwrap_or(i64::MAX);
        let summary = match store.summary(now, stale_good_ms) {
            Ok(s) => s,
            Err(err) => {
                warn!("stats: store summary failed: {err}");
                return;
            }
        };
        let c = self.crawler.snapshot();
        let d = self.dns.snapshot();
        let w = self.web.snapshot();
        let lines = render(&Render {
            uptime: self.started.elapsed(),
            network: self.network,
            version: self.version,
            summary_total: summary.total,
            summary_good: summary.good,
            summary_failed: summary.failed,
            summary_v4: summary.v4,
            summary_v6: summary.v6,
            avg_age: Duration::from_millis(summary.avg_success_age_ms),
            crawler: c,
            dns: d,
            web: w,
        });
        for line in lines {
            info!("{line}");
        }
        self.save_to(store, now);
    }
}

pub async fn stats_loop(
    metrics: Arc<Metrics>,
    store: PeerStore,
    interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
) {
    if interval.is_zero() {
        return;
    }
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                debug!("stats: shutdown signal received");
                metrics.dump(&store);
                return;
            }
            _ = ticker.tick() => {
                metrics.dump(&store);
            }
        }
    }
}

struct Render {
    uptime: Duration,
    network: NetworkId,
    version: &'static str,
    summary_total: u64,
    summary_good: u64,
    summary_failed: u64,
    summary_v4: u64,
    summary_v6: u64,
    avg_age: Duration,
    crawler: CrawlerSnapshot,
    dns: DnsSnapshot,
    web: WebSnapshot,
}

const RULE_TOP: &str = "=========================================================================================================";
const RULE_MID: &str = "  ---------------------------------------------------------------------------------------------------";

fn render(r: &Render) -> Vec<String> {
    let mut out = Vec::with_capacity(16);
    out.push(RULE_TOP.to_string());
    out.push(row("node", "up", &format_uptime(r.uptime), "network", &r.network.to_string(), "version", r.version));
    out.push(RULE_MID.to_string());
    out.push(row(
        "peers",
        "good",
        &format_count(r.summary_good),
        "failed",
        &format_count(r.summary_failed),
        "total",
        &format_count(r.summary_total),
    ));
    out.push(row(
        "",
        "v4",
        &format_count(r.summary_v4),
        "v6",
        &format_count(r.summary_v6),
        "avg-age",
        &format_age(r.avg_age),
    ));
    out.push(RULE_MID.to_string());
    out.push(row(
        "crawler",
        "ok",
        &format_count(r.crawler.ok),
        "failed",
        &format_count(r.crawler.failed),
        "in-flight",
        &format_count(r.crawler.in_flight),
    ));
    out.push(RULE_MID.to_string());
    out.push(row(
        "dns",
        "answered",
        &format_count(r.dns.answered),
        "empty",
        &format_count(r.dns.empty),
        "refused",
        &format_count(r.dns.refused),
    ));
    out.push(row("", "A", &format_count(r.dns.a), "AAAA", &format_count(r.dns.aaaa), "throttled", &format_count(r.dns.throttled)));
    out.push(RULE_MID.to_string());
    out.push(row(
        "web",
        "requests",
        &format_count(r.web.requests),
        "accepted",
        &format_count(r.web.accepted),
        "rejected",
        &format_count(r.web.rejected),
    ));
    out.push(RULE_TOP.to_string());
    out
}

/// Render a single stats row. `label` occupies the 8-wide section column (blank
/// for continuation rows). Each cell pads `key` to 9 chars and `value` to 17.
fn row(label: &str, k1: &str, v1: &str, k2: &str, v2: &str, k3: &str, v3: &str) -> String {
    format!(
        "  {label:<8}{k1:<9} {v1:<17} \u{2502} {k2:<9} {v2:<17} \u{2502} {k3:<9} {v3}",
        label = label,
        k1 = k1,
        v1 = v1,
        k2 = k2,
        v2 = v2,
        k3 = k3,
        v3 = v3,
    )
}

fn format_count(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    let len = bytes.len();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            out.push(' ');
        }
        out.push(*b as char);
    }
    out
}

fn format_uptime(d: Duration) -> String {
    let secs = d.as_secs();
    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {minutes:02}m")
    } else {
        format!("{minutes}m {seconds:02}s")
    }
}

fn format_age(d: Duration) -> String {
    let secs = d.as_secs();
    if secs == 0 {
        return "-".to_string();
    }
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn now_ms() -> i64 {
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}
