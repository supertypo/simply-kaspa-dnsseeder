//! Persisted snapshot of cumulative subsystem counters.
//!
//! Stored in the peer-store under a single key so totals survive restarts
//! (modulo the last in-flight interval).

use log::{debug, warn};
use serde::{Deserialize, Serialize};
use simply_kaspa_dnsseeder_crawler::{CrawlerMetrics, CrawlerSnapshot};
use simply_kaspa_dnsseeder_dns::{DnsMetrics, DnsSnapshot};
use simply_kaspa_dnsseeder_store::PeerStore;
use simply_kaspa_dnsseeder_web::{WebMetrics, WebSnapshot};

pub(super) const METRICS_KEY: &str = "stats/snapshot_v1";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct MetricsSnapshot {
    pub crawler: CrawlerSnap,
    pub dns: DnsSnap,
    pub web: WebSnap,
    pub persisted_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub(super) struct CrawlerSnap {
    pub ok: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub(super) struct DnsSnap {
    pub answered: u64,
    pub empty: u64,
    pub refused: u64,
    pub throttled: u64,
    pub a: u64,
    pub aaaa: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub(super) struct WebSnap {
    pub requests: u64,
    pub accepted: u64,
    pub rejected: u64,
}

/// Restore counters from the persisted blob, if any. Silently no-ops on a
/// missing or unreadable blob — counters simply start at zero.
pub(super) fn load(store: &PeerStore, crawler: &CrawlerMetrics, dns: &DnsMetrics, web: &WebMetrics) {
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
    crawler.restore(&CrawlerSnapshot { ok: snap.crawler.ok, failed: snap.crawler.failed, in_flight: 0 });
    dns.restore(&DnsSnapshot {
        answered: snap.dns.answered,
        empty: snap.dns.empty,
        refused: snap.dns.refused,
        throttled: snap.dns.throttled,
        a: snap.dns.a,
        aaaa: snap.dns.aaaa,
    });
    web.restore(&WebSnapshot {
        requests: snap.web.requests,
        accepted: snap.web.accepted,
        rejected: snap.web.rejected,
    });
    debug!("stats: restored snapshot persisted at {}", snap.persisted_at_ms);
}

pub(super) fn save(
    store: &PeerStore,
    crawler: &CrawlerSnapshot,
    dns: &DnsSnapshot,
    web: &WebSnapshot,
    now_ms: i64,
) {
    let snap = MetricsSnapshot {
        crawler: CrawlerSnap { ok: crawler.ok, failed: crawler.failed },
        dns: DnsSnap {
            answered: dns.answered,
            empty: dns.empty,
            refused: dns.refused,
            throttled: dns.throttled,
            a: dns.a,
            aaaa: dns.aaaa,
        },
        web: WebSnap { requests: web.requests, accepted: web.accepted, rejected: web.rejected },
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
