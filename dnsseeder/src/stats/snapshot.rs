//! Persisted snapshot of cumulative subsystem counters.
//!
//! Stored in the peer-store under a single key so totals survive restarts
//! (modulo the last in-flight interval).
//!
//! Layered:
//! * [`MetricsSnapshot`] — the persisted shape plus `build`/`restore_into` projection.
//! * `codec` — pure bincode encode/decode.
//! * `io` — store I/O via `get_blob`/`put_blob`.
//!
//! [`load`] and [`save`] are thin orchestrators on top.

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

impl MetricsSnapshot {
    fn build(crawler: &CrawlerSnapshot, dns: &DnsSnapshot, web: &WebSnapshot, now_ms: i64) -> Self {
        Self {
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
        }
    }

    fn restore_into(&self, crawler: &CrawlerMetrics, dns: &DnsMetrics, web: &WebMetrics) {
        crawler.restore(&CrawlerSnapshot { ok: self.crawler.ok, failed: self.crawler.failed, in_flight: 0 });
        dns.restore(&DnsSnapshot {
            answered: self.dns.answered,
            empty: self.dns.empty,
            refused: self.dns.refused,
            throttled: self.dns.throttled,
            a: self.dns.a,
            aaaa: self.dns.aaaa,
        });
        web.restore(&WebSnapshot { requests: self.web.requests, accepted: self.web.accepted, rejected: self.web.rejected });
    }
}

mod codec {
    use super::MetricsSnapshot;

    pub(super) fn encode(snap: &MetricsSnapshot) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::serde::encode_to_vec(snap, bincode::config::standard())
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<MetricsSnapshot, bincode::error::DecodeError> {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard()).map(|(v, _)| v)
    }
}

mod io {
    use super::METRICS_KEY;
    use simply_kaspa_dnsseeder_store::{Error, PeerStore};

    pub(super) fn read(store: &PeerStore) -> Result<Option<Vec<u8>>, Error> {
        store.get_blob(METRICS_KEY)
    }

    pub(super) fn write(store: &PeerStore, bytes: &[u8]) -> Result<(), Error> {
        store.put_blob(METRICS_KEY, bytes)
    }
}

/// Restore counters from the persisted blob, if any. Silently no-ops on a
/// missing or unreadable blob — counters simply start at zero.
pub(super) fn load(store: &PeerStore, crawler: &CrawlerMetrics, dns: &DnsMetrics, web: &WebMetrics) {
    let bytes = match io::read(store) {
        Ok(Some(b)) => b,
        Ok(None) => return,
        Err(err) => {
            warn!("stats: failed to read persisted snapshot: {err}");
            return;
        }
    };
    let snap = match codec::decode(&bytes) {
        Ok(s) => s,
        Err(err) => {
            warn!("stats: persisted snapshot is unreadable, starting fresh: {err}");
            return;
        }
    };
    snap.restore_into(crawler, dns, web);
    debug!("stats: restored snapshot persisted at {}", snap.persisted_at_ms);
}

pub(super) fn save(store: &PeerStore, crawler: &CrawlerSnapshot, dns: &DnsSnapshot, web: &WebSnapshot, now_ms: i64) {
    let snap = MetricsSnapshot::build(crawler, dns, web, now_ms);
    let bytes = match codec::encode(&snap) {
        Ok(b) => b,
        Err(err) => {
            warn!("stats: failed to encode snapshot: {err}");
            return;
        }
    };
    if let Err(err) = io::write(store, &bytes) {
        warn!("stats: failed to persist snapshot: {err}");
    }
}
