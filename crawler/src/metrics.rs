//! Atomic counters exposed by the crawler for the periodic stats dump.
//!
//! Counters are cumulative since process start (well, since the last persisted
//! snapshot was loaded). They are bumped from hot paths in [`crate::scheduler`]
//! and read from the dnsseeder binary's stats loop.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct CrawlerMetrics {
    pub ok: AtomicU64,
    pub failed: AtomicU64,
    pub in_flight: AtomicU64,
}

impl CrawlerMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_ok(&self) {
        self.ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn in_flight_inc(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn in_flight_dec(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> CrawlerSnapshot {
        CrawlerSnapshot {
            ok: self.ok.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
        }
    }

    /// Restore cumulative counters from a previous snapshot. `in_flight` is
    /// intentionally NOT restored — it is an instantaneous gauge.
    pub fn restore(&self, snap: &CrawlerSnapshot) {
        self.ok.store(snap.ok, Ordering::Relaxed);
        self.failed.store(snap.failed, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CrawlerSnapshot {
    pub ok: u64,
    pub failed: u64,
    pub in_flight: u64,
}
