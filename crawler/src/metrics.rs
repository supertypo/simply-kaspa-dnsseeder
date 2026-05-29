//! Atomic counters exposed by the crawler for the periodic stats dump.
//!
//! Counters are cumulative since process start (well, since the last persisted
//! snapshot was loaded). They are bumped from hot paths in [`crate::scheduler`]
//! and read from the dnsseeder binary's stats loop.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::ProbeError;

#[derive(Debug, Default)]
pub struct CrawlerMetrics {
    pub ok: AtomicU64,
    pub failed: AtomicU64,
    pub in_flight: AtomicU64,
    pub failed_connect: AtomicU64,
    pub failed_handshake: AtomicU64,
    pub failed_addresses: AtomicU64,
    pub failed_timeout: AtomicU64,
    pub failed_too_many_addresses: AtomicU64,
    pub probes_skipped_backpressure: AtomicU64,
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

    pub fn record_failed_kind(&self, err: &ProbeError) {
        let counter = match err {
            ProbeError::Connection(_) => &self.failed_connect,
            ProbeError::Handshake(_) | ProbeError::NetworkMismatch { .. } => &self.failed_handshake,
            ProbeError::Addresses(_) => &self.failed_addresses,
            ProbeError::Timeout => &self.failed_timeout,
            ProbeError::TooManyAddresses(_) => &self.failed_too_many_addresses,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_skipped_backpressure(&self, n: u64) {
        if n > 0 {
            self.probes_skipped_backpressure.fetch_add(n, Ordering::Relaxed);
        }
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
            failed_connect: self.failed_connect.load(Ordering::Relaxed),
            failed_handshake: self.failed_handshake.load(Ordering::Relaxed),
            failed_addresses: self.failed_addresses.load(Ordering::Relaxed),
            failed_timeout: self.failed_timeout.load(Ordering::Relaxed),
            failed_too_many_addresses: self.failed_too_many_addresses.load(Ordering::Relaxed),
            probes_skipped_backpressure: self.probes_skipped_backpressure.load(Ordering::Relaxed),
        }
    }

    /// Restore cumulative counters from a previous snapshot. `in_flight` is
    /// intentionally NOT restored — it is an instantaneous gauge.
    pub fn restore(&self, snap: &CrawlerSnapshot) {
        self.ok.store(snap.ok, Ordering::Relaxed);
        self.failed.store(snap.failed, Ordering::Relaxed);
        self.failed_connect.store(snap.failed_connect, Ordering::Relaxed);
        self.failed_handshake.store(snap.failed_handshake, Ordering::Relaxed);
        self.failed_addresses.store(snap.failed_addresses, Ordering::Relaxed);
        self.failed_timeout.store(snap.failed_timeout, Ordering::Relaxed);
        self.failed_too_many_addresses
            .store(snap.failed_too_many_addresses, Ordering::Relaxed);
        self.probes_skipped_backpressure
            .store(snap.probes_skipped_backpressure, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CrawlerSnapshot {
    pub ok: u64,
    pub failed: u64,
    pub in_flight: u64,
    pub failed_connect: u64,
    pub failed_handshake: u64,
    pub failed_addresses: u64,
    pub failed_timeout: u64,
    pub failed_too_many_addresses: u64,
    pub probes_skipped_backpressure: u64,
}
