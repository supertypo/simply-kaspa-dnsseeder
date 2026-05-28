//! Atomic counters exposed by the DNS handler for the periodic stats dump.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct DnsMetrics {
    pub answered: AtomicU64,
    pub empty: AtomicU64,
    pub refused: AtomicU64,
    pub throttled: AtomicU64,
    pub a: AtomicU64,
    pub aaaa: AtomicU64,
}

impl DnsMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_answered(&self, qtype_is_a: bool, qtype_is_aaaa: bool, answer_count: usize) {
        if answer_count == 0 {
            self.empty.fetch_add(1, Ordering::Relaxed);
        } else {
            self.answered.fetch_add(1, Ordering::Relaxed);
        }
        if qtype_is_a {
            self.a.fetch_add(1, Ordering::Relaxed);
        } else if qtype_is_aaaa {
            self.aaaa.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_refused(&self) {
        self.refused.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_throttled(&self) {
        self.throttled.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> DnsSnapshot {
        DnsSnapshot {
            answered: self.answered.load(Ordering::Relaxed),
            empty: self.empty.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
            throttled: self.throttled.load(Ordering::Relaxed),
            a: self.a.load(Ordering::Relaxed),
            aaaa: self.aaaa.load(Ordering::Relaxed),
        }
    }

    pub fn restore(&self, snap: &DnsSnapshot) {
        self.answered.store(snap.answered, Ordering::Relaxed);
        self.empty.store(snap.empty, Ordering::Relaxed);
        self.refused.store(snap.refused, Ordering::Relaxed);
        self.throttled.store(snap.throttled, Ordering::Relaxed);
        self.a.store(snap.a, Ordering::Relaxed);
        self.aaaa.store(snap.aaaa, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DnsSnapshot {
    pub answered: u64,
    pub empty: u64,
    pub refused: u64,
    pub throttled: u64,
    pub a: u64,
    pub aaaa: u64,
}
