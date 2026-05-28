//! Atomic counters exposed by the HTTP API for the periodic stats dump.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct WebMetrics {
    pub requests: AtomicU64,
    pub accepted: AtomicU64,
    pub rejected: AtomicU64,
}

impl WebMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> WebSnapshot {
        WebSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
        }
    }

    pub fn restore(&self, snap: &WebSnapshot) {
        self.requests.store(snap.requests, Ordering::Relaxed);
        self.accepted.store(snap.accepted, Ordering::Relaxed);
        self.rejected.store(snap.rejected, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WebSnapshot {
    pub requests: u64,
    pub accepted: u64,
    pub rejected: u64,
}
