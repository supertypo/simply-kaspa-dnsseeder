//! Atomic counters exposed by the HTTP API for the periodic stats dump.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct WebMetrics {
    pub requests: AtomicU64,
    pub accepted: AtomicU64,
    pub rejected: AtomicU64,
    pub post_rejected_auth: AtomicU64,
    pub post_rejected_cors: AtomicU64,
    pub post_rejected_ratelimit: AtomicU64,
    pub post_rejected_format: AtomicU64,
    pub post_rejected_unroutable: AtomicU64,
    pub post_rejected_probe: AtomicU64,
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

    pub fn record_post_rejected_auth(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        self.post_rejected_auth.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_post_rejected_cors(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        self.post_rejected_cors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_post_rejected_ratelimit(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        self.post_rejected_ratelimit.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_post_rejected_format(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        self.post_rejected_format.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_post_rejected_unroutable(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        self.post_rejected_unroutable.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_post_rejected_probe(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        self.post_rejected_probe.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> WebSnapshot {
        WebSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            post_rejected_auth: self.post_rejected_auth.load(Ordering::Relaxed),
            post_rejected_cors: self.post_rejected_cors.load(Ordering::Relaxed),
            post_rejected_ratelimit: self.post_rejected_ratelimit.load(Ordering::Relaxed),
            post_rejected_format: self.post_rejected_format.load(Ordering::Relaxed),
            post_rejected_unroutable: self.post_rejected_unroutable.load(Ordering::Relaxed),
            post_rejected_probe: self.post_rejected_probe.load(Ordering::Relaxed),
        }
    }

    pub fn restore(&self, snap: &WebSnapshot) {
        self.requests.store(snap.requests, Ordering::Relaxed);
        self.accepted.store(snap.accepted, Ordering::Relaxed);
        self.rejected.store(snap.rejected, Ordering::Relaxed);
        self.post_rejected_auth.store(snap.post_rejected_auth, Ordering::Relaxed);
        self.post_rejected_cors.store(snap.post_rejected_cors, Ordering::Relaxed);
        self.post_rejected_ratelimit.store(snap.post_rejected_ratelimit, Ordering::Relaxed);
        self.post_rejected_format.store(snap.post_rejected_format, Ordering::Relaxed);
        self.post_rejected_unroutable
            .store(snap.post_rejected_unroutable, Ordering::Relaxed);
        self.post_rejected_probe.store(snap.post_rejected_probe, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WebSnapshot {
    pub requests: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub post_rejected_auth: u64,
    pub post_rejected_cors: u64,
    pub post_rejected_ratelimit: u64,
    pub post_rejected_format: u64,
    pub post_rejected_unroutable: u64,
    pub post_rejected_probe: u64,
}
