//! Atomic counters exposed by the HTTP API for the periodic stats dump.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct WebMetrics {
    pub requests: AtomicU64,
    pub accepted: AtomicU64,
    pub rejected: AtomicU64,
    pub post_rejected_auth: AtomicU64,
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

    pub fn record_post_rejection(&self, reason: PostRejection) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        reason.counter(self).fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> WebSnapshot {
        WebSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            accepted: self.accepted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            post_rejected_auth: self.post_rejected_auth.load(Ordering::Relaxed),
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
    pub post_rejected_ratelimit: u64,
    pub post_rejected_format: u64,
    pub post_rejected_unroutable: u64,
    pub post_rejected_probe: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum PostRejection {
    Auth,
    RateLimit,
    Format,
    Unroutable,
    Probe,
}

impl PostRejection {
    fn counter(self, m: &WebMetrics) -> &AtomicU64 {
        match self {
            Self::Auth => &m.post_rejected_auth,
            Self::RateLimit => &m.post_rejected_ratelimit,
            Self::Format => &m.post_rejected_format,
            Self::Unroutable => &m.post_rejected_unroutable,
            Self::Probe => &m.post_rejected_probe,
        }
    }
}
