//! Per-IP token-bucket rate limiter with lazy bucket eviction.
//!
//! `capacity == 0` disables the limiter (every call returns `true`).

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Sweep stale buckets every N successful `check` calls. O(buckets) but
/// sharded across DashMap stripes.
const SWEEP_INTERVAL: u64 = 1024;

#[derive(Debug)]
pub struct RateLimiter {
    capacity: u32,
    window: Duration,
    buckets: DashMap<IpAddr, Bucket>,
    ops: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: u32,
    refill_at: Instant,
}

impl RateLimiter {
    #[must_use]
    pub fn new(capacity: u32, window: Duration) -> Self {
        Self {
            capacity,
            window,
            buckets: DashMap::new(),
            ops: AtomicU64::new(0),
        }
    }

    /// Returns `true` when the caller may proceed (and consumes one token).
    /// When `capacity == 0` the limiter is disabled and always returns `true`.
    #[must_use]
    pub fn check(&self, ip: IpAddr) -> bool {
        if self.capacity == 0 {
            return true;
        }
        let now = Instant::now();
        let allowed = {
            let mut entry = self.buckets.entry(ip).or_insert(Bucket {
                tokens: self.capacity,
                refill_at: now + self.window,
            });
            if now >= entry.refill_at {
                entry.tokens = self.capacity;
                entry.refill_at = now + self.window;
            }
            if entry.tokens == 0 {
                false
            } else {
                entry.tokens -= 1;
                true
            }
        };
        self.maybe_sweep(now);
        allowed
    }

    fn maybe_sweep(&self, now: Instant) {
        if !self.ops.fetch_add(1, Ordering::Relaxed).is_multiple_of(SWEEP_INTERVAL) {
            return;
        }
        self.buckets.retain(|_, b| now < b.refill_at);
    }

    /// Total times `check` has been invoked since process start.
    #[must_use]
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }

    /// Number of per-IP buckets currently tracked.
    #[must_use]
    pub fn tracked_ips(&self) -> usize {
        self.buckets.len()
    }

    /// Configured per-window token capacity. `0` means the limiter is disabled.
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Configured refill window.
    #[must_use]
    pub fn window(&self) -> Duration {
        self.window
    }

    #[doc(hidden)]
    pub fn force_sweep(&self) {
        self.buckets.retain(|_, b| Instant::now() < b.refill_at);
    }
}
