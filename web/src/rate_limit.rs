//! Per-IP token bucket used to throttle `POST /peers`. Buckets are evicted
//! lazily when they refill, so memory usage stays bounded by the number of
//! recent submitters.

use std::net::IpAddr;
use std::time::{Duration, Instant};

use dashmap::DashMap;

#[derive(Debug)]
pub struct RateLimiter {
    capacity: u32,
    window: Duration,
    buckets: DashMap<IpAddr, Bucket>,
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: u32,
    refill_at: Instant,
}

impl RateLimiter {
    #[must_use]
    pub fn new(capacity: u32, window: Duration) -> Self {
        Self { capacity, window, buckets: DashMap::new() }
    }

    /// Returns `true` when the caller is within the limit and consumes one
    /// token. Returns `false` when the bucket is empty.
    #[must_use]
    pub fn check(&self, ip: IpAddr) -> bool {
        if self.capacity == 0 {
            return false;
        }
        let now = Instant::now();
        let mut entry = self.buckets.entry(ip).or_insert(Bucket { tokens: self.capacity, refill_at: now + self.window });
        if now >= entry.refill_at {
            entry.tokens = self.capacity;
            entry.refill_at = now + self.window;
        }
        if entry.tokens == 0 {
            return false;
        }
        entry.tokens -= 1;
        true
    }
}
