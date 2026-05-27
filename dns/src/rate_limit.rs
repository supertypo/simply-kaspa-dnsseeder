use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

// Sweep stale buckets every N successful `check` calls. The sweep is O(buckets)
// but DashMap shards it across stripes so the impact per call is small.
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
        Self { capacity, window, buckets: DashMap::new(), ops: AtomicU64::new(0) }
    }

    #[must_use]
    pub fn check(&self, ip: IpAddr) -> bool {
        if self.capacity == 0 {
            return true;
        }
        let now = Instant::now();
        let allowed = {
            let mut entry = self.buckets.entry(ip).or_insert(Bucket { tokens: self.capacity, refill_at: now + self.window });
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
        self.sweep(now);
    }

    fn sweep(&self, now: Instant) {
        self.buckets.retain(|_, b| now < b.refill_at);
    }

    #[cfg(test)]
    pub(crate) fn tracked_ips(&self) -> usize {
        self.buckets.len()
    }

    #[cfg(test)]
    pub(crate) fn force_sweep(&self) {
        self.sweep(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn disabled_when_capacity_zero() {
        let rl = RateLimiter::new(0, Duration::from_secs(1));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        for _ in 0..10_000 {
            assert!(rl.check(ip));
        }
    }

    #[test]
    fn enforces_capacity_per_ip() {
        let rl = RateLimiter::new(2, Duration::from_secs(60));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert!(rl.check(ip));
        assert!(rl.check(ip));
        assert!(!rl.check(ip));
    }

    #[test]
    fn refills_after_window() {
        let rl = RateLimiter::new(1, Duration::from_millis(50));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert!(rl.check(ip));
        assert!(!rl.check(ip));
        std::thread::sleep(Duration::from_millis(80));
        assert!(rl.check(ip));
    }

    #[test]
    fn independent_per_ip() {
        let rl = RateLimiter::new(1, Duration::from_secs(60));
        assert!(rl.check(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        assert!(rl.check(IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2))));
        assert!(!rl.check(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
    }

    #[test]
    fn stale_buckets_are_evicted() {
        let rl = RateLimiter::new(1, Duration::from_millis(10));
        for i in 0..2_000u32 {
            let octets = i.to_be_bytes();
            let ip = IpAddr::V4(Ipv4Addr::new(10, octets[1], octets[2], octets[3]));
            let _ = rl.check(ip);
        }
        let populated = rl.tracked_ips();
        std::thread::sleep(Duration::from_millis(30));
        rl.force_sweep();
        assert!(rl.tracked_ips() < populated, "expected eviction from {populated}, still have {}", rl.tracked_ips());
    }
}
