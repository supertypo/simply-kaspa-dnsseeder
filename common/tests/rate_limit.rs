use simply_kaspa_dnsseeder_common::RateLimiter;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

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
    assert!(
        rl.tracked_ips() < populated,
        "expected eviction from {populated}, still have {}",
        rl.tracked_ips()
    );
}
