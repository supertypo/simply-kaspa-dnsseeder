use chrono::Utc;
use std::net::IpAddr;
use std::time::Duration;

/// Current time as unix milliseconds.
#[must_use]
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Collapse IPv4-mapped IPv6 (`::ffff:a.b.c.d`) to plain IPv4 so we never
/// store both representations of the same host.
#[must_use]
pub fn canonicalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_canonical(),
        IpAddr::V4(_) => ip,
    }
}

/// Convert a `Duration` to milliseconds as `i64`. Saturates at `i64::MAX`
/// (only reachable past ~292M years).
#[must_use]
pub fn duration_to_ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}
