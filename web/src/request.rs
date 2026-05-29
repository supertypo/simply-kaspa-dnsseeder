//! Helpers for inspecting incoming requests.

use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use axum::http::{HeaderMap, HeaderName};

const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

/// Best-effort client IP: honours the first entry of `X-Forwarded-For` when present.
pub(crate) fn client_ip(headers: &HeaderMap, fallback: SocketAddr) -> IpAddr {
    if let Some(raw) = headers.get(&X_FORWARDED_FOR).and_then(|v| v.to_str().ok())
        && let Some(first) = raw.split(',').next()
        && let Ok(ip) = IpAddr::from_str(first.trim())
    {
        return ip;
    }
    fallback.ip()
}
