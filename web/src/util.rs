//! Small helpers shared by router and handlers.

use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderName};

pub(crate) const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
pub(crate) const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

pub(crate) fn now_ms() -> i64 {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}

pub(crate) fn canonicalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_canonical(),
        IpAddr::V4(_) => ip,
    }
}

/// True iff a request is allowed to see peer IPs: either no api key is
/// configured, or the request carries the matching key.
pub(crate) fn expose_ip(headers: &HeaderMap, api_key: Option<&str>) -> bool {
    match api_key {
        None => true,
        Some(expected) => headers.get(&X_API_KEY).and_then(|v| v.to_str().ok()) == Some(expected),
    }
}

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

/// Normalize an URL prefix: trim trailing `/`, ensure a leading `/`. Empty input
/// returns empty (router serves at root).
pub(crate) fn normalize_prefix(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}
