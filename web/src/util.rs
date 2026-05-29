//! Small helpers shared by router and handlers.

use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use axum::http::{HeaderMap, HeaderName};

pub(crate) const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
pub(crate) const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

/// True iff the request carries the matching `X-API-KEY` header.
pub(crate) fn expose_ip(headers: &HeaderMap, api_key: &str) -> bool {
    headers.get(&X_API_KEY).and_then(|v| v.to_str().ok()) == Some(api_key)
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
