//! API-key authentication helpers.

use axum::http::{HeaderMap, HeaderName};

pub(crate) const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");

/// True iff the request carries the matching `X-API-KEY` header.
pub(crate) fn authenticated(headers: &HeaderMap, api_key: &str) -> bool {
    headers.get(&X_API_KEY).and_then(|v| v.to_str().ok()) == Some(api_key)
}
