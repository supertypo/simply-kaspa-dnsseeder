//! Axum router + handlers.
//!
//! The handlers stay thin so that the bulk of the logic lives in the store /
//! crawler crates and can be exercised without standing up an HTTP server.

use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use log::{debug, warn};
use serde_json::json;
use simply_kaspa_dnsseeder_store::{Filter, PeerRecord};

use crate::dto::PeerDto;
use crate::state::AppState;

const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

#[must_use]
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/ping", get(ping))
        .route("/health", get(health))
        .route("/peers", get(list_peers).post(submit_peer))
        .route("/peers/{id}", get(get_peer))
        .with_state(state)
}

async fn ping() -> &'static str {
    "pong"
}

async fn health(State(state): State<AppState>) -> Response {
    match state.store.len() {
        Ok(count) => Json(json!({ "status": "ok", "peers": count })).into_response(),
        Err(err) => {
            warn!("/health store error: {err}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "status": "error" }))).into_response()
        }
    }
}

async fn list_peers(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let filter = Filter {
        now_ms: now_ms(),
        dead_after_ms: i64::MAX,
        stale_good_ms: None,
        family: None,
        min_protocol_version: None,
        min_user_agent: None,
        default_port: None,
    };
    let mut records = match state.store.collect_matching(&filter) {
        Ok(v) => v,
        Err(err) => {
            warn!("/peers store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    records.sort_by_key(|r| std::cmp::Reverse(r.last_success_ms));
    let expose = expose_ip(&headers, state.config.api_key.as_deref());
    let dtos: Vec<PeerDto> = records.iter().map(|r| PeerDto::from_record(r, expose)).collect();
    Json(dtos).into_response()
}

async fn get_peer(State(state): State<AppState>, Path(id_hex): Path<String>, headers: HeaderMap) -> Response {
    let Ok(bytes) = hex::decode(&id_hex) else {
        return (StatusCode::BAD_REQUEST, "id must be hex").into_response();
    };
    if bytes.len() != 16 {
        return (StatusCode::BAD_REQUEST, "id must be 16 bytes").into_response();
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&bytes);
    // Store is keyed by address now; scan iter_all for a matching id. This
    // endpoint is exposed for ops debugging, not heavy traffic.
    let rec = match state.store.iter_all() {
        Ok(records) => records.into_iter().find(|r| r.id == id),
        Err(err) => {
            warn!("/peers/{{id}} store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    match rec {
        Some(rec) => {
            let expose = expose_ip(&headers, state.config.api_key.as_deref());
            Json(PeerDto::from_record(&rec, expose)).into_response()
        }
        None => (StatusCode::NOT_FOUND, "peer not found").into_response(),
    }
}

async fn submit_peer(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: String,
) -> Response {
    // Auth: when an api key is configured, the POST is gated by it.
    if let Some(expected) = state.config.api_key.as_deref() {
        let presented = headers.get(&X_API_KEY).and_then(|v| v.to_str().ok());
        if presented != Some(expected) {
            return (StatusCode::UNAUTHORIZED, "missing or invalid api key").into_response();
        }
    }

    // Origin allow-list.
    if !state.config.allowed_origins.is_empty() {
        let origin = headers.get(axum::http::header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or("");
        if !state.config.allowed_origins.iter().any(|o| o == origin) {
            return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
        }
    }

    let client_ip = client_ip(&headers, remote);
    if !state.limiter.check(client_ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response();
    }

    let addr = match SocketAddr::from_str(body.trim()) {
        Ok(a) => a,
        Err(err) => return (StatusCode::BAD_REQUEST, format!("invalid ip:port — {err}")).into_response(),
    };

    match state.prober.probe(addr).await {
        Ok(rec) => {
            let expose = expose_ip(&headers, state.config.api_key.as_deref());
            (StatusCode::OK, Json(PeerDto::from_record(&rec, expose))).into_response()
        }
        Err(err) => {
            debug!("POST /peers probe of {addr} failed: {err}");
            (StatusCode::BAD_GATEWAY, format!("probe failed: {err}")).into_response()
        }
    }
}

fn expose_ip(headers: &HeaderMap, api_key: Option<&str>) -> bool {
    match api_key {
        None => true,
        Some(expected) => headers.get(&X_API_KEY).and_then(|v| v.to_str().ok()) == Some(expected),
    }
}

fn client_ip(headers: &HeaderMap, fallback: SocketAddr) -> IpAddr {
    if let Some(raw) = headers.get(&X_FORWARDED_FOR).and_then(|v| v.to_str().ok()) {
        if let Some(first) = raw.split(',').next() {
            if let Ok(ip) = IpAddr::from_str(first.trim()) {
                return ip;
            }
        }
    }
    fallback.ip()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}

// Helper kept to surface PeerRecord types in tests without unused imports.
#[allow(dead_code)]
fn _phantom(_: PeerRecord) {}
