//! Peer-related handlers: list, lookup, submit.

use std::net::SocketAddr;
use std::str::FromStr;

use axum::Json;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use log::{debug, warn};
use simply_kaspa_dnsseeder_crawler::is_acceptable_address;
use simply_kaspa_dnsseeder_store::{Filter, NetAddress};

use crate::dto::PeerDto;
use crate::state::AppState;
use crate::util::{X_API_KEY, canonicalize_ip, client_ip, expose_ip, now_ms};

pub(crate) async fn ping(State(state): State<AppState>) -> &'static str {
    state.metrics.record_request();
    "pong"
}

pub(crate) async fn list(State(state): State<AppState>, headers: HeaderMap) -> Response {
    state.metrics.record_request();
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
            warn!("web: GET /peers store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    records.sort_by_key(|r| std::cmp::Reverse(r.last_success_ms));
    let expose = expose_ip(&headers, state.config.api_key.as_deref());
    let default_port = state.config.network_default_port;
    let dtos: Vec<PeerDto> = records.iter().map(|r| PeerDto::from_record(r, expose, default_port)).collect();
    Json(dtos).into_response()
}

pub(crate) async fn get(State(state): State<AppState>, Path(addr_str): Path<String>, headers: HeaderMap) -> Response {
    state.metrics.record_request();
    let addr = match SocketAddr::from_str(&addr_str) {
        Ok(a) => a,
        Err(err) => return (StatusCode::BAD_REQUEST, format!("addr must be ip:port — {err}")).into_response(),
    };
    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    match state.store.get(&net) {
        Ok(Some(rec)) => {
            let expose = expose_ip(&headers, state.config.api_key.as_deref());
            Json(PeerDto::from_record(&rec, expose, state.config.network_default_port)).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "peer not found").into_response(),
        Err(err) => {
            warn!("web: GET /peers/<addr> store error: {err}");
            (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response()
        }
    }
}

pub(crate) async fn submit(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: String,
) -> Response {
    state.metrics.record_request();
    if let Some(expected) = state.config.api_key.as_deref() {
        let presented = headers.get(&X_API_KEY).and_then(|v| v.to_str().ok());
        if presented != Some(expected) {
            state.metrics.record_rejected();
            return (StatusCode::UNAUTHORIZED, "missing or invalid api key").into_response();
        }
    }

    if !state.config.allowed_origins.is_empty() {
        let origin = headers.get(axum::http::header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or("");
        if !state.config.allowed_origins.iter().any(|o| o == origin) {
            state.metrics.record_rejected();
            return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
        }
    }

    let client = client_ip(&headers, remote);
    if !state.limiter.check(client) {
        state.metrics.record_rejected();
        return (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response();
    }

    let addr = match SocketAddr::from_str(body.trim()) {
        Ok(a) => a,
        Err(err) => {
            state.metrics.record_rejected();
            return (StatusCode::BAD_REQUEST, format!("invalid ip:port — {err}")).into_response();
        }
    };

    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    if !is_acceptable_address(&net, state.config.network_default_port, state.config.strict_port) {
        state.metrics.record_rejected();
        return (
            StatusCode::BAD_REQUEST,
            "address is not publicly routable or uses a disallowed port",
        )
            .into_response();
    }
    let addr = SocketAddr::new(net.ip, net.port);

    match state.prober.probe(addr).await {
        Ok(rec) => {
            state.metrics.record_accepted();
            debug!("web: POST /peers accepted {addr} (probe ok)");
            let expose = expose_ip(&headers, state.config.api_key.as_deref());
            (
                StatusCode::OK,
                Json(PeerDto::from_record(&rec, expose, state.config.network_default_port)),
            )
                .into_response()
        }
        Err(err) => {
            state.metrics.record_rejected();
            debug!("web: POST /peers probe of {addr} failed: {err}");
            (StatusCode::BAD_GATEWAY, format!("probe failed: {err}")).into_response()
        }
    }
}
