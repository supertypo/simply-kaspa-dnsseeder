//! Peer-related handlers: list, lookup, submit.

use std::net::SocketAddr;
use std::str::FromStr;

use axum::Json;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use log::{debug, warn};
use serde::Deserialize;
use simply_kaspa_dnsseeder_crawler::is_acceptable_address;
use simply_kaspa_dnsseeder_store::{Filter, NetAddress};

use crate::config::WebConfig;
use crate::dto::PeerDto;
use crate::state::AppState;
use crate::util::{X_API_KEY, canonicalize_ip, client_ip, expose_ip, now_ms};

/// Hard cap on `GET /peers` response size so a large store can't OOM a client
/// or the process. The 1000 most-recently-successful peers is plenty for any
/// UI use; clients that need more should use the DNS interface.
const MAX_LIST_RESPONSE: usize = 1000;

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ListQuery {
    /// When true, the protocol-version and user-agent filters are skipped but
    /// freshness (stale-good window) and stub exclusion still apply.
    #[serde(default)]
    all: bool,
}

/// Build the filter used by `GET /peers` and `GET /peers/{addr}`. Always
/// enforces the stale-good window (which also implicitly hides stubs, since
/// stubs have `last_success_ms = 0`). When `all` is false, also enforces the
/// configured protocol-version and user-agent floors, matching the DNS path.
fn list_filter(cfg: &WebConfig, all: bool) -> Filter {
    let stale_good_ms = i64::try_from(cfg.stale_good.as_millis()).unwrap_or(i64::MAX);
    Filter {
        now_ms: now_ms(),
        dead_after_ms: i64::MAX,
        stale_good_ms: Some(stale_good_ms),
        family: None,
        min_protocol_version: if all { None } else { cfg.min_protocol_version },
        min_user_agent: if all { None } else { cfg.min_user_agent.clone() },
        default_port: None,
    }
}

pub(crate) async fn ping(State(state): State<AppState>) -> &'static str {
    state.metrics.record_request();
    "pong"
}

pub(crate) async fn list(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
    headers: HeaderMap,
) -> Response {
    state.metrics.record_request();
    let filter = list_filter(&state.config, q.all);
    let mut records = match state.store.collect_matching(&filter) {
        Ok(v) => v,
        Err(err) => {
            warn!("web: GET /peers store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    records.sort_by_key(|r| std::cmp::Reverse(r.last_success_ms));
    if records.len() > MAX_LIST_RESPONSE {
        records.truncate(MAX_LIST_RESPONSE);
    }
    let expose = expose_ip(&headers, state.config.api_key.as_deref());
    let dtos: Vec<PeerDto> = records.iter().map(|r| PeerDto::from_record(r, expose)).collect();
    Json(dtos).into_response()
}

pub(crate) async fn get(
    State(state): State<AppState>,
    Path(addr_str): Path<String>,
    Query(q): Query<ListQuery>,
    headers: HeaderMap,
) -> Response {
    state.metrics.record_request();
    let Ok(addr) = SocketAddr::from_str(&addr_str) else {
        return (StatusCode::BAD_REQUEST, "addr must be ip:port").into_response();
    };
    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    let filter = list_filter(&state.config, q.all);
    match state.store.get(&net) {
        Ok(Some(rec)) if filter.matches(&rec) => {
            let expose = expose_ip(&headers, state.config.api_key.as_deref());
            Json(PeerDto::from_record(&rec, expose)).into_response()
        }
        Ok(_) => (StatusCode::NOT_FOUND, "peer not found").into_response(),
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

    let Ok(addr) = SocketAddr::from_str(body.trim()) else {
        state.metrics.record_rejected();
        return (StatusCode::BAD_REQUEST, "invalid ip:port").into_response();
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
                Json(PeerDto::from_record(&rec, expose)),
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
