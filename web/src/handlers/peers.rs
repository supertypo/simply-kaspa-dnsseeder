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
use simply_kaspa_dnsseeder_common::{canonicalize_ip, duration_to_ms, now_ms};

use crate::util::{authenticated, client_ip};

/// Hard cap on `GET /peers` response size to prevent OOM. Clients needing
/// bulk data should use the DNS interface.
const MAX_LIST_RESPONSE: usize = 1000;

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ListQuery {
    /// Skip protocol-version and user-agent filters when true; freshness and
    /// stub exclusion still apply.
    #[serde(default)]
    all: bool,
}

/// Build the filter used by `GET /peers` and `GET /peers/{addr}`. Always
/// enforces the stale-good window (which also implicitly hides stubs, since
/// stubs have `last_success_ms = 0`). When `all` is false, also enforces the
/// configured protocol-version and user-agent floors, matching the DNS path.
fn list_filter(cfg: &WebConfig, all: bool) -> Filter {
    Filter::serving(
        now_ms(),
        duration_to_ms(cfg.stale_good),
        if all { None } else { cfg.min_protocol_version },
        if all { None } else { cfg.min_user_agent.clone() },
        None,
        None,
    )
}

pub(crate) async fn ping(State(state): State<AppState>) -> &'static str {
    state.obs.metrics.record_request();
    "pong"
}

pub(crate) async fn list(State(state): State<AppState>, Query(q): Query<ListQuery>, headers: HeaderMap) -> Response {
    state.obs.metrics.record_request();
    let expose = authenticated(&headers, &state.config.api_key);
    let cache_key = crate::peers_cache::Key { all: q.all, expose };
    if let Some(body) = state.peers_cache.get(cache_key) {
        return ([(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response();
    }
    let filter = list_filter(&state.config, q.all);
    let mut records = match state.runtime.store.blocking(move |s| s.collect_matching(&filter)).await {
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
    let dtos: Vec<PeerDto> = records.iter().map(|r| PeerDto::from_record(r, expose)).collect();
    let body = match serde_json::to_vec(&dtos) {
        Ok(v) => axum::body::Bytes::from(v),
        Err(err) => {
            warn!("web: GET /peers serialize error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "serialize error").into_response();
        }
    };
    state.peers_cache.put(cache_key, body.clone());
    ([(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response()
}

pub(crate) async fn get(
    State(state): State<AppState>,
    Path(addr_str): Path<String>,
    Query(q): Query<ListQuery>,
    headers: HeaderMap,
) -> Response {
    state.obs.metrics.record_request();
    if !authenticated(&headers, &state.config.api_key) {
        return (StatusCode::UNAUTHORIZED, "missing or invalid api key").into_response();
    }
    let Ok(addr) = SocketAddr::from_str(&addr_str) else {
        return (StatusCode::BAD_REQUEST, "addr must be ip:port").into_response();
    };
    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    let filter = list_filter(&state.config, q.all);
    match state.runtime.store.blocking(move |s| s.get(&net)).await {
        Ok(Some(rec)) if filter.matches(&rec) => Json(PeerDto::from_record(&rec, true)).into_response(),
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
    state.obs.metrics.record_request();
    if !authenticated(&headers, &state.config.api_key) {
        state.obs.metrics.record_post_rejected_auth();
        return (StatusCode::UNAUTHORIZED, "missing or invalid api key").into_response();
    }

    if !state.config.allowed_origins.is_empty() {
        let origin = headers.get(axum::http::header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or("");
        if !state.config.allowed_origins.iter().any(|o| o == origin) {
            state.obs.metrics.record_post_rejected_cors();
            return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
        }
    }

    let client = client_ip(&headers, remote);
    if !state.limiter.check(client) {
        state.obs.metrics.record_post_rejected_ratelimit();
        return (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response();
    }

    let Ok(addr) = SocketAddr::from_str(body.trim()) else {
        state.obs.metrics.record_post_rejected_format();
        return (StatusCode::BAD_REQUEST, "invalid ip:port").into_response();
    };

    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    if !is_acceptable_address(&net, state.config.network_default_port, state.config.strict_port) {
        state.obs.metrics.record_post_rejected_unroutable();
        return (
            StatusCode::BAD_REQUEST,
            "address is not publicly routable or uses a disallowed port",
        )
            .into_response();
    }
    let addr = SocketAddr::new(net.ip, net.port);

    match state.runtime.prober.probe(addr).await {
        Ok(rec) => {
            state.obs.metrics.record_accepted();
            debug!("web: POST /peers accepted {addr} (probe ok)");
            (StatusCode::OK, Json(PeerDto::from_record(&rec, true))).into_response()
        }
        Err(err) => {
            state.obs.metrics.record_post_rejected_probe();
            debug!("web: POST /peers probe of {addr} failed: {err}");
            (StatusCode::BAD_GATEWAY, format!("probe failed: {err}")).into_response()
        }
    }
}
