//! Peer-related handlers: list, lookup, submit, delete.

use std::net::{IpAddr, SocketAddr};
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
use crate::dto::{PeerDto, SubmitPeerRequest};
use crate::error::ApiError;
use crate::http::auth::authenticated;
use crate::http::request::client_ip;
use crate::metrics::PostRejection;
use crate::state::AppState;
use simply_kaspa_dnsseeder_common::{RateLimiter, canonicalize_ip, duration_to_ms, now_ms};

/// Hard cap on `GET /peers` response size to prevent OOM. Clients needing
/// bulk data should use the DNS interface.
const MAX_LIST_RESPONSE: usize = 1000;

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ListQuery {
    /// When true, bypass the stale-good window plus the protocol-version and
    /// user-agent filters: return every peer that has succeeded at least once
    /// and has not yet been pruned. Stubs are still excluded.
    #[serde(default)]
    all: bool,
}

/// Build the filter used by `GET /peers` and `GET /peers/{addr_port}`. When
/// `all` is false, mirrors the DNS-serving filter (stale-good window plus the
/// configured protocol-version and user-agent floors). When `all` is true,
/// drops all three; stub exclusion is enforced separately by the caller.
fn list_filter(cfg: &WebConfig, all: bool) -> Filter {
    Filter::serving(
        now_ms(),
        if all { i64::MAX } else { duration_to_ms(cfg.stale_good) },
        if all { None } else { cfg.min_protocol_version },
        if all { None } else { cfg.min_user_agent.clone() },
        None,
        None,
    )
}

pub(crate) const LIST_PATH: &str = "/peers";
pub(crate) const GET_PATH: &str = "/peers/{addr_port}";
pub(crate) const SUBMIT_PATH: &str = "/peers";
pub(crate) const DELETE_PATH: &str = "/peers/{addr_port}";

#[utoipa::path(
    get,
    path = LIST_PATH,
    tag = "peers",
    params(
        ("all" = Option<bool>, Query, description = "Bypass stale-good window plus protocol-version and user-agent filters; stubs still excluded"),
    ),
    responses(
        (status = 200, description = "List of peers (IPs stripped without valid X-API-KEY)", body = [PeerDto]),
    ),
    security((), ("api_key" = [])),
)]
pub(crate) async fn list(State(state): State<AppState>, Query(q): Query<ListQuery>, headers: HeaderMap) -> Response {
    let expose = authenticated(&headers, &state.config.api_key);
    let cache_key = crate::runtime::peers_cache::Key { all: q.all, expose };
    let result = state
        .peers_cache
        .get_or_compute(cache_key, || compute_list_body(state.clone(), q.all, expose))
        .await;
    match result {
        Ok(body) => ([(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(err) => err.into_response(),
    }
}

async fn compute_list_body(state: AppState, all: bool, expose: bool) -> Result<axum::body::Bytes, ApiError> {
    let filter = list_filter(&state.config, all);
    let mut records = state
        .runtime
        .store
        .blocking(move |s| s.collect_matching(&filter))
        .await
        .map_err(|err| {
            warn!("web: GET /peers store error: {err}");
            ApiError::Internal("store error")
        })?;
    if all {
        records.retain(|r| r.last_success_ms > 0);
    }
    records.sort_by_key(|r| std::cmp::Reverse(r.last_success_ms));
    if records.len() > MAX_LIST_RESPONSE {
        records.truncate(MAX_LIST_RESPONSE);
    }
    let dtos: Vec<PeerDto> = records.iter().map(|r| PeerDto::from_record(r, expose)).collect();
    serde_json::to_vec(&dtos).map(axum::body::Bytes::from).map_err(|err| {
        warn!("web: GET /peers serialize error: {err}");
        ApiError::Internal("serialize error")
    })
}

#[utoipa::path(
    get,
    path = GET_PATH,
    tag = "peers",
    params(
        ("addr_port" = String, Path, description = "Peer address as ip:port (IPv6 wrapped in brackets, e.g. [::1]:16111)"),
        ("all" = Option<bool>, Query, description = "Bypass stale-good window plus protocol-version and user-agent filters; stubs still excluded"),
    ),
    responses(
        (status = 200, description = "Peer record", body = PeerDto),
        (status = 400, description = "Bad address"),
        (status = 401, description = "Missing or invalid X-API-KEY"),
        (status = 404, description = "Peer not found"),
    ),
    security(("api_key" = [])),
)]
pub(crate) async fn get(State(state): State<AppState>, Path(addr_port): Path<String>, Query(q): Query<ListQuery>) -> Response {
    let Ok(addr) = SocketAddr::from_str(&addr_port) else {
        return ApiError::BadRequest("addr must be ip:port").into_response();
    };
    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    let filter = list_filter(&state.config, q.all);
    match state.runtime.store.blocking(move |s| s.get(&net)).await {
        Ok(Some(rec)) if filter.matches(&rec) && rec.last_success_ms > 0 => Json(PeerDto::from_record(&rec, true)).into_response(),
        Ok(_) => ApiError::NotFound("peer not found").into_response(),
        Err(err) => {
            warn!("web: GET /peers/<addr> store error: {err}");
            ApiError::Internal("store error").into_response()
        }
    }
}

/// Reasons a `POST /peers` body is rejected before the probe stage.
enum SubmitRejection {
    RateLimit,
    Format,
    Unroutable,
}

impl SubmitRejection {
    const fn as_post_rejection(&self) -> PostRejection {
        match self {
            Self::RateLimit => PostRejection::RateLimit,
            Self::Format => PostRejection::Format,
            Self::Unroutable => PostRejection::Unroutable,
        }
    }

    fn into_api_error(self) -> ApiError {
        match self {
            Self::RateLimit => ApiError::RateLimited("rate limited"),
            Self::Format => ApiError::BadRequest("invalid ip:port"),
            Self::Unroutable => ApiError::BadRequest("address is not publicly routable or uses a disallowed port"),
        }
    }
}

/// Run all pre-probe checks: rate limit, parse, canonicalize, routability.
fn validate_submission(
    addr_port: &str,
    client: IpAddr,
    cfg: &WebConfig,
    limiter: &RateLimiter,
) -> Result<SocketAddr, SubmitRejection> {
    if !limiter.check(client) {
        return Err(SubmitRejection::RateLimit);
    }
    let addr = SocketAddr::from_str(addr_port.trim()).map_err(|_| SubmitRejection::Format)?;
    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    if !is_acceptable_address(&net, cfg.network_default_port, cfg.strict_port) {
        return Err(SubmitRejection::Unroutable);
    }
    Ok(SocketAddr::new(net.ip, net.port))
}

#[utoipa::path(
    post,
    path = SUBMIT_PATH,
    tag = "peers",
    request_body = SubmitPeerRequest,
    responses(
        (status = 200, description = "Peer probed and accepted", body = PeerDto),
        (status = 400, description = "Bad address or not publicly routable"),
        (status = 401, description = "Missing or invalid X-API-KEY"),
        (status = 429, description = "Rate limited"),
        (status = 502, description = "Probe failed"),
    ),
    security(("api_key" = [])),
)]
pub(crate) async fn submit(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<SubmitPeerRequest>,
) -> Response {
    let client = client_ip(&headers, remote);
    let addr = match validate_submission(&body.addr_port, client, &state.config, &state.limiter) {
        Ok(addr) => addr,
        Err(rej) => {
            state.obs.metrics.record_post_rejection(rej.as_post_rejection());
            return rej.into_api_error().into_response();
        }
    };
    match state.runtime.prober.probe(addr).await {
        Ok(rec) => {
            state.obs.metrics.record_accepted();
            debug!("web: POST /peers accepted {addr} (probe ok)");
            (StatusCode::OK, Json(PeerDto::from_record(&rec, true))).into_response()
        }
        Err(err) => {
            state.obs.metrics.record_post_rejection(PostRejection::Probe);
            debug!("web: POST /peers probe of {addr} failed: {err}");
            ApiError::BadGateway("probe failed").into_response()
        }
    }
}

#[utoipa::path(
    delete,
    path = DELETE_PATH,
    tag = "peers",
    params(
        ("addr_port" = String, Path, description = "Peer address as ip:port (IPv6 wrapped in brackets, e.g. [::1]:16111)"),
    ),
    responses(
        (status = 204, description = "Peer removed"),
        (status = 400, description = "Bad address"),
        (status = 401, description = "Missing or invalid X-API-KEY"),
        (status = 404, description = "Peer not found"),
    ),
    security(("api_key" = [])),
)]
pub(crate) async fn delete(State(state): State<AppState>, Path(addr_port): Path<String>) -> Response {
    let Ok(addr) = SocketAddr::from_str(addr_port.trim()) else {
        return ApiError::BadRequest("addr must be ip:port").into_response();
    };
    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    match state.runtime.store.blocking(move |s| s.delete(&net)).await {
        Ok(true) => {
            debug!("web: DELETE /peers/{addr_port} removed");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => ApiError::NotFound("peer not found").into_response(),
        Err(err) => {
            warn!("web: DELETE /peers/<addr> store error: {err}");
            ApiError::Internal("store error").into_response()
        }
    }
}
