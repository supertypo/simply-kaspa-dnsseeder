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
use simply_kaspa_dnsseeder_crawler::is_acceptable_address;
use simply_kaspa_dnsseeder_store::{Filter, NetAddress};

use crate::dto::PeerDto;
use crate::state::AppState;

const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

pub fn build_router(state: AppState) -> Router {
    let prefix = normalize_prefix(&state.config.api_prefix);
    let api = Router::new()
        .route("/ping", get(ping))
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/peers", get(list_peers).post(submit_peer))
        .route("/peers/{addr}", get(get_peer))
        .with_state(state);
    if prefix.is_empty() {
        api
    } else {
        Router::new().nest(&prefix, api)
    }
}

fn normalize_prefix(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with('/') { trimmed.to_string() } else { format!("/{trimmed}") }
}

async fn ping(State(state): State<AppState>) -> &'static str {
    state.metrics.record_request();
    "pong"
}

async fn health(State(state): State<AppState>) -> Response {
    state.metrics.record_request();
    let now = now_ms();
    let stale_good_ms = i64::try_from(state.config.stale_good.as_millis()).unwrap_or(i64::MAX);
    let summary = match state.store.summary(now, stale_good_ms) {
        Ok(s) => s,
        Err(err) => {
            warn!("/health store error: {err}");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"status": "down", "reason": "store error"})),
            )
                .into_response();
        }
    };
    if summary.good > 0 {
        Json(json!({
            "status": "ok",
            "good": summary.good,
            "total": summary.total,
            "service": state.config.service_name,
            "version": state.config.service_version,
        }))
        .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "down",
                "reason": "no peers with successful probe within stale-good window",
                "total": summary.total,
                "service": state.config.service_name,
                "version": state.config.service_version,
            })),
        )
            .into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    state.metrics.record_request();
    let now = now_ms();
    let stale_good_ms = i64::try_from(state.config.stale_good.as_millis()).unwrap_or(i64::MAX);
    let summary = match state.store.summary(now, stale_good_ms) {
        Ok(s) => s,
        Err(err) => {
            warn!("/metrics store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    let process = collect_process(&state).await;
    let disk = collect_disk(&state.config.db_path);
    let web = state.metrics.snapshot();
    let body = json!({
        "service": state.config.service_name,
        "version": state.config.service_version,
        "uptime_ms": state.started.elapsed().as_millis(),
        "process": process,
        "disk": disk,
        "peers": {
            "total": summary.total,
            "good": summary.good,
            "failed": summary.failed,
            "v4": summary.v4,
            "v6": summary.v6,
            "avg_success_age_ms": summary.avg_success_age_ms,
        },
        "web": {
            "requests": web.requests,
            "accepted": web.accepted,
            "rejected": web.rejected,
        },
        "subsystems": state.metrics_source.extra(),
    });
    Json(body).into_response()
}

async fn collect_process(state: &AppState) -> serde_json::Value {
    use sysinfo::{Pid, ProcessRefreshKind};
    let pid = Pid::from_u32(std::process::id());
    let mut system = state.system.write().await;
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::new().with_cpu().with_memory(),
    );
    system.refresh_memory();
    let (cpu, mem_used) = if let Some(proc_) = system.process(pid) {
        ((proc_.cpu_usage() * 10.0).round() / 10.0, proc_.memory())
    } else {
        (0.0_f32, 0_u64)
    };
    let mem_free = if system.available_memory() > 0 { system.available_memory() } else { system.free_memory() };
    json!({
        "cpu_used_percent": cpu,
        "memory_used_bytes": mem_used,
        "memory_used_pretty": bytesize::ByteSize(mem_used).to_string(),
        "memory_free_bytes": mem_free,
        "memory_free_pretty": bytesize::ByteSize(mem_free).to_string(),
    })
}

fn collect_disk(db_path: &std::path::Path) -> serde_json::Value {
    let db_size_bytes = std::fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let canonical = std::fs::canonicalize(db_path).unwrap_or_else(|_| db_path.to_path_buf());
    let mut best: Option<&sysinfo::Disk> = None;
    let mut best_len = 0usize;
    for disk in disks.list() {
        let mp = disk.mount_point();
        if canonical.starts_with(mp) && mp.as_os_str().len() > best_len {
            best_len = mp.as_os_str().len();
            best = Some(disk);
        }
    }
    let (free_bytes, total_bytes, mount) = match best {
        Some(d) => (d.available_space(), d.total_space(), d.mount_point().display().to_string()),
        None => (0, 0, String::new()),
    };
    json!({
        "db_path": db_path.display().to_string(),
        "db_size_bytes": db_size_bytes,
        "db_size_pretty": bytesize::ByteSize(db_size_bytes).to_string(),
        "mount_point": mount,
        "free_bytes": free_bytes,
        "free_pretty": bytesize::ByteSize(free_bytes).to_string(),
        "total_bytes": total_bytes,
        "total_pretty": bytesize::ByteSize(total_bytes).to_string(),
    })
}

async fn list_peers(State(state): State<AppState>, headers: HeaderMap) -> Response {
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
            warn!("/peers store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    records.sort_by_key(|r| std::cmp::Reverse(r.last_success_ms));
    let expose = expose_ip(&headers, state.config.api_key.as_deref());
    let default_port = state.config.network_default_port;
    let dtos: Vec<PeerDto> = records.iter().map(|r| PeerDto::from_record(r, expose, default_port)).collect();
    Json(dtos).into_response()
}

async fn get_peer(State(state): State<AppState>, Path(addr_str): Path<String>, headers: HeaderMap) -> Response {
    state.metrics.record_request();
    let addr = match SocketAddr::from_str(&addr_str) {
        Ok(a) => a,
        Err(err) => return (StatusCode::BAD_REQUEST, format!("addr must be ip:port — {err}")).into_response(),
    };
    let net = NetAddress { ip: canonicalize_ip(addr.ip()), port: addr.port() };
    match state.store.get(&net) {
        Ok(Some(rec)) => {
            let expose = expose_ip(&headers, state.config.api_key.as_deref());
            Json(PeerDto::from_record(&rec, expose, state.config.network_default_port)).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "peer not found").into_response(),
        Err(err) => {
            warn!("/peers/{{addr}} store error: {err}");
            (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response()
        }
    }
}

fn canonicalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_canonical(),
        IpAddr::V4(_) => ip,
    }
}

async fn submit_peer(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: String,
) -> Response {
    state.metrics.record_request();
    // Auth: when an api key is configured, the POST is gated by it.
    if let Some(expected) = state.config.api_key.as_deref() {
        let presented = headers.get(&X_API_KEY).and_then(|v| v.to_str().ok());
        if presented != Some(expected) {
            state.metrics.record_rejected();
            return (StatusCode::UNAUTHORIZED, "missing or invalid api key").into_response();
        }
    }

    // Origin allow-list.
    if !state.config.allowed_origins.is_empty() {
        let origin = headers.get(axum::http::header::ORIGIN).and_then(|v| v.to_str().ok()).unwrap_or("");
        if !state.config.allowed_origins.iter().any(|o| o == origin) {
            state.metrics.record_rejected();
            return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
        }
    }

    let client_ip = client_ip(&headers, remote);
    if !state.limiter.check(client_ip) {
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

    let net = NetAddress { ip: canonicalize_ip(addr.ip()), port: addr.port() };
    if !is_acceptable_address(&net, state.config.network_default_port, state.config.strict_port) {
        state.metrics.record_rejected();
        return (StatusCode::BAD_REQUEST, "address is not publicly routable or uses a disallowed port").into_response();
    }
    let addr = SocketAddr::new(net.ip, net.port);

    match state.prober.probe(addr).await {
        Ok(rec) => {
            state.metrics.record_accepted();
            debug!("web: POST /peers accepted {addr} (probe ok)");
            let expose = expose_ip(&headers, state.config.api_key.as_deref());
            (StatusCode::OK, Json(PeerDto::from_record(&rec, expose, state.config.network_default_port))).into_response()
        }
        Err(err) => {
            state.metrics.record_rejected();
            debug!("web: POST /peers probe of {addr} failed: {err}");
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
    if let Some(raw) = headers.get(&X_FORWARDED_FOR).and_then(|v| v.to_str().ok())
        && let Some(first) = raw.split(',').next()
        && let Ok(ip) = IpAddr::from_str(first.trim())
    {
        return ip;
    }
    fallback.ip()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}
