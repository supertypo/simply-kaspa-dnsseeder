//! `/metrics` handler — JSON snapshot of service, process, disk, peer-store
//! and per-subsystem counters.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use log::warn;
use serde::Serialize;
use serde_json::{Map, Value, json};

use simply_kaspa_dnsseeder_common::{duration_to_ms, now_ms};
use simply_kaspa_dnsseeder_store::Filter;

use crate::state::AppState;
use crate::system::{DiskInfo, ProcessInfo, collect_disk, collect_process};

#[derive(Debug, Clone, Serialize)]
pub struct MetricsResponse {
    pub service: ServiceInfo,
    pub process: ProcessInfo,
    pub disk: DiskInfo,
    pub peers: PeerCounts,
    pub subsystems: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    pub commit: String,
    pub network: String,
    pub uptime_secs: u64,
    pub uptime_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerCounts {
    pub total: u64,
    pub good: u64,
    pub filtered: u64,
    pub stale: u64,
    pub failed: u64,
    pub stub: u64,
    pub v4: u64,
    pub v6: u64,
    pub avg_success_age_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WebSubsystem {
    pub requests: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub post_rejected: PostRejected,
}

#[derive(Debug, Clone, Serialize)]
pub struct PostRejected {
    pub auth: u64,
    pub cors: u64,
    pub ratelimit: u64,
    pub format: u64,
    pub unroutable: u64,
    pub probe: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RateLimiterSubsystem {
    pub ops: u64,
    pub tracked_ips: usize,
}

pub(crate) async fn handler(State(state): State<AppState>) -> Response {
    state.obs.metrics.record_request();
    let now = now_ms();
    let stale_good_ms = duration_to_ms(state.config.stale_good);
    let validity = Filter::serving(
        now,
        stale_good_ms,
        state.config.min_protocol_version,
        state.config.min_user_agent.clone(),
        None,
        state.config.strict_port.then_some(state.config.network_default_port),
    );
    let summary = match state
        .runtime
        .store
        .blocking(move |s| s.summary(now, stale_good_ms, Some(&validity)))
        .await
    {
        Ok(s) => s,
        Err(err) => {
            warn!("web: GET /metrics store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    let process = collect_process(&state.obs.system).await;
    let disk = collect_disk(&state.config.db_path);
    let web = state.obs.metrics.snapshot();
    let elapsed = state.obs.started.elapsed();
    let mut subsystems = match state.obs.metrics_source.extra() {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    let web_subsystem = WebSubsystem {
        requests: web.requests,
        accepted: web.accepted,
        rejected: web.rejected,
        post_rejected: PostRejected {
            auth: web.post_rejected_auth,
            cors: web.post_rejected_cors,
            ratelimit: web.post_rejected_ratelimit,
            format: web.post_rejected_format,
            unroutable: web.post_rejected_unroutable,
            probe: web.post_rejected_probe,
        },
    };
    let rate_limiter_subsystem = RateLimiterSubsystem {
        ops: state.limiter.ops(),
        tracked_ips: state.limiter.tracked_ips(),
    };
    subsystems.insert("web".to_string(), json!(web_subsystem));
    subsystems.insert("rate_limiter".to_string(), json!(rate_limiter_subsystem));
    let response = MetricsResponse {
        service: ServiceInfo {
            name: state.config.service_name.to_string(),
            version: state.config.service_version.to_string(),
            commit: state.config.service_commit.to_string(),
            network: state.config.service_network.clone(),
            uptime_secs: elapsed.as_secs(),
            uptime_ms: elapsed.as_millis(),
        },
        process,
        disk,
        peers: PeerCounts {
            total: summary.total,
            good: summary.good,
            filtered: summary.filtered,
            stale: summary.stale,
            failed: summary.failed,
            stub: summary.stub,
            v4: summary.v4,
            v6: summary.v6,
            avg_success_age_ms: summary.avg_success_age_ms,
        },
        subsystems,
    };
    Json(response).into_response()
}
