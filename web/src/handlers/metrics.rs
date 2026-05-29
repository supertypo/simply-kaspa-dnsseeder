//! `/metrics` handler — JSON snapshot of service, process, disk, peer-store
//! and per-subsystem counters.

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use log::warn;

use simply_kaspa_dnsseeder_common::{duration_to_ms, now_ms};
use simply_kaspa_dnsseeder_store::Filter;

use crate::api_error::ApiError;
use crate::dto::{
    MetricsResponse, PeerCounts, PeerFamilyCounts, PeerStatusCounts, PostRejected, RateLimiterSubsystem, ServiceInfo, WebSubsystem,
};
use crate::state::AppState;
use crate::system::{collect_disk, collect_process};

pub(crate) const PATH: &str = "/metrics";

#[utoipa::path(
    get,
    path = PATH,
    tag = "info",
    responses(
        (status = 200, description = "Metrics snapshot", body = MetricsResponse),
    ),
)]
pub(crate) async fn handler(State(state): State<AppState>) -> Response {
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
            return ApiError::Internal("store error").into_response();
        }
    };
    let process = collect_process(&state.obs.system).await;
    let disk = collect_disk(&state.config.db_path);
    let web = state.obs.metrics.snapshot();
    let elapsed = state.obs.started.elapsed();
    let mut subsystems = state.obs.metrics_source.extra();
    let web_subsystem = WebSubsystem {
        requests: web.requests,
        accepted: web.accepted,
        rejected: web.rejected,
        post_rejected: PostRejected {
            auth: web.post_rejected_auth,
            rate_limit: web.post_rejected_ratelimit,
            format: web.post_rejected_format,
            unroutable: web.post_rejected_unroutable,
            probe: web.post_rejected_probe,
        },
        rate_limiter: RateLimiterSubsystem {
            capacity: state.limiter.capacity(),
            window_ms: u64::try_from(state.limiter.window().as_millis()).unwrap_or(u64::MAX),
            ops: state.limiter.ops(),
            tracked_ips: state.limiter.tracked_ips(),
            denied: web.post_rejected_ratelimit,
        },
    };
    subsystems.insert(
        "web".to_string(),
        serde_json::to_value(&web_subsystem).expect("WebSubsystem is plain data and serializes infallibly"),
    );
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
            by_status: PeerStatusCounts {
                good: summary.good,
                filtered: summary.filtered,
                stale: summary.stale,
                failed: summary.failed,
                stub: summary.stub,
            },
            by_family: PeerFamilyCounts {
                v4: summary.v4,
                v6: summary.v6,
            },
            avg_success_age_ms: summary.avg_success_age_ms,
        },
        subsystems,
    };
    Json(response).into_response()
}
