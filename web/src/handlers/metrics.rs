//! `/metrics` handler — JSON snapshot of service, process, disk, peer-store
//! and per-subsystem counters.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use log::warn;
use serde_json::json;

use simply_kaspa_dnsseeder_common::{duration_to_ms, now_ms};

use crate::state::AppState;
use crate::system::{collect_disk, collect_process};

pub(crate) async fn handler(State(state): State<AppState>) -> Response {
    state.obs.metrics.record_request();
    let now = now_ms();
    let stale_good_ms = duration_to_ms(state.config.stale_good);
    let summary = match state.runtime.store.blocking(move |s| s.summary(now, stale_good_ms)).await {
        Ok(s) => s,
        Err(err) => {
            warn!("web: GET /metrics store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    let process = collect_process(&state.obs.system).await;
    let disk = collect_disk(&state.config.db_path);
    let web = state.obs.metrics.snapshot();
    let uptime_ms = state.obs.started.elapsed().as_millis();
    let uptime_secs = state.obs.started.elapsed().as_secs();
    let mut subsystems = match state.obs.metrics_source.extra() {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    subsystems.insert(
        "web".to_string(),
        json!({
            "requests": web.requests,
            "accepted": web.accepted,
            "rejected": web.rejected,
            "post_rejected": {
                "auth": web.post_rejected_auth,
                "cors": web.post_rejected_cors,
                "ratelimit": web.post_rejected_ratelimit,
                "format": web.post_rejected_format,
                "unroutable": web.post_rejected_unroutable,
                "probe": web.post_rejected_probe,
            },
        }),
    );
    subsystems.insert(
        "rate_limiter".to_string(),
        json!({
            "ops": state.limiter.ops(),
            "tracked_ips": state.limiter.tracked_ips(),
        }),
    );
    Json(json!({
        "service": {
            "name": state.config.service_name,
            "version": state.config.service_version,
            "commit": state.config.service_commit,
            "network": state.config.service_network,
            "uptime_secs": uptime_secs,
            "uptime_ms": uptime_ms,
        },
        "process": process,
        "disk": disk,
        "peers": {
            "total": summary.total,
            "good": summary.good,
            "stale": summary.stale,
            "failed": summary.failed,
            "stub": summary.stub,
            "v4": summary.v4,
            "v6": summary.v6,
            "avg_success_age_ms": summary.avg_success_age_ms,
        },
        "subsystems": subsystems,
    }))
    .into_response()
}
