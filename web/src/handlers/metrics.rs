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
    state.metrics.record_request();
    let now = now_ms();
    let stale_good_ms = duration_to_ms(state.config.stale_good);
    let summary = match state.store.summary(now, stale_good_ms) {
        Ok(s) => s,
        Err(err) => {
            warn!("web: GET /metrics store error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "store error").into_response();
        }
    };
    let process = collect_process(&state.system).await;
    let disk = collect_disk(&state.config.db_path);
    let web = state.metrics.snapshot();
    Json(json!({
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
    }))
    .into_response()
}
