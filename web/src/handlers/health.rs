//! Liveness/readiness handler.
//!
//! Returns 200 while the store reports at least one peer with a successful
//! probe inside `--stale-good`; 503 otherwise.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use log::warn;
use serde_json::json;

use simply_kaspa_dnsseeder_common::{duration_to_ms, now_ms};

use crate::state::AppState;

pub(crate) async fn handler(State(state): State<AppState>) -> Response {
    state.metrics.record_request();
    let now = now_ms();
    let stale_good_ms = duration_to_ms(state.config.stale_good);
    let summary = match state.store.blocking(move |s| s.summary(now, stale_good_ms)).await {
        Ok(s) => s,
        Err(err) => {
            warn!("web: GET /health store error: {err}");
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
