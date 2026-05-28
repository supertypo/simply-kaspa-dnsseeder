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

use crate::state::AppState;
use crate::util::now_ms;

pub(crate) async fn handler(State(state): State<AppState>) -> Response {
    state.metrics.record_request();
    let now = now_ms();
    let stale_good_ms = i64::try_from(state.config.stale_good.as_millis()).unwrap_or(i64::MAX);
    let summary = match state.store.summary(now, stale_good_ms) {
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
