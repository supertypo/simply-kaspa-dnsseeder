//! Liveness/readiness handler.
//!
//! Returns 200 while the store reports at least one peer with a successful
//! probe inside `--stale-good`; 503 otherwise.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use log::warn;
use serde::Serialize;

use simply_kaspa_dnsseeder_common::{duration_to_ms, now_ms};

use crate::state::AppState;

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub good: Option<u64>,
    pub total: u64,
    pub service: String,
    pub version: String,
}

pub(crate) async fn handler(State(state): State<AppState>) -> Response {
    let now = now_ms();
    let stale_good_ms = duration_to_ms(state.config.stale_good);
    let summary = match state.runtime.store.blocking(move |s| s.summary(now, stale_good_ms, None)).await {
        Ok(s) => s,
        Err(err) => {
            warn!("web: GET /health store error: {err}");
            let body = HealthResponse {
                status: "down",
                reason: Some("store error"),
                good: None,
                total: 0,
                service: state.config.service_name.to_string(),
                version: state.config.service_version.to_string(),
            };
            return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
        }
    };
    let service = state.config.service_name.to_string();
    let version = state.config.service_version.to_string();
    if summary.good > 0 {
        let body = HealthResponse {
            status: "ok",
            reason: None,
            good: Some(summary.good),
            total: summary.total,
            service,
            version,
        };
        Json(body).into_response()
    } else {
        let body = HealthResponse {
            status: "down",
            reason: Some("no peers with successful probe within stale-good window"),
            good: None,
            total: summary.total,
            service,
            version,
        };
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    }
}
