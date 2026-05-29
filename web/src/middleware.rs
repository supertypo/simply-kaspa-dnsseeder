//! Tower middleware: request counter and api-key auth gate.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{body::Body, http::Request};

use crate::metrics::PostRejection;
use crate::state::AppState;
use crate::util::authenticated;

pub(crate) async fn count_requests(State(state): State<AppState>, req: Request<Body>, next: Next) -> Response {
    state.obs.metrics.record_request();
    next.run(req).await
}

pub(crate) async fn require_api_key(State(state): State<AppState>, headers: HeaderMap, req: Request<Body>, next: Next) -> Response {
    if !authenticated(&headers, &state.config.api_key) {
        state.obs.metrics.record_post_rejection(PostRejection::Auth);
        return (StatusCode::UNAUTHORIZED, "missing or invalid api key").into_response();
    }
    next.run(req).await
}
