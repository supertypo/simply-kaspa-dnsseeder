//! Tower middleware: request counter and api-key auth gate.

use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{body::Body, http::Request};

use super::auth::authenticated;
use crate::error::ApiError;
use crate::metrics::{PostRejection, WebMetrics};

pub(crate) async fn count_requests(State(metrics): State<Arc<WebMetrics>>, req: Request<Body>, next: Next) -> Response {
    metrics.record_request();
    next.run(req).await
}

#[derive(Clone)]
pub(crate) struct AuthState {
    pub key: Arc<str>,
    pub metrics: Arc<WebMetrics>,
}

pub(crate) async fn require_api_key(State(auth): State<AuthState>, headers: HeaderMap, req: Request<Body>, next: Next) -> Response {
    if !authenticated(&headers, &auth.key) {
        auth.metrics.record_post_rejection(PostRejection::Auth);
        return ApiError::Unauthorized("missing or invalid api key").into_response();
    }
    next.run(req).await
}
