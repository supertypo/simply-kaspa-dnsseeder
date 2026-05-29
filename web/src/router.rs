//! Axum router assembly.
//!
//! Handlers live in [`crate::handlers`]; this module only wires routes,
//! applies request-counter and api-key middleware, and the optional
//! `--api-prefix`.

use axum::Router;
use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};

use crate::handlers::{health, metrics, peers};
use crate::middleware::{count_requests, require_api_key};
use crate::state::AppState;
use crate::util::normalize_prefix;

pub fn build_router(state: AppState) -> Router {
    let prefix = normalize_prefix(&state.config.api_prefix);

    let gated = Router::new()
        .route("/peers", post(peers::submit))
        .route("/peers/{addr}", get(peers::get))
        .route_layer(from_fn_with_state(state.clone(), require_api_key));

    let open = Router::new()
        .route("/ping", get(peers::ping))
        .route("/health", get(health::handler))
        .route("/metrics", get(metrics::handler))
        .route("/peers", get(peers::list));

    let api = open
        .merge(gated)
        .layer(from_fn_with_state(state.clone(), count_requests))
        .with_state(state);

    if prefix.is_empty() { api } else { Router::new().nest(&prefix, api) }
}
