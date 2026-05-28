//! Axum router assembly.
//!
//! Handlers live in [`crate::handlers`]; this module only wires routes and
//! applies the optional `--api-prefix`.

use axum::Router;
use axum::routing::get;

use crate::handlers::{health, metrics, peers};
use crate::state::AppState;
use crate::util::normalize_prefix;

pub fn build_router(state: AppState) -> Router {
    let prefix = normalize_prefix(&state.config.api_prefix);
    let api = Router::new()
        .route("/ping", get(peers::ping))
        .route("/health", get(health::handler))
        .route("/metrics", get(metrics::handler))
        .route("/peers", get(peers::list).post(peers::submit))
        .route("/peers/{addr}", get(peers::get))
        .with_state(state);
    if prefix.is_empty() { api } else { Router::new().nest(&prefix, api) }
}
