//! Axum router assembly.
//!
//! Handlers live in [`crate::handlers`]; this module only wires routes,
//! applies request-counter and api-key middleware, the optional
//! `--api-prefix`, and Swagger UI / `OpenAPI` docs.

use std::sync::Arc;

use axum::Router;
use axum::http::{HeaderValue, Method, header};
use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;
use utoipa_swagger_ui::{Config, SwaggerUi};

use crate::handlers::{health, metrics, peers};
use crate::middleware::{AuthState, count_requests, require_api_key};
use crate::openapi;
use crate::state::AppState;

/// Normalize an URL prefix: trim trailing `/`, ensure a leading `/`. Empty input
/// returns empty (router serves at root).
fn normalize_prefix(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

pub fn build_router(state: AppState) -> Router {
    let prefix = normalize_prefix(&state.config.api_prefix);
    let p = |s: &str| format!("{prefix}{s}");

    let auth_state = AuthState {
        key: Arc::<str>::from(state.config.api_key.as_str()),
        metrics: state.obs.metrics.clone(),
    };
    let metrics_state = state.obs.metrics.clone();

    let gated = Router::new()
        .route(&p("/peers"), post(peers::submit))
        .route(&p("/peers/{addr_port}"), get(peers::get))
        .route_layer(from_fn_with_state(auth_state, require_api_key));

    let open = Router::new()
        .route(&p("/health"), get(health::handler))
        .route(&p("/metrics"), get(metrics::handler))
        .route(&p("/peers"), get(peers::list));

    let swagger_base = if prefix.is_empty() {
        "/swagger".to_string()
    } else {
        prefix.clone()
    };
    let openapi_url = format!("{swagger_base}/openapi.json");
    let swagger: Router<AppState> = SwaggerUi::new(swagger_base)
        .url(openapi_url, openapi::document(&prefix))
        .config(Config::default().try_it_out_enabled(true).use_base_layout())
        .into();

    open.merge(gated)
        .merge(swagger)
        .layer(build_cors(&state.config.allowed_origins))
        .layer(from_fn_with_state(metrics_state, count_requests))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=5"),
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn build_cors(allowed_origins: &[String]) -> CorsLayer {
    let base = CorsLayer::new().allow_methods([Method::GET, Method::POST]).allow_headers(Any);
    if allowed_origins.is_empty() {
        return base.allow_origin(Any);
    }
    let parsed: Vec<HeaderValue> = allowed_origins.iter().filter_map(|o| HeaderValue::from_str(o).ok()).collect();
    base.allow_origin(AllowOrigin::list(parsed))
}
