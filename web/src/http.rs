//! HTTP layer: router assembly, middleware, auth, request helpers, handlers, `OpenAPI`.

pub mod auth;
pub mod handlers;
pub mod middleware;
pub mod openapi;
pub mod request;
pub mod router;

#[cfg(test)]
mod router_tests;

pub use router::build_router;
