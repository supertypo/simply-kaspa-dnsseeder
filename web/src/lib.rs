//! HTTP/JSON façade for the seeder.
//!
//! Endpoints are intentionally tiny:
//! - `GET /ping`        — liveness, returns `pong`.
//! - `GET /health`      — readiness with a peer count.
//! - `GET /peers`       — JSON array of [`PeerDto`], sorted by `last_success_ms` desc.
//! - `GET /peers/{id}`  — single peer by hex id.
//! - `POST /peers`      — body `ip:port`, probes the peer and stores it on success.
//!
//! IP addresses are only included in responses when no `--api-key` is
//! configured, or when the request carries a matching `X-API-KEY` header.
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod config;
pub mod dto;
pub mod error;
pub mod metrics;
pub mod prober;
pub mod rate_limit;
pub mod router;
pub mod server;
pub mod state;

#[cfg(test)]
mod dto_tests;
#[cfg(test)]
mod router_tests;

pub use config::WebConfig;
pub use dto::PeerDto;
pub use error::Error;
pub use metrics::{WebMetrics, WebSnapshot};
pub use prober::{Prober, SchedulerProber};
pub use router::build_router;
pub use server::run_web_server;
pub use state::AppState;
