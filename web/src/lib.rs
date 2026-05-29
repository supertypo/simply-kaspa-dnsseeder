//! HTTP/JSON façade for the seeder.
//!
//! Routes: `/ping`, `/health`, `/peers` (list/get/post), `/metrics`.
//! IP addresses are exposed only when no `--api-key` is set or a matching
//! `X-API-KEY` header is present.
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod config;
pub mod dto;
pub mod error;
mod handlers;
pub mod metrics;
pub mod metrics_source;
mod middleware;
mod peers_cache;
pub mod prober;
pub mod router;
pub mod server;
pub mod state;
mod system;
mod util;

#[cfg(test)]
mod dto_tests;
#[cfg(test)]
mod router_tests;

pub use config::WebConfig;
pub use dto::PeerDto;
pub use error::Error;
pub use metrics::{WebMetrics, WebSnapshot};
pub use metrics_source::{MetricsSource, NullMetricsSource};
pub use prober::{Prober, SchedulerProber};
pub use router::build_router;
pub use server::run_web_server;
pub use state::AppState;
