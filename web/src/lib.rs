//! HTTP/JSON façade for the seeder.
//!
//! Routes: `/health`, `/peers` (list/get/post/delete), `/metrics`.
//! IP addresses are exposed only when no `--api-key` is set or a matching
//! `X-API-KEY` header is present.
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod config;
pub mod dto;
pub mod error;
mod http;
pub mod metrics;
mod runtime;
pub mod server;
pub mod state;

#[cfg(test)]
mod dto_tests;

pub use config::WebConfig;
pub use dto::PeerDto;
pub use error::Error;
pub use http::build_router;
pub use metrics::{MetricsSource, NullMetricsSource, WebMetrics, WebSnapshot};
pub use runtime::{Prober, SchedulerProber};
pub use server::run_web_server;
pub use state::AppState;
