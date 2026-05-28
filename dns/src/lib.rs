//! Tiny hickory-server based DNS server.
//!
//! The seeder is authoritative for a single apex name (`--dns-zone`) and only
//! answers `A`, `AAAA`, `NS` and `SOA` queries for that exact name. Anything
//! else gets `REFUSED`. Address records are served from a small in-memory
//! `ServingCache` rebuilt every 60s from
//! [`simply_kaspa_dnsseeder_store::PeerStore`].
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod config;
pub mod error;
pub mod handler;
pub mod metrics;
pub mod server;
pub mod serving_cache;

#[cfg(test)]
mod handler_tests;

pub use config::DnsConfig;
pub use error::Error;
pub use handler::SeederHandler;
pub use metrics::{DnsMetrics, DnsSnapshot};
pub use server::{build_serving_cache, run_dns_server, run_dns_server_with_handler};
pub use serving_cache::{REFRESH_INTERVAL, SNAPSHOT_MULTIPLIER, ServingCache, refresh_now, spawn_refresher};
