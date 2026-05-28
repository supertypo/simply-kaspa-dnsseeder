//! Tiny hickory-server based DNS server.
//!
//! The seeder is authoritative for a single apex name (`--dns-zone`) and only
//! answers `A`, `AAAA`, `NS` and `SOA` queries for that exact name. Anything
//! else gets `REFUSED`. Address records are pulled from the
//! [`simply_kaspa_dnsseeder_store::PeerStore`] live on every request.
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod config;
pub mod error;
pub mod handler;
pub mod rate_limit;
pub mod server;

#[cfg(test)]
mod handler_tests;
#[cfg(test)]
mod rate_limit_tests;

pub use config::DnsConfig;
pub use error::Error;
pub use handler::SeederHandler;
pub use server::run_dns_server;
