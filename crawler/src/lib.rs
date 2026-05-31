//! Concurrent kaspa peer crawler.
//!
//! The [`scheduler::Scheduler`] consumes a stream of `SocketAddr` candidates,
//! delegates the actual handshake-and-RequestAddresses dance to a [`probe::Probe`]
//! implementation (default: [`probe::KaspadProbe`]) and writes results to the
//! [`simply_kaspa_dnsseeder_store::PeerStore`].
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod error;
pub mod metrics;
pub mod model;
pub mod probe;
pub mod scheduler;
pub mod seeders;
mod worker_pool;

#[cfg(test)]
mod model_tests;
#[cfg(test)]
mod seeders_tests;

pub use error::{Error, ProbeError};
pub use metrics::{CrawlerMetrics, CrawlerSnapshot};
pub use model::{EPHEMERAL_PORT_FLOOR, ProbeResult, is_acceptable_address, peer_record_from_version};
pub use probe::initializer::ProbeInitializerConfig;
pub use probe::runner::probe_and_store;
pub use probe::{KaspadProbe, Probe};
pub use scheduler::{Scheduler, SchedulerConfig};
pub use seeders::{Resolver, TokioResolver, dns_seed_many};
