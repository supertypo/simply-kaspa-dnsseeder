//! Persistent peer store backed by `redb`, plus a query filter.
//!
//! Records are keyed by `NetAddress` (ip + port). A record is written on
//! every probe attempt (including failures) so `last_attempt_ms` can gate
//! re-probes without any in-memory backoff bookkeeping.
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

pub mod error;
pub mod filter;
pub mod peer_store;
pub mod record;

#[cfg(test)]
mod filter_tests;
#[cfg(test)]
mod peer_store_tests;

pub use error::Error;
pub use filter::{Family, Filter};
pub use peer_store::{PeerStore, StoreSummary, UNKNOWN_PEER_ID, is_eligible_for_probe};
pub use record::{NetAddress, PeerId, PeerRecord};
