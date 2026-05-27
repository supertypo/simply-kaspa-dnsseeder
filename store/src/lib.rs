//! Persistent peer store backed by `redb`, plus a query filter.
//!
//! Records are keyed by the peer-reported 16-byte `id`. Last-write-wins
//! semantics: re-observing an `id` at a new address overwrites the old one.
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
pub use peer_store::PeerStore;
pub use record::{NetAddress, PeerRecord, PeerId};
