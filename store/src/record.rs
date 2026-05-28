use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Raw 16-byte peer id reported in a `VersionMessage`.
pub type PeerId = [u8; 16];

/// IP + port pair, in native types for compact storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetAddress {
    pub ip: IpAddr,
    pub port: u16,
}

/// A persisted peer record. Stored as bincode in redb, keyed by `id`.
///
/// All timestamps are unix milliseconds. `subnetwork_id` is the raw 20-byte
/// kaspa subnetwork identifier when present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRecord {
    pub id: PeerId,
    pub protocol_version: u32,
    pub timestamp_ms: i64,
    pub address: NetAddress,
    pub user_agent: String,
    pub subnetwork_id: Option<[u8; 20]>,
    pub first_seen_ms: i64,
    pub last_attempt_ms: i64,
    pub last_success_ms: i64,
    pub last_seen_ms: i64,
}

impl PeerRecord {
    /// Parses the kaspad-style user-agent token (`/kaspad:X.Y.Z/...`)
    /// and returns the version found in the first segment matching `kaspad:`,
    /// or `None` if the field cannot be parsed.
    #[must_use]
    pub fn parse_kaspad_version(user_agent: &str) -> Option<semver::Version> {
        for segment in user_agent.split('/') {
            if let Some(rest) = segment.strip_prefix("kaspad:") {
                // strip trailing "(comment)" if present
                let v = rest.split_once('(').map_or(rest, |(v, _)| v).trim();
                if let Ok(v) = semver::Version::parse(v) {
                    return Some(v);
                }
            }
        }
        None
    }
}
