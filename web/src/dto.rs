//! JSON projections of [`simply_kaspa_dnsseeder_store::PeerRecord`].
//!
//! Two shapes are exposed via an `#[serde(untagged)]` enum so each
//! authentication state serializes as a plain object with exactly the fields
//! that view contains — no `Option<…>`/`skip_serializing_if` juggling:
//!
//! - [`FullPeerDto`]: authenticated callers (or when no `--api-key` is set).
//! - [`PublicPeerDto`]: anonymous callers when an API key is configured.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use simply_kaspa_dnsseeder_store::PeerRecord;

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum PeerDto {
    Full(FullPeerDto),
    Public(PublicPeerDto),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicPeerDto {
    pub protocol_version: u32,
    pub user_agent: String,
    pub kaspad_version: Option<String>,
    pub port: u16,
    pub default_port: bool,
    pub last_seen_ms: i64,
    pub last_seen: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FullPeerDto {
    pub id: String,
    pub protocol_version: u32,
    pub user_agent: String,
    pub kaspad_version: Option<String>,
    pub ip: String,
    pub port: u16,
    pub default_port: bool,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub last_attempt_ms: i64,
    pub last_success_ms: i64,
    pub first_seen: String,
    pub last_seen: String,
    pub last_attempt: String,
    pub last_success: String,
}

impl PeerDto {
    #[must_use]
    pub fn from_record(rec: &PeerRecord, expose_full: bool, network_default_port: u16) -> Self {
        if expose_full {
            Self::Full(FullPeerDto::from_record(rec, network_default_port))
        } else {
            Self::Public(PublicPeerDto::from_record(rec, network_default_port))
        }
    }
}

impl PublicPeerDto {
    #[must_use]
    pub fn from_record(rec: &PeerRecord, network_default_port: u16) -> Self {
        Self {
            protocol_version: rec.protocol_version,
            user_agent: rec.user_agent.clone(),
            kaspad_version: PeerRecord::parse_kaspad_version(&rec.user_agent).map(|v| v.to_string()),
            port: rec.address.port,
            default_port: rec.address.port == network_default_port,
            last_seen_ms: rec.last_seen_ms,
            last_seen: format_iso(rec.last_seen_ms),
        }
    }
}

impl FullPeerDto {
    #[must_use]
    pub fn from_record(rec: &PeerRecord, network_default_port: u16) -> Self {
        Self {
            id: hex::encode(rec.id),
            protocol_version: rec.protocol_version,
            user_agent: rec.user_agent.clone(),
            kaspad_version: PeerRecord::parse_kaspad_version(&rec.user_agent).map(|v| v.to_string()),
            ip: rec.address.ip.to_string(),
            port: rec.address.port,
            default_port: rec.address.port == network_default_port,
            first_seen_ms: rec.first_seen_ms,
            last_seen_ms: rec.last_seen_ms,
            last_attempt_ms: rec.last_attempt_ms,
            last_success_ms: rec.last_success_ms,
            first_seen: format_iso(rec.first_seen_ms),
            last_seen: format_iso(rec.last_seen_ms),
            last_attempt: format_iso(rec.last_attempt_ms),
            last_success: format_iso(rec.last_success_ms),
        }
    }
}

fn format_iso(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}
