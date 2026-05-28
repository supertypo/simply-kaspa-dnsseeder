//! JSON projections of [`simply_kaspa_dnsseeder_store::PeerRecord`].
//!
//! Two shapes are exposed via an `#[serde(untagged)]` enum so each
//! authentication state serializes as a plain object with exactly the fields
//! that view contains:
//!
//! - [`FullPeerDto`]: authenticated callers (or when no `--api-key` is set).
//! - [`PublicPeerDto`]: anonymous callers when an API key is configured.
//!
//! Fields that may legitimately be "not yet known" are serialized as JSON
//! `null` rather than sentinel values (`0`, `""`, `"0000…"`).

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use simply_kaspa_dnsseeder_store::{PeerRecord, UNKNOWN_PEER_ID};

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum PeerDto {
    Full(FullPeerDto),
    Public(PublicPeerDto),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicPeerDto {
    pub protocol_version: Option<u32>,
    pub user_agent: Option<String>,
    pub kaspad_version: Option<String>,
    pub port: u16,
    pub last_seen_ms: i64,
    pub last_seen: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FullPeerDto {
    pub id: Option<String>,
    pub protocol_version: Option<u32>,
    pub user_agent: Option<String>,
    pub kaspad_version: Option<String>,
    pub ip: String,
    pub port: u16,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub last_attempt_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub first_seen: String,
    pub last_seen: String,
    pub last_attempt: Option<String>,
    pub last_success: Option<String>,
}

impl PeerDto {
    #[must_use]
    pub fn from_record(rec: &PeerRecord, expose_full: bool) -> Self {
        if expose_full {
            Self::Full(FullPeerDto::from_record(rec))
        } else {
            Self::Public(PublicPeerDto::from_record(rec))
        }
    }
}

fn opt_string(s: &str) -> Option<String> {
    if s.is_empty() { None } else { Some(s.to_string()) }
}

fn opt_protocol(v: u32) -> Option<u32> {
    if v == 0 { None } else { Some(v) }
}

fn opt_ms(ms: i64) -> Option<i64> {
    if ms > 0 { Some(ms) } else { None }
}

fn opt_id(id: [u8; 16]) -> Option<String> {
    if id == UNKNOWN_PEER_ID { None } else { Some(hex::encode(id)) }
}

impl PublicPeerDto {
    #[must_use]
    pub fn from_record(rec: &PeerRecord) -> Self {
        Self {
            protocol_version: opt_protocol(rec.protocol_version),
            user_agent: opt_string(&rec.user_agent),
            kaspad_version: PeerRecord::parse_kaspad_version(&rec.user_agent).map(|v| v.to_string()),
            port: rec.address.port,
            last_seen_ms: rec.last_seen_ms,
            last_seen: format_iso(rec.last_seen_ms),
        }
    }
}

impl FullPeerDto {
    #[must_use]
    pub fn from_record(rec: &PeerRecord) -> Self {
        Self {
            id: opt_id(rec.id),
            protocol_version: opt_protocol(rec.protocol_version),
            user_agent: opt_string(&rec.user_agent),
            kaspad_version: PeerRecord::parse_kaspad_version(&rec.user_agent).map(|v| v.to_string()),
            ip: rec.address.ip.to_string(),
            port: rec.address.port,
            first_seen_ms: rec.first_seen_ms,
            last_seen_ms: rec.last_seen_ms,
            last_attempt_ms: opt_ms(rec.last_attempt_ms),
            last_success_ms: opt_ms(rec.last_success_ms),
            first_seen: format_iso(rec.first_seen_ms),
            last_seen: format_iso(rec.last_seen_ms),
            last_attempt: opt_ms(rec.last_attempt_ms).map(format_iso),
            last_success: opt_ms(rec.last_success_ms).map(format_iso),
        }
    }
}

fn format_iso(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}
