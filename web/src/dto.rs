//! JSON projections of [`simply_kaspa_dnsseeder_store::PeerRecord`].
//!
//! The `id` is hex-encoded (32 chars) and `ip` is `Option<String>` so the
//! handlers can scrub it depending on the caller's authentication state.

use serde::Serialize;
use simply_kaspa_dnsseeder_store::PeerRecord;

#[derive(Debug, Clone, Serialize)]
pub struct PeerDto {
    pub id: String,
    pub protocol_version: u32,
    pub network: String,
    pub services: u64,
    pub user_agent: String,
    pub disable_relay_tx: bool,
    pub ip: Option<String>,
    pub port: u16,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub last_attempt_ms: i64,
    pub last_success_ms: i64,
}

impl PeerDto {
    #[must_use]
    pub fn from_record(rec: &PeerRecord, expose_ip: bool) -> Self {
        Self {
            id: hex::encode(rec.id),
            protocol_version: rec.protocol_version,
            network: rec.network.clone(),
            services: rec.services,
            user_agent: rec.user_agent.clone(),
            disable_relay_tx: rec.disable_relay_tx,
            ip: if expose_ip { Some(rec.address.ip.to_string()) } else { None },
            port: rec.address.port,
            first_seen_ms: rec.first_seen_ms,
            last_seen_ms: rec.last_seen_ms,
            last_attempt_ms: rec.last_attempt_ms,
            last_success_ms: rec.last_success_ms,
        }
    }
}
