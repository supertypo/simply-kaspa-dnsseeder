//! Conversion from p2p `VersionMessage` + observed address-list to the storage record.

use std::net::{IpAddr, SocketAddr};

use chrono::Utc;
use simply_kaspa_dnsseeder_store::{NetAddress, PeerId, PeerRecord};

use kaspa_p2p_lib::pb::VersionMessage;
use kaspa_utils::networking::IpAddress;

/// Result of a successful probe: the peer's [`VersionMessage`] plus the
/// addresses it advertised in response to our `RequestAddresses` message.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub version: VersionMessage,
    pub addresses: Vec<(IpAddress, u16)>,
}

/// Build a [`PeerRecord`] for a freshly probed peer. `now_ms` is captured by
/// the caller so tests can pin time deterministically.
#[must_use]
pub fn peer_record_from_version(addr: SocketAddr, version: &VersionMessage, now_ms: i64, existing: Option<&PeerRecord>) -> PeerRecord {
    let id = peer_id_from_bytes(&version.id);
    let canonical_ip = canonicalize_ip(addr.ip());
    let address = NetAddress { ip: canonical_ip, port: addr.port() };
    let subnetwork_id: Option<[u8; 20]> = version.subnetwork_id.as_ref().and_then(|s| <[u8; 20]>::try_from(s.bytes.as_slice()).ok());

    let first_seen_ms = existing.map_or(now_ms, |r| r.first_seen_ms);

    PeerRecord {
        id,
        protocol_version: version.protocol_version,
        timestamp_ms: version.timestamp,
        address,
        user_agent: version.user_agent.clone(),
        subnetwork_id,
        first_seen_ms,
        last_attempt_ms: now_ms,
        last_success_ms: now_ms,
        last_seen_ms: now_ms,
    }
}

/// Coerce a `Vec<u8>` of arbitrary length to a 16-byte peer id, zero-padding
/// or truncating as needed (matches kaspad's behavior of using a UUID v4).
#[must_use]
pub fn peer_id_from_bytes(bytes: &[u8]) -> PeerId {
    let mut out = [0u8; 16];
    let n = bytes.len().min(16);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

/// Collapse IPv4-mapped IPv6 (`::ffff:a.b.c.d`) to plain IPv4 so we never
/// store both representations of the same host.
#[must_use]
pub fn canonicalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_canonical(),
        IpAddr::V4(_) => ip,
    }
}

/// Convenience: current UTC time in millis since the epoch.
#[must_use]
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}
