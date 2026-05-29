//! Conversion from p2p `VersionMessage` + observed address-list to the storage record.

use std::net::SocketAddr;

use simply_kaspa_dnsseeder_common::canonicalize_ip;
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
    let address = NetAddress {
        ip: canonical_ip,
        port: addr.port(),
    };
    let subnetwork_id: Option<[u8; 20]> = version
        .subnetwork_id
        .as_ref()
        .and_then(|s| <[u8; 20]>::try_from(s.bytes.as_slice()).ok());

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

/// Ephemeral port floor (IANA dynamic range; Linux default `ip_local_port_range` start).
/// Ports at or above this value are almost always client-side source ports, not listening sockets.
pub const EPHEMERAL_PORT_FLOOR: u16 = 32_768;

#[must_use]
pub fn is_acceptable_address(addr: &NetAddress, default_port: u16, strict_port: bool) -> bool {
    if addr.port == 0 {
        return false;
    }
    if strict_port {
        if addr.port != default_port {
            return false;
        }
    } else if addr.port >= EPHEMERAL_PORT_FLOOR {
        return false;
    }
    if addr.ip.is_multicast() {
        return false;
    }
    IpAddress::from(addr.ip).is_publicly_routable()
}
