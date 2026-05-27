use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use kaspa_p2p_lib::pb::{SubnetworkId, VersionMessage};
use simply_kaspa_dnsseeder_store::PeerRecord;

use crate::model::{canonicalize_ip, peer_id_from_bytes, peer_record_from_version};

fn version_with(id: Vec<u8>, ua: &str, port_token: u32) -> VersionMessage {
    VersionMessage {
        protocol_version: 7,
        services: 0,
        timestamp: 1_700_000_000_000,
        address: None,
        id,
        user_agent: ua.to_string(),
        disable_relay_tx: true,
        subnetwork_id: Some(SubnetworkId { bytes: vec![0u8; 20] }),
        network: "kaspa-mainnet".to_string(),
    }
}

#[test]
fn peer_id_pads_short_input() {
    let id = peer_id_from_bytes(&[1, 2, 3]);
    assert_eq!(&id[..3], &[1, 2, 3]);
    assert!(id[3..].iter().all(|b| *b == 0));
}

#[test]
fn peer_id_truncates_long_input() {
    let id = peer_id_from_bytes(&[42u8; 32]);
    assert_eq!(id, [42u8; 16]);
}

#[test]
fn canonicalize_collapses_v4_mapped() {
    let v4mapped: IpAddr = Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0102, 0x0304).into();
    assert_eq!(canonicalize_ip(v4mapped), IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)));
}

#[test]
fn peer_record_from_version_seeds_first_seen() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 16111);
    let v = version_with(b"abcdefghijklmnop".to_vec(), "/kaspad:1.2.3/", 0);
    let now = 5_000_000;
    let rec: PeerRecord = peer_record_from_version(addr, &v, now, None);
    assert_eq!(rec.first_seen_ms, now);
    assert_eq!(rec.last_attempt_ms, now);
    assert_eq!(rec.last_success_ms, now);
    assert_eq!(rec.last_seen_ms, now);
    assert_eq!(rec.address.ip, addr.ip());
    assert_eq!(rec.address.port, addr.port());
    assert_eq!(rec.user_agent, "/kaspad:1.2.3/");
    assert_eq!(rec.protocol_version, 7);
    assert_eq!(rec.subnetwork_id, Some([0u8; 20]));
}

#[test]
fn peer_record_preserves_first_seen_on_update() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 16111);
    let v = version_with(b"abcdefghijklmnop".to_vec(), "/kaspad:1.2.3/", 0);
    let initial = peer_record_from_version(addr, &v, 1, None);
    let later = peer_record_from_version(addr, &v, 100, Some(&initial));
    assert_eq!(later.first_seen_ms, 1);
    assert_eq!(later.last_success_ms, 100);
}
