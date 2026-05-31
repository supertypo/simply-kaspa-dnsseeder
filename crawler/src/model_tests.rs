use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use kaspa_p2p_lib::pb::{SubnetworkId, VersionMessage};
use simply_kaspa_dnsseeder_store::PeerRecord;

use crate::model::{peer_id_from_bytes, peer_record_from_version};
use simply_kaspa_dnsseeder_common::canonicalize_ip;

fn version_with(id: Vec<u8>, ua: &str, _port_token: u32) -> VersionMessage {
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

mod is_acceptable_address {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use simply_kaspa_dnsseeder_store::NetAddress;

    use crate::model::is_acceptable_address;

    const DEFAULT_PORT: u16 = 16111;

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }
    fn v6(s: &str) -> IpAddr {
        IpAddr::V6(s.parse::<Ipv6Addr>().unwrap())
    }
    fn net(ip: IpAddr, port: u16) -> NetAddress {
        NetAddress { ip, port }
    }

    #[test]
    fn accepts_public_v4() {
        assert!(is_acceptable_address(&net(v4(8, 8, 8, 8), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(is_acceptable_address(&net(v4(1, 1, 1, 1), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn accepts_public_v6() {
        assert!(is_acceptable_address(
            &net(v6("2001:4860:4860::8888"), DEFAULT_PORT),
            DEFAULT_PORT,
            false
        ));
        assert!(is_acceptable_address(
            &net(v6("2606:4700:4700::1111"), DEFAULT_PORT),
            DEFAULT_PORT,
            false
        ));
    }

    #[test]
    fn rejects_port_zero() {
        assert!(!is_acceptable_address(&net(v4(8, 8, 8, 8), 0), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_ephemeral_ports_when_not_strict() {
        assert!(!is_acceptable_address(&net(v4(8, 8, 8, 8), 32768), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v4(8, 8, 8, 8), 55000), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v4(8, 8, 8, 8), 65535), DEFAULT_PORT, false));
    }

    #[test]
    fn accepts_non_default_low_ports_when_not_strict() {
        assert!(is_acceptable_address(&net(v4(8, 8, 8, 8), 16110), DEFAULT_PORT, false));
        assert!(is_acceptable_address(&net(v4(8, 8, 8, 8), 1234), DEFAULT_PORT, false));
    }

    #[test]
    fn strict_port_rejects_non_default() {
        assert!(!is_acceptable_address(&net(v4(8, 8, 8, 8), 16110), DEFAULT_PORT, true));
        assert!(!is_acceptable_address(&net(v4(8, 8, 8, 8), 1234), DEFAULT_PORT, true));
        assert!(is_acceptable_address(&net(v4(8, 8, 8, 8), DEFAULT_PORT), DEFAULT_PORT, true));
    }

    #[test]
    fn rejects_unspecified() {
        assert!(!is_acceptable_address(&net(v4(0, 0, 0, 0), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v6("::"), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_loopback() {
        assert!(!is_acceptable_address(&net(v4(127, 0, 0, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v6("::1"), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_v4_private_ranges() {
        assert!(!is_acceptable_address(&net(v4(10, 0, 0, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v4(192, 168, 1, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v4(172, 16, 0, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(
            &net(v4(172, 31, 255, 254), DEFAULT_PORT),
            DEFAULT_PORT,
            false
        ));
    }

    #[test]
    fn rejects_v4_cgnat() {
        assert!(!is_acceptable_address(&net(v4(100, 64, 0, 1), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_v4_link_local_and_broadcast() {
        assert!(!is_acceptable_address(&net(v4(169, 254, 1, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(
            &net(v4(255, 255, 255, 255), DEFAULT_PORT),
            DEFAULT_PORT,
            false
        ));
    }

    #[test]
    fn rejects_v4_documentation_ranges() {
        assert!(!is_acceptable_address(&net(v4(192, 0, 2, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v4(198, 51, 100, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v4(203, 0, 113, 1), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_v6_unique_local_and_link_local() {
        assert!(!is_acceptable_address(&net(v6("fc00::1"), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(
            &net(v6("fd12:3456:789a::1"), DEFAULT_PORT),
            DEFAULT_PORT,
            false
        ));
        assert!(!is_acceptable_address(&net(v6("fe80::1"), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_multicast() {
        assert!(!is_acceptable_address(&net(v4(224, 0, 0, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v6("ff02::1"), DEFAULT_PORT), DEFAULT_PORT, false));
    }
}
