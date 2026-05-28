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
        assert!(is_acceptable_address(&net(v6("2001:4860:4860::8888"), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(is_acceptable_address(&net(v6("2606:4700:4700::1111"), DEFAULT_PORT), DEFAULT_PORT, false));
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
        assert!(!is_acceptable_address(&net(v4(172, 31, 255, 254), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_v4_cgnat() {
        assert!(!is_acceptable_address(&net(v4(100, 64, 0, 1), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_v4_link_local_and_broadcast() {
        assert!(!is_acceptable_address(&net(v4(169, 254, 1, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v4(255, 255, 255, 255), DEFAULT_PORT), DEFAULT_PORT, false));
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
        assert!(!is_acceptable_address(&net(v6("fd12:3456:789a::1"), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v6("fe80::1"), DEFAULT_PORT), DEFAULT_PORT, false));
    }

    #[test]
    fn rejects_multicast() {
        assert!(!is_acceptable_address(&net(v4(224, 0, 0, 1), DEFAULT_PORT), DEFAULT_PORT, false));
        assert!(!is_acceptable_address(&net(v6("ff02::1"), DEFAULT_PORT), DEFAULT_PORT, false));
    }
}

mod probe_one_fanout {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use async_trait::async_trait;
    use kaspa_p2p_lib::pb::VersionMessage;
    use kaspa_utils::networking::IpAddress;
    use tempfile::TempDir;

    use simply_kaspa_dnsseeder_store::{NetAddress, PeerStore};

    use crate::error::ProbeError;
    use crate::model::ProbeResult;
    use crate::probe::Probe;
    use crate::scheduler::Scheduler;

    const DEFAULT_PORT: u16 = 16111;

    struct FanoutProbe {
        addresses: Vec<(IpAddress, u16)>,
    }

    #[async_trait]
    impl Probe for FanoutProbe {
        async fn probe(&self, _addr: SocketAddr) -> Result<ProbeResult, ProbeError> {
            Ok(ProbeResult { version: version_msg(), addresses: self.addresses.clone() })
        }
    }

    fn version_msg() -> VersionMessage {
        VersionMessage {
            protocol_version: 7,
            services: 0,
            timestamp: 1,
            address: None,
            id: vec![0x42; 16],
            user_agent: "/kaspad:1.0.0/".into(),
            disable_relay_tx: true,
            subnetwork_id: None,
            network: "kaspa-mainnet".into(),
        }
    }

    fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpAddress {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d)).into()
    }

    fn ip6(s: &str) -> IpAddress {
        IpAddr::V6(s.parse::<Ipv6Addr>().unwrap()).into()
    }

    fn open_store() -> (TempDir, PeerStore) {
        let dir = TempDir::new().unwrap();
        let store = PeerStore::open(dir.path().join("p.redb")).unwrap();
        (dir, store)
    }

    fn net(ip: IpAddr, port: u16) -> NetAddress {
        NetAddress { ip, port }
    }

    #[tokio::test]
    async fn inserts_stubs_for_routable_advertised_addresses() {
        let probe = FanoutProbe {
            addresses: vec![
                (ip4(8, 8, 8, 8), 16111),
                (ip4(1, 1, 1, 1), 16222),
            ],
        };
        let (_d, store) = open_store();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

        Scheduler::probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

        // Source got upserted as a full record (probe succeeded), plus two stubs.
        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111)).unwrap().is_some());
        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 16222)).unwrap().is_some());
    }

    #[tokio::test]
    async fn falls_back_to_default_port_when_zero() {
        let probe = FanoutProbe { addresses: vec![(ip4(8, 8, 4, 4), 0)] };
        let (_d, store) = open_store();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

        Scheduler::probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 4, 4)), DEFAULT_PORT)).unwrap().is_some());
    }

    #[tokio::test]
    async fn skips_non_routable_addresses() {
        let probe = FanoutProbe {
            addresses: vec![
                (ip4(10, 0, 0, 1), 16111),       // private
                (ip4(127, 0, 0, 1), 16111),      // loopback
                (ip4(169, 254, 1, 1), 16111),    // link-local
                (ip4(224, 0, 0, 1), 16111),      // multicast
                (ip6("fc00::1"), 16111),         // v6 ULA
                (ip4(8, 8, 8, 8), 16111),        // ← only this should pass
            ],
        };
        let (_d, store) = open_store();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

        Scheduler::probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 16111)).unwrap().is_none());
        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::LOCALHOST), 16111)).unwrap().is_none());
        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)), 16111)).unwrap().is_none());
        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), 16111)).unwrap().is_none());
        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111)).unwrap().is_some());
    }

    #[tokio::test]
    async fn duplicates_within_one_result_only_insert_once() {
        let probe = FanoutProbe {
            addresses: vec![
                (ip4(8, 8, 8, 8), 16111),
                (ip4(8, 8, 8, 8), 16111),
                (ip4(8, 8, 8, 8), 16111),
            ],
        };
        let (_d, store) = open_store();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

        Scheduler::probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

        let rec = store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111)).unwrap().unwrap();
        // Stub: last_attempt should be 0 (we haven't probed it yet — discovery only).
        assert_eq!(rec.last_attempt_ms, 0);
        assert_eq!(rec.last_success_ms, 0);
    }

    #[tokio::test]
    async fn discovery_does_not_overwrite_existing_record() {
        let probe = FanoutProbe { addresses: vec![(ip4(8, 8, 8, 8), 16111)] };
        let (_d, store) = open_store();
        // Pre-existing record with a recent attempt+success — discovery must not touch it.
        let addr = net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111);
        store.record_attempt(&addr, 500).unwrap();
        let pre = store.get(&addr).unwrap().unwrap();

        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
        Scheduler::probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

        let post = store.get(&addr).unwrap().unwrap();
        assert_eq!(pre, post, "discovery must not touch an existing record");
    }

    #[tokio::test]
    async fn canonicalizes_ipv4_mapped_ipv6() {
        let probe = FanoutProbe { addresses: vec![(ip6("::ffff:8.8.8.8"), 16111)] };
        let (_d, store) = open_store();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

        Scheduler::probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

        assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111)).unwrap().is_some());
    }
}

mod is_eligible {
    use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord};
    use std::net::{IpAddr, Ipv4Addr};

    use crate::scheduler::is_eligible;

    fn rec(last_attempt: i64, last_success: i64, first_seen: i64, last_seen: i64) -> PeerRecord {
        PeerRecord {
            id: [0u8; 16],
            protocol_version: 0,
            timestamp_ms: 0,
            address: NetAddress { ip: IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), port: 16111 },
            user_agent: String::new(),
            subnetwork_id: None,
            first_seen_ms: first_seen,
            last_attempt_ms: last_attempt,
            last_success_ms: last_success,
            last_seen_ms: last_seen,
        }
    }

    const NOW: i64 = 10_000_000;
    const GOOD: i64 = 900_000; // 15m
    const BAD: i64 = 7_200_000; // 2h
    const DEAD_CUTOFF: i64 = 0;

    #[test]
    fn good_recent_not_eligible() {
        let r = rec(NOW - 100_000, NOW - 200_000, NOW - 1_000_000, NOW - 100_000);
        assert!(!is_eligible(&r, NOW, GOOD, BAD, DEAD_CUTOFF));
    }

    #[test]
    fn good_stale_eligible() {
        let r = rec(NOW - GOOD - 1, NOW - 2_000_000, NOW - 10_000_000, NOW - GOOD - 1);
        assert!(is_eligible(&r, NOW, GOOD, BAD, DEAD_CUTOFF));
    }

    #[test]
    fn bad_recent_not_eligible() {
        // never succeeded; last attempt 30min ago — under stale_bad threshold
        let r = rec(NOW - 1_800_000, 0, NOW - 2_000_000, 0);
        assert!(!is_eligible(&r, NOW, GOOD, BAD, DEAD_CUTOFF));
    }

    #[test]
    fn bad_stale_eligible() {
        // never succeeded; last attempt 3h ago — past stale_bad
        let r = rec(NOW - BAD - 1, 0, NOW - BAD - 1, 0);
        assert!(is_eligible(&r, NOW, GOOD, BAD, DEAD_CUTOFF));
    }

    #[test]
    fn never_attempted_stub_eligible() {
        // brand-new stub: last_attempt=0, last_success=0; bad threshold applies
        let r = rec(0, 0, NOW - 60_000, NOW - 60_000);
        assert!(is_eligible(&r, NOW, GOOD, BAD, DEAD_CUTOFF));
    }

    #[test]
    fn past_dead_cutoff_not_eligible() {
        // Both first_seen and last_seen below cutoff → considered dead, skip.
        let r = rec(NOW - BAD - 1, 0, 100, 100);
        assert!(!is_eligible(&r, NOW, GOOD, BAD, 1_000));
    }
}
