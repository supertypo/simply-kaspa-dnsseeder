mod is_routable {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use crate::scheduler::is_routable;

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }
    fn v6(s: &str) -> IpAddr {
        IpAddr::V6(s.parse::<Ipv6Addr>().unwrap())
    }

    #[test]
    fn accepts_public_v4() {
        assert!(is_routable(v4(8, 8, 8, 8)));
        assert!(is_routable(v4(1, 1, 1, 1)));
    }

    #[test]
    fn rejects_unspecified() {
        assert!(!is_routable(v4(0, 0, 0, 0)));
        assert!(!is_routable(v6("::")));
    }

    #[test]
    fn rejects_loopback() {
        assert!(!is_routable(v4(127, 0, 0, 1)));
        assert!(!is_routable(v6("::1")));
    }

    #[test]
    fn rejects_v4_private_ranges() {
        assert!(!is_routable(v4(10, 0, 0, 1)));
        assert!(!is_routable(v4(192, 168, 1, 1)));
        assert!(!is_routable(v4(172, 16, 0, 1)));
        assert!(!is_routable(v4(172, 31, 255, 254)));
    }

    #[test]
    fn rejects_v4_link_local_and_broadcast() {
        assert!(!is_routable(v4(169, 254, 1, 1)));
        assert!(!is_routable(v4(255, 255, 255, 255)));
    }

    #[test]
    fn rejects_v4_documentation_ranges() {
        assert!(!is_routable(v4(192, 0, 2, 1)));
        assert!(!is_routable(v4(198, 51, 100, 1)));
        assert!(!is_routable(v4(203, 0, 113, 1)));
    }

    #[test]
    fn rejects_multicast() {
        assert!(!is_routable(v4(224, 0, 0, 1)));
        assert!(!is_routable(v6("ff02::1")));
    }

    #[test]
    fn rejects_v6_unique_local_and_link_local() {
        assert!(!is_routable(v6("fc00::1"))); // fc00::/7
        assert!(!is_routable(v6("fd12:3456:789a::1"))); // fc00::/7
        assert!(!is_routable(v6("fe80::1"))); // fe80::/10
    }

    #[test]
    fn accepts_public_v6() {
        assert!(is_routable(v6("2001:4860:4860::8888")));
        assert!(is_routable(v6("2606:4700:4700::1111")));
    }
}

mod probe_one_fanout {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use async_trait::async_trait;
    use dashmap::DashSet;
    use kaspa_p2p_lib::pb::VersionMessage;
    use kaspa_utils::networking::IpAddress;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use simply_kaspa_dnsseeder_store::PeerStore;

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

    fn drain(rx: &mut mpsc::Receiver<SocketAddr>) -> Vec<SocketAddr> {
        let mut out = Vec::new();
        while let Ok(addr) = rx.try_recv() {
            out.push(addr);
        }
        out
    }

    #[tokio::test]
    async fn enqueues_routable_advertised_addresses() {
        let probe = FanoutProbe {
            addresses: vec![
                (ip4(8, 8, 8, 8), 16111),
                (ip4(1, 1, 1, 1), 16222),
            ],
        };
        let (_d, store) = open_store();
        let (tx, mut rx) = mpsc::channel(16);
        let in_flight = DashSet::new();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
        in_flight.insert(source);

        Scheduler::probe_one(&probe, &store, &tx, &in_flight, source, DEFAULT_PORT).await;

        let enqueued = drain(&mut rx);
        assert_eq!(enqueued.len(), 2);
        assert!(enqueued.contains(&"8.8.8.8:16111".parse().unwrap()));
        assert!(enqueued.contains(&"1.1.1.1:16222".parse().unwrap()));
    }

    #[tokio::test]
    async fn falls_back_to_default_port_when_zero() {
        let probe = FanoutProbe { addresses: vec![(ip4(8, 8, 4, 4), 0)] };
        let (_d, store) = open_store();
        let (tx, mut rx) = mpsc::channel(16);
        let in_flight = DashSet::new();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
        in_flight.insert(source);

        Scheduler::probe_one(&probe, &store, &tx, &in_flight, source, DEFAULT_PORT).await;

        let enqueued = drain(&mut rx);
        assert_eq!(enqueued, vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 4, 4)), DEFAULT_PORT)]);
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
        let (tx, mut rx) = mpsc::channel(16);
        let in_flight = DashSet::new();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
        in_flight.insert(source);

        Scheduler::probe_one(&probe, &store, &tx, &in_flight, source, DEFAULT_PORT).await;

        let enqueued = drain(&mut rx);
        assert_eq!(enqueued, vec!["8.8.8.8:16111".parse().unwrap()]);
    }

    #[tokio::test]
    async fn duplicates_within_one_result_are_only_enqueued_once() {
        let probe = FanoutProbe {
            addresses: vec![
                (ip4(8, 8, 8, 8), 16111),
                (ip4(8, 8, 8, 8), 16111),
                (ip4(8, 8, 8, 8), 16111),
            ],
        };
        let (_d, store) = open_store();
        let (tx, mut rx) = mpsc::channel(16);
        let in_flight = DashSet::new();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
        in_flight.insert(source);

        Scheduler::probe_one(&probe, &store, &tx, &in_flight, source, DEFAULT_PORT).await;

        let enqueued = drain(&mut rx);
        assert_eq!(enqueued, vec!["8.8.8.8:16111".parse().unwrap()]);
    }

    #[tokio::test]
    async fn already_in_flight_addresses_are_not_re_enqueued() {
        let probe = FanoutProbe {
            addresses: vec![(ip4(8, 8, 8, 8), 16111), (ip4(1, 1, 1, 1), 16111)],
        };
        let (_d, store) = open_store();
        let (tx, mut rx) = mpsc::channel(16);
        let in_flight = DashSet::new();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
        in_flight.insert(source);
        // Pretend 8.8.8.8 is already being crawled.
        in_flight.insert("8.8.8.8:16111".parse().unwrap());

        Scheduler::probe_one(&probe, &store, &tx, &in_flight, source, DEFAULT_PORT).await;

        let enqueued = drain(&mut rx);
        assert_eq!(enqueued, vec!["1.1.1.1:16111".parse().unwrap()]);
    }

    #[tokio::test]
    async fn tx_full_rolls_back_in_flight_insert() {
        let probe = FanoutProbe {
            addresses: vec![
                (ip4(8, 8, 8, 8), 16111),  // fills the 1-slot tx
                (ip4(1, 1, 1, 1), 16111),  // tx full → rollback in_flight, break
                (ip4(2, 2, 2, 2), 16111),  // never reached
            ],
        };
        let (_d, store) = open_store();
        let (tx, mut rx) = mpsc::channel(1); // capacity 1
        let in_flight = DashSet::new();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
        in_flight.insert(source);

        Scheduler::probe_one(&probe, &store, &tx, &in_flight, source, DEFAULT_PORT).await;

        // Only the first one made it into tx; the second was rolled back.
        let first = rx.try_recv().unwrap();
        assert_eq!(first, "8.8.8.8:16111".parse::<SocketAddr>().unwrap());

        assert!(in_flight.contains(&"8.8.8.8:16111".parse::<SocketAddr>().unwrap()));
        assert!(
            !in_flight.contains(&"1.1.1.1:16111".parse::<SocketAddr>().unwrap()),
            "rollback should remove the address that failed to enqueue",
        );
        assert!(!in_flight.contains(&"2.2.2.2:16111".parse::<SocketAddr>().unwrap()));
    }

    #[tokio::test]
    async fn canonicalizes_ipv4_mapped_ipv6() {
        // ::ffff:8.8.8.8 should be enqueued as plain IPv4.
        let probe = FanoutProbe { addresses: vec![(ip6("::ffff:8.8.8.8"), 16111)] };
        let (_d, store) = open_store();
        let (tx, mut rx) = mpsc::channel(16);
        let in_flight = DashSet::new();
        let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
        in_flight.insert(source);

        Scheduler::probe_one(&probe, &store, &tx, &in_flight, source, DEFAULT_PORT).await;

        let enqueued = drain(&mut rx);
        assert_eq!(enqueued, vec!["8.8.8.8:16111".parse::<SocketAddr>().unwrap()]);
    }
}
