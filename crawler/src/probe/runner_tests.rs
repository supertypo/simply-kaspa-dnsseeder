use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use async_trait::async_trait;
use kaspa_p2p_lib::pb::VersionMessage;
use kaspa_utils::networking::IpAddress;
use tempfile::TempDir;

use simply_kaspa_dnsseeder_store::{NetAddress, PeerStore};

use crate::error::ProbeError;
use crate::model::ProbeResult;
use crate::probe::Probe;
use crate::probe::runner::{probe_and_store, probe_one};

const DEFAULT_PORT: u16 = 16111;

struct FanoutProbe {
    addresses: Vec<(IpAddress, u16)>,
}

#[async_trait]
impl Probe for FanoutProbe {
    async fn probe(&self, _addr: SocketAddr) -> Result<ProbeResult, ProbeError> {
        Ok(ProbeResult {
            version: version_msg(),
            addresses: self.addresses.clone(),
        })
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
        addresses: vec![(ip4(8, 8, 8, 8), 16111), (ip4(1, 1, 1, 1), 16222)],
    };
    let (_d, store) = open_store();
    let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

    probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

    assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111)).unwrap().is_some());
    assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 16222)).unwrap().is_some());
}

#[tokio::test]
async fn falls_back_to_default_port_when_zero() {
    let probe = FanoutProbe {
        addresses: vec![(ip4(8, 8, 4, 4), 0)],
    };
    let (_d, store) = open_store();
    let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

    probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

    assert!(
        store
            .get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 4, 4)), DEFAULT_PORT))
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn skips_non_routable_addresses() {
    let probe = FanoutProbe {
        addresses: vec![
            (ip4(10, 0, 0, 1), 16111),    // private
            (ip4(127, 0, 0, 1), 16111),   // loopback
            (ip4(169, 254, 1, 1), 16111), // link-local
            (ip4(224, 0, 0, 1), 16111),   // multicast
            (ip6("fc00::1"), 16111),      // v6 ULA
            (ip4(8, 8, 8, 8), 16111),     // ← only this should pass
        ],
    };
    let (_d, store) = open_store();
    let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

    probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

    assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 16111)).unwrap().is_none());
    assert!(store.get(&net(IpAddr::V4(Ipv4Addr::LOCALHOST), 16111)).unwrap().is_none());
    assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)), 16111)).unwrap().is_none());
    assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), 16111)).unwrap().is_none());
    assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111)).unwrap().is_some());
}

#[tokio::test]
async fn duplicates_within_one_result_only_insert_once() {
    let probe = FanoutProbe {
        addresses: vec![(ip4(8, 8, 8, 8), 16111), (ip4(8, 8, 8, 8), 16111), (ip4(8, 8, 8, 8), 16111)],
    };
    let (_d, store) = open_store();
    let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

    probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

    let rec = store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111)).unwrap().unwrap();
    assert_eq!(rec.last_attempt_ms, 0);
    assert_eq!(rec.last_success_ms, 0);
}

#[tokio::test]
async fn discovery_does_not_overwrite_existing_record() {
    let probe = FanoutProbe {
        addresses: vec![(ip4(8, 8, 8, 8), 16111)],
    };
    let (_d, store) = open_store();
    let addr = net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111);
    store.record_attempt(&addr, 500).unwrap();
    let pre = store.get(&addr).unwrap().unwrap();

    let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();
    probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

    let post = store.get(&addr).unwrap().unwrap();
    assert_eq!(pre, post, "discovery must not touch an existing record");
}

#[tokio::test]
async fn canonicalizes_ipv4_mapped_ipv6() {
    let probe = FanoutProbe {
        addresses: vec![(ip6("::ffff:8.8.8.8"), 16111)],
    };
    let (_d, store) = open_store();
    let source: SocketAddr = "9.9.9.9:16111".parse().unwrap();

    probe_one(&probe, &store, source, DEFAULT_PORT, false, None).await;

    assert!(store.get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111)).unwrap().is_some());
}

struct FailingProbe;

#[async_trait]
impl Probe for FailingProbe {
    async fn probe(&self, _addr: SocketAddr) -> Result<ProbeResult, ProbeError> {
        Err(ProbeError::Connection("simulated failure".into()))
    }
}

#[tokio::test]
async fn probe_and_store_failure_bumps_attempt() {
    let (_d, store) = open_store();
    let addr: SocketAddr = "8.8.8.8:16111".parse().unwrap();

    let result = probe_and_store(&FailingProbe, &store, addr).await;
    assert!(result.is_err());

    let rec = store
        .get(&net(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 16111))
        .unwrap()
        .expect("attempt creates record");
    assert!(rec.last_attempt_ms > 0, "bump_attempt should have set last_attempt_ms");
    assert_eq!(rec.last_success_ms, 0, "failure must not record success");
}
