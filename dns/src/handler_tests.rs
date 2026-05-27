use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use hickory_resolver::config::{NameServerConfig, ResolverConfig, ResolverOpts};
use hickory_resolver::proto::xfer::Protocol;
use hickory_resolver::TokioResolver;
use kaspa_consensus_core::network::{NetworkId, NetworkType};
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
use tempfile::TempDir;
use tokio::sync::broadcast;

use crate::{DnsConfig, run_dns_server};

fn make_record(id: u8, ip: IpAddr, now_ms: i64) -> PeerRecord {
    let mut peer_id = [0u8; 16];
    peer_id[0] = id;
    PeerRecord {
        id: peer_id,
        protocol_version: 7,
        network: "kaspa-mainnet".to_string(),
        services: 0,
        timestamp_ms: now_ms,
        address: NetAddress { ip, port: NetworkId::new(NetworkType::Mainnet).default_p2p_port() },
        user_agent: "/kaspad:1.0.0/".to_string(),
        disable_relay_tx: true,
        subnetwork_id: None,
        first_seen_ms: now_ms,
        last_attempt_ms: now_ms,
        last_success_ms: now_ms,
        last_seen_ms: now_ms,
    }
}

async fn start_server(store: PeerStore) -> (SocketAddr, broadcast::Sender<()>, tokio::task::JoinHandle<()>) {
    // Bind on an ephemeral port by asking the OS to choose.
    let listen: SocketAddr = "127.0.0.1:0".parse().unwrap();
    // Bind temporarily to discover a free port, then release it. There's an
    // unavoidable TOCTOU here for unit tests, but at worst the test will be
    // retried by the developer.
    let probe = std::net::UdpSocket::bind(listen).unwrap();
    let bound = probe.local_addr().unwrap();
    drop(probe);

    let cfg = DnsConfig::new(
        NetworkId::new(NetworkType::Mainnet),
        bound,
        "seeder.example.test".to_string(),
        "ns.example.test".to_string(),
    );
    let (tx, rx) = broadcast::channel(1);
    let handle = tokio::spawn(async move {
        let _ = run_dns_server(cfg, store, rx).await;
    });
    // Give the server a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (bound, tx, handle)
}

fn resolver(server: SocketAddr) -> TokioResolver {
    let mut cfg = ResolverConfig::new();
    cfg.add_name_server(NameServerConfig::new(server, Protocol::Udp));
    let mut opts = ResolverOpts::default();
    opts.attempts = 1;
    opts.timeout = Duration::from_secs(2);
    TokioResolver::builder_with_config(cfg, hickory_resolver::name_server::TokioConnectionProvider::default())
        .with_options(opts)
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn answers_a_records_from_store() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = 1_700_000_000_000;
    store.upsert(&make_record(1, IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), now)).unwrap();
    store.upsert(&make_record(2, IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8)), now)).unwrap();

    let (server, shutdown, handle) = start_server(store).await;

    let res = resolver(server);
    let lookup = res.ipv4_lookup("seeder.example.test.").await.unwrap();
    let ips: Vec<Ipv4Addr> = lookup.iter().map(|a| a.0).collect();
    assert!(ips.contains(&Ipv4Addr::new(1, 2, 3, 4)));
    assert!(ips.contains(&Ipv4Addr::new(5, 6, 7, 8)));

    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aaaa_emits_musl_sentinel_when_no_ipv6_peers() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = 1_700_000_000_000;
    store.upsert(&make_record(1, IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), now)).unwrap();

    let (server, shutdown, handle) = start_server(store).await;

    let res = resolver(server);
    let lookup = res.ipv6_lookup("seeder.example.test.").await.unwrap();
    let sentinel = Ipv6Addr::from_str("100::").unwrap();
    let ips: Vec<Ipv6Addr> = lookup.iter().map(|a| a.0).collect();
    assert_eq!(ips, vec![sentinel]);

    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_non_apex_queries() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let (server, shutdown, handle) = start_server(store).await;

    let res = resolver(server);
    let err = res.ipv4_lookup("other.example.test.").await.err().expect("must error");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("refused") || msg.contains("no records found"),
        "unexpected error: {msg}"
    );

    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignores_peers_with_non_default_port() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = 1_700_000_000_000;
    let mut bad = make_record(1, IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)), now);
    bad.address.port = 1234;
    store.upsert(&bad).unwrap();

    let (server, shutdown, handle) = start_server(store).await;
    let res = resolver(server);
    let res = res.ipv4_lookup("seeder.example.test.").await;
    // Either NoRecordsFound or empty answer is acceptable.
    if let Ok(lookup) = res {
        assert_eq!(lookup.iter().count(), 0);
    }

    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}
