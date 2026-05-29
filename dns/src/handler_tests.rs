use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{DNSClass, Name, RData, RecordType};
use hickory_proto::serialize::binary::BinDecodable;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::{ConnectionConfig, NameServerConfig, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use kaspa_consensus_core::network::{NetworkId, NetworkType};
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
use tempfile::TempDir;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;

use crate::{DnsConfig, run_dns_server};

const APEX: &str = "seeder.example.test";
const APEX_FQDN: &str = "seeder.example.test.";
const NS: &str = "ns.example.test";

fn current_now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .expect("system clock fits in i64")
}

fn make_record(id: u8, ip: IpAddr, now_ms: i64) -> PeerRecord {
    let mut peer_id = [0u8; 16];
    peer_id[0] = id;
    PeerRecord {
        id: peer_id,
        protocol_version: 7,
        timestamp_ms: now_ms,
        address: NetAddress {
            ip,
            port: NetworkId::new(NetworkType::Mainnet).default_p2p_port(),
        },
        user_agent: "/kaspad:1.0.0/".to_string(),
        subnetwork_id: None,
        first_seen_ms: now_ms,
        last_attempt_ms: now_ms,
        last_success_ms: now_ms,
        last_seen_ms: now_ms,
    }
}

/// Test baseline config: rate-limiting disabled and a generous `max_records`
/// so behavior under test is not coupled to production defaults. Each test
/// further overrides only the fields it exercises.
fn baseline_cfg(listen: SocketAddr) -> DnsConfig {
    let mut cfg = DnsConfig::new(NetworkId::new(NetworkType::Mainnet), vec![listen], APEX.to_string(), NS.to_string());
    cfg.queries_per_ip_per_second = 0;
    cfg.max_records = 1024;
    cfg
}

async fn start_server_with(
    cfg_override: impl FnOnce(&mut DnsConfig),
    store: PeerStore,
) -> (SocketAddr, DnsConfig, broadcast::Sender<()>, tokio::task::JoinHandle<()>) {
    let listen: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let probe = std::net::UdpSocket::bind(listen).unwrap();
    let bound = probe.local_addr().unwrap();
    drop(probe);

    let mut cfg = baseline_cfg(bound);
    cfg_override(&mut cfg);
    let cfg_clone = cfg.clone();
    let (tx, rx) = broadcast::channel(1);
    let handle = tokio::spawn(async move {
        let _ = run_dns_server(cfg_clone, store, rx).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (bound, cfg, tx, handle)
}

async fn start_server(store: PeerStore) -> (SocketAddr, DnsConfig, broadcast::Sender<()>, tokio::task::JoinHandle<()>) {
    start_server_with(|_| {}, store).await
}

fn resolver(server: SocketAddr) -> TokioResolver {
    let mut conn = ConnectionConfig::udp();
    conn.port = server.port();
    let ns = NameServerConfig::new(server.ip(), true, vec![conn]);
    let cfg = ResolverConfig::from_parts(None, vec![], vec![ns]);
    let mut opts = ResolverOpts::default();
    opts.attempts = 1;
    opts.timeout = Duration::from_secs(2);
    TokioResolver::builder_with_config(cfg, TokioRuntimeProvider::default())
        .with_options(opts)
        .build()
        .unwrap()
}

async fn send_raw_query(server: SocketAddr, query: Message, timeout: Duration) -> Result<Message, &'static str> {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.connect(server).await.unwrap();
    let buf = query.to_vec().unwrap();
    sock.send(&buf).await.unwrap();

    let mut resp_buf = vec![0u8; 4096];
    let recv = tokio::time::timeout(timeout, sock.recv(&mut resp_buf)).await;
    match recv {
        Ok(Ok(n)) => Ok(Message::from_bytes(&resp_buf[..n]).expect("parse response")),
        Ok(Err(_)) | Err(_) => Err("no response"),
    }
}

async fn send_raw_query_bytes(server: SocketAddr, query: Message, timeout: Duration) -> Result<Vec<u8>, &'static str> {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.connect(server).await.unwrap();
    sock.send(&query.to_vec().unwrap()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    match tokio::time::timeout(timeout, sock.recv(&mut buf)).await {
        Ok(Ok(n)) => Ok(buf[..n].to_vec()),
        Ok(Err(_)) | Err(_) => Err("no response"),
    }
}

fn craft_query(name: &str, qtype: RecordType, class: DNSClass, id: u16, op: OpCode) -> Message {
    let mut msg = Message::new(id, MessageType::Query, op);
    msg.metadata.recursion_desired = false;
    let mut q = Query::new();
    q.set_name(Name::from_str(name).unwrap());
    q.set_query_type(qtype);
    q.set_query_class(class);
    msg.add_query(q);
    msg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn answers_a_records_from_store() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    store.upsert(&make_record(1, IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), now)).unwrap();
    store.upsert(&make_record(2, IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8)), now)).unwrap();

    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let res = resolver(server);
    let lookup = res.ipv4_lookup(APEX_FQDN).await.unwrap();
    let ips: Vec<Ipv4Addr> = lookup
        .answers()
        .iter()
        .filter_map(|r| if let RData::A(a) = &r.data { Some(a.0) } else { None })
        .collect();
    assert!(ips.contains(&Ipv4Addr::new(1, 2, 3, 4)));
    assert!(ips.contains(&Ipv4Addr::new(5, 6, 7, 8)));
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aaaa_emits_musl_sentinel_when_no_ipv6_peers() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    store.upsert(&make_record(1, IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), now)).unwrap();
    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let res = resolver(server);
    let lookup = res.ipv6_lookup(APEX_FQDN).await.unwrap();
    let sentinel = Ipv6Addr::from_str("100::").unwrap();
    let ips: Vec<Ipv6Addr> = lookup
        .answers()
        .iter()
        .filter_map(|r| if let RData::AAAA(a) = &r.data { Some(a.0) } else { None })
        .collect();
    assert_eq!(ips, vec![sentinel]);
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_non_apex_queries() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let res = resolver(server);
    let err = res.ipv4_lookup("other.example.test.").await.expect_err("must error");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("refused") || msg.contains("no records found"),
        "unexpected error: {msg}"
    );
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignores_peers_whose_last_success_is_too_old() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    // last_success well past the configured stale_good window.
    let stale_ts = now - 16 * 60 * 1000;
    let mut stale = make_record(1, IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), stale_ts);
    // first_seen/last_seen recent enough to escape pruning, last_attempt recent too.
    stale.first_seen_ms = now;
    stale.last_seen_ms = now;
    stale.last_attempt_ms = now;
    stale.last_success_ms = stale_ts;
    store.upsert(&stale).unwrap();

    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let res = resolver(server);
    let lookup = res.ipv4_lookup(APEX_FQDN).await;
    if let Ok(l) = lookup {
        assert_eq!(l.answers().len(), 0, "stale peer must not appear in DNS answers");
    }
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignores_stub_peers_without_success() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    // Stub: never succeeded.
    let mut stub = make_record(2, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), now);
    stub.last_success_ms = 0;
    store.upsert(&stub).unwrap();

    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let res = resolver(server);
    let lookup = res.ipv4_lookup(APEX_FQDN).await;
    if let Ok(l) = lookup {
        assert_eq!(l.answers().len(), 0, "stub peer must not appear in DNS answers");
    }
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignores_peers_with_non_default_port() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    let mut bad = make_record(1, IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)), now);
    bad.address.port = 1234;
    store.upsert(&bad).unwrap();
    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let res = resolver(server);
    let res = res.ipv4_lookup(APEX_FQDN).await;
    if let Ok(lookup) = res {
        assert_eq!(lookup.answers().len(), 0);
    }
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_any_query() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    for i in 0..10 {
        store.upsert(&make_record(i, IpAddr::V4(Ipv4Addr::new(10, 0, 0, i)), now)).unwrap();
    }
    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let query = craft_query(APEX_FQDN, RecordType::ANY, DNSClass::IN, 1, OpCode::Query);
    let resp = send_raw_query(server, query, Duration::from_secs(1)).await.expect("must reply");
    assert_eq!(resp.response_code, ResponseCode::Refused);
    assert!(resp.answers.is_empty());
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_axfr_query() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let query = craft_query(APEX_FQDN, RecordType::AXFR, DNSClass::IN, 2, OpCode::Query);
    let resp = send_raw_query(server, query, Duration::from_secs(1)).await.expect("must reply");
    assert_eq!(resp.response_code, ResponseCode::Refused);
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_disallowed_qtypes() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let (server, _cfg, shutdown, handle) = start_server(store).await;
    for (id, qtype) in [
        (10, RecordType::TXT),
        (11, RecordType::MX),
        (12, RecordType::CNAME),
        (13, RecordType::PTR),
    ] {
        let query = craft_query(APEX_FQDN, qtype, DNSClass::IN, id, OpCode::Query);
        let resp = send_raw_query(server, query, Duration::from_secs(1)).await.expect("must reply");
        assert_eq!(resp.response_code, ResponseCode::Refused, "qtype {qtype:?} should be REFUSED");
        assert!(resp.answers.is_empty());
    }
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_non_in_class() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let (server, _cfg, shutdown, handle) = start_server(store).await;
    for (id, class) in [(20, DNSClass::CH), (21, DNSClass::HS), (22, DNSClass::ANY), (23, DNSClass::NONE)] {
        let query = craft_query(APEX_FQDN, RecordType::A, class, id, OpCode::Query);
        let resp = send_raw_query(server, query, Duration::from_secs(1)).await.expect("must reply");
        assert_eq!(resp.response_code, ResponseCode::Refused, "class {class:?} should be REFUSED");
    }
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_update_opcode() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let (server, _cfg, shutdown, handle) = start_server(store).await;
    let query = craft_query(APEX_FQDN, RecordType::A, DNSClass::IN, 30, OpCode::Update);
    let resp = send_raw_query(server, query, Duration::from_secs(1)).await.expect("must reply");
    assert_eq!(resp.response_code, ResponseCode::Refused);
    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_is_capped_and_randomized() {
    let cap: usize = 16;
    let peer_count: u8 = 100;
    let trials: u16 = 5;

    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    for i in 0..peer_count {
        store.upsert(&make_record(i, IpAddr::V4(Ipv4Addr::new(10, 0, 0, i)), now)).unwrap();
    }
    let (server, cfg, shutdown, handle) = start_server_with(
        |c| {
            c.max_records = cap;
        },
        store,
    )
    .await;
    assert_eq!(cfg.max_records, cap);

    let mut seen_sets: Vec<HashSet<Ipv4Addr>> = Vec::new();
    for trial in 0..trials {
        let query = craft_query(APEX_FQDN, RecordType::A, DNSClass::IN, 100 + trial, OpCode::Query);
        let resp = send_raw_query(server, query, Duration::from_secs(1)).await.expect("must reply");
        assert_eq!(resp.response_code, ResponseCode::NoError);
        assert!(resp.answers.len() <= cfg.max_records);
        let ips: HashSet<Ipv4Addr> = resp
            .answers
            .iter()
            .filter_map(|r| if let RData::A(a) = &r.data { Some(a.0) } else { None })
            .collect();
        seen_sets.push(ips);
    }
    let all_identical = seen_sets.windows(2).all(|w| w[0] == w[1]);
    assert!(!all_identical, "responses should be randomized, got: {seen_sets:?}");

    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limit_drops_silently() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    store.upsert(&make_record(1, IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), now)).unwrap();
    let (server, _cfg, shutdown, handle) = start_server_with(
        |c| {
            c.queries_per_ip_per_second = 1;
            c.rate_limit_window = Duration::from_mins(1);
        },
        store,
    )
    .await;

    let first = craft_query(APEX_FQDN, RecordType::A, DNSClass::IN, 200, OpCode::Query);
    let resp = send_raw_query(server, first, Duration::from_secs(1))
        .await
        .expect("first must reply");
    assert_eq!(resp.response_code, ResponseCode::NoError);

    let second = craft_query(APEX_FQDN, RecordType::A, DNSClass::IN, 201, OpCode::Query);
    let dropped = send_raw_query(server, second, Duration::from_millis(400)).await;
    assert!(dropped.is_err(), "rate-limited query must NOT receive a response");

    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_fits_in_udp_mtu() {
    const UDP_MTU: usize = 512;
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let now = current_now_ms();
    for i in 0..200u8 {
        store.upsert(&make_record(i, IpAddr::V4(Ipv4Addr::new(10, 0, 0, i)), now)).unwrap();
    }
    let prod_defaults = DnsConfig::new(
        NetworkId::new(NetworkType::Mainnet),
        vec!["127.0.0.1:0".parse().unwrap()],
        APEX.to_string(),
        NS.to_string(),
    );
    let (server, _cfg, shutdown, handle) = start_server_with(
        |c| {
            c.max_records = prod_defaults.max_records;
        },
        store,
    )
    .await;

    let query = craft_query(APEX_FQDN, RecordType::A, DNSClass::IN, 300, OpCode::Query);
    let bytes = send_raw_query_bytes(server, query, Duration::from_secs(1))
        .await
        .expect("must reply");
    assert!(
        bytes.len() <= UDP_MTU,
        "response was {} bytes, exceeds UDP MTU {}",
        bytes.len(),
        UDP_MTU
    );

    shutdown.send(()).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}
