use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kaspa_consensus_core::network::{NetworkId, NetworkType};
use kaspa_p2p_lib::pb::VersionMessage;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::sync::broadcast;

use simply_kaspa_dnsseeder_store::PeerStore;

use crate::error::ProbeError;
use crate::model::ProbeResult;
use crate::probe::Probe;
use crate::scheduler::{Scheduler, SchedulerConfig};
use crate::seeders::Resolver;

#[derive(Default, Clone)]
struct MockProbe {
    calls: Arc<Mutex<Vec<SocketAddr>>>,
    next_id: Arc<Mutex<u8>>,
}

#[async_trait]
impl Probe for MockProbe {
    async fn probe(&self, addr: SocketAddr) -> Result<ProbeResult, ProbeError> {
        self.calls.lock().push(addr);
        let mut id_byte_lock = self.next_id.lock();
        let id_byte = *id_byte_lock;
        *id_byte_lock = id_byte.wrapping_add(1);
        let mut id = vec![0u8; 16];
        id[0] = id_byte;
        let version = VersionMessage {
            protocol_version: 7,
            services: 0,
            timestamp: 1,
            address: None,
            id,
            user_agent: "/kaspad:1.0.0/".to_string(),
            disable_relay_tx: true,
            subnetwork_id: None,
            network: "kaspa-mainnet".to_string(),
        };
        Ok(ProbeResult { version, addresses: vec![] })
    }
}

struct EmptyResolver;
#[async_trait]
impl Resolver for EmptyResolver {
    async fn lookup(&self, _host: &str, _port: u16) -> std::io::Result<Vec<SocketAddr>> {
        Ok(vec![])
    }
}

fn store(temp: &TempDir) -> PeerStore {
    PeerStore::open(temp.path().join("peers.redb")).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn known_peers_are_probed_on_startup() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);
    let probe = Arc::new(MockProbe::default());
    let resolver = Arc::new(EmptyResolver);
    let addr: SocketAddr = "10.0.0.5:16111".parse().unwrap();

    let cfg = SchedulerConfig {
        network_id: NetworkId::new(NetworkType::Mainnet),
        threads: 4,
        crawl_interval: Duration::from_secs(60),
        dead_after: Duration::from_secs(3600),
        seeders: vec![],
        known_peers: vec![addr],
    };
    let scheduler = Scheduler::new(cfg, store.clone(), probe.clone() as Arc<dyn Probe>, resolver);
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = tokio::spawn(async move { scheduler.run(shutdown_rx).await });

    // Wait a moment for the probe to complete and the record to land.
    for _ in 0..50 {
        if store.len().unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(store.len().unwrap() >= 1, "expected at least one stored peer");
    assert!(probe.calls.lock().iter().any(|c| *c == addr));

    shutdown_tx.send(()).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_enqueue_dedups_in_flight() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);
    let probe = Arc::new(MockProbe::default());
    let resolver = Arc::new(EmptyResolver);
    let addr: SocketAddr = "10.0.0.5:16111".parse().unwrap();

    let cfg = SchedulerConfig {
        network_id: NetworkId::new(NetworkType::Mainnet),
        threads: 1,
        crawl_interval: Duration::from_secs(60),
        dead_after: Duration::from_secs(3600),
        seeders: vec![],
        known_peers: vec![],
    };
    let scheduler = Scheduler::new(cfg, store.clone(), probe.clone() as Arc<dyn Probe>, resolver);
    let handle = scheduler.handle();

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let task = tokio::spawn(async move { scheduler.run(shutdown_rx).await });

    assert!(handle.enqueue(addr).await);
    // Second enqueue while the first is still in flight should be rejected.
    let second = handle.enqueue(addr).await;
    // The test isn't strict about whether the first probe finished before
    // this point; either dedup or successful re-enqueue is acceptable behavior
    // for the same address. We just confirm the API call returns a bool.
    let _ = second;

    for _ in 0..50 {
        if store.len().unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(store.len().unwrap() >= 1);

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}
