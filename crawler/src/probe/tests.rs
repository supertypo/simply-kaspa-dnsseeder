use std::net::SocketAddr;
use std::time::{Duration, Instant};

use kaspa_consensus_core::network::{NetworkId, NetworkType};
use tokio::net::TcpListener;

use crate::probe::{KaspadProbe, Probe};
use crate::probe::initializer::ProbeInitializerConfig;

/// Bind a listener that accepts but never speaks h2 — the gRPC client hangs
/// past `probe_timeout`, forcing the timeout-and-cleanup path.
async fn black_hole_listener() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let _ = listener.accept().await;
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_returns_within_budget_and_cleans_pending() {
    let cfg = ProbeInitializerConfig::new(NetworkId::new(NetworkType::Mainnet), Duration::from_millis(300), 1);
    let probe = KaspadProbe::new(cfg);
    let addr = black_hole_listener().await;

    let start = Instant::now();
    let res = probe.probe(addr).await;
    let elapsed = start.elapsed();

    assert!(res.is_err(), "expected error against a black-hole listener, got {res:?}");
    assert!(
        elapsed < Duration::from_secs(4),
        "probe took {elapsed:?}; must return within probe_timeout + TERMINATE_GRACE + slack"
    );
    assert_eq!(probe.pending_len(), 0, "pending map leaked entries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_refused_cleans_pending() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let cfg = ProbeInitializerConfig::new(NetworkId::new(NetworkType::Mainnet), Duration::from_secs(2), 1);
    let probe = KaspadProbe::new(cfg);

    let res = probe.probe(addr).await;
    assert!(res.is_err());
    assert_eq!(probe.pending_len(), 0, "pending map leaked entries on connection error");
}
