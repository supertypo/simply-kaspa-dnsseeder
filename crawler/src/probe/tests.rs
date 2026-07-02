use std::net::SocketAddr;
use std::time::{Duration, Instant};

use kaspa_consensus_core::network::{NetworkId, NetworkType};
use tokio::net::TcpListener;

use crate::probe::initializer::ProbeInitializerConfig;
use crate::probe::{KaspadProbe, Probe};

/// Bind a listener that accepts TCP but never speaks h2. The failure is driven
/// by tonic's internal `communication_timeout` (10 s in `ConnectionHandler`), not
/// by an outer `connect_budget` timeout — the latter was removed because it
/// cancelled the future mid-handshake and orphaned the router's receive-loop task.
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
    // Failure is driven by tonic's communication_timeout (10 s in ConnectionHandler).
    // The probe must return well before that plus slack, but is no longer bounded by
    // probe_timeout (which only controls do_probe step timeouts, not the h2 layer).
    assert!(
        elapsed < Duration::from_secs(13),
        "probe took {elapsed:?}; h2 communication_timeout (10 s) + slack should have fired"
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
