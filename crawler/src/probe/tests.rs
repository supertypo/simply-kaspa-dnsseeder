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

// ---------------------------------------------------------------------------
// Connection-lifecycle tests.
//
// Invariant: a probe connection's lifetime is bounded by *our* side, never by
// the remote's willingness to hang up. `Adaptor::terminate` cannot provide
// that — it only closes routers the Hub has already registered, and the Hub
// event loop usually hasn't processed `NewPeer` when the probe finishes — so
// the probe closes its own router and a watchdog reaps anything the probe path
// can't reach. Each test checks the observable result: no Hub entry and no
// stream left open on the peer side.
// ---------------------------------------------------------------------------

use crate::probe::fake_peer::{Behavior, FakePeer};

fn lifecycle_config(probe_timeout: Duration) -> ProbeInitializerConfig {
    ProbeInitializerConfig::new(NetworkId::new(NetworkType::Mainnet), probe_timeout, 1)
}

/// Poll `cond` every 50 ms until it holds or `deadline` elapses.
async fn wait_until(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while !cond() {
        if start.elapsed() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    true
}

async fn assert_nothing_lingers(probe: &KaspadProbe, peer: &FakePeer, deadline: Duration, what: &str) {
    let settled = wait_until(deadline, || probe.active_peers_len() == 0 && peer.open_streams() == 0).await;
    assert!(
        settled,
        "{what}: leaked connections — hub_peers={} peer_open_streams={} after {deadline:?}",
        probe.active_peers_len(),
        peer.open_streams()
    );
    assert_eq!(probe.pending_len(), 0, "{what}: pending map leaked entries");
}

/// A peer that completes the handshake but never reacts to our `Reject`. Every
/// probe must still end with the connection closed *by us* and the router
/// removed from the Hub.
///
/// A Hub-dependent close only fails when it loses a scheduling race against the
/// Hub event loop — roughly 10 % of probes in this debug/multi-worker setup — so
/// 200 probes make a silent pass vanishingly unlikely (0.9^200 ≈ 7e-10) while
/// still finishing in a few seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_closes_connection_even_if_peer_ignores_reject() {
    const PROBES: usize = 200;
    let peer = FakePeer::start(Behavior::IgnoreReject).await;
    let probe = KaspadProbe::new(lifecycle_config(Duration::from_secs(2)));

    for i in 0..PROBES {
        let res = probe.probe(peer.addr).await;
        let result = res.unwrap_or_else(|e| panic!("probe #{i} failed: {e}"));
        assert_eq!(result.addresses.len(), 1, "probe #{i}: handshake did not reach address collection");
    }

    assert_nothing_lingers(&probe, &peer, Duration::from_secs(3), "stubborn peer").await;
}

/// Sanity: the same holds when the peer closes on `Reject` (the common case on
/// mainnet). Guards against a fix that only works because the remote hangs up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_closes_connection_when_peer_closes_on_reject() {
    let peer = FakePeer::start(Behavior::CloseOnReject).await;
    let probe = KaspadProbe::new(lifecycle_config(Duration::from_secs(2)));

    for i in 0..10 {
        probe.probe(peer.addr).await.unwrap_or_else(|e| panic!("probe #{i} failed: {e}"));
    }

    assert_nothing_lingers(&probe, &peer, Duration::from_secs(3), "polite peer").await;
}

/// Handshake failures take a different path (`ConnectionHandler::connect`
/// closes the router before `NewPeer` is queued). Make sure that path frees the
/// connection too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_handshake_does_not_leak_connection() {
    let peer = FakePeer::start(Behavior::WrongNetwork).await;
    let probe = KaspadProbe::new(lifecycle_config(Duration::from_secs(2)));

    for _ in 0..10 {
        let res = probe.probe(peer.addr).await;
        assert!(res.is_err(), "expected network mismatch, got {res:?}");
    }

    assert_nothing_lingers(&probe, &peer, Duration::from_secs(3), "wrong-network peer").await;
}

/// A probe future that gets cancelled mid-handshake (e.g. by an outer timeout)
/// orphans its router: nobody holds it except the receive loop, and a silent
/// peer never triggers a route error that would end that loop. The lifetime
/// watchdog must reap it within `max_connection_lifetime`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watchdog_reaps_router_orphaned_by_cancelled_probe() {
    const PROBES: usize = 5;
    let peer = FakePeer::start(Behavior::Silent).await;
    let cfg = lifecycle_config(Duration::from_secs(1));
    let lifetime = cfg.max_connection_lifetime();
    let probe = KaspadProbe::new(cfg);

    for _ in 0..PROBES {
        // Cancel well before `probe_timeout` so `do_probe` never gets to fail on its own.
        let cancelled = tokio::time::timeout(Duration::from_millis(300), probe.probe(peer.addr)).await;
        assert!(cancelled.is_err(), "probe should have been cancelled by the outer timeout");
    }
    assert_eq!(
        peer.open_streams(),
        PROBES,
        "precondition: cancelled probes left their streams open"
    );

    assert_nothing_lingers(&probe, &peer, lifetime + Duration::from_secs(5), "cancelled probes").await;
}
