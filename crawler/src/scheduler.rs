//! Concurrent scheduler driving peer probes.
//!
//! Discovery (via [`Scheduler::probe_one`]) only writes stubs to the store; a
//! periodic ticker drives [`Scheduler::enqueue_probes`], which feeds a bounded
//! channel consumed by a fixed pool of worker tasks. Two-tier reprobe cadence
//! mirrors the Go dnsseeder: `stale_good` (default 15m) for previously-
//! successful peers, `stale_bad` (default 2h) otherwise.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use kaspa_consensus_core::network::NetworkId;
use log::{debug, info, warn};
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::metrics::CrawlerMetrics;
use crate::model::{ProbeResult, is_acceptable_address, peer_record_from_version};
use crate::probe::Probe;
use crate::seeders::{Resolver, dns_seed_many};
use simply_kaspa_dnsseeder_common::{canonicalize_ip, duration_to_ms, now_ms};

/// Probe queue depth per worker. Bounds how many ready peers can sit waiting
/// for a worker before the ticker back-pressures via `try_send`.
const CHANNEL_PER_THREAD: usize = 4;
/// Pruning runs on this fixed cadence (matches Go dnsseeder).
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);
/// Per-host timeout for `--seeder` lookups. Mirrors the built-in DNS-seeder
/// timeout so a single dead host can't stall bootstrap indefinitely.
const SEEDER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Static configuration for the scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub network_id: NetworkId,
    pub threads: usize,
    /// How often `enqueue_probes` scans the store for eligible peers.
    pub probe_tick: Duration,
    /// Re-probe interval for peers that have succeeded at least once.
    pub stale_good: Duration,
    /// Re-probe interval for peers that have never succeeded.
    pub stale_bad: Duration,
    pub dead_after: Duration,
    /// Explicit DNS seeder hosts (`--seeder`), tried at bootstrap if non-empty.
    pub seeders: Vec<String>,
    /// When true, only addresses on the network's default P2P port are accepted.
    pub strict_port: bool,
}

impl SchedulerConfig {
    fn dead_after_ms(&self) -> i64 {
        duration_to_ms(self.dead_after)
    }

    fn stale_good_ms(&self) -> i64 {
        duration_to_ms(self.stale_good)
    }

    fn stale_bad_ms(&self) -> i64 {
        duration_to_ms(self.stale_bad)
    }
}

pub struct Scheduler {
    config: SchedulerConfig,
    store: PeerStore,
    probe: Arc<dyn Probe>,
    resolver: Arc<dyn Resolver>,
    in_flight: Arc<DashSet<SocketAddr>>,
    metrics: Arc<CrawlerMetrics>,
    cancel: CancellationToken,
}

impl Scheduler {
    #[must_use]
    pub fn new(
        config: SchedulerConfig,
        store: PeerStore,
        probe: Arc<dyn Probe>,
        resolver: Arc<dyn Resolver>,
        metrics: Arc<CrawlerMetrics>,
    ) -> Self {
        Self {
            config,
            store,
            probe,
            resolver,
            in_flight: Arc::new(DashSet::new()),
            metrics,
            cancel: CancellationToken::new(),
        }
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<CrawlerMetrics> {
        self.metrics.clone()
    }

    /// Run the scheduler. Returns when `shutdown` fires.
    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) -> Result<(), Error> {
        self.bootstrap().await?;

        let threads = self.config.threads.max(1);
        let default_port = self.config.network_id.default_p2p_port();
        let strict_port = self.config.strict_port;
        let (tx, rx) = mpsc::channel::<NetAddress>(threads.saturating_mul(CHANNEL_PER_THREAD));

        let dispatcher = tokio::spawn(run_dispatcher(
            rx,
            self.probe.clone(),
            self.store.clone(),
            self.in_flight.clone(),
            self.metrics.clone(),
            self.cancel.clone(),
            threads,
            default_port,
            strict_port,
        ));

        let mut probe_ticker = tokio::time::interval(self.config.probe_tick);
        probe_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut prune_ticker = tokio::time::interval(PRUNE_INTERVAL);
        prune_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        prune_ticker.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("crawler: shutdown signal received");
                    self.cancel.cancel();
                    self.probe.close().await;
                    break;
                }
                _ = probe_ticker.tick() => {
                    if let Err(err) = self.enqueue_probes(&tx).await {
                        warn!("crawler: probe enqueue failed: {err}");
                    }
                }
                _ = prune_ticker.tick() => {
                    let cutoff = now_ms().saturating_sub(self.config.dead_after_ms());
                    debug!("crawler: prune tick (cutoff={cutoff}, dead_after={:?})", self.config.dead_after);
                    match self.store.blocking(move |s| s.prune_dead(cutoff)).await {
                        Ok(n) if n > 0 => info!("crawler: pruned {n} dead peer(s)"),
                        Ok(_) => debug!("crawler: prune tick removed 0 peers"),
                        Err(err) => warn!("crawler: prune failed: {err}"),
                    }
                }
            }
        }
        drop(tx);
        let _ = dispatcher.await;
        Ok(())
    }

    async fn bootstrap(&self) -> Result<(), Error> {
        // Idempotent: re-running recovers from a previous interrupted bootstrap that only inserted some stubs.
        let bootstrap_addrs = if self.config.seeders.is_empty() {
            info!(
                "crawler: bootstrapping from built-in dns seeders for network {}",
                self.config.network_id
            );
            dns_seed_many(self.config.network_id, self.resolver.clone()).await
        } else {
            info!("crawler: bootstrapping from --seeder hosts: {:?}", self.config.seeders);
            self.resolve_explicit_seeders().await
        };

        let default_port = self.config.network_id.default_p2p_port();
        let now = now_ms();
        let mut inserted = 0usize;
        for addr in bootstrap_addrs {
            let net = net_from(addr);
            if !is_acceptable_address(&net, default_port, self.config.strict_port) {
                debug!("crawler: rejected bootstrap address {addr}");
                continue;
            }
            match self.store.blocking(move |s| s.insert_stub_if_missing(&net, now)).await {
                Ok(true) => inserted += 1,
                Ok(false) => {}
                Err(err) => warn!("crawler: failed to insert bootstrap stub for {addr}: {err}"),
            }
        }
        if inserted > 0 {
            info!("crawler: bootstrap inserted {inserted} address stub(s)");
        }
        Ok(())
    }

    async fn resolve_explicit_seeders(&self) -> Vec<SocketAddr> {
        let port = self.config.network_id.default_p2p_port();
        let mut out = Vec::new();
        for host in &self.config.seeders {
            match tokio::time::timeout(SEEDER_LOOKUP_TIMEOUT, self.resolver.lookup(host, port)).await {
                Ok(Ok(list)) => out.extend(list),
                Ok(Err(err)) => warn!("crawler: --seeder {host} failed: {err}"),
                Err(_) => warn!("crawler: --seeder {host} timed out after {SEEDER_LOOKUP_TIMEOUT:?}"),
            }
        }
        out
    }

    /// Pull the K most-overdue peers from the store's attempt-time index and
    /// hand them to the worker pool via the bounded channel. Stops scanning as
    /// soon as the channel back-pressures so slow workers can drain.
    async fn enqueue_probes(&self, tx: &mpsc::Sender<NetAddress>) -> Result<(), Error> {
        let capacity = tx.capacity();
        if capacity == 0 {
            debug!("crawler: probe tick skipped (channel full, in_flight={})", self.in_flight.len());
            return Ok(());
        }

        let now = now_ms();
        let dead_cutoff = now.saturating_sub(self.config.dead_after_ms());
        let stale_good_ms = self.config.stale_good_ms();
        let stale_bad_ms = self.config.stale_bad_ms();
        let default_port = self.config.network_id.default_p2p_port();
        let strict_port = self.config.strict_port;

        // Overfetch to absorb records filtered by `in_flight` / `strict_port` /
        // private-IP guards without doing another index walk.
        let fetch_target = capacity.saturating_mul(2).max(capacity);
        let candidates = self
            .store
            .blocking(move |s| s.due_for_probe(now, stale_good_ms, stale_bad_ms, dead_cutoff, fetch_target))
            .await?;
        let scanned = candidates.len();
        let mut dispatched = 0usize;
        for rec in candidates {
            if !is_acceptable_address(&rec.address, default_port, strict_port) {
                continue;
            }
            let addr = SocketAddr::new(rec.address.ip, rec.address.port);
            if !self.in_flight.insert(addr) {
                continue;
            }
            let net = rec.address;
            if let Err(err) = self.store.blocking(move |s| s.record_attempt(&net, now)).await {
                warn!("crawler: failed to record attempt for {addr}: {err}");
            }
            match tx.try_send(net) {
                Ok(()) => dispatched += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.in_flight.remove(&addr);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.in_flight.remove(&addr);
                    return Ok(());
                }
            }
        }
        debug!(
            "crawler: probe tick (index_scanned={scanned}, dispatched={dispatched}, in_flight={})",
            self.in_flight.len()
        );
        Ok(())
    }

    /// Probe a single peer, apply the outcome to the store, and record any
    /// freshly discovered addresses as stubs for the next scheduling tick.
    pub(crate) async fn probe_one(
        probe: &dyn Probe,
        store: &PeerStore,
        addr: SocketAddr,
        default_port: u16,
        strict_port: bool,
        metrics: Option<&CrawlerMetrics>,
    ) {
        match probe.probe(addr).await {
            Ok(result) => {
                if let Some(m) = metrics {
                    m.record_ok();
                }
                debug!(
                    "crawler: probe {addr} succeeded (protocol={}, ua={:?})",
                    result.version.protocol_version, result.version.user_agent
                );
                if let Err(err) = apply_success(store, addr, &result).await {
                    warn!("crawler: failed to persist successful probe of {addr}: {err}");
                }
                let discovered = result.addresses.len();
                let now = now_ms();
                let addresses = result.addresses.clone();
                let new_stubs = store
                    .blocking(move |s| {
                        let mut inserted = 0usize;
                        for (ip_addr, port) in &addresses {
                            let port = if *port == 0 { default_port } else { *port };
                            let ip: std::net::IpAddr = (*ip_addr).into();
                            let canonical = canonicalize_ip(ip);
                            let net = NetAddress { ip: canonical, port };
                            if !is_acceptable_address(&net, default_port, strict_port) {
                                continue;
                            }
                            match s.insert_stub_if_missing(&net, now) {
                                Ok(true) => inserted += 1,
                                Ok(false) => {}
                                Err(err) => warn!("crawler: failed to insert stub for {canonical}:{port}: {err}"),
                            }
                        }
                        inserted
                    })
                    .await;
                if discovered > 0 {
                    debug!("crawler: {addr} advertised {discovered} address(es), {new_stubs} new");
                }
            }
            Err(err) => {
                if let Some(m) = metrics {
                    m.record_failed();
                }
                debug!("crawler: probe {addr} failed: {err}");
                // `last_attempt` was already bumped by enqueue_probes before dispatch.
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_dispatcher(
    mut rx: mpsc::Receiver<NetAddress>,
    probe: Arc<dyn Probe>,
    store: PeerStore,
    in_flight: Arc<DashSet<SocketAddr>>,
    metrics: Arc<CrawlerMetrics>,
    cancel: CancellationToken,
    threads: usize,
    default_port: u16,
    strict_port: bool,
) {
    let mut workers: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            Some(net) = rx.recv(), if workers.len() < threads => {
                let addr = SocketAddr::new(net.ip, net.port);
                let probe = probe.clone();
                let store = store.clone();
                let in_flight = in_flight.clone();
                let metrics = metrics.clone();
                let cancel = cancel.clone();
                workers.spawn(async move {
                    let _guard = InFlightGuard::new(addr, in_flight, metrics.clone());
                    tokio::select! {
                        () = Scheduler::probe_one(probe.as_ref(), &store, addr, default_port, strict_port, Some(&metrics)) => {}
                        () = cancel.cancelled() => debug!("crawler: probe {addr} dropped on shutdown"),
                    }
                });
            }
            Some(_) = workers.join_next(), if !workers.is_empty() => {}
            else => break,
        }
    }
    workers.abort_all();
    while workers.join_next().await.is_some() {}
}

/// Frees the `in_flight` slot and decrements the gauge even on panic.
struct InFlightGuard {
    addr: SocketAddr,
    in_flight: Arc<DashSet<SocketAddr>>,
    metrics: Arc<CrawlerMetrics>,
}

impl InFlightGuard {
    fn new(addr: SocketAddr, in_flight: Arc<DashSet<SocketAddr>>, metrics: Arc<CrawlerMetrics>) -> Self {
        metrics.in_flight_inc();
        Self { addr, in_flight, metrics }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.in_flight_dec();
        self.in_flight.remove(&self.addr);
    }
}

async fn apply_success(
    store: &PeerStore,
    addr: SocketAddr,
    result: &ProbeResult,
) -> Result<PeerRecord, simply_kaspa_dnsseeder_store::Error> {
    let net = net_from(addr);
    let version = result.version.clone();
    store
        .blocking(move |s| {
            let existing = s.get(&net)?;
            let record = peer_record_from_version(addr, &version, now_ms(), existing.as_ref());
            s.upsert(&record)?;
            Ok(record)
        })
        .await
}

async fn bump_attempt(store: &PeerStore, addr: SocketAddr) -> Result<(), simply_kaspa_dnsseeder_store::Error> {
    let net = net_from(addr);
    let now = now_ms();
    store.blocking(move |s| s.record_attempt(&net, now).map(|_| ())).await
}

fn net_from(addr: SocketAddr) -> NetAddress {
    NetAddress { ip: canonicalize_ip(addr.ip()), port: addr.port() }
}

impl Scheduler {
    /// Run a single probe synchronously, used by the web crate to handle
    /// HTTP submissions through the same code path as scheduled probes.
    /// Probe errors are returned to the caller; storage errors are logged
    /// and surfaced as an `Err` as well.
    pub async fn probe_and_store(
        probe: &dyn Probe,
        store: &PeerStore,
        addr: SocketAddr,
    ) -> Result<PeerRecord, crate::error::ProbeError> {
        match probe.probe(addr).await {
            Ok(result) => apply_success(store, addr, &result).await.map_err(|e| crate::error::ProbeError::Connection(e.to_string())),
            Err(err) => {
                let _ = bump_attempt(store, addr).await;
                Err(err)
            }
        }
    }
}
