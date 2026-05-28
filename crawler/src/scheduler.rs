//! Concurrent scheduler driving peer probes.
//!
//! Discovery (via [`Scheduler::probe_one`]) only writes stubs to the store; a
//! periodic ticker drives [`Scheduler::enqueue_probes`], which fans out
//! semaphore-bounded probes. Two-tier reprobe cadence mirrors the Go
//! dnsseeder: `stale_good` (default 15m) for previously-successful peers,
//! `stale_bad` (default 2h) otherwise.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use kaspa_consensus_core::network::NetworkId;
use log::{debug, info, warn};
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
use tokio::sync::{Semaphore, broadcast};
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::metrics::CrawlerMetrics;
use crate::model::{ProbeResult, is_acceptable_address, peer_record_from_version};
use crate::probe::Probe;
use crate::seeders::{Resolver, dns_seed_many};
use simply_kaspa_dnsseeder_common::{canonicalize_ip, duration_to_ms, now_ms};

/// Max probes dispatched per tick per thread.
pub(crate) const BATCH_PER_THREAD: usize = 10;
/// Max in-flight probes per thread; new ticks dispatch nothing when the
/// backlog hits this so it drains instead of growing.
pub(crate) const MAX_IN_FLIGHT_PER_THREAD: usize = 10;
/// Pruning runs on this fixed cadence (matches Go dnsseeder).
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);
/// Per-host timeout for `--seeder` lookups. Mirrors the built-in DNS-seeder
/// timeout so a single dead host can't stall bootstrap indefinitely.
const SEEDER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
/// Time an in-flight probe gets to wind down cleanly after shutdown fires.
const SHUTDOWN_PROBE_GRACE: Duration = Duration::from_secs(3);

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
    semaphore: Arc<Semaphore>,
    metrics: Arc<CrawlerMetrics>,
    cancel: CancellationToken,
}

/// Bundle of `Arc`s and clones a single probe task needs. Lets the dispatch
/// loop hand off a single value to `tokio::spawn` instead of five.
struct ProbeTaskCtx {
    probe: Arc<dyn Probe>,
    store: PeerStore,
    in_flight: Arc<DashSet<SocketAddr>>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<CrawlerMetrics>,
    cancel: CancellationToken,
}

impl ProbeTaskCtx {
    fn snapshot(s: &Scheduler) -> Self {
        Self {
            probe: s.probe.clone(),
            store: s.store.clone(),
            in_flight: s.in_flight.clone(),
            semaphore: s.semaphore.clone(),
            metrics: s.metrics.clone(),
            cancel: s.cancel.clone(),
        }
    }
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
        let semaphore = Arc::new(Semaphore::new(config.threads.max(1)));
        Self {
            config,
            store,
            probe,
            resolver,
            in_flight: Arc::new(DashSet::new()),
            semaphore,
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
                    break;
                }
                _ = probe_ticker.tick() => {
                    if let Err(err) = self.enqueue_probes().await {
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
    /// dispatch probes through the semaphore. Skips dispatch entirely when
    /// the in-flight backlog has reached `threads * MAX_IN_FLIGHT_PER_THREAD`
    /// so slow probes can drain.
    async fn enqueue_probes(&self) -> Result<(), Error> {
        let threads = self.config.threads.max(1);
        let in_flight_cap = threads.saturating_mul(MAX_IN_FLIGHT_PER_THREAD);
        let current_in_flight = self.in_flight.len();
        if current_in_flight >= in_flight_cap {
            debug!("crawler: probe tick skipped (in_flight={current_in_flight} >= cap={in_flight_cap})");
            return Ok(());
        }

        let now = now_ms();
        let dead_cutoff = now.saturating_sub(self.config.dead_after_ms());
        let stale_good_ms = self.config.stale_good_ms();
        let stale_bad_ms = self.config.stale_bad_ms();
        let default_port = self.config.network_id.default_p2p_port();
        let strict_port = self.config.strict_port;

        let headroom = in_flight_cap.saturating_sub(current_in_flight);
        let batch_max = threads.saturating_mul(BATCH_PER_THREAD).min(headroom);
        // Overfetch to absorb records filtered by `in_flight` / `strict_port` /
        // private-IP guards without doing another index walk.
        let fetch_target = batch_max.saturating_mul(2).max(batch_max);
        let candidates = self
            .store
            .blocking(move |s| s.due_for_probe(now, stale_good_ms, stale_bad_ms, dead_cutoff, fetch_target))
            .await?;
        let scanned = candidates.len();
        let mut selected: Vec<NetAddress> = Vec::with_capacity(batch_max);
        for rec in candidates {
            if selected.len() >= batch_max {
                break;
            }
            if !is_acceptable_address(&rec.address, default_port, strict_port) {
                continue;
            }
            if self.in_flight.contains(&SocketAddr::new(rec.address.ip, rec.address.port)) {
                continue;
            }
            selected.push(rec.address);
        }

        if selected.is_empty() {
            debug!(
                "crawler: probe tick (index_scanned={scanned}, dispatched=0, in_flight={})",
                self.in_flight.len()
            );
            return Ok(());
        }

        let count = selected.len();
        for net in selected {
            let addr = SocketAddr::new(net.ip, net.port);
            if !self.in_flight.insert(addr) {
                continue;
            }
            if let Err(err) = self.store.blocking(move |s| s.record_attempt(&net, now)).await {
                warn!("crawler: failed to record attempt for {addr}: {err}");
            }
            let ctx = ProbeTaskCtx::snapshot(self);
            tokio::spawn(run_probe_task(ctx, addr, default_port, strict_port));
        }
        debug!(
            "crawler: probe tick (index_scanned={scanned}, dispatched={count}, in_flight={})",
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

async fn run_probe_task(ctx: ProbeTaskCtx, addr: SocketAddr, default_port: u16, strict_port: bool) {
    // Drop guard frees the in_flight slot and decrements the gauge even on panic.
    struct InFlightGuard {
        addr: SocketAddr,
        in_flight: Arc<DashSet<SocketAddr>>,
        metrics: Arc<CrawlerMetrics>,
        armed: bool,
    }
    impl Drop for InFlightGuard {
        fn drop(&mut self) {
            if self.armed {
                self.metrics.in_flight_dec();
            }
            self.in_flight.remove(&self.addr);
        }
    }
    let ProbeTaskCtx { probe, store, in_flight, semaphore, metrics, cancel } = ctx;
    let mut guard = InFlightGuard { addr, in_flight, metrics: metrics.clone(), armed: false };
    let Ok(_permit) = semaphore.acquire_owned().await else { return };
    metrics.in_flight_inc();
    guard.armed = true;
    let mut probe_fut = std::pin::pin!(Scheduler::probe_one(probe.as_ref(), &store, addr, default_port, strict_port, Some(&metrics)));
    tokio::select! {
        () = &mut probe_fut => {}
        () = cancel.cancelled() => {
            if tokio::time::timeout(SHUTDOWN_PROBE_GRACE, probe_fut).await.is_err() {
                debug!("crawler: probe {addr} dropped after {SHUTDOWN_PROBE_GRACE:?} shutdown grace");
            }
        }
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
