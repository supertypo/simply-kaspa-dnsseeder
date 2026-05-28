//! Concurrent scheduler driving peer probes.
//!
//! Design:
//! - The discovery loop in [`Scheduler::probe_one`] only writes to the store
//!   (creates a stub via [`PeerStore::insert_stub_if_missing`]) when a probed
//!   peer advertises new addresses. It does NOT enqueue probes.
//! - A periodic ticker ([`SchedulerConfig::probe_tick`], default 10s) drives
//!   [`Scheduler::enqueue_probes`], which scans the store for eligible peers,
//!   picks a random batch of up to `threads * BATCH_PER_THREAD`, and spawns
//!   probes bounded by a semaphore.
//! - Two-tier reprobe cadence (mirroring the Go dnsseeder): peers that have
//!   succeeded at least once are re-probed every `stale_good` (default 15m);
//!   peers that never succeeded are re-probed every `stale_bad` (default 2h).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use kaspa_consensus_core::network::NetworkId;
use log::{debug, info, warn};
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
use tokio::sync::{Semaphore, broadcast};

use crate::error::Error;
use crate::metrics::CrawlerMetrics;
use crate::model::{ProbeResult, is_acceptable_address, peer_record_from_version};
use simply_kaspa_dnsseeder_common::{canonicalize_ip, duration_to_ms, now_ms};
use crate::probe::Probe;
use crate::seeders::{Resolver, dns_seed_many};

/// Maximum probes dispatched per `probe_tick` per configured thread.
pub(crate) const BATCH_PER_THREAD: usize = 10;
/// Upper bound on outstanding probe tasks (waiting + running), expressed as a
/// multiple of `threads`. When the in-flight set is at or above
/// `threads * MAX_IN_FLIGHT_PER_THREAD`, ticks dispatch nothing new so the
/// backlog can drain instead of growing unboundedly when probes take longer
/// than `probe_tick`.
pub(crate) const MAX_IN_FLIGHT_PER_THREAD: usize = 10;
/// Pruning runs on this fixed cadence (matches Go dnsseeder).
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

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
}

/// Bundle of `Arc`s and clones a single probe task needs. Lets the dispatch
/// loop hand off a single value to `tokio::spawn` instead of five.
struct ProbeTaskCtx {
    probe: Arc<dyn Probe>,
    store: PeerStore,
    in_flight: Arc<DashSet<SocketAddr>>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<CrawlerMetrics>,
}

impl ProbeTaskCtx {
    fn snapshot(s: &Scheduler) -> Self {
        Self {
            probe: s.probe.clone(),
            store: s.store.clone(),
            in_flight: s.in_flight.clone(),
            semaphore: s.semaphore.clone(),
            metrics: s.metrics.clone(),
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
                    break;
                }
                _ = probe_ticker.tick() => {
                    if let Err(err) = self.enqueue_probes() {
                        warn!("crawler: probe enqueue failed: {err}");
                    }
                }
                _ = prune_ticker.tick() => {
                    let cutoff = now_ms().saturating_sub(self.config.dead_after_ms());
                    debug!("crawler: prune tick (cutoff={cutoff}, dead_after={:?})", self.config.dead_after);
                    match self.store.prune_dead(cutoff) {
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
        let store_empty = self.store.is_empty()?;
        if !store_empty {
            debug!("crawler: store non-empty, skipping DNS bootstrap");
            return Ok(());
        }

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
            let net = NetAddress {
                ip: canonicalize_ip(addr.ip()),
                port: addr.port(),
            };
            if !is_acceptable_address(&net, default_port, self.config.strict_port) {
                debug!("crawler: rejected bootstrap address {addr}");
                continue;
            }
            match self.store.insert_stub_if_missing(&net, now) {
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
            match self.resolver.lookup(host, port).await {
                Ok(list) => out.extend(list),
                Err(err) => warn!("crawler: --seeder {host} failed: {err}"),
            }
        }
        out
    }

    /// Pull the K most-overdue peers from the store's attempt-time index and
    /// dispatch probes through the semaphore. Skips dispatch entirely when
    /// the in-flight backlog has reached `threads * MAX_IN_FLIGHT_PER_THREAD`
    /// so slow probes can drain.
    fn enqueue_probes(&self) -> Result<(), Error> {
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
        let candidates = self.store.due_for_probe(now, stale_good_ms, stale_bad_ms, dead_cutoff, fetch_target)?;
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
            if let Err(err) = self.store.record_attempt(&net, now) {
                warn!("crawler: failed to record attempt for {addr}: {err}");
            }
            let ctx = ProbeTaskCtx::snapshot(self);
            // Acquire the permit inside the task so the dispatch loop stays responsive to shutdown.
            tokio::spawn(async move {
                // Drop guard ensures `in_flight` is freed and the gauge is decremented
                // even if `probe_one` panics — without this, a panic would leak the slot
                // and starve the back-pressure cap forever.
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
                let ProbeTaskCtx { probe, store, in_flight, semaphore, metrics } = ctx;
                let mut guard = InFlightGuard { addr, in_flight, metrics: metrics.clone(), armed: false };
                let Ok(_permit) = semaphore.acquire_owned().await else {
                    return;
                };
                metrics.in_flight_inc();
                guard.armed = true;
                Self::probe_one(probe.as_ref(), &store, addr, default_port, strict_port, Some(&metrics)).await;
            });
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
                if let Err(err) = apply_success(store, addr, &result) {
                    warn!("crawler: failed to persist successful probe of {addr}: {err}");
                }
                let discovered = result.addresses.len();
                let mut new_stubs = 0usize;
                let now = now_ms();
                for (ip_addr, port) in &result.addresses {
                    let port = if *port == 0 { default_port } else { *port };
                    let ip: std::net::IpAddr = (*ip_addr).into();
                    let canonical = canonicalize_ip(ip);
                    let net = NetAddress { ip: canonical, port };
                    if !is_acceptable_address(&net, default_port, strict_port) {
                        continue;
                    }
                    match store.insert_stub_if_missing(&net, now) {
                        Ok(true) => new_stubs += 1,
                        Ok(false) => {}
                        Err(err) => warn!("crawler: failed to insert stub for {canonical}:{port}: {err}"),
                    }
                }
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

fn apply_success(
    store: &PeerStore,
    addr: SocketAddr,
    result: &ProbeResult,
) -> Result<PeerRecord, simply_kaspa_dnsseeder_store::Error> {
    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    let existing = store.get(&net)?;
    let record = peer_record_from_version(addr, &result.version, now_ms(), existing.as_ref());
    store.upsert(&record)?;
    Ok(record)
}

fn bump_attempt(store: &PeerStore, addr: SocketAddr) -> Result<(), simply_kaspa_dnsseeder_store::Error> {
    let net = NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    };
    store.record_attempt(&net, now_ms()).map(|_| ())
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
            Ok(result) => apply_success(store, addr, &result).map_err(|e| crate::error::ProbeError::Connection(e.to_string())),
            Err(err) => {
                let _ = bump_attempt(store, addr);
                Err(err)
            }
        }
    }
}
