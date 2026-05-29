//! Periodic scheduler driving peer probes.
//!
//! Responsibilities (only):
//! * Bootstrap the store from DNS seeders.
//! * Run the probe ticker, selecting overdue peers from the store and handing
//!   them to a [`WorkerPool`].
//! * Run the prune ticker.
//! * Propagate shutdown to the worker pool and probe backend.
//!
//! Probe execution lives in [`crate::probe_runner`]; the concurrent worker
//! pool lives in [`crate::worker_pool`].

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use kaspa_consensus_core::network::NetworkId;
use log::{debug, info, warn};
use simply_kaspa_dnsseeder_store::PeerStore;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::metrics::CrawlerMetrics;
use crate::model::is_acceptable_address;
use crate::probe::Probe;
use crate::probe_runner::net_from;
use crate::seeders::{Resolver, dns_seed_many};
use crate::worker_pool::{EnqueueOutcome, WorkerCtx, WorkerPool};
use simply_kaspa_dnsseeder_common::{duration_to_ms, now_ms};

/// Pruning runs on this fixed cadence (matches Go dnsseeder).
const PRUNE_INTERVAL: Duration = Duration::from_mins(1);
/// Per-host timeout for `--seeder` lookups. Mirrors the built-in DNS-seeder
/// timeout so a single dead host can't stall bootstrap indefinitely.
const SEEDER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
/// Cadence at which the built-in DNS seeders are re-resolved so newly added
/// entries percolate in without restarting the seeder. `--seeder` overrides
/// are intentionally only used at bootstrap.
const SEEDER_REFRESH_INTERVAL: Duration = Duration::from_mins(10);
/// Cap on the bulk peer-close at shutdown so hung remotes can't keep the
/// process alive past Docker's SIGTERM grace.
const PROBE_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

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

        let pool = WorkerPool::spawn(
            WorkerCtx {
                probe: self.probe.clone(),
                store: self.store.clone(),
                in_flight: Arc::new(DashSet::new()),
                metrics: self.metrics.clone(),
                cancel: self.cancel.clone(),
                default_port: self.config.network_id.default_p2p_port(),
                strict_port: self.config.strict_port,
            },
            self.config.threads,
        );

        let mut probe_ticker = tokio::time::interval(self.config.probe_tick);
        probe_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut prune_ticker = tokio::time::interval(PRUNE_INTERVAL);
        prune_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        prune_ticker.tick().await;
        let mut seeder_ticker = tokio::time::interval(SEEDER_REFRESH_INTERVAL);
        seeder_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        seeder_ticker.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("crawler: shutdown signal received");
                    self.cancel.cancel();
                    if tokio::time::timeout(PROBE_CLOSE_TIMEOUT, self.probe.close()).await.is_err() {
                        warn!("crawler: probe close exceeded {PROBE_CLOSE_TIMEOUT:?}, continuing shutdown");
                    }
                    break;
                }
                _ = probe_ticker.tick() => {
                    if let Err(err) = self.enqueue_probes(&pool).await {
                        warn!("crawler: probe enqueue failed: {err}");
                    }
                }
                _ = prune_ticker.tick() => {
                    prune_once(&self.store, self.config.dead_after_ms(), self.config.dead_after).await;
                }
                _ = seeder_ticker.tick() => {
                    self.refresh_builtin_seeders().await;
                }
            }
        }
        pool.shutdown().await;
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
        let inserted = insert_bootstrap_stubs(&self.store, bootstrap_addrs, default_port, self.config.strict_port).await;
        if inserted > 0 {
            info!("crawler: bootstrap inserted {inserted} address stub(s)");
        }
        Ok(())
    }

    /// Periodic re-resolution of the built-in DNS seeders. Always uses
    /// `Params::dns_seeders` regardless of whether `--seeder` was set at
    /// startup — `--seeder` is intentionally one-shot.
    async fn refresh_builtin_seeders(&self) {
        let addrs = dns_seed_many(self.config.network_id, self.resolver.clone()).await;
        let pulled = addrs.len();
        let default_port = self.config.network_id.default_p2p_port();
        let inserted = insert_bootstrap_stubs(&self.store, addrs, default_port, self.config.strict_port).await;
        info!("crawler: refresh: pulled {pulled} built-in seeder address(es), {inserted} new");
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
    /// hand them to the worker pool. Stops as soon as the pool back-pressures.
    async fn enqueue_probes(&self, pool: &WorkerPool) -> Result<(), Error> {
        let now = now_ms();
        let dead_cutoff = now.saturating_sub(self.config.dead_after_ms());
        let stale_good_ms = self.config.stale_good_ms();
        let stale_bad_ms = self.config.stale_bad_ms();
        let default_port = self.config.network_id.default_p2p_port();
        let strict_port = self.config.strict_port;
        let threads = self.config.threads.max(1);

        // Overfetch to absorb records filtered by `in_flight` / port / private-IP guards.
        let fetch_target = threads.saturating_mul(8);
        let candidates = self
            .store
            .blocking(move |s| s.due_for_probe(now, stale_good_ms, stale_bad_ms, dead_cutoff, fetch_target))
            .await?;
        let scanned = candidates.len();
        let mut dispatched = 0usize;
        let mut iter = candidates.into_iter();
        let mut skipped_backpressure: u64 = 0;
        let mut full = false;
        for rec in iter.by_ref() {
            if !is_acceptable_address(&rec.address, default_port, strict_port) {
                continue;
            }
            match pool.try_enqueue(rec.address) {
                EnqueueOutcome::Accepted => {
                    let net = rec.address;
                    let addr = SocketAddr::new(net.ip, net.port);
                    if let Err(err) = self.store.blocking(move |s| s.record_attempt(&net, now)).await {
                        warn!("crawler: failed to record attempt for {addr}: {err}");
                    }
                    dispatched += 1;
                }
                EnqueueOutcome::Duplicate => {}
                EnqueueOutcome::Full => {
                    full = true;
                    break;
                }
                EnqueueOutcome::Closed => return Ok(()),
            }
        }
        if full {
            skipped_backpressure = 1 + u64::try_from(iter.count()).unwrap_or(0);
            self.metrics.record_skipped_backpressure(skipped_backpressure);
        }
        debug!(
            "crawler: probe tick (index_scanned={scanned}, dispatched={dispatched}, skipped_backpressure={skipped_backpressure}, in_flight={})",
            pool.in_flight_len()
        );
        Ok(())
    }
}

async fn insert_bootstrap_stubs(store: &PeerStore, addrs: Vec<SocketAddr>, default_port: u16, strict_port: bool) -> usize {
    let now = now_ms();
    let mut inserted = 0usize;
    for addr in addrs {
        let net = net_from(addr);
        if !is_acceptable_address(&net, default_port, strict_port) {
            debug!("crawler: rejected bootstrap address {addr}");
            continue;
        }
        match store.blocking(move |s| s.insert_stub_if_missing(&net, now)).await {
            Ok(true) => inserted += 1,
            Ok(false) => {}
            Err(err) => warn!("crawler: failed to insert bootstrap stub for {addr}: {err}"),
        }
    }
    inserted
}

async fn prune_once(store: &PeerStore, dead_after_ms: i64, dead_after: Duration) {
    let cutoff = now_ms().saturating_sub(dead_after_ms);
    debug!("crawler: prune tick (cutoff={cutoff}, dead_after={dead_after:?})");
    match store.blocking(move |s| s.prune_dead(cutoff)).await {
        Ok(n) if n > 0 => info!("crawler: pruned {n} dead peer(s)"),
        Ok(_) => debug!("crawler: prune tick removed 0 peers"),
        Err(err) => warn!("crawler: prune failed: {err}"),
    }
}
