//! Concurrent scheduler driving peer probes.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use kaspa_consensus_core::network::NetworkId;
use log::{debug, info, warn};
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
use tokio::sync::{Semaphore, broadcast, mpsc};

use crate::error::Error;
use crate::model::{ProbeResult, canonicalize_ip, now_ms, peer_id_from_bytes, peer_record_from_version};
use crate::probe::Probe;
use crate::seeders::{Resolver, dns_seed_many};

/// Static configuration for the scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub network_id: NetworkId,
    pub threads: usize,
    pub crawl_interval: Duration,
    pub dead_after: Duration,
    /// Explicit DNS seeder hosts (`--seeder`), tried at bootstrap if non-empty.
    pub seeders: Vec<String>,
    /// Explicit peer socket addresses (`--known-peers`) enqueued at startup.
    pub known_peers: Vec<SocketAddr>,
}

impl SchedulerConfig {
    fn dead_after_ms(&self) -> i64 {
        i64::try_from(self.dead_after.as_millis()).unwrap_or(i64::MAX)
    }

    fn crawl_interval_ms(&self) -> i64 {
        i64::try_from(self.crawl_interval.as_millis()).unwrap_or(i64::MAX)
    }
}

/// Cheap handle for outside-the-loop enqueuers (HTTP POST submissions).
#[derive(Clone)]
pub struct SchedulerHandle {
    tx: mpsc::Sender<SocketAddr>,
    in_flight: Arc<DashSet<SocketAddr>>,
}

impl SchedulerHandle {
    /// Attempt to enqueue an address. Returns `true` if accepted, `false` if
    /// already in flight or the channel is closed.
    pub async fn enqueue(&self, addr: SocketAddr) -> bool {
        if !self.in_flight.insert(addr) {
            return false;
        }
        match self.tx.send(addr).await {
            Ok(()) => true,
            Err(_) => {
                self.in_flight.remove(&addr);
                false
            }
        }
    }
}

pub struct Scheduler {
    config: SchedulerConfig,
    store: PeerStore,
    probe: Arc<dyn Probe>,
    resolver: Arc<dyn Resolver>,
    tx: mpsc::Sender<SocketAddr>,
    rx: mpsc::Receiver<SocketAddr>,
    in_flight: Arc<DashSet<SocketAddr>>,
}

impl Scheduler {
    #[must_use]
    pub fn new(config: SchedulerConfig, store: PeerStore, probe: Arc<dyn Probe>, resolver: Arc<dyn Resolver>) -> Self {
        // Queue capacity tuned to comfortably absorb a full re-probe sweep
        // without blocking the ticker even on a small `--threads` setting.
        let (tx, rx) = mpsc::channel(8192);
        Self { config, store, probe, resolver, tx, rx, in_flight: Arc::new(DashSet::new()) }
    }

    #[must_use]
    pub fn handle(&self) -> SchedulerHandle {
        SchedulerHandle { tx: self.tx.clone(), in_flight: self.in_flight.clone() }
    }

    /// Run the scheduler. Returns when `shutdown` fires.
    pub async fn run(mut self, mut shutdown: broadcast::Receiver<()>) -> Result<(), Error> {
        self.bootstrap().await?;

        let semaphore = Arc::new(Semaphore::new(self.config.threads.max(1)));
        let mut reprobe_ticker = tokio::time::interval(self.config.crawl_interval);
        reprobe_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick fires immediately; we already bootstrapped, so skip it.
        reprobe_ticker.tick().await;
        let mut prune_ticker = tokio::time::interval(self.config.crawl_interval);
        prune_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        prune_ticker.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("crawler: shutdown signal received");
                    break;
                }
                _ = reprobe_ticker.tick() => {
                    if let Err(err) = self.enqueue_reprobes().await {
                        warn!("crawler: re-probe enqueue failed: {err}");
                    }
                }
                _ = prune_ticker.tick() => {
                    let cutoff = now_ms().saturating_sub(self.config.dead_after_ms());
                    match self.store.prune_dead(cutoff) {
                        Ok(n) if n > 0 => info!("crawler: pruned {n} dead peer(s)"),
                        Ok(_) => {}
                        Err(err) => warn!("crawler: prune failed: {err}"),
                    }
                }
                maybe = self.rx.recv() => {
                    let Some(addr) = maybe else { break };
                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => break,
                    };
                    let probe = self.probe.clone();
                    let store = self.store.clone();
                    let in_flight = self.in_flight.clone();
                    let tx = self.tx.clone();
                    let default_port = self.config.network_id.default_p2p_port();
                    let dead_after_ms = self.config.dead_after_ms();
                    tokio::spawn(async move {
                        let _permit = permit;
                        Self::probe_one(probe.as_ref(), &store, &tx, &in_flight, addr, default_port, dead_after_ms).await;
                        in_flight.remove(&addr);
                    });
                }
            }
        }
        Ok(())
    }

    async fn bootstrap(&self) -> Result<(), Error> {
        for addr in &self.config.known_peers {
            if self.in_flight.insert(*addr) {
                let _ = self.tx.send(*addr).await;
            }
        }

        let store_empty = self.store.is_empty()?;
        if !store_empty {
            debug!("crawler: store non-empty, skipping DNS bootstrap");
            return Ok(());
        }

        if !self.config.known_peers.is_empty() {
            debug!("crawler: --known-peers supplied, skipping DNS bootstrap");
            return Ok(());
        }

        let bootstrap_addrs = if self.config.seeders.is_empty() {
            info!("crawler: bootstrapping from built-in dns seeders for network {}", self.config.network_id);
            dns_seed_many(self.config.network_id, self.resolver.clone()).await
        } else {
            info!("crawler: bootstrapping from --seeder hosts: {:?}", self.config.seeders);
            self.resolve_explicit_seeders().await
        };

        for addr in bootstrap_addrs {
            if self.in_flight.insert(addr) {
                let _ = self.tx.send(addr).await;
            }
        }
        Ok(())
    }

    async fn resolve_explicit_seeders(&self) -> Vec<SocketAddr> {
        let port = self.config.network_id.default_p2p_port();
        let mut out = Vec::new();
        for host in &self.config.seeders {
            match self.resolver.lookup(host, port).await {
                Ok(list) => out.extend(list),
                Err(err) => warn!("--seeder {host} failed: {err}"),
            }
        }
        out
    }

    async fn enqueue_reprobes(&self) -> Result<(), Error> {
        let now = now_ms();
        let interval_ms = self.config.crawl_interval_ms();
        let dead_ms = self.config.dead_after_ms();
        let records = self.store.iter_all()?;
        let mut count = 0;
        for rec in records {
            if now.saturating_sub(rec.last_seen_ms) >= dead_ms {
                continue;
            }
            if now.saturating_sub(rec.last_attempt_ms) < interval_ms {
                continue;
            }
            let addr = SocketAddr::new(rec.address.ip, rec.address.port);
            if !self.in_flight.insert(addr) {
                continue;
            }
            if self.tx.send(addr).await.is_err() {
                self.in_flight.remove(&addr);
                break;
            }
            count += 1;
        }
        if count > 0 {
            debug!("crawler: enqueued {count} address(es) for re-probe");
        }
        Ok(())
    }

    /// Probe a single peer, apply the outcome to the store, and enqueue any
    /// freshly discovered addresses for further crawling.
    pub(crate) async fn probe_one(
        probe: &dyn Probe,
        store: &PeerStore,
        tx: &mpsc::Sender<SocketAddr>,
        in_flight: &DashSet<SocketAddr>,
        addr: SocketAddr,
        default_port: u16,
        _dead_after_ms: i64,
    ) {
        match probe.probe(addr).await {
            Ok(result) => {
                if let Err(err) = apply_success(store, addr, &result) {
                    warn!("crawler: failed to persist successful probe of {addr}: {err}");
                }
                let discovered = result.addresses.len();
                let mut enqueued = 0usize;
                for (ip_addr, port) in &result.addresses {
                    let port = if *port == 0 { default_port } else { *port };
                    let ip: std::net::IpAddr = (*ip_addr).into();
                    let canonical = canonicalize_ip(ip);
                    if !is_routable(canonical) {
                        continue;
                    }
                    let new_addr = SocketAddr::new(canonical, port);
                    if !in_flight.insert(new_addr) {
                        continue;
                    }
                    if tx.try_send(new_addr).is_err() {
                        in_flight.remove(&new_addr);
                        break;
                    }
                    enqueued += 1;
                }
                if discovered > 0 {
                    debug!("crawler: {addr} advertised {discovered} address(es), enqueued {enqueued} new");
                }
            }
            Err(err) => {
                debug!("crawler: probe {addr} failed: {err}");
                if let Err(err) = bump_attempt(store, addr) {
                    warn!("crawler: failed to bump last_attempt for {addr}: {err}");
                }
            }
        }
    }
}

pub(crate) fn is_routable(ip: IpAddr) -> bool {
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return false;
    }
    match ip {
        IpAddr::V4(v4) => !(v4.is_private() || v4.is_link_local() || v4.is_broadcast() || v4.is_documentation()),
        IpAddr::V6(v6) => {
            let seg = v6.segments()[0];
            // fc00::/7 unique local, fe80::/10 link-local
            !((seg & 0xfe00) == 0xfc00 || (seg & 0xffc0) == 0xfe80)
        }
    }
}

fn apply_success(store: &PeerStore, addr: SocketAddr, result: &ProbeResult) -> Result<(), simply_kaspa_dnsseeder_store::Error> {
    let id = peer_id_from_bytes(&result.version.id);
    let existing = store.get(&id)?;
    let record = peer_record_from_version(addr, &result.version, now_ms(), existing.as_ref());
    store.upsert(&record)
}

fn bump_attempt(store: &PeerStore, addr: SocketAddr) -> Result<(), simply_kaspa_dnsseeder_store::Error> {
    let canonical = canonicalize_ip(addr.ip());
    let net = NetAddress { ip: canonical, port: addr.port() };
    let now = now_ms();
    let records = store.iter_all()?;
    for mut rec in records {
        if rec.address == net {
            rec.last_attempt_ms = now;
            return store.upsert(&rec);
        }
    }
    Ok(())
}

// Allow the web crate to issue ad-hoc submissions through the same code path.
impl Scheduler {
    /// Run a single probe synchronously (used by HTTP submissions). Errors
    /// from the probe are returned to the caller; storage errors are logged
    /// and surfaced as an `Err` too.
    pub async fn probe_and_store(
        probe: &dyn Probe,
        store: &PeerStore,
        addr: SocketAddr,
    ) -> Result<PeerRecord, crate::error::ProbeError> {
        match probe.probe(addr).await {
            Ok(result) => {
                let id = peer_id_from_bytes(&result.version.id);
                let existing = store.get(&id).map_err(|e| crate::error::ProbeError::Connection(e.to_string()))?;
                let record = peer_record_from_version(addr, &result.version, now_ms(), existing.as_ref());
                store.upsert(&record).map_err(|e| crate::error::ProbeError::Connection(e.to_string()))?;
                Ok(record)
            }
            Err(err) => {
                // Best-effort bump of last_attempt for an existing entry; ignore failure.
                let _ = bump_attempt(store, addr);
                Err(err)
            }
        }
    }
}

#[allow(dead_code)]
fn _ip_addr_marker(_: IpAddr) {}
