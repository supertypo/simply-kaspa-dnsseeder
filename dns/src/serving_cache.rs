use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{debug, warn};
use simply_kaspa_dnsseeder_common::{duration_to_ms, now_ms};
use simply_kaspa_dnsseeder_store::{Family, Filter, PeerStore};
use tokio::task::JoinHandle;

use crate::config::DnsConfig;

pub const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
pub const SNAPSHOT_MULTIPLIER: usize = 10;

/// Pre-filtered set of currently-serving peer IPs, split by family. Each slice
/// is bounded to `max_records * SNAPSHOT_MULTIPLIER` and ordered freshest-first
/// at build time (random access from the DNS path doesn't rely on the order).
#[derive(Default)]
pub struct Snapshot {
    pub v4: Box<[IpAddr]>,
    pub v6: Box<[IpAddr]>,
}

pub struct ServingCache {
    inner: Mutex<Arc<Snapshot>>,
}

impl ServingCache {
    #[must_use]
    pub fn new() -> Self {
        Self { inner: Mutex::new(Arc::new(Snapshot::default())) }
    }

    pub fn load(&self) -> Arc<Snapshot> {
        self.inner.lock().expect("serving cache mutex poisoned").clone()
    }

    pub fn store(&self, snap: Arc<Snapshot>) {
        *self.inner.lock().expect("serving cache mutex poisoned") = snap;
    }
}

impl Default for ServingCache {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub fn build_snapshot(store: &PeerStore, config: &DnsConfig, p2p_port: u16, cap: usize) -> Snapshot {
    Snapshot {
        v4: pick_freshest(store, config, p2p_port, Family::V4, cap),
        v6: pick_freshest(store, config, p2p_port, Family::V6, cap),
    }
}

fn pick_freshest(store: &PeerStore, config: &DnsConfig, p2p_port: u16, family: Family, cap: usize) -> Box<[IpAddr]> {
    let filter = Filter::serving(
        now_ms(),
        duration_to_ms(config.stale_good),
        config.min_protocol_version,
        config.min_user_agent.clone(),
        Some(family),
        Some(p2p_port),
    );
    let mut peers = match store.collect_matching(&filter) {
        Ok(v) => v,
        Err(err) => {
            warn!("dns: serving cache scan failed: {err}");
            return Box::new([]);
        }
    };
    peers.sort_unstable_by(|a, b| b.last_success_ms.cmp(&a.last_success_ms));
    peers.truncate(cap);
    peers.into_iter().map(|p| p.address.ip).collect::<Vec<IpAddr>>().into_boxed_slice()
}

/// Synchronously rebuild and publish a snapshot. Cheap enough for startup and tests.
pub fn refresh_now(cache: &ServingCache, store: &PeerStore, config: &DnsConfig, p2p_port: u16, cap: usize) {
    let snap = build_snapshot(store, config, p2p_port, cap);
    debug!("dns: serving cache refreshed: v4={} v6={}", snap.v4.len(), snap.v6.len());
    cache.store(Arc::new(snap));
}

/// Periodically rebuild the snapshot off the async runtime until shutdown fires.
pub fn spawn_refresher(
    cache: Arc<ServingCache>,
    store: PeerStore,
    config: Arc<DnsConfig>,
    p2p_port: u16,
    cap: usize,
    interval: Duration,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick; the caller is expected to have done a sync refresh.
        tick.tick().await;
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let store = store.clone();
                    let config = config.clone();
                    let cache = cache.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        let snap = build_snapshot(&store, &config, p2p_port, cap);
                        debug!("dns: serving cache refreshed: v4={} v6={}", snap.v4.len(), snap.v6.len());
                        cache.store(Arc::new(snap));
                    })
                    .await;
                }
                _ = shutdown.recv() => break,
            }
        }
    })
}
