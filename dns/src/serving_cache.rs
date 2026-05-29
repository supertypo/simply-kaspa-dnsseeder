use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{debug, warn};
use simply_kaspa_dnsseeder_common::{duration_to_ms, now_ms};
use simply_kaspa_dnsseeder_store::{Family, Filter, PeerStore};
use tokio::task::JoinHandle;

use crate::config::DnsConfig;

pub const REFRESH_INTERVAL: Duration = Duration::from_mins(1);
pub const SNAPSHOT_MULTIPLIER: usize = 10;

/// Pre-filtered set of currently-serving peer IPs, split by family. Each slice
/// is bounded to `max_records * SNAPSHOT_MULTIPLIER` and contains the most
/// recently successful peers; the handler picks a random subset per query.
#[derive(Default)]
pub struct Snapshot {
    pub(crate) v4: Box<[IpAddr]>,
    pub(crate) v6: Box<[IpAddr]>,
}

impl Snapshot {
    #[must_use]
    pub fn v4_len(&self) -> usize {
        self.v4.len()
    }

    #[must_use]
    pub fn v6_len(&self) -> usize {
        self.v6.len()
    }
}

pub struct ServingCache {
    inner: Mutex<Arc<Snapshot>>,
    last_refresh_ms: AtomicI64,
}

impl ServingCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Arc::new(Snapshot::default())),
            last_refresh_ms: AtomicI64::new(0),
        }
    }

    pub fn load(&self) -> Arc<Snapshot> {
        self.inner.lock().expect("serving cache mutex poisoned").clone()
    }

    pub fn store(&self, snap: Arc<Snapshot>) {
        *self.inner.lock().expect("serving cache mutex poisoned") = snap;
        self.last_refresh_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// Wall-clock timestamp (ms since epoch) of the last successful refresh,
    /// or 0 if no refresh has completed yet.
    #[must_use]
    pub fn last_refresh_ms(&self) -> i64 {
        self.last_refresh_ms.load(Ordering::Relaxed)
    }
}

impl Default for ServingCache {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub(crate) fn build_snapshot(store: &PeerStore, config: &DnsConfig, p2p_port: u16, cap: usize) -> Snapshot {
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
    peers.sort_unstable_by_key(|p| std::cmp::Reverse(p.last_success_ms));
    peers.truncate(cap);
    peers.into_iter().map(|p| p.address.ip).collect::<Vec<IpAddr>>().into_boxed_slice()
}

/// Synchronously rebuild and publish a snapshot. Cheap enough for startup and tests.
pub(crate) fn refresh_now(cache: &ServingCache, store: &PeerStore, config: &DnsConfig, p2p_port: u16, cap: usize) {
    let snap = build_snapshot(store, config, p2p_port, cap);
    debug!("dns: serving cache refreshed: v4={} v6={}", snap.v4.len(), snap.v6.len());
    cache.store(Arc::new(snap));
}

/// Periodically rebuild the snapshot off the async runtime until shutdown fires.
pub(crate) fn spawn_refresher(
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use kaspa_consensus_core::network::{NetworkId, NetworkType};
    use simply_kaspa_dnsseeder_common::now_ms;
    use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
    use tempfile::TempDir;

    use super::{SNAPSHOT_MULTIPLIER, build_snapshot};
    use crate::config::DnsConfig;

    fn rec(id: u8, ip: IpAddr, last_success_ms: i64, port: u16) -> PeerRecord {
        let mut peer_id = [0u8; 16];
        peer_id[0] = id;
        PeerRecord {
            id: peer_id,
            protocol_version: 7,
            timestamp_ms: last_success_ms,
            address: NetAddress { ip, port },
            user_agent: "/kaspad:1.0.0/".to_string(),
            subnetwork_id: None,
            first_seen_ms: last_success_ms,
            last_attempt_ms: last_success_ms,
            last_success_ms,
            last_seen_ms: last_success_ms,
        }
    }

    fn cfg() -> (DnsConfig, u16) {
        let net = NetworkId::new(NetworkType::Mainnet);
        let port = net.default_p2p_port();
        let cfg = DnsConfig::new(net, vec!["127.0.0.1:0".parse().unwrap()], "seed.test.".into(), "ns.test.".into());
        (cfg, port)
    }

    #[test]
    fn empty_store_yields_empty_snapshot() {
        let temp = TempDir::new().unwrap();
        let store = PeerStore::open(temp.path().join("p.redb")).unwrap();
        let (config, port) = cfg();
        let snap = build_snapshot(&store, &config, port, 10 * SNAPSHOT_MULTIPLIER);
        assert!(snap.v4.is_empty());
        assert!(snap.v6.is_empty());
    }

    #[test]
    fn splits_by_family_and_keeps_top_n_by_freshness() {
        let temp = TempDir::new().unwrap();
        let store = PeerStore::open(temp.path().join("p.redb")).unwrap();
        let (config, port) = cfg();
        let base = now_ms();
        // Distinct, strictly ordered success timestamps so freshness ordering is unambiguous.
        for (i, offset) in [5_000, 1_000, 4_000, 2_000, 3_000].into_iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let id = i as u8 + 1;
            store
                .upsert(&rec(id, IpAddr::V4(Ipv4Addr::new(10, 0, 0, id)), base - offset, port))
                .unwrap();
        }
        store.upsert(&rec(99, IpAddr::V6(Ipv6Addr::LOCALHOST), base - 1_000, port)).unwrap();

        let snap = build_snapshot(&store, &config, port, 3);
        assert_eq!(snap.v4.len(), 3);
        assert_eq!(snap.v6.len(), 1);
        // Smallest offsets (freshest) survive truncation.
        let v4_ids: Vec<u8> = snap
            .v4
            .iter()
            .filter_map(|ip| if let IpAddr::V4(v) = ip { Some(v.octets()[3]) } else { None })
            .collect();
        assert_eq!(v4_ids, vec![2, 4, 5]);
    }

    #[test]
    fn drops_peers_on_wrong_port() {
        let temp = TempDir::new().unwrap();
        let store = PeerStore::open(temp.path().join("p.redb")).unwrap();
        let (config, port) = cfg();
        let base = now_ms();
        store
            .upsert(&rec(1, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), base - 1_000, port))
            .unwrap();
        store
            .upsert(&rec(2, IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)), base - 1_000, port + 1))
            .unwrap();
        let snap = build_snapshot(&store, &config, port, 10);
        assert_eq!(snap.v4.len(), 1);
    }
}
