//! DNS bootstrap: parallel resolution of the built-in `Params::dns_seeders` list.

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use kaspa_consensus_core::config::params::Params;
use kaspa_consensus_core::network::NetworkId;
use log::{info, warn};
use tokio::task::JoinSet;

/// Per-seeder DNS lookup timeout. Without this, a single non-responsive
/// upstream resolver hangs bootstrap and blocks the scheduler from starting.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Abstract DNS lookup so tests can run offline.
#[async_trait]
pub trait Resolver: Send + Sync {
    async fn lookup(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>>;
}

/// Tokio-based DNS resolver.
pub struct TokioResolver;

#[async_trait]
impl Resolver for TokioResolver {
    async fn lookup(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
        Ok(addrs)
    }
}

/// Resolve all configured DNS seeders in parallel. Per-seeder failures and
/// timeouts are logged but don't abort bootstrap.
pub async fn dns_seed_many<R: Resolver + ?Sized + 'static>(network_id: NetworkId, resolver: std::sync::Arc<R>) -> Vec<SocketAddr> {
    let port = network_id.default_p2p_port();
    let seeders: Vec<&'static str> = if NetworkId::iter().any(|n| n == network_id) {
        let params: Params = network_id.into();
        params.dns_seeders.to_vec()
    } else {
        Vec::new()
    };

    if seeders.is_empty() {
        warn!("crawler: no dns seeders configured for network {network_id}");
        return Vec::new();
    }

    let mut joins = JoinSet::new();
    for host in seeders {
        let resolver = resolver.clone();
        joins.spawn(async move {
            match tokio::time::timeout(LOOKUP_TIMEOUT, resolver.lookup(host, port)).await {
                Ok(Ok(list)) => {
                    info!("crawler: dns seeder {host} resolved to {} address(es)", list.len());
                    list
                }
                Ok(Err(err)) => {
                    warn!("crawler: dns seeder {host} failed: {err}");
                    Vec::new()
                }
                Err(_) => {
                    warn!("crawler: dns seeder {host} timed out after {LOOKUP_TIMEOUT:?}");
                    Vec::new()
                }
            }
        });
    }

    let mut out = Vec::new();
    while let Some(res) = joins.join_next().await {
        match res {
            Ok(v) => out.extend(v),
            Err(err) => warn!("crawler: dns seeder join error: {err}"),
        }
    }
    out
}
