//! DNS bootstrap: parallel resolution of the built-in `Params::dns_seeders` list.

use std::net::SocketAddr;

use async_trait::async_trait;
use kaspa_consensus_core::config::params::Params;
use kaspa_consensus_core::network::NetworkId;
use log::{info, warn};
use tokio::task::JoinSet;

/// Abstract DNS lookup so tests can run offline.
#[async_trait]
pub trait Resolver: Send + Sync {
    async fn lookup(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>>;
}

/// Default resolver delegating to `tokio::net::lookup_host`.
pub struct TokioResolver;

#[async_trait]
impl Resolver for TokioResolver {
    async fn lookup(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
        Ok(addrs)
    }
}

/// Resolve every seeder in `Params::dns_seeders` in parallel, port-tagged with
/// `NetworkId::default_p2p_port`. Failures from individual seeders are logged
/// but never abort the bootstrap.
pub async fn dns_seed_many<R: Resolver + ?Sized + 'static>(network_id: NetworkId, resolver: std::sync::Arc<R>) -> Vec<SocketAddr> {
    let params: Params = network_id.into();
    let port = network_id.default_p2p_port();
    let seeders: Vec<&'static str> = params.dns_seeders.to_vec();

    if seeders.is_empty() {
        warn!("crawler: no dns seeders configured for network {network_id}");
        return Vec::new();
    }

    let mut joins = JoinSet::new();
    for host in seeders {
        let resolver = resolver.clone();
        joins.spawn(async move {
            match resolver.lookup(host, port).await {
                Ok(list) => {
                    info!("crawler: dns seeder {host} resolved to {} address(es)", list.len());
                    list
                }
                Err(err) => {
                    warn!("crawler: dns seeder {host} failed: {err}");
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
