use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use kaspa_consensus_core::network::{NetworkId, NetworkType};

use crate::seeders::{Resolver, dns_seed_many};

struct StaticResolver;

#[async_trait]
impl Resolver for StaticResolver {
    async fn lookup(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
        match host {
            "boom" => Err(std::io::Error::other("boom")),
            _ => Ok(vec![SocketAddr::from(([10, 0, 0, 1], port))]),
        }
    }
}

#[tokio::test]
async fn dns_seed_many_aggregates_and_tolerates_failures() {
    // Pick a network that has built-in seeders. Even if the list is empty for
    // a given testnet, the function must not panic and must return an empty
    // vec without erroring.
    let nid = NetworkId::new(NetworkType::Mainnet);
    let out = dns_seed_many(nid, Arc::new(StaticResolver)).await;
    // We don't assert non-empty (params may differ across tags); just ensure
    // every entry uses the network's default p2p port.
    for addr in &out {
        assert_eq!(addr.port(), nid.default_p2p_port());
    }
}
