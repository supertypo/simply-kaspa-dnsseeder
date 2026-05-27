use std::net::SocketAddr;
use std::time::Duration;

use kaspa_consensus_core::network::NetworkId;

#[derive(Debug, Clone)]
pub struct DnsConfig {
    pub network_id: NetworkId,
    pub dns_listen: SocketAddr,
    pub dns_host: String,
    pub nameserver: String,
    pub max_records: usize,
    pub ttl_seconds: u32,
    pub queries_per_ip_per_second: u32,
    pub rate_limit_window: Duration,
    pub tcp_idle_timeout: Duration,
}

impl DnsConfig {
    #[must_use]
    pub fn new(network_id: NetworkId, dns_listen: SocketAddr, dns_host: String, nameserver: String) -> Self {
        Self {
            network_id,
            dns_listen,
            dns_host,
            nameserver,
            max_records: 10,
            ttl_seconds: 60,
            queries_per_ip_per_second: 1,
            rate_limit_window: Duration::from_secs(5),
            tcp_idle_timeout: Duration::from_secs(5),
        }
    }
}
