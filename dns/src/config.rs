use std::net::SocketAddr;

use kaspa_consensus_core::network::NetworkId;

/// Static DNS server config. `dns_host` is the apex name we are authoritative
/// for and `nameserver` is the `NS` record we publish for that apex.
#[derive(Debug, Clone)]
pub struct DnsConfig {
    pub network_id: NetworkId,
    pub dns_listen: SocketAddr,
    pub dns_host: String,
    pub nameserver: String,
    /// Maximum address records returned per response (mirrors kaspa-dnsseeder
    /// `defaultMaxAddresses = 25`).
    pub max_records: usize,
    /// TTL for A/AAAA records in seconds.
    pub ttl_seconds: u32,
}

impl DnsConfig {
    #[must_use]
    pub fn new(network_id: NetworkId, dns_listen: SocketAddr, dns_host: String, nameserver: String) -> Self {
        Self { network_id, dns_listen, dns_host, nameserver, max_records: 25, ttl_seconds: 60 }
    }
}
