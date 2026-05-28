use std::net::SocketAddr;
use std::time::Duration;

use kaspa_consensus_core::network::NetworkId;
use semver::Version;

#[derive(Debug, Clone)]
pub struct DnsConfig {
    pub network_id: NetworkId,
    pub dns_listen: Vec<SocketAddr>,
    pub dns_zone: String,
    pub nameserver: String,
    pub max_records: usize,
    pub queries_per_ip_per_second: u32,
    pub rate_limit_window: Duration,
    pub tcp_idle_timeout: Duration,
    /// Maximum age of `last_success_ms` for a peer to appear in DNS answers.
    pub stale_good: Duration,
    /// If set, only peers reporting at least this protocol version are returned.
    pub min_protocol_version: Option<u32>,
    /// If set, only peers whose parsed kaspad semver is >= this are returned.
    pub min_user_agent: Option<Version>,
}

impl DnsConfig {
    #[must_use]
    pub fn new(network_id: NetworkId, dns_listen: Vec<SocketAddr>, dns_zone: String, nameserver: String) -> Self {
        Self {
            network_id,
            dns_listen,
            dns_zone,
            nameserver,
            max_records: 10,
            queries_per_ip_per_second: 1,
            rate_limit_window: Duration::from_secs(5),
            tcp_idle_timeout: Duration::from_secs(5),
            stale_good: Duration::from_mins(15),
            min_protocol_version: None,
            min_user_agent: None,
        }
    }
}
