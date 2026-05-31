//! Startup gate for networks without bundled DNS seeders.
//!
//! Only networks whose `Params::dns_seeders` is non-empty in the linked
//! rusty-kaspa version can self-bootstrap. Anything else (devnet, simnet,
//! unknown testnet suffixes) must be bootstrapped via `--seeder IP:port`.
//! For those unknown networks the seeder's port also defines the network's
//! default p2p port (used for DNS responses and crawl discovery). For
//! built-in networks the default port always comes from `Params`.

use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::{Result, anyhow};
use kaspa_consensus_core::config::params::Params;
use kaspa_consensus_core::network::NetworkId;

fn supported_networks() -> impl Iterator<Item = NetworkId> {
    NetworkId::iter().filter(|n| {
        let params: Params = (*n).into();
        !params.dns_seeders.is_empty()
    })
}

fn is_supported(network_id: NetworkId) -> bool {
    supported_networks().any(|n| n == network_id)
}

/// Validate that, when supplied, `--seeder` is a literal `IP:port` (IPv4 or
/// `[IPv6]:port`). Hostnames are rejected to keep behavior predictable.
pub(crate) fn validate_seeder_format(seeder: Option<&str>) -> Result<SocketAddr> {
    let raw = seeder.ok_or_else(|| anyhow!("--seeder is required"))?;
    SocketAddr::from_str(raw.trim()).map_err(|_| anyhow!("--seeder must be a literal IP:port (IPv4 or `[IPv6]:port`); got `{raw}`"))
}

pub(crate) fn require_seeder_for_unknown_network(network_id: NetworkId, seeder: Option<&str>) -> Result<()> {
    if is_supported(network_id) {
        if seeder.is_some() {
            validate_seeder_format(seeder)?;
        }
        return Ok(());
    }
    if seeder.is_none() {
        let builtins = supported_networks().map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
        return Err(anyhow!(
            "network `{network_id}` has no built-in DNS seeders; pass --seeder IP:port to bootstrap. Built-in networks: {builtins}"
        ));
    }
    validate_seeder_format(seeder)?;
    Ok(())
}

/// Effective default p2p port for the configured network.
///
/// - Built-in networks (those with non-empty `Params::dns_seeders`) always use
///   `NetworkId::default_p2p_port()`. A `--seeder` peer's port is unrelated
///   to the network default and is treated like any non-standard-port node.
/// - Unknown networks derive their default port from the mandatory
///   `--seeder IP:port`.
pub(crate) fn effective_default_port(network_id: NetworkId, seeder: Option<&str>) -> u16 {
    if is_supported(network_id) {
        return network_id.default_p2p_port();
    }
    match validate_seeder_format(seeder) {
        Ok(addr) => addr.port(),
        Err(_) => network_id.default_p2p_port(),
    }
}
