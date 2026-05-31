use kaspa_consensus_core::network::{NetworkId, NetworkType};

use crate::network_gate::{effective_default_port, require_seeder_for_unknown_network};

#[test]
fn known_network_passes_without_seeder() {
    let nid = NetworkId::new(NetworkType::Mainnet);
    require_seeder_for_unknown_network(nid, None).expect("mainnet must be accepted without --seeder");
}

#[test]
fn testnet10_passes_without_seeder() {
    let nid = NetworkId::with_suffix(NetworkType::Testnet, 10);
    require_seeder_for_unknown_network(nid, None).expect("testnet-10 must be accepted without --seeder");
}

#[test]
fn devnet_without_seeder_errors() {
    let nid = NetworkId::new(NetworkType::Devnet);
    require_seeder_for_unknown_network(nid, None).expect_err("devnet has no built-in seeders");
}

#[test]
fn simnet_without_seeder_errors() {
    let nid = NetworkId::new(NetworkType::Simnet);
    require_seeder_for_unknown_network(nid, None).expect_err("simnet has no built-in seeders");
}

#[test]
fn unknown_network_without_seeder_errors_with_builtins_listed() {
    let nid = NetworkId::with_suffix(NetworkType::Testnet, 12);
    let err = require_seeder_for_unknown_network(nid, None).unwrap_err().to_string();
    assert!(err.contains("mainnet"), "error must list mainnet: {err}");
    assert!(err.contains("testnet-10"), "error must list testnet-10: {err}");
    assert!(!err.contains("devnet"), "error must not advertise devnet as built-in: {err}");
    assert!(!err.contains("simnet"), "error must not advertise simnet as built-in: {err}");
}

#[test]
fn unknown_network_with_seeder_passes() {
    let nid = NetworkId::with_suffix(NetworkType::Testnet, 12);
    require_seeder_for_unknown_network(nid, Some("1.2.3.4:16311")).expect("--seeder unblocks unknown networks");
}

#[test]
fn devnet_with_seeder_passes() {
    let nid = NetworkId::new(NetworkType::Devnet);
    require_seeder_for_unknown_network(nid, Some("1.2.3.4:16611")).expect("--seeder unblocks devnet");
}

#[test]
fn unknown_network_with_hostname_seeder_errors() {
    let nid = NetworkId::with_suffix(NetworkType::Testnet, 12);
    let err = require_seeder_for_unknown_network(nid, Some("seed.example.org:16211"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("IP:port"), "error must demand IP:port: {err}");
}

#[test]
fn known_network_with_hostname_seeder_errors() {
    let nid = NetworkId::new(NetworkType::Mainnet);
    let err = require_seeder_for_unknown_network(nid, Some("seed.example.org:16111"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("IP:port"), "error must demand IP:port: {err}");
}

#[test]
fn effective_port_built_in_uses_network_default() {
    let nid = NetworkId::new(NetworkType::Mainnet);
    assert_eq!(effective_default_port(nid, None), nid.default_p2p_port());
    // Built-in networks always use the network default; the seeder's port is irrelevant.
    assert_eq!(effective_default_port(nid, Some("1.2.3.4:9999")), nid.default_p2p_port());
}

#[test]
fn effective_port_unknown_uses_seeder_port() {
    let nid = NetworkId::with_suffix(NetworkType::Testnet, 12);
    assert_eq!(effective_default_port(nid, Some("86.48.24.208:16211")), 16211);
    assert_eq!(effective_default_port(nid, Some("[2001:db8::1]:16211")), 16211);
}
