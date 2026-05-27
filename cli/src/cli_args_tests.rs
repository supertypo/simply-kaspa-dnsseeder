use super::cli_args::CliArgs;
use clap::Parser;
use std::time::Duration;

fn parse(args: &[&str]) -> CliArgs {
    let mut full = vec!["simply-kaspa-dnsseeder"];
    full.extend_from_slice(args);
    CliArgs::try_parse_from(full).expect("parse")
}

#[test]
fn parses_required_network_id() {
    let cli = parse(&["--network-id", "kaspa-mainnet"]);
    assert_eq!(cli.network_id, "kaspa-mainnet");
}

#[test]
fn defaults_match_spec() {
    let cli = parse(&["--network-id", "kaspa-mainnet"]);
    assert_eq!(cli.threads, 8);
    assert_eq!(cli.probe_timeout, Duration::from_secs(10));
    assert_eq!(cli.handshake_timeout, Duration::from_secs(5));
    assert_eq!(cli.addresses_timeout, Duration::from_secs(5));
    assert_eq!(cli.crawl_interval, Duration::from_secs(15 * 60));
    assert_eq!(cli.dead_after, Duration::from_secs(7 * 24 * 60 * 60));
    assert_eq!(cli.dns_listen, "0.0.0.0:53");
    assert_eq!(cli.http_listen, "127.0.0.1:8080");
    assert_eq!(cli.post_rate_limit, 5);
    assert_eq!(cli.rate_limit_window, Duration::from_secs(60));
    assert!(cli.api_key.is_none());
    assert!(cli.allowed_origins.is_empty());
    assert!(!cli.dns_enabled());
}

#[test]
fn dns_enabled_only_when_both_set() {
    let cli = parse(&["--network-id", "kaspa-mainnet", "--dns-host", "seed.test"]);
    assert!(!cli.dns_enabled());
    let cli = parse(&[
        "--network-id",
        "kaspa-mainnet",
        "--dns-host",
        "seed.test",
        "--dns-nameserver",
        "ns.test",
    ]);
    assert!(cli.dns_enabled());
}

#[test]
fn humantime_durations_parse() {
    let cli = parse(&[
        "--network-id",
        "kaspa-mainnet",
        "--crawl-interval",
        "1h30m",
        "--dead-after",
        "30d",
    ]);
    assert_eq!(cli.crawl_interval, Duration::from_secs(90 * 60));
    assert_eq!(cli.dead_after, Duration::from_secs(30 * 24 * 60 * 60));
}

#[test]
fn min_user_agent_parses_semver() {
    let cli = parse(&["--network-id", "kaspa-mainnet", "--min-user-agent", "1.2.3"]);
    let v = cli.min_user_agent.expect("min_user_agent");
    assert_eq!(v.major, 1);
    assert_eq!(v.minor, 2);
    assert_eq!(v.patch, 3);
}

#[test]
fn min_user_agent_rejects_garbage() {
    assert!(CliArgs::try_parse_from(["simply-kaspa-dnsseeder", "--network-id", "kaspa-mainnet", "--min-user-agent", "not-a-version"]).is_err());
}

#[test]
fn known_peers_and_origins_parse_csv() {
    let cli = parse(&[
        "--network-id",
        "kaspa-mainnet",
        "--known-peers",
        "1.2.3.4:16111,5.6.7.8:16111",
        "--allowed-origins",
        "http://a,http://b",
    ]);
    assert_eq!(cli.known_peers, vec!["1.2.3.4:16111", "5.6.7.8:16111"]);
    assert_eq!(cli.allowed_origins, vec!["http://a", "http://b"]);
}
