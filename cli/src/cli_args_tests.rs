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
    assert_eq!(cli.crawler.threads, 8);
    assert_eq!(cli.crawler.probes_per_peer, 3);
    assert_eq!(cli.crawler.probe_timeout, Duration::from_secs(4));
    assert_eq!(cli.crawler.probe_tick, Duration::from_secs(5));
    assert_eq!(cli.crawler.stale_good, Duration::from_secs(30 * 60));
    assert_eq!(cli.crawler.stale_bad, Duration::from_secs(2 * 60 * 60));
    assert_eq!(cli.crawler.dead_after, Duration::from_secs(7 * 24 * 60 * 60));
    assert_eq!(cli.dns.dns_listen, vec!["0.0.0.0:53".parse().unwrap(), "[::]:53".parse().unwrap()]);
    assert_eq!(cli.http.http_listen, "127.0.0.1:8080");
    assert_eq!(cli.http.post_rate_limit, 5);
    assert_eq!(cli.http.rate_limit_window, Duration::from_secs(60));
    assert!(cli.http.api_key.is_none());
    assert!(cli.http.allowed_origins.is_empty());
    assert!(!cli.dns_enabled());
    assert!(!cli.crawler.strict_port);
    assert_eq!(cli.datadir, "data");
    assert_eq!(cli.http.api_prefix, "/api");
    assert_eq!(cli.logging.log_level, "warn,simply_kaspa_dnsseeder=info");
    assert!(!cli.logging.log_no_color);
    assert_eq!(cli.stats_interval, Duration::from_secs(60));
    assert!(cli.dns.min_protocol_version.is_none());
    assert!(cli.dns.min_user_agent.is_none());
}

#[test]
fn threads_zero_rejected() {
    let res = CliArgs::try_parse_from(["simply-kaspa-dnsseeder", "--network-id", "kaspa-mainnet", "--threads", "0"]);
    assert!(res.is_err(), "--threads 0 must be rejected");
}

#[test]
fn probes_per_peer_defaults_to_three() {
    let cli = parse(&["--network-id", "kaspa-mainnet"]);
    assert_eq!(cli.crawler.probes_per_peer, 3);
}

#[test]
fn probes_per_peer_bounds_enforced() {
    for bad in ["0", "11", "100"] {
        let res = CliArgs::try_parse_from(["simply-kaspa-dnsseeder", "--network-id", "kaspa-mainnet", "--probes-per-peer", bad]);
        assert!(res.is_err(), "--probes-per-peer {bad} must be rejected");
    }
    for good in ["1", "5", "10"] {
        let cli = parse(&["--network-id", "kaspa-mainnet", "--probes-per-peer", good]);
        assert_eq!(cli.crawler.probes_per_peer, good.parse::<u8>().unwrap());
    }
}

#[test]
fn strict_port_flag_toggles() {
    let cli = parse(&["--network-id", "kaspa-mainnet", "--strict-port"]);
    assert!(cli.crawler.strict_port);
}

#[test]
fn dns_enabled_only_when_both_set() {
    let cli = parse(&["--network-id", "kaspa-mainnet", "--dns-zone", "seed.test"]);
    assert!(!cli.dns_enabled());
    let cli = parse(&[
        "--network-id",
        "kaspa-mainnet",
        "--dns-zone",
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
        "--probe-tick",
        "5s",
        "--stale-good",
        "1h30m",
        "--stale-bad",
        "4h",
        "--dead-after",
        "30d",
    ]);
    assert_eq!(cli.crawler.probe_tick, Duration::from_secs(5));
    assert_eq!(cli.crawler.stale_good, Duration::from_secs(90 * 60));
    assert_eq!(cli.crawler.stale_bad, Duration::from_secs(4 * 60 * 60));
    assert_eq!(cli.crawler.dead_after, Duration::from_secs(30 * 24 * 60 * 60));
}

#[test]
fn min_user_agent_parses_semver() {
    let cli = parse(&["--network-id", "kaspa-mainnet", "--min-user-agent", "1.2.3"]);
    let v = cli.dns.min_user_agent.expect("min_user_agent");
    assert_eq!(v.major, 1);
    assert_eq!(v.minor, 2);
    assert_eq!(v.patch, 3);
}

#[test]
fn min_user_agent_rejects_garbage() {
    assert!(
        CliArgs::try_parse_from([
            "simply-kaspa-dnsseeder",
            "--network-id",
            "kaspa-mainnet",
            "--min-user-agent",
            "not-a-version"
        ])
        .is_err()
    );
}

#[test]
fn allowed_origins_parses_csv() {
    let cli = parse(&["--network-id", "kaspa-mainnet", "--allowed-origins", "http://a,http://b"]);
    assert_eq!(cli.http.allowed_origins, vec!["http://a", "http://b"]);
}
