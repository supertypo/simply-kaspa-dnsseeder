use super::cli_args::CliArgs;
use clap::Parser;
use std::time::Duration;

fn parse(args: &[&str]) -> CliArgs {
    let mut full = vec!["simply-kaspa-dnsseeder"];
    full.extend_from_slice(args);
    CliArgs::try_parse_from(full).expect("parse")
}

#[test]
fn network_id_defaults_to_mainnet() {
    let cli = parse(&[]);
    assert_eq!(cli.network_id, "mainnet");
    let cli = parse(&["--network-id", "testnet-10"]);
    assert_eq!(cli.network_id, "testnet-10");
}

#[test]
fn threads_zero_rejected() {
    let res = CliArgs::try_parse_from(["simply-kaspa-dnsseeder", "--network-id", "mainnet", "--threads", "0"]);
    assert!(res.is_err(), "--threads 0 must be rejected");
}

#[test]
fn probes_per_peer_bounds_enforced() {
    for bad in ["0", "11", "100"] {
        let res = CliArgs::try_parse_from(["simply-kaspa-dnsseeder", "--network-id", "mainnet", "--probes-per-peer", bad]);
        assert!(res.is_err(), "--probes-per-peer {bad} must be rejected");
    }
    for good in ["1", "5", "10"] {
        let cli = parse(&["--network-id", "mainnet", "--probes-per-peer", good]);
        assert_eq!(cli.crawler.probes_per_peer, good.parse::<u8>().unwrap());
    }
}

#[test]
fn strict_port_flag_toggles() {
    let cli = parse(&["--network-id", "mainnet", "--strict-port"]);
    assert!(cli.crawler.strict_port);
}

#[test]
fn dns_enabled_only_when_both_set() {
    let cli = parse(&["--network-id", "mainnet", "--dns-zone", "seed.test"]);
    assert!(!cli.dns_enabled());
    let cli = parse(&["--network-id", "mainnet", "--dns-zone", "seed.test", "--dns-nameserver", "ns.test"]);
    assert!(cli.dns_enabled());
}

#[test]
fn humantime_durations_parse() {
    let cli = parse(&[
        "--network-id",
        "mainnet",
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
    assert_eq!(cli.crawler.stale_good, Duration::from_mins(90));
    assert_eq!(cli.crawler.stale_bad, Duration::from_hours(4));
    assert_eq!(cli.crawler.dead_after, Duration::from_hours(720));
}

#[test]
fn min_user_agent_parses_semver() {
    let cli = parse(&["--network-id", "mainnet", "--min-user-agent", "1.2.3"]);
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
            "mainnet",
            "--min-user-agent",
            "not-a-version"
        ])
        .is_err()
    );
}

#[test]
fn dns_max_records_defaults_to_25() {
    let cli = parse(&[]);
    assert_eq!(cli.dns.dns_max_records, 25);
}

#[test]
fn dns_max_records_accepts_upper_bound() {
    let cli = parse(&["--dns-max-records", "100"]);
    assert_eq!(cli.dns.dns_max_records, 100);
}

#[test]
fn dns_max_records_rejects_out_of_range() {
    for bad in ["0", "101", "1000"] {
        let res = CliArgs::try_parse_from(["simply-kaspa-dnsseeder", "--dns-max-records", bad]);
        assert!(res.is_err(), "--dns-max-records {bad} must be rejected");
    }
}

#[test]
fn allowed_origins_parses_csv() {
    let cli = parse(&["--network-id", "mainnet", "--allowed-origins", "http://a,http://b"]);
    assert_eq!(cli.http.allowed_origins, vec!["http://a", "http://b"]);
}
