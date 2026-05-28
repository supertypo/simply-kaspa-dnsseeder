//! Command-line argument parsing.
//!
//! Top-level [`CliArgs`] holds only truly global options (`--network-id`,
//! `--datadir`, `--stats-interval`) and composes four subsystem groups via
//! clap's `#[command(flatten)]`:
//! * [`CrawlerArgs`] — probe scheduling and store cadence.
//! * [`DnsArgs`] — authoritative DNS server.
//! * [`HttpArgs`] — HTTP/management API.
//! * [`LoggingArgs`] — logger configuration.
//!
//! Each subsystem's bootstrap code in `main.rs` reads only its own group.

use clap::{Args, Parser};
use semver::Version;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Parser, Clone, Debug)]
#[command(name = "simply-kaspa-dnsseeder", version = env!("VERGEN_GIT_DESCRIBE"), about = "Kaspa DNS seeder")]
pub struct CliArgs {
    /// Network identifier (e.g. `kaspa-mainnet`, `kaspa-testnet-10`).
    #[clap(short = 'n', long)]
    pub network_id: String,

    /// Directory used for persistent storage.
    #[clap(long, default_value = "data")]
    pub datadir: String,

    /// Periodic stats dump interval. Set to `0s` to disable.
    #[clap(long, default_value = "1m", value_parser = humantime::parse_duration)]
    pub stats_interval: Duration,

    #[command(flatten)]
    pub crawler: CrawlerArgs,

    #[command(flatten)]
    pub dns: DnsArgs,

    #[command(flatten)]
    pub http: HttpArgs,

    #[command(flatten)]
    pub logging: LoggingArgs,
}

/// Crawler / probe scheduling options.
#[derive(Args, Clone, Debug)]
pub struct CrawlerArgs {
    /// Optional bootstrap node `host:port`.
    #[clap(short = 's', long)]
    pub seeder: Option<String>,

    /// Number of concurrent probe workers.
    #[clap(long, default_value = "8", value_parser = parse_positive_usize)]
    pub threads: usize,

    /// Maximum total duration of a single probe (connect + handshake + addresses).
    #[clap(long, default_value = "8s", value_parser = humantime::parse_duration)]
    pub probe_timeout: Duration,

    /// Interval between probe scheduling ticks.
    #[clap(long, default_value = "10s", value_parser = humantime::parse_duration)]
    pub probe_tick: Duration,

    /// Re-probe interval for peers that have succeeded at least once.
    #[clap(long, default_value = "30m", value_parser = humantime::parse_duration)]
    pub stale_good: Duration,

    /// Re-probe interval for peers that have never succeeded.
    #[clap(long, default_value = "2h", value_parser = humantime::parse_duration)]
    pub stale_bad: Duration,

    /// A peer is removed when `now - last_seen` exceeds this duration.
    #[clap(long, default_value = "7d", value_parser = humantime::parse_duration)]
    pub dead_after: Duration,

    /// Only accept addresses whose port matches the network's default P2P port.
    #[clap(long)]
    pub strict_port: bool,
}

/// DNS server options. The server is enabled iff both `--dns-zone` and `--dns-nameserver` are set.
#[derive(Args, Clone, Debug)]
pub struct DnsArgs {
    /// Authoritative DNS zone (FQDN apex) the seeder answers for.
    #[clap(long)]
    pub dns_zone: Option<String>,

    /// Nameserver FQDN returned for NS queries.
    #[clap(long)]
    pub dns_nameserver: Option<String>,

    /// DNS server bind addresses (comma-separated). Defaults to dual-stack.
    #[clap(long, default_value = "0.0.0.0:53,[::]:53", value_delimiter = ',', num_args = 1..)]
    pub dns_listen: Vec<SocketAddr>,

    /// Minimum protocol version accepted in DNS responses (optional).
    #[clap(long)]
    pub min_protocol_version: Option<u32>,

    /// Minimum kaspad semver accepted in DNS responses (optional, e.g. `1.1.0`).
    #[clap(long, value_parser = parse_semver)]
    pub min_user_agent: Option<Version>,
}

/// HTTP server options.
#[derive(Args, Clone, Debug)]
pub struct HttpArgs {
    /// HTTP server bind address.
    #[clap(long, default_value = "127.0.0.1:8080")]
    pub http_listen: String,

    /// URL prefix for all HTTP endpoints (e.g. `/api`). Use `""` to serve at the root.
    #[clap(long, default_value = "/api")]
    pub api_prefix: String,

    /// Optional API key. When set, `POST /peers` requires it and `GET /peers` exposes `ip` only when the request matches.
    #[clap(long)]
    pub api_key: Option<String>,

    /// Comma-separated allowed CORS origins. When empty, any origin is accepted.
    #[clap(long, value_delimiter = ',')]
    pub allowed_origins: Vec<String>,

    /// `POST /peers` requests per `--rate-limit-window` per source IP.
    #[clap(long, default_value = "5")]
    pub post_rate_limit: u32,

    /// Window length for `--post-rate-limit`.
    #[clap(long, default_value = "1m", value_parser = humantime::parse_duration)]
    pub rate_limit_window: Duration,
}

/// Logging options.
#[derive(Args, Clone, Debug)]
pub struct LoggingArgs {
    /// `env_logger` filter (e.g. `info`, `debug`, `simply_kaspa_dnsseeder=trace,info`).
    #[clap(long, default_value = "warn,simply_kaspa_dnsseeder=info")]
    pub log_level: String,

    /// Disable colored stdout output.
    #[clap(long)]
    pub log_no_color: bool,
}

impl CliArgs {
    /// Build-time version string (from `vergen`).
    #[must_use]
    pub fn version() -> &'static str {
        env!("VERGEN_GIT_DESCRIBE")
    }

    /// Build-time commit SHA (from `vergen`).
    #[must_use]
    pub fn commit_id() -> &'static str {
        env!("VERGEN_GIT_SHA")
    }

    /// Returns true iff the DNS server should be started.
    #[must_use]
    pub fn dns_enabled(&self) -> bool {
        self.dns.dns_zone.is_some() && self.dns.dns_nameserver.is_some()
    }
}

fn parse_semver(s: &str) -> Result<Version, String> {
    Version::parse(s).map_err(|e| format!("invalid semver `{s}`: {e}"))
}

fn parse_positive_usize(s: &str) -> Result<usize, String> {
    match s.parse::<usize>() {
        Ok(0) => Err("value must be >= 1".to_string()),
        Ok(n) => Ok(n),
        Err(e) => Err(format!("invalid integer `{s}`: {e}")),
    }
}
