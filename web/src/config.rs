use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use semver::Version;

/// Configuration for the HTTP server. None of these values are mutable at
/// runtime: a fresh config means a fresh [`crate::AppState`].
#[derive(Debug, Clone)]
pub struct WebConfig {
    pub listen: SocketAddr,
    /// When `Some(key)`, IP addresses are stripped from responses unless the
    /// caller presents a matching `X-API-KEY` header. `POST /peers` is also
    /// gated by this key when present.
    pub api_key: Option<String>,
    /// Allow-list checked against the `Origin` header for `POST /peers`. An
    /// empty list permits all origins.
    pub allowed_origins: Vec<String>,
    /// Maximum `POST /peers` requests per `rate_limit_window` per client IP.
    pub post_rate_limit: u32,
    pub rate_limit_window: Duration,
    /// The network's default P2P port. Used to validate `POST /peers`
    /// submissions when `strict_port` is set.
    pub network_default_port: u16,
    /// When true, `POST /peers` rejects any address whose port isn't `network_default_port`.
    pub strict_port: bool,
    /// URL prefix prepended to every route (empty string serves at the root).
    pub api_prefix: String,
    /// Path to the on-disk peer store, used for `db_size` metric.
    pub db_path: PathBuf,
    /// Stale-good window used by `/health` and `/metrics` to count "good" peers.
    pub stale_good: Duration,
    /// Minimum kaspad protocol version peers must advertise to appear in
    /// `GET /peers` (mirrors the DNS filter). `?all=true` bypasses this.
    pub min_protocol_version: Option<u32>,
    /// Minimum kaspad semver peers must advertise to appear in `GET /peers`
    /// (mirrors the DNS filter). `?all=true` bypasses this.
    pub min_user_agent: Option<Version>,
    /// Process name reported by `/metrics`.
    pub service_name: &'static str,
    /// Service version reported by `/metrics` and `/health`.
    pub service_version: &'static str,
    /// Git short SHA reported by `/metrics`.
    pub service_commit: &'static str,
    /// Network id label reported by `/metrics`.
    pub service_network: String,
}
