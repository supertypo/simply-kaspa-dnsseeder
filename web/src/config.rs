use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use semver::Version;

/// Configuration for the HTTP server. None of these values are mutable at
/// runtime: a fresh config means a fresh [`crate::AppState`].
#[derive(Debug, Clone)]
pub struct WebConfig {
    pub listen: Vec<SocketAddr>,
    /// Required `X-API-KEY` header value. IP addresses are stripped from
    /// responses unless the caller presents this header, and `POST /peers`
    /// is gated by it.
    pub api_key: String,
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
    pub service_name: &'static str,
    pub service_version: &'static str,
    pub service_commit: &'static str,
    /// Network id label reported by `/metrics`.
    pub service_network: String,
    /// PEM certificate (or full chain). When set together with `tls_key`, the
    /// server listens over HTTPS instead of plain HTTP.
    pub tls_cert: Option<PathBuf>,
    /// PEM private key (PKCS8 or PKCS1). Required iff `tls_cert` is set.
    pub tls_key: Option<PathBuf>,
}
