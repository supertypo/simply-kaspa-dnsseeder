use std::net::SocketAddr;
use std::time::Duration;

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
    /// The network's default P2P port, used to compute the `defaultPort` flag
    /// on outgoing peer DTOs and to validate `POST /peers` when `strict_port` is set.
    pub network_default_port: u16,
    /// When true, `POST /peers` rejects any address whose port isn't `network_default_port`.
    pub strict_port: bool,
}
