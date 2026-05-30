//! Subsystem-specific metric shapes contributed by the binary via
//! [`MetricsSource`](crate::metrics::source::MetricsSource). Each value lives
//! under a named key in `MetricsResponse::subsystems`.

use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CrawlerSubsystem {
    pub ok: u64,
    pub failed: u64,
    pub in_flight: u64,
    pub failed_connect: u64,
    pub failed_handshake: u64,
    pub failed_addresses: u64,
    pub failed_timeout: u64,
    pub failed_too_many_addresses: u64,
    pub probes_skipped_backpressure: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DnsSubsystem {
    pub answered: u64,
    pub empty: u64,
    pub refused: u64,
    pub a: u64,
    pub aaaa: u64,
    pub rate_limiter: DnsRateLimiterSubsystem,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DnsRateLimiterSubsystem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracked_ips: Option<usize>,
    pub denied: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServingCacheSubsystem {
    pub v4_size: usize,
    pub v6_size: usize,
    pub last_refresh_ms: i64,
    pub last_refresh_age_ms: i64,
}
