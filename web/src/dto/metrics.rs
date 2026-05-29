//! `/metrics` response DTOs.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::system::{DiskInfo, ProcessInfo};

/// Map of subsystem name → already-serialized JSON DTO. Each `Value` is
/// produced via `serde_json::to_value(<typed dto>)` by a
/// [`MetricsSource`](crate::metrics_source::MetricsSource) implementation.
/// `BTreeMap` so output order is deterministic.
pub type SubsystemMap = BTreeMap<String, Value>;

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricsResponse {
    pub service: ServiceInfo,
    pub process: ProcessInfo,
    pub disk: DiskInfo,
    pub peers: PeerCounts,
    #[schema(value_type = Object)]
    pub subsystems: SubsystemMap,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    pub commit: String,
    pub network: String,
    pub uptime_secs: u64,
    #[schema(value_type = u64)]
    pub uptime_ms: u128,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PeerCounts {
    pub total: u64,
    pub by_status: PeerStatusCounts,
    pub by_family: PeerFamilyCounts,
    pub avg_success_age_ms: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PeerStatusCounts {
    pub good: u64,
    pub filtered: u64,
    pub stale: u64,
    pub failed: u64,
    pub stub: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PeerFamilyCounts {
    pub v4: u64,
    pub v6: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebSubsystem {
    pub requests: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub post_rejected: PostRejected,
    pub rate_limiter: RateLimiterSubsystem,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PostRejected {
    pub auth: u64,
    pub rate_limit: u64,
    pub format: u64,
    pub unroutable: u64,
    pub probe: u64,
}

/// Per-IP token-bucket gauges. Invariant: `denied ≤ ops`.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RateLimiterSubsystem {
    pub capacity: u32,
    pub window_ms: u64,
    pub ops: u64,
    pub tracked_ips: usize,
    pub denied: u64,
}
