//! Process and disk-info DTOs surfaced via `/metrics`.

use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProcessInfo {
    pub cpu_used_percent: f32,
    pub memory_used_bytes: u64,
    pub memory_used_pretty: String,
    pub memory_free_bytes: u64,
    pub memory_free_pretty: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiskInfo {
    pub db_path: String,
    pub db_size_bytes: u64,
    pub db_size_pretty: String,
    pub mount_point: String,
    pub total_bytes: u64,
    pub total_pretty: String,
    pub free_bytes: u64,
    pub free_pretty: String,
}
