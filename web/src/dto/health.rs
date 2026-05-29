//! `/health` response DTO.

use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub good: Option<u64>,
    pub total: u64,
    pub service: String,
    pub version: String,
}
