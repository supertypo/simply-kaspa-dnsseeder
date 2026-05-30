//! Pluggable hook that lets the binary inject subsystem-specific JSON into
//! `/metrics`. The web crate cannot depend on the crawler or dns crates
//! directly without pulling those crates into its dependency graph, so we
//! accept a trait object instead.
//!
//! Implementations MUST populate each value via `serde_json::to_value` of a
//! typed DTO (see [`crate::dto::subsystems`]). The map outer container is
//! heterogeneous because different binaries contribute different subsystems.

use crate::dto::SubsystemMap;

pub trait MetricsSource: Send + Sync + 'static {
    /// Returns a map merged into the `/metrics` response under the top-level
    /// `subsystems` key. Each value should be the JSON form of a typed DTO.
    fn extra(&self) -> SubsystemMap;
}

/// No-op default: contributes an empty map.
pub struct NullMetricsSource;

impl MetricsSource for NullMetricsSource {
    fn extra(&self) -> SubsystemMap {
        SubsystemMap::new()
    }
}
