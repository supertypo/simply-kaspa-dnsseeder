//! Pluggable hook that lets the binary inject subsystem-specific JSON into
//! `/metrics`. The web crate cannot depend on the crawler or dns metric types
//! directly without pulling those crates into its dependency graph, so we
//! accept a trait object instead.

use serde_json::Value;

pub trait MetricsSource: Send + Sync + 'static {
    /// Returns a JSON object merged into the `/metrics` response under the
    /// top-level `subsystems` key.
    fn extra(&self) -> Value;
}

/// No-op default: contributes an empty object.
pub struct NullMetricsSource;

impl MetricsSource for NullMetricsSource {
    fn extra(&self) -> Value {
        serde_json::json!({})
    }
}
