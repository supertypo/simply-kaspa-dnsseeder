//! Tiny shared utilities used by every crate in the workspace.
//!
//! Keep this crate dependency-light so it can be pulled in from anywhere
//! without dragging redb, axum, etc.

mod rate_limit;
mod time;

pub use rate_limit::RateLimiter;
pub use time::{canonicalize_ip, duration_to_ms, now_ms};
