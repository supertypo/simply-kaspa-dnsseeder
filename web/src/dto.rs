//! Public DTO surface for the HTTP API. Submodules group related response/request shapes.

pub mod api_error;
pub mod health;
pub mod metrics;
pub mod peer;
pub mod subsystems;
pub mod system;

pub use api_error::ApiErrorBody;
pub use health::HealthResponse;
pub use metrics::{
    MetricsResponse, PeerCounts, PeerFamilyCounts, PeerStatusCounts, PostRejected, RateLimiterSubsystem, ServiceInfo, SubsystemMap,
    WebSubsystem,
};
pub use peer::{FullPeerDto, PeerDto, PublicPeerDto, SubmitPeerRequest};
pub use subsystems::{CrawlerSubsystem, DnsRateLimiterSubsystem, DnsSubsystem, ServingCacheSubsystem};
pub use system::{DiskInfo, ProcessInfo};
