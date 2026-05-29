//! `OpenAPI` document for the seeder's HTTP surface.

use utoipa::OpenApi;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};

use crate::dto::{FullPeerDto, PeerDto, PublicPeerDto};
use crate::handlers::health::HealthResponse;
use crate::handlers::metrics::{
    MetricsResponse, PeerCounts, PeerFamilyCounts, PeerStatusCounts, PostRejected, RateLimiterSubsystem, ServiceInfo, WebSubsystem,
};
use crate::system::{DiskInfo, ProcessInfo};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Simply Kaspa DNSseeder REST API",
        description = "Public seeding service for the Kaspa P2P network",
    ),
    paths(
        crate::handlers::health::handler,
        crate::handlers::metrics::handler,
        crate::handlers::peers::list,
        crate::handlers::peers::get,
        crate::handlers::peers::submit,
    ),
    components(schemas(
        HealthResponse,
        MetricsResponse,
        ServiceInfo,
        PeerCounts,
        PeerStatusCounts,
        PeerFamilyCounts,
        WebSubsystem,
        PostRejected,
        RateLimiterSubsystem,
        ProcessInfo,
        DiskInfo,
        PeerDto,
        FullPeerDto,
        PublicPeerDto,
    )),
    tags(
        (name = "info", description = "Health and metrics"),
        (name = "peers", description = "Peer list, lookup, and submission"),
    ),
    modifiers(&SecurityAddon),
)]
pub(crate) struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme("api_key", SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("x-api-key"))));
    }
}

pub(crate) fn document(base_path: &str) -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    if !base_path.is_empty() {
        let prefixed = std::mem::take(&mut doc.paths.paths)
            .into_iter()
            .map(|(path, item)| (format!("{base_path}{path}"), item))
            .collect();
        doc.paths.paths = prefixed;
    }
    doc
}
