//! `OpenAPI` document for the seeder's HTTP surface.

use utoipa::OpenApi;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};

use crate::dto::{
    ApiErrorBody, CrawlerSubsystem, DiskInfo, DnsRateLimiterSubsystem, DnsSubsystem, FullPeerDto, HealthResponse, MetricsResponse,
    PeerCounts, PeerDto, PeerFamilyCounts, PeerStatusCounts, PostRejected, ProcessInfo, PublicPeerDto, RateLimiterSubsystem,
    ServiceInfo, ServingCacheSubsystem, SubmitPeerRequest, WebSubsystem,
};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Simply Kaspa DNSseeder REST API",
        description = "Public seeding service for the Kaspa P2P network",
    ),
    paths(
        crate::http::handlers::health::handler,
        crate::http::handlers::metrics::handler,
        crate::http::handlers::peers::list,
        crate::http::handlers::peers::get,
        crate::http::handlers::peers::submit,
        crate::http::handlers::peers::delete,
    ),
    components(schemas(
        ApiErrorBody,
        HealthResponse,
        MetricsResponse,
        ServiceInfo,
        PeerCounts,
        PeerStatusCounts,
        PeerFamilyCounts,
        WebSubsystem,
        PostRejected,
        RateLimiterSubsystem,
        CrawlerSubsystem,
        DnsSubsystem,
        DnsRateLimiterSubsystem,
        ServingCacheSubsystem,
        ProcessInfo,
        DiskInfo,
        PeerDto,
        FullPeerDto,
        PublicPeerDto,
        SubmitPeerRequest,
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
