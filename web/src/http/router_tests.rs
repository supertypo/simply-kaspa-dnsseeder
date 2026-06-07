use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use simply_kaspa_dnsseeder_crawler::ProbeError;
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
use tempfile::TempDir;
use tower::{Service, ServiceExt};

use crate::runtime::Prober;
use crate::{AppState, WebConfig, build_router};

#[derive(Default, Clone)]
struct MockProber {
    fail: bool,
}

#[async_trait]
impl Prober for MockProber {
    async fn probe(&self, addr: SocketAddr) -> Result<PeerRecord, ProbeError> {
        if self.fail {
            return Err(ProbeError::Connection("mock failure".to_string()));
        }
        let mut id = [0u8; 16];
        id[0] = 0xAB;
        Ok(PeerRecord {
            id,
            protocol_version: 7,
            timestamp_ms: 0,
            address: NetAddress {
                ip: addr.ip(),
                port: addr.port(),
            },
            user_agent: "/mock:1.0.0/".to_string(),
            subnetwork_id: None,
            first_seen_ms: 1,
            last_attempt_ms: 1,
            last_success_ms: 1,
            last_seen_ms: 1,
        })
    }
}

fn make_state(prober: Arc<dyn Prober>, store: PeerStore, api_key: &str) -> AppState {
    let cfg = WebConfig {
        listen: vec!["127.0.0.1:0".parse().unwrap()],
        api_key: api_key.to_string(),
        allowed_origins: Vec::new(),
        post_rate_limit: 5,
        rate_limit_window: Duration::from_mins(1),
        network_default_port: 16111,
        strict_port: false,
        api_prefix: String::new(),
        db_path: std::path::PathBuf::from("_test_unused.redb"),
        stale_good: Duration::from_mins(15),
        min_protocol_version: None,
        min_user_agent: None,
        service_name: "test",
        service_version: "0.0.0",
        service_commit: "test",
        service_network: String::from("kaspa-mainnet"),
        tls_cert: None,
        tls_key: None,
    };
    AppState::builder(store, prober, cfg).build()
}

fn seeded_store() -> (TempDir, PeerStore) {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let mut id = [0u8; 16];
    id[0] = 0x11;
    // Use a recent timestamp so the peer falls within the test stale_good window.
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0);
    store
        .upsert(&PeerRecord {
            id,
            protocol_version: 7,
            timestamp_ms: 0,
            address: NetAddress {
                ip: IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
                port: 16111,
            },
            user_agent: "/kaspad:1.0.0/".to_string(),
            subnetwork_id: None,
            first_seen_ms: now,
            last_attempt_ms: now,
            last_success_ms: now,
            last_seen_ms: now,
        })
        .unwrap();
    (temp, store)
}

#[tokio::test]
async fn list_peers_strips_ip_when_unauthenticated() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, "secret");
    let app = build_router(state);

    // No api key header → 200, ip stripped. Same for ?all=true.
    for url in ["/peers", "/peers?all=true"] {
        let res = app.clone().oneshot(Request::get(url).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "{url}");
        let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
        assert!(!body.windows(7).any(|w| w == b"1.2.3.4"), "{url} leaked ip without auth");
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["ip"], serde_json::Value::Null);
    }

    // Wrong api key → still treated as unauthenticated (ip stripped).
    let res = app
        .clone()
        .oneshot(Request::get("/peers").header("x-api-key", "wrong").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    assert!(!body.windows(7).any(|w| w == b"1.2.3.4"));

    // Correct api key → ip exposed.
    let res = app
        .oneshot(Request::get("/peers").header("x-api-key", "secret").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json[0]["ip"], "1.2.3.4");
}

#[tokio::test]
async fn get_peer_requires_api_key() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, "secret");
    let app = build_router(state);

    let res = app
        .clone()
        .oneshot(Request::get("/peers/1.2.3.4:16111").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    assert!(!body.windows(7).any(|w| w == b"1.2.3.4"), "leaked ip without auth");

    let res = app
        .oneshot(
            Request::get("/peers/1.2.3.4:16111")
                .header("x-api-key", "wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_peer_returns_404_when_missing() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app
        .oneshot(
            Request::get("/peers/9.9.9.9:16111")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_peer_returns_record_by_addr() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app
        .oneshot(
            Request::get("/peers/1.2.3.4:16111")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ip"], "1.2.3.4");
    assert_eq!(json["port"], 16111);
}

#[tokio::test]
async fn get_peer_returns_400_on_bad_addr() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app
        .oneshot(
            Request::get("/peers/not-an-addr")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_peers_rejects_without_api_key() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, "secret");
    // We need ConnectInfo present — use into_make_service_with_connect_info path.
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

    let mut svc = app;
    let mut conn = svc.call("127.0.0.1:1234".parse::<SocketAddr>().unwrap()).await.unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/peers")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"addrPort":"9.9.9.9:16111"}"#))
        .unwrap();
    let res = conn.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn post_peers_probes_and_returns_record() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store.clone(), "test-key");
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

    let mut svc = app;
    let mut conn = svc.call(SocketAddr::from_str("127.0.0.1:1234").unwrap()).await.unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/peers")
        .header("x-api-key", "test-key")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"addrPort":"9.9.9.9:16111"}"#))
        .unwrap();
    let res = conn.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ip"], "9.9.9.9");
    assert_eq!(json["port"], 16111);
}

#[tokio::test]
async fn post_peers_returns_502_on_probe_failure() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber { fail: true }), store, "test-key");
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

    let mut svc = app;
    let mut conn = svc.call(SocketAddr::from_str("127.0.0.1:1234").unwrap()).await.unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/peers")
        .header("x-api-key", "test-key")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"addrPort":"9.9.9.9:16111"}"#))
        .unwrap();
    let res = conn.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn rate_limit_blocks_repeated_posts() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let cfg = WebConfig {
        listen: vec!["127.0.0.1:0".parse().unwrap()],
        api_key: "test-key".to_string(),
        allowed_origins: Vec::new(),
        post_rate_limit: 1,
        rate_limit_window: Duration::from_mins(1),
        network_default_port: 16111,
        strict_port: false,
        api_prefix: String::new(),
        db_path: std::path::PathBuf::from("_test_unused.redb"),
        stale_good: Duration::from_mins(15),
        min_protocol_version: None,
        min_user_agent: None,
        service_name: "test",
        service_version: "0.0.0",
        service_commit: "test",
        service_network: String::from("kaspa-mainnet"),
        tls_cert: None,
        tls_key: None,
    };
    let state = AppState::builder(store, Arc::new(MockProber::default()), cfg).build();
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

    let mut svc = app;
    let mut conn = svc.call(SocketAddr::from_str("127.0.0.1:1234").unwrap()).await.unwrap();

    let req = || {
        Request::builder()
            .method(Method::POST)
            .uri("/peers")
            .header("x-api-key", "test-key")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"addrPort":"9.9.9.9:16111"}"#))
            .unwrap()
    };

    let first = conn.call(req()).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = conn.call(req()).await.unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn get_peer_returns_port() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app
        .oneshot(
            Request::get("/peers/1.2.3.4:16111")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["port"], 16111);
}

#[tokio::test]
async fn list_peers_uses_camel_case_keys() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app
        .oneshot(Request::get("/peers").header("x-api-key", "test-key").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let entry = &json[0];
    for key in ["protocolVersion", "userAgent", "lastSeenMs", "firstSeenMs", "ip", "port"] {
        assert!(entry.get(key).is_some(), "missing camelCase field `{key}` in {entry}");
    }
    assert!(
        entry.get("protocol_version").is_none(),
        "stale snake_case `protocol_version` still present"
    );
}

#[tokio::test]
async fn post_peers_rejects_private_ip() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

    let mut svc = app;
    let mut conn = svc.call(SocketAddr::from_str("127.0.0.1:1234").unwrap()).await.unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/peers")
        .header("x-api-key", "test-key")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"addrPort":"10.0.0.1:16111"}"#))
        .unwrap();
    let res = conn.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_peers_rejects_ephemeral_port() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

    let mut svc = app;
    let mut conn = svc.call(SocketAddr::from_str("127.0.0.1:1234").unwrap()).await.unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/peers")
        .header("x-api-key", "test-key")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"addrPort":"1.2.3.4:55000"}"#))
        .unwrap();
    let res = conn.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_peers_applies_protocol_version_filter_unless_all() {
    let (_temp, store) = seeded_store();
    // Seed peer protocol_version = 7; require >= 10 → default list should be empty,
    // ?all=true should still include it (freshness only).
    let cfg = WebConfig {
        listen: vec!["127.0.0.1:0".parse().unwrap()],
        api_key: "test-key".to_string(),
        allowed_origins: Vec::new(),
        post_rate_limit: 5,
        rate_limit_window: Duration::from_mins(1),
        network_default_port: 16111,
        strict_port: false,
        api_prefix: String::new(),
        db_path: std::path::PathBuf::from("_test_unused.redb"),
        stale_good: Duration::from_mins(15),
        min_protocol_version: Some(10),
        min_user_agent: None,
        service_name: "test",
        service_version: "0.0.0",
        service_commit: "test",
        service_network: String::from("kaspa-mainnet"),
        tls_cert: None,
        tls_key: None,
    };
    let state = AppState::builder(store, Arc::new(MockProber::default()), cfg).build();
    let app = build_router(state);

    let res = app
        .clone()
        .oneshot(Request::get("/peers").header("x-api-key", "test-key").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json.as_array().unwrap().len(), 0, "default list filters out protocol_version < 10");

    let res = app
        .oneshot(
            Request::get("/peers?all=true")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        json.as_array().unwrap().len(),
        1,
        "?all=true returns the peer regardless of protocol_version"
    );
}

#[tokio::test]
async fn list_peers_hides_stubs_in_both_modes() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    // A stub: last_success_ms = 0 → fails the freshness gate.
    let net = NetAddress {
        ip: IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
        port: 16111,
    };
    store.insert_or_refresh_seen(&net, 0).unwrap();

    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);

    for url in ["/peers", "/peers?all=true"] {
        let res = app
            .clone()
            .oneshot(Request::get(url).header("x-api-key", "test-key").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 0, "stubs must never appear (url={url})");
    }
}

#[tokio::test]
async fn openapi_json_served_unauthenticated() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, "secret");
    let app = build_router(state);
    let res = app
        .oneshot(Request::get("/swagger/openapi.json").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), 256 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["openapi"].as_str().unwrap().chars().next(), Some('3'));
    assert!(json["paths"]["/peers"].is_object());
    assert!(json["paths"]["/health"].is_object());
    assert!(json["paths"]["/metrics"].is_object());
}

#[tokio::test]
async fn responses_have_default_cache_control() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app.oneshot(Request::get("/peers").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers().get(axum::http::header::CACHE_CONTROL).map(|v| v.to_str().unwrap()),
        Some("public, max-age=5")
    );
}

#[tokio::test]
async fn delete_peer_requires_api_key() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, "secret");
    let app = build_router(state);
    let res = app
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/peers/1.2.3.4:16111")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn delete_peer_removes_existing() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/peers/1.2.3.4:16111")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // Subsequent GET should now report not-found.
    let res = app
        .oneshot(
            Request::get("/peers/1.2.3.4:16111")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_peer_returns_404_when_missing() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/peers/9.9.9.9:16111")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_peer_returns_400_on_bad_addr() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, "test-key");
    let app = build_router(state);
    let res = app
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/peers/not-an-addr")
                .header("x-api-key", "test-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
