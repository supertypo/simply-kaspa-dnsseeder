use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use simply_kaspa_dnsseeder_crawler::ProbeError;
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};
use tempfile::TempDir;
use tower::ServiceExt;

use crate::prober::Prober;
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
            network: "kaspa-mainnet".to_string(),
            services: 0,
            timestamp_ms: 0,
            address: NetAddress { ip: addr.ip(), port: addr.port() },
            user_agent: "/mock:1.0.0/".to_string(),
            disable_relay_tx: true,
            subnetwork_id: None,
            first_seen_ms: 1,
            last_attempt_ms: 1,
            last_success_ms: 1,
            last_seen_ms: 1,
        })
    }
}

fn make_state(prober: Arc<dyn Prober>, store: PeerStore, api_key: Option<String>) -> AppState {
    let cfg = WebConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        api_key,
        allowed_origins: Vec::new(),
        post_rate_limit: 5,
        rate_limit_window: Duration::from_secs(60),
    };
    AppState::new(store, prober, cfg)
}

fn seeded_store() -> (TempDir, PeerStore) {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let mut id = [0u8; 16];
    id[0] = 0x11;
    store
        .upsert(&PeerRecord {
            id,
            protocol_version: 7,
            network: "kaspa-mainnet".to_string(),
            services: 0,
            timestamp_ms: 0,
            address: NetAddress { ip: IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), port: 16111 },
            user_agent: "/kaspad:1.0.0/".to_string(),
            disable_relay_tx: true,
            subnetwork_id: None,
            first_seen_ms: 100,
            last_attempt_ms: 100,
            last_success_ms: 100,
            last_seen_ms: 100,
        })
        .unwrap();
    (temp, store)
}

#[tokio::test]
async fn ping_returns_pong() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, None);
    let app = build_router(state);
    let res = app.oneshot(Request::get("/ping").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], b"pong");
}

#[tokio::test]
async fn list_peers_strips_ip_when_api_key_set() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, Some("secret".to_string()));
    let app = build_router(state);

    // No api key header → ip omitted.
    let res = app.clone().oneshot(Request::get("/peers").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json[0]["ip"], serde_json::Value::Null);

    // Correct api key → ip exposed.
    let res = app
        .oneshot(Request::get("/peers").header("x-api-key", "secret").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json[0]["ip"], "1.2.3.4");
}

#[tokio::test]
async fn list_peers_exposes_ip_with_no_api_key_configured() {
    let (_temp, store) = seeded_store();
    let state = make_state(Arc::new(MockProber::default()), store, None);
    let app = build_router(state);
    let res = app.oneshot(Request::get("/peers").body(Body::empty()).unwrap()).await.unwrap();
    let body = to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json[0]["ip"], "1.2.3.4");
}

#[tokio::test]
async fn get_peer_returns_404_when_missing() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, None);
    let app = build_router(state);
    let id = hex::encode([0u8; 16]);
    let res = app.oneshot(Request::get(format!("/peers/{id}")).body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn post_peers_rejects_without_api_key() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store, Some("secret".to_string()));
    // We need ConnectInfo present — use into_make_service_with_connect_info path.
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();
    use tower::Service;
    let mut svc = app;
    let mut conn = svc.call("127.0.0.1:1234".parse::<SocketAddr>().unwrap()).await.unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/peers")
        .body(Body::from("9.9.9.9:16111"))
        .unwrap();
    let res = conn.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn post_peers_probes_and_returns_record() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let state = make_state(Arc::new(MockProber::default()), store.clone(), None);
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();
    use tower::Service;
    let mut svc = app;
    let mut conn = svc.call(SocketAddr::from_str("127.0.0.1:1234").unwrap()).await.unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/peers")
        .body(Body::from("9.9.9.9:16111"))
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
    let state = make_state(Arc::new(MockProber { fail: true }), store, None);
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();
    use tower::Service;
    let mut svc = app;
    let mut conn = svc.call(SocketAddr::from_str("127.0.0.1:1234").unwrap()).await.unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/peers")
        .body(Body::from("9.9.9.9:16111"))
        .unwrap();
    let res = conn.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn rate_limit_blocks_repeated_posts() {
    let temp = TempDir::new().unwrap();
    let store = PeerStore::open(temp.path().join("peers.redb")).unwrap();
    let cfg = WebConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        api_key: None,
        allowed_origins: Vec::new(),
        post_rate_limit: 1,
        rate_limit_window: Duration::from_secs(60),
    };
    let state = AppState::new(store, Arc::new(MockProber::default()), cfg);
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();
    use tower::Service;
    let mut svc = app;
    let mut conn = svc.call(SocketAddr::from_str("127.0.0.1:1234").unwrap()).await.unwrap();

    let req = || Request::builder().method(Method::POST).uri("/peers").body(Body::from("9.9.9.9:16111")).unwrap();

    let first = conn.call(req()).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = conn.call(req()).await.unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
}
