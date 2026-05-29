use std::net::SocketAddr;
use std::sync::Once;
use std::time::Duration;

use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use log::{info, warn};
use tokio::sync::broadcast;

use crate::error::Error;
use crate::router::build_router;
use crate::state::AppState;

const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

static CRYPTO_PROVIDER: Once = Once::new();

fn install_crypto_provider() {
    CRYPTO_PROVIDER.call_once(|| {
        if rustls::crypto::ring::default_provider().install_default().is_err() {
            warn!("rustls: a crypto provider was already installed; reusing existing one");
        }
    });
}

/// Run the HTTP(S) server until shutdown.
pub async fn run_web_server(state: AppState, mut shutdown: broadcast::Receiver<()>) -> Result<(), Error> {
    let listen = state.config.listen;
    let tls_cert = state.config.tls_cert.clone();
    let tls_key = state.config.tls_key.clone();
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

    let handle = Handle::new();
    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = shutdown.recv().await;
        info!("http: shutdown signal received");
        shutdown_handle.graceful_shutdown(Some(GRACEFUL_SHUTDOWN_TIMEOUT));
    });

    if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        install_crypto_provider();
        let tls = RustlsConfig::from_pem_file(&cert, &key).await.map_err(Error::Tls)?;
        info!("https: listening on {listen}");
        axum_server::bind_rustls(listen, tls).handle(handle).serve(app).await?;
    } else {
        info!("http: listening on {listen}");
        axum_server::bind(listen).handle(handle).serve(app).await?;
    }
    Ok(())
}
