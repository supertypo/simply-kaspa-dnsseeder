use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use log::{info, warn};
use tokio::sync::broadcast;
use tokio::task::JoinSet;

use crate::error::{Error, TlsFile};
use crate::router::build_router;
use crate::state::AppState;

const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

async fn load_tls(cert: &Path, key: &Path) -> Result<RustlsConfig, Error> {
    let cert_bytes = tokio::fs::read(cert).await.map_err(|source| Error::Tls {
        kind: TlsFile::Cert,
        path: cert.to_path_buf(),
        source,
    })?;
    let key_bytes = tokio::fs::read(key).await.map_err(|source| Error::Tls {
        kind: TlsFile::Key,
        path: key.to_path_buf(),
        source,
    })?;
    RustlsConfig::from_pem(cert_bytes, key_bytes).await.map_err(|source| Error::Tls {
        kind: TlsFile::Cert,
        path: cert.to_path_buf(),
        source,
    })
}

/// Run the HTTP(S) server until shutdown.
pub async fn run_web_server(state: AppState, mut shutdown: broadcast::Receiver<()>) -> Result<(), Error> {
    let listen = state.config.listen.clone();
    if listen.is_empty() {
        return Err(Error::NoListenAddrs);
    }
    let tls_cert = state.config.tls_cert.clone();
    let tls_key = state.config.tls_key.clone();
    let make_service = build_router(state).into_make_service_with_connect_info::<SocketAddr>();

    let handle = Handle::new();
    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = shutdown.recv().await;
        info!("http: shutdown signal received");
        shutdown_handle.graceful_shutdown(Some(GRACEFUL_SHUTDOWN_TIMEOUT));
    });

    let tls_config = if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
        if rustls::crypto::ring::default_provider().install_default().is_err() {
            warn!("rustls: default crypto provider was already installed");
        }
        Some(load_tls(&cert, &key).await?)
    } else {
        None
    };

    let scheme = if tls_config.is_some() { "https" } else { "http" };
    let mut servers = JoinSet::new();
    for addr in listen {
        let handle = handle.clone();
        let svc = make_service.clone();
        info!("{scheme}: listening on {addr}");
        match tls_config.clone() {
            Some(tls) => {
                servers.spawn(async move { axum_server::bind_rustls(addr, tls).handle(handle).serve(svc).await });
            }
            None => {
                servers.spawn(async move { axum_server::bind(addr).handle(handle).serve(svc).await });
            }
        }
    }

    let mut first_error: Option<std::io::Error> = None;
    while let Some(joined) = servers.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
                handle.graceful_shutdown(Some(GRACEFUL_SHUTDOWN_TIMEOUT));
            }
            Err(join_err) => {
                warn!("http: server task panicked: {join_err}");
                handle.graceful_shutdown(Some(GRACEFUL_SHUTDOWN_TIMEOUT));
            }
        }
    }
    if let Some(err) = first_error {
        return Err(err.into());
    }
    Ok(())
}
