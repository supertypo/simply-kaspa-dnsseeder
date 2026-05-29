use std::net::SocketAddr;

use log::info;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use crate::error::Error;
use crate::router::build_router;
use crate::state::AppState;

/// Run the HTTP server until shutdown.
pub async fn run_web_server(state: AppState, mut shutdown: broadcast::Receiver<()>) -> Result<(), Error> {
    let listen = state.config.listen;
    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();
    let listener = TcpListener::bind(listen).await?;
    info!("http: listening on {listen}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.recv().await;
            info!("http: shutdown signal received");
        })
        .await?;
    Ok(())
}
