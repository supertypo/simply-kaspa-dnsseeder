//! Wire up the [`SeederHandler`] to a UDP + TCP `ServerFuture`.

use std::time::Duration;

use hickory_server::ServerFuture;
use log::info;
use simply_kaspa_dnsseeder_store::PeerStore;
use tokio::net::{TcpListener, UdpSocket};

use crate::config::DnsConfig;
use crate::error::Error;
use crate::handler::SeederHandler;

/// Bind UDP+TCP on `config.dns_listen` and serve until `shutdown` fires.
/// A bind failure is propagated, not swallowed; the binary turns this into a
/// hard exit so operators notice misconfiguration.
pub async fn run_dns_server(
    config: DnsConfig,
    store: PeerStore,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Error> {
    let listen = config.dns_listen;
    let handler = SeederHandler::new(config, store)?;

    let mut server = ServerFuture::new(handler);
    let udp = UdpSocket::bind(listen).await?;
    info!("dns: udp listening on {listen}");
    server.register_socket(udp);

    let tcp = TcpListener::bind(listen).await?;
    info!("dns: tcp listening on {listen}");
    server.register_listener(tcp, Duration::from_secs(5));

    let _ = shutdown.recv().await;
    info!("dns: shutdown signal received");
    let _ = server.shutdown_gracefully().await;
    Ok(())
}
