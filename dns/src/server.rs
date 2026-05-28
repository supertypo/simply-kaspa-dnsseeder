use hickory_server::ServerFuture;
use log::info;
use simply_kaspa_dnsseeder_store::PeerStore;
use tokio::net::{TcpListener, UdpSocket};

use crate::config::DnsConfig;
use crate::error::Error;
use crate::handler::SeederHandler;

pub async fn run_dns_server(config: DnsConfig, store: PeerStore, shutdown: tokio::sync::broadcast::Receiver<()>) -> Result<(), Error> {
    let listen = config.dns_listen;
    let tcp_idle = config.tcp_idle_timeout;
    let handler = SeederHandler::new(config, store)?;
    run_dns_server_with_handler(handler, listen, tcp_idle, shutdown).await
}

pub async fn run_dns_server_with_handler(
    handler: SeederHandler,
    listen: std::net::SocketAddr,
    tcp_idle: std::time::Duration,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Error> {
    let mut server = ServerFuture::new(handler);
    let udp = UdpSocket::bind(listen).await?;
    info!("dns: udp listening on {listen}");
    server.register_socket(udp);

    let tcp = TcpListener::bind(listen).await?;
    info!("dns: tcp listening on {listen}");
    server.register_listener(tcp, tcp_idle);

    let _ = shutdown.recv().await;
    info!("dns: shutdown signal received");
    let _ = server.shutdown_gracefully().await;
    Ok(())
}
