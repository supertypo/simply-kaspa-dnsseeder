use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hickory_server::server::Server;
use log::{info, warn};
use simply_kaspa_dnsseeder_store::PeerStore;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::{TcpListener, UdpSocket};

use crate::config::DnsConfig;
use crate::error::Error;
use crate::handler::SeederHandler;
use crate::serving_cache::{self, REFRESH_INTERVAL, SNAPSHOT_MULTIPLIER, ServingCache};

pub async fn run_dns_server(config: DnsConfig, store: PeerStore, shutdown: tokio::sync::broadcast::Receiver<()>) -> Result<(), Error> {
    let listen = config.dns_listen.clone();
    let tcp_idle = config.tcp_idle_timeout;
    let (cache, _refresher) = build_serving_cache(&config, store, shutdown.resubscribe());
    let handler = SeederHandler::new(config, cache)?;
    run_dns_server_with_handler(handler, listen, tcp_idle, shutdown).await
}

/// Build the serving cache, do a synchronous initial refresh so the first
/// query sees current data, and spawn the periodic refresher.
#[must_use]
pub fn build_serving_cache(
    config: &DnsConfig,
    store: PeerStore,
    shutdown: tokio::sync::broadcast::Receiver<()>,
) -> (Arc<ServingCache>, tokio::task::JoinHandle<()>) {
    let cache = Arc::new(ServingCache::new());
    let p2p_port = config.network_id.default_p2p_port();
    let cap = config.max_records.saturating_mul(SNAPSHOT_MULTIPLIER);
    serving_cache::refresh_now(&cache, &store, config, p2p_port, cap);
    let handle = serving_cache::spawn_refresher(
        cache.clone(),
        store,
        Arc::new(config.clone()),
        p2p_port,
        cap,
        REFRESH_INTERVAL,
        shutdown,
    );
    (cache, handle)
}

pub async fn run_dns_server_with_handler(
    handler: SeederHandler,
    listen: Vec<SocketAddr>,
    tcp_idle: Duration,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Error> {
    assert!(!listen.is_empty(), "listen must be non-empty");

    let mut server = Server::new(handler);
    let mut bound_any = false;
    let mut last_err: Option<io::Error> = None;

    for addr in &listen {
        match bind_pair(*addr, needs_v6_only(*addr, &listen)) {
            Ok((udp, tcp)) => {
                info!("dns: listening on {addr} (udp+tcp)");
                server.register_socket(udp);
                server.register_listener(tcp, tcp_idle, 512);
                bound_any = true;
            }
            Err(err) if is_soft_bind_failure(&err) => {
                warn!("dns: skipping {addr}: {err}");
                last_err = Some(err);
            }
            Err(err) => return Err(Error::Io(err)),
        }
    }

    if !bound_any {
        return Err(Error::Io(last_err.expect("!bound_any implies a soft failure was recorded")));
    }

    let _ = shutdown.recv().await;
    info!("dns: shutdown signal received");
    let _ = server.shutdown_gracefully().await;
    Ok(())
}

// On Linux a v6 wildcard accepts v4-mapped traffic by default; force V6ONLY only when a v4 sibling on the same port would otherwise collide.
fn needs_v6_only(addr: SocketAddr, listen: &[SocketAddr]) -> bool {
    addr.is_ipv6() && listen.iter().any(|other| other.is_ipv4() && other.port() == addr.port())
}

fn bind_pair(addr: SocketAddr, v6_only: bool) -> io::Result<(UdpSocket, TcpListener)> {
    let udp = UdpSocket::from_std(prepare_socket(addr, Type::DGRAM, Protocol::UDP, v6_only)?.into())?;
    let tcp_sock = prepare_socket(addr, Type::STREAM, Protocol::TCP, v6_only)?;
    tcp_sock.set_reuse_address(true)?;
    tcp_sock.listen(1024)?;
    let tcp = TcpListener::from_std(tcp_sock.into())?;
    Ok((udp, tcp))
}

fn prepare_socket(addr: SocketAddr, ty: Type, proto: Protocol, v6_only: bool) -> io::Result<Socket> {
    let domain = if addr.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };
    let sock = Socket::new(domain, ty, Some(proto))?;
    if addr.is_ipv6() {
        sock.set_only_v6(v6_only)?;
    }
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    Ok(sock)
}

// EAFNOSUPPORT / EADDRNOTAVAIL: address family or local address unavailable (e.g. IPv6-less Docker). Hard failures still propagate.
fn is_soft_bind_failure(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported)
}
