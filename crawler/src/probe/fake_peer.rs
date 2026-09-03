//! In-process fake kaspad peer for probe lifecycle tests.
//!
//! Speaks just enough of the P2P protocol for `ProbeInitializer::do_probe` to
//! succeed (`Version` / `Verack` / `Ready` / `RequestAddresses` / `Addresses`) and lets
//! the test choose how the peer reacts afterwards. The key observable is
//! [`FakePeer::open_streams`]: it counts gRPC streams the *peer* still holds
//! open, so it only drops to zero once our side really tears the connection
//! down — which is exactly what a leaked router fails to do.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::Stream;
use kaspa_p2p_lib::pb::p2p_server::{P2p, P2pServer};
use kaspa_p2p_lib::pb::{self, KaspadMessage, kaspad_message::Payload};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status, Streaming};

pub(crate) const FAKE_NETWORK: &str = "kaspa-mainnet";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Behavior {
    /// Full handshake, then ignore our `Reject` and keep the stream open forever.
    /// This is the peer type that leaked before `probe()` closed routers itself.
    IgnoreReject,
    /// Full handshake, then close the stream on `Reject` (healthy rusty-kaspa node).
    CloseOnReject,
    /// Open the stream but never send anything, not even `Version`.
    Silent,
    /// Announce a different network so the handshake fails on our side.
    WrongNetwork,
}

pub(crate) struct FakePeer {
    pub addr: SocketAddr,
    open_streams: Arc<AtomicUsize>,
}

impl FakePeer {
    pub(crate) async fn start(behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let open_streams = Arc::new(AtomicUsize::new(0));
        let service = Service {
            behavior,
            open_streams: open_streams.clone(),
        };
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    P2pServer::new(service)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self { addr, open_streams }
    }

    /// Streams the peer currently holds open, i.e. connections our side has not closed.
    pub(crate) fn open_streams(&self) -> usize {
        self.open_streams.load(Ordering::SeqCst)
    }
}

struct Service {
    behavior: Behavior,
    open_streams: Arc<AtomicUsize>,
}

struct StreamGuard(Arc<AtomicUsize>);

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn msg(payload: Payload) -> KaspadMessage {
    KaspadMessage {
        request_id: 0,
        response_id: 0,
        payload: Some(payload),
    }
}

fn version(network: &str) -> Payload {
    Payload::Version(pb::VersionMessage {
        protocol_version: 7,
        services: 0,
        timestamp: 0,
        address: None,
        id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        user_agent: "/fake-kaspad/".into(),
        disable_relay_tx: true,
        subnetwork_id: None,
        network: network.into(),
    })
}

#[tonic::async_trait]
impl P2p for Service {
    type MessageStreamStream = Pin<Box<dyn Stream<Item = Result<KaspadMessage, Status>> + Send + 'static>>;

    async fn message_stream(&self, req: Request<Streaming<KaspadMessage>>) -> Result<Response<Self::MessageStreamStream>, Status> {
        let behavior = self.behavior;
        let mut inbound = req.into_inner();
        let (tx, rx) = mpsc::channel(16);
        self.open_streams.fetch_add(1, Ordering::SeqCst);
        let guard = StreamGuard(self.open_streams.clone());
        tokio::spawn(async move {
            let _guard = guard;
            let network = if behavior == Behavior::WrongNetwork {
                "kaspa-testnet-10"
            } else {
                FAKE_NETWORK
            };
            if behavior != Behavior::Silent && tx.send(Ok(msg(version(network)))).await.is_err() {
                return;
            }
            // Blocks until the client ends the stream; `Silent` peers just sit here.
            while let Ok(Some(m)) = inbound.message().await {
                let reply = match m.payload {
                    Some(Payload::Version(_)) => Some(Payload::Verack(pb::VerackMessage {})),
                    Some(Payload::Ready(_)) => {
                        if tx.send(Ok(msg(Payload::Ready(pb::ReadyMessage {})))).await.is_err() {
                            return;
                        }
                        Some(Payload::RequestAddresses(pb::RequestAddressesMessage {
                            include_all_subnetworks: true,
                            subnetwork_id: None,
                        }))
                    }
                    Some(Payload::RequestAddresses(_)) => Some(Payload::Addresses(pb::AddressesMessage {
                        address_list: vec![pb::NetAddress {
                            timestamp: 0,
                            ip: vec![8, 8, 8, 8],
                            port: 16111,
                        }],
                    })),
                    Some(Payload::Reject(_)) if behavior == Behavior::CloseOnReject => return,
                    _ => None,
                };
                if let Some(reply) = reply
                    && tx.send(Ok(msg(reply))).await.is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
