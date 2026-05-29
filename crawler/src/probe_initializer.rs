use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use kaspa_core::time::unix_now;
use kaspa_p2p_lib::common::ProtocolError;
use kaspa_p2p_lib::pb::{
    self, AddressesMessage, ReadyMessage, RejectMessage, RequestAddressesMessage, VerackMessage, VersionMessage,
    kaspad_message::Payload,
};
use kaspa_p2p_lib::{ConnectionInitializer, IncomingRoute, KaspadMessagePayloadType, Router, dequeue_with_timeout, make_message};
use kaspa_utils::networking::{IpAddress, PeerId};
use log::{debug, info, trace, warn};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::error::ProbeError;
use crate::model::ProbeResult;

const MAX_ADDRESSES_RECEIVE: usize = 2500;
const USER_AGENT: &str = "/simply-kaspa-dnsseeder:0.1.0/";
/// Gap between back-to-back `RequestAddresses` rounds in a single probe.
/// Matches the legacy Go seeder so peers see the same cadence.
const PROBE_REPEAT_DELAY: Duration = Duration::from_millis(250);

pub(crate) type ProbeChannel = oneshot::Sender<Result<ProbeResult, ProbeError>>;
pub(crate) type PendingMap = Arc<DashMap<std::net::SocketAddr, ProbeChannel>>;

#[derive(Debug, Clone)]
pub struct ProbeInitializerConfig {
    pub network_id: kaspa_consensus_core::network::NetworkId,
    pub probe_timeout: Duration,
    /// Number of back-to-back `RequestAddresses` rounds per probe.
    pub probes_per_peer: u8,
}

impl ProbeInitializerConfig {
    #[must_use]
    pub fn new(network_id: kaspa_consensus_core::network::NetworkId, probe_timeout: Duration, probes_per_peer: u8) -> Self {
        Self {
            network_id,
            probe_timeout,
            probes_per_peer,
        }
    }
}

pub struct ProbeInitializer {
    config: ProbeInitializerConfig,
    pending: PendingMap,
}

impl ProbeInitializer {
    #[must_use]
    pub fn new(config: ProbeInitializerConfig, pending: PendingMap) -> Self {
        Self { config, pending }
    }

    /// Go-dnsseeder style handshake: read the peer's `Version` first, mirror
    /// `protocol_version` and `services` back so we are compatible with both
    /// v7 and v10 mainnet peers without picking a fixed local version.
    async fn do_probe(&self, router: &Arc<Router>) -> Result<ProbeResult, ProtocolError> {
        let mut version_route = router.subscribe(vec![KaspadMessagePayloadType::Version]);
        let mut verack_route = router.subscribe(vec![KaspadMessagePayloadType::Verack]);
        let mut ready_route = router.subscribe(vec![KaspadMessagePayloadType::Ready]);
        let mut request_addr_route = router.subscribe(vec![KaspadMessagePayloadType::RequestAddresses]);
        let mut addresses_route = router.subscribe(vec![KaspadMessagePayloadType::Addresses]);
        // After Ready the peer enters its operational state and starts pushing
        // relay traffic (InvRelayBlock, Ping, etc.). Without a subscriber the
        // router treats those as "no flow registered" and closes the socket
        // before our Addresses arrives — so we drain everything else.
        let drain_route = router.subscribe(drain_payload_types());

        router.start();

        let timeout = self.config.probe_timeout;

        let peer_version: VersionMessage = dequeue_with_timeout!(version_route, Payload::Version, timeout)?;
        let local_network = self.config.network_id.to_prefixed();
        if peer_version.network != local_network {
            return Err(ProtocolError::WrongNetwork(local_network, peer_version.network));
        }
        debug!(
            "crawler: probe {}: peer version protocol={} ua={:?} network={}",
            router.net_address(),
            peer_version.protocol_version,
            peer_version.user_agent,
            peer_version.network,
        );
        // Adopt the peer's reported UUID so the Hub keys connections by the
        // real peer id instead of `Uuid::nil()`, avoiding spurious duplicate-key
        // evictions when the same IP is re-probed before the previous Hub entry
        // expires.
        if let Ok(peer_id) = PeerId::from_slice(&peer_version.id) {
            router.set_identity(peer_id);
        }

        let our_version = pb::VersionMessage {
            protocol_version: peer_version.protocol_version,
            services: peer_version.services,
            timestamp: i64::try_from(unix_now()).unwrap_or(i64::MAX),
            address: None,
            id: Vec::from(Uuid::new_v4().as_bytes()),
            user_agent: USER_AGENT.to_string(),
            disable_relay_tx: true,
            subnetwork_id: None,
            network: local_network,
        };
        router.enqueue(make_message!(Payload::Version, our_version)).await?;

        let _verack: VerackMessage = dequeue_with_timeout!(verack_route, Payload::Verack, timeout)?;
        router.enqueue(make_message!(Payload::Verack, pb::VerackMessage {})).await?;

        // kaspa-p2p-lib peers send Ready and wait for ours (8s peer-side timeout)
        // before they will service any further messages.
        router.enqueue(make_message!(Payload::Ready, ReadyMessage {})).await?;
        let _ready: ReadyMessage = dequeue_with_timeout!(ready_route, Payload::Ready, timeout)?;

        let _peer_req: RequestAddressesMessage = dequeue_with_timeout!(request_addr_route, Payload::RequestAddresses, timeout)?;
        router
            .enqueue(make_message!(Payload::Addresses, pb::AddressesMessage { address_list: vec![] }))
            .await?;

        let address_list = self.collect_addresses(router, &mut addresses_route, timeout).await?;
        info!(
            "crawler: probe {}: received {} address(es)",
            router.net_address(),
            address_list.len()
        );
        trace!("crawler: probe {}: addresses = {:?}", router.net_address(), address_list);

        // Pre-empt the peer's 120s PingFlow timeout; `DUPLICATE_CONNECTION` is mapped to `IgnorableReject` on the remote.
        router
            .enqueue(make_message!(
                Payload::Reject,
                RejectMessage {
                    reason: "DUPLICATE_CONNECTION".to_string()
                }
            ))
            .await
            .ok();

        // Bridge the close-handshake race window so late inbound messages don't hit a dropped route channel.
        spawn_route_drain(vec![
            version_route,
            verack_route,
            ready_route,
            request_addr_route,
            addresses_route,
            drain_route,
        ]);

        Ok(ProbeResult {
            version: peer_version,
            addresses: address_list,
        })
    }
    /// Issue `probes_per_peer` rounds of `RequestAddresses`, merging unique
    /// `(ip, port)` pairs across rounds. After the first successful batch,
    /// transport errors / timeouts short-circuit the loop and return what we
    /// already have — matching the Go seeder's resilient collection behavior.
    async fn collect_addresses(
        &self,
        router: &Arc<Router>,
        addresses_route: &mut IncomingRoute,
        timeout: Duration,
    ) -> Result<Vec<(IpAddress, u16)>, ProtocolError> {
        let rounds = self.config.probes_per_peer.max(1);
        let mut address_set: std::collections::HashSet<(IpAddress, u16)> = std::collections::HashSet::new();
        for round in 0..rounds {
            if round > 0 {
                tokio::time::sleep(PROBE_REPEAT_DELAY).await;
            }
            if let Err(err) = router
                .enqueue(make_message!(
                    Payload::RequestAddresses,
                    RequestAddressesMessage {
                        include_all_subnetworks: true,
                        subnetwork_id: None
                    }
                ))
                .await
            {
                if !address_set.is_empty() {
                    break;
                }
                return Err(err);
            }
            let recv: Result<AddressesMessage, ProtocolError> = dequeue_with_timeout!(addresses_route, Payload::Addresses, timeout);
            let msg = match recv {
                Ok(m) => m,
                Err(err) => {
                    if !address_set.is_empty() {
                        break;
                    }
                    return Err(err);
                }
            };
            let batch: Vec<(IpAddress, u16)> = msg.try_into()?;
            address_set.extend(batch);
            if address_set.len() > MAX_ADDRESSES_RECEIVE {
                return Err(ProtocolError::OtherOwned(format!(
                    "address count {} exceeded {MAX_ADDRESSES_RECEIVE}",
                    address_set.len(),
                )));
            }
        }
        Ok(address_set.into_iter().collect())
    }
}

#[async_trait]
impl ConnectionInitializer for ProbeInitializer {
    async fn initialize_connection(&self, router: Arc<Router>) -> Result<(), ProtocolError> {
        let addr = router.net_address();
        let result = self.do_probe(&router).await;
        let sender = self.pending.remove(&addr).map(|(_, tx)| tx);
        // Do NOT close the router here: `initialize_connection` runs BEFORE
        // the connection handler queues `HubEvent::NewPeer`, so a close at this
        // point would push `PeerClosing` ahead of `NewPeer` and the Hub would
        // leak the router. `probe.rs` terminates after `connect_peer` returns,
        // which is after `NewPeer` is queued.

        match result {
            Ok(probe_result) => {
                if let Some(tx) = sender {
                    let _ = tx.send(Ok(probe_result));
                }
                Ok(())
            }
            Err(err) => {
                if let Some(tx) = sender {
                    let probe_err = match &err {
                        ProtocolError::Timeout(_) => ProbeError::Timeout,
                        ProtocolError::WrongNetwork(local, remote) => {
                            ProbeError::NetworkMismatch { local: local.clone(), remote: remote.clone() }
                        }
                        other => ProbeError::Handshake(other.to_string()),
                    };
                    let _ = tx.send(Err(probe_err));
                }
                warn!("crawler: probe {addr}: failed during initializer: {err}");
                Err(err)
            }
        }
    }
}

/// Every payload type *except* the five `do_probe` drives explicitly.
/// Subscribed as a silent drain so the router doesn't close the connection on
/// operational-state traffic.
pub(crate) fn drain_payload_types() -> Vec<KaspadMessagePayloadType> {
    use KaspadMessagePayloadType as T;
    vec![
        T::Block,
        T::Transaction,
        T::BlockLocator,
        T::RequestRelayBlocks,
        T::RequestTransactions,
        T::IbdBlock,
        T::InvRelayBlock,
        T::InvTransactions,
        T::Ping,
        T::Pong,
        T::TransactionNotFound,
        T::Reject,
        T::PruningPointUtxoSetChunk,
        T::RequestIbdBlocks,
        T::UnexpectedPruningPoint,
        T::IbdBlockLocator,
        T::IbdBlockLocatorHighestHash,
        T::RequestNextPruningPointUtxoSetChunk,
        T::DonePruningPointUtxoSetChunks,
        T::IbdBlockLocatorHighestHashNotFound,
        T::BlockWithTrustedData,
        T::DoneBlocksWithTrustedData,
        T::RequestPruningPointAndItsAnticone,
        T::BlockHeaders,
        T::RequestNextHeaders,
        T::DoneHeaders,
        T::RequestPruningPointUtxoSet,
        T::RequestHeaders,
        T::RequestBlockLocator,
        T::PruningPoints,
        T::RequestPruningPointProof,
        T::PruningPointProof,
        T::BlockWithTrustedDataV4,
        T::TrustedData,
        T::RequestIbdChainBlockLocator,
        T::IbdChainBlockLocator,
        T::RequestAntipast,
        T::RequestNextPruningPointAndItsAnticoneBlocks,
        T::BlockBody,
        T::RequestBlockBodies,
        T::RequestPruningPointSmtState,
        T::SmtMetadata,
        T::SmtLaneChunk,
        T::RequestNextPruningPointSmtChunk,
    ]
}

/// Spawns a background task that owns the given route receivers and drains
/// any messages until the router shuts down (sender side closes). Keeps the
/// channels open so the router does not log "peer connection is closed" warns
/// after a successful probe.
fn spawn_route_drain(routes: Vec<IncomingRoute>) {
    for mut route in routes {
        tokio::spawn(async move { while route.recv().await.is_some() {} });
    }
}
