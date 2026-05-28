use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use kaspa_core::time::unix_now;
use kaspa_p2p_lib::common::ProtocolError;
use kaspa_p2p_lib::pb::{
    self, AddressesMessage, ReadyMessage, RequestAddressesMessage, VerackMessage, VersionMessage, kaspad_message::Payload,
};
use kaspa_p2p_lib::{
    ConnectionInitializer, KaspadMessagePayloadType, Router, dequeue_with_timeout, make_message,
};
use kaspa_utils::networking::{IpAddress, PeerId};
use log::{debug, info, trace, warn};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::error::ProbeError;
use crate::model::ProbeResult;

const MAX_ADDRESSES_RECEIVE: usize = 2500;
const USER_AGENT: &str = "/simply-kaspa-dnsseeder:0.1.0/";

pub(crate) type ProbeChannel = oneshot::Sender<Result<ProbeResult, ProbeError>>;
pub(crate) type PendingMap = Arc<DashMap<std::net::SocketAddr, ProbeChannel>>;

#[derive(Debug, Clone)]
pub struct ProbeInitializerConfig {
    pub network_id: kaspa_consensus_core::network::NetworkId,
    pub probe_timeout: Duration,
}

impl ProbeInitializerConfig {
    #[must_use]
    pub fn new(
        network_id: kaspa_consensus_core::network::NetworkId,
        probe_timeout: Duration,
    ) -> Self {
        Self { network_id, probe_timeout }
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
        let _drain_route = router.subscribe(drain_payload_types());

        router.start();

        let timeout = self.config.probe_timeout;

        // 1. Peer pushes its Version unprompted on connect.
        let peer_version: VersionMessage = dequeue_with_timeout!(version_route, Payload::Version, timeout)?;
        debug!(
            "probe {}: peer version protocol={} ua={:?} network={}",
            router.net_address(),
            peer_version.protocol_version,
            peer_version.user_agent,
            peer_version.network,
        );
        // Update the router's identity from the peer's reported UUID so the
        // Hub keys connections by the real peer id instead of `Uuid::nil()`,
        // eliminating spurious "duplicate key" evictions when the same IP is
        // re-probed before the previous Hub entry expires.
        if let Ok(peer_id) = PeerId::from_slice(&peer_version.id) {
            router.set_identity(peer_id);
        }

        // 2. Reply with a Version that mirrors the peer's protocol_version + services.
        let our_version = pb::VersionMessage {
            protocol_version: peer_version.protocol_version,
            services: peer_version.services,
            timestamp: i64::try_from(unix_now()).unwrap_or(i64::MAX),
            address: None,
            id: Vec::from(Uuid::new_v4().as_bytes()),
            user_agent: USER_AGENT.to_string(),
            disable_relay_tx: true,
            subnetwork_id: None,
            network: self.config.network_id.to_prefixed(),
        };
        router.enqueue(make_message!(Payload::Version, our_version)).await?;

        // 3. Receive peer's Verack.
        let _verack: VerackMessage = dequeue_with_timeout!(verack_route, Payload::Verack, timeout)?;

        // 4. Send our Verack.
        router.enqueue(make_message!(Payload::Verack, pb::VerackMessage {})).await?;

        // 5. Ready exchange — kaspa-p2p-lib peers send Ready and wait for ours
        // (8s peer-side timeout) before they will service any further messages.
        router.enqueue(make_message!(Payload::Ready, ReadyMessage {})).await?;
        let _ready: ReadyMessage = dequeue_with_timeout!(ready_route, Payload::Ready, timeout)?;

        // 6. Peer sends RequestAddresses; reply with an empty Addresses payload.
        let _peer_req: RequestAddressesMessage =
            dequeue_with_timeout!(request_addr_route, Payload::RequestAddresses, timeout)?;
        router
            .enqueue(make_message!(Payload::Addresses, pb::AddressesMessage { address_list: vec![] }))
            .await?;

        // 7. Now ask the peer for its address book.
        router
            .enqueue(make_message!(
                Payload::RequestAddresses,
                RequestAddressesMessage { include_all_subnetworks: true, subnetwork_id: None }
            ))
            .await?;

        // 8. Receive the peer's Addresses.
        let msg: AddressesMessage = dequeue_with_timeout!(addresses_route, Payload::Addresses, timeout)?;
        let address_list: Vec<(IpAddress, u16)> = msg.try_into()?;
        if address_list.len() > MAX_ADDRESSES_RECEIVE {
            return Err(ProtocolError::OtherOwned(format!(
                "address count {} exceeded {MAX_ADDRESSES_RECEIVE}",
                address_list.len(),
            )));
        }
        info!("probe {}: received {} address(es)", router.net_address(), address_list.len());
        trace!("probe {}: addresses = {:?}", router.net_address(), address_list);

        Ok(ProbeResult { version: peer_version, addresses: address_list })
    }
}

#[async_trait]
impl ConnectionInitializer for ProbeInitializer {
    async fn initialize_connection(&self, router: Arc<Router>) -> Result<(), ProtocolError> {
        let addr = router.net_address();
        let result = self.do_probe(&router).await;
        let sender = self.pending.remove(&addr).map(|(_, tx)| tx);
        // Do NOT close the router here: `initialize_connection` runs BEFORE
        // the connection handler pushes `HubEvent::NewPeer`. Closing here
        // would queue `PeerClosing` ahead of `NewPeer`, the Hub would no-op
        // the closing (nothing to remove yet), then insert the router and
        // never remove it. probe.rs terminates after `connect_peer` returns,
        // which is after `NewPeer` is queued — that ordering is correct.

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
                        other => ProbeError::Handshake(other.to_string()),
                    };
                    let _ = tx.send(Err(probe_err));
                }
                warn!("probe {addr}: failed during initializer: {err}");
                Err(err)
            }
        }
    }
}

/// Every payload type except the five we explicitly drive in `do_probe`.
/// Subscribed as a single silent drain so the router does not close the
/// connection when the peer pushes operational-state traffic.
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

