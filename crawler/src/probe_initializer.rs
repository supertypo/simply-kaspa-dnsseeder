//! The [`ConnectionInitializer`] used by the crawler.
//!
//! For every outbound connection it performs the kaspa handshake, fires a
//! `RequestAddresses` and waits up to `addresses_timeout` for the matching
//! `Addresses` response. The outcome is forwarded to the originating
//! [`crate::probe::KaspadProbe`] via the per-address oneshot map.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use kaspa_core::time::unix_now;
use kaspa_p2p_lib::common::ProtocolError;
use kaspa_p2p_lib::pb::{self, AddressesMessage, RequestAddressesMessage, VersionMessage, kaspad_message::Payload};
use kaspa_p2p_lib::{
    ConnectionInitializer, KaspadHandshake, KaspadMessagePayloadType, Router, dequeue_with_timeout, make_message,
};
use kaspa_utils::networking::IpAddress;
use log::{debug, trace, warn};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::error::ProbeError;
use crate::model::ProbeResult;

const MAX_ADDRESSES_RECEIVE: usize = 2500;

/// Routing key for an in-flight probe: the peer's `SocketAddr` as reported by
/// [`Router::net_address`].
pub(crate) type ProbeChannel = oneshot::Sender<Result<ProbeResult, ProbeError>>;
pub(crate) type PendingMap = Arc<DashMap<std::net::SocketAddr, ProbeChannel>>;

/// Configuration shared between the scheduler and the initializer.
#[derive(Debug, Clone)]
pub struct ProbeInitializerConfig {
    pub network_id: kaspa_consensus_core::network::NetworkId,
    pub handshake_timeout: Duration,
    pub addresses_timeout: Duration,
    /// Protocol version advertised in our `VersionMessage`. We use the highest
    /// version `kaspa-p2p-lib` from `tn10-toc3` supports (7) so the peer
    /// negotiates the most recent flow set.
    pub our_protocol_version: u32,
}

impl ProbeInitializerConfig {
    #[must_use]
    pub fn new(
        network_id: kaspa_consensus_core::network::NetworkId,
        handshake_timeout: Duration,
        addresses_timeout: Duration,
    ) -> Self {
        Self { network_id, handshake_timeout, addresses_timeout, our_protocol_version: 7 }
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

    fn build_version_message(&self) -> VersionMessage {
        pb::VersionMessage {
            protocol_version: self.config.our_protocol_version,
            services: 0,
            timestamp: unix_now() as i64,
            address: None,
            id: Vec::from(Uuid::new_v4().as_bytes()),
            user_agent: "/simply-kaspa-dnsseeder:0.1.0/".to_string(),
            disable_relay_tx: true,
            subnetwork_id: None,
            network: self.config.network_id.to_prefixed(),
        }
    }

    async fn do_probe(&self, router: &Arc<Router>) -> Result<ProbeResult, ProtocolError> {
        let mut handshake = KaspadHandshake::new(router);
        let addresses_route = router.subscribe(vec![KaspadMessagePayloadType::Addresses]);
        // We don't speak the full v7 flow set; subscribe to common request types just so
        // the receive loop has consumers (avoids "no subscriber" errors closing the router).
        let _request_addresses_drain = router.subscribe(vec![KaspadMessagePayloadType::RequestAddresses]);

        router.start();

        let peer_version = tokio::time::timeout(self.config.handshake_timeout, handshake.handshake(self.build_version_message()))
            .await
            .map_err(|_| ProtocolError::Timeout(self.config.handshake_timeout))??;

        debug!(
            "probe {}: peer version protocol={} ua={:?} network={}",
            router.net_address(),
            peer_version.protocol_version,
            peer_version.user_agent,
            peer_version.network,
        );

        handshake.exchange_ready_messages().await?;

        router
            .enqueue(make_message!(Payload::RequestAddresses, RequestAddressesMessage { include_all_subnetworks: false, subnetwork_id: None }))
            .await?;

        let mut addresses_route = addresses_route;
        let msg: AddressesMessage =
            dequeue_with_timeout!(addresses_route, Payload::Addresses, self.config.addresses_timeout)?;
        let address_list: Vec<(IpAddress, u16)> = msg.try_into()?;
        if address_list.len() > MAX_ADDRESSES_RECEIVE {
            return Err(ProtocolError::OtherOwned(format!(
                "address count {} exceeded {}",
                address_list.len(),
                MAX_ADDRESSES_RECEIVE
            )));
        }
        trace!("probe {}: received {} addresses", router.net_address(), address_list.len());

        Ok(ProbeResult { version: peer_version, addresses: address_list })
    }
}

#[async_trait]
impl ConnectionInitializer for ProbeInitializer {
    async fn initialize_connection(&self, router: Arc<Router>) -> Result<(), ProtocolError> {
        let addr = router.net_address();
        let result = self.do_probe(&router).await;

        let sender = self.pending.remove(&addr).map(|(_, tx)| tx);

        match result {
            Ok(probe_result) => {
                if let Some(tx) = sender {
                    let _ = tx.send(Ok(probe_result));
                }
                // We don't need the connection any longer; closing the router
                // here would race with the connection_handler tearing it down,
                // so we let `KaspadProbe::probe` call `adaptor.terminate` once
                // it observes the success.
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
