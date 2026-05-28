use std::collections::HashSet;

use kaspa_p2p_lib::KaspadMessagePayloadType;

use crate::probe_initializer::drain_payload_types;

/// Every variant of `KaspadMessagePayloadType` defined in the `rusty-kaspa`
/// version we depend on. If this list ever diverges from the upstream enum,
/// `drain_covers_every_non_handshake_payload_type` fails and forces us to
/// update `drain_payload_types`.
fn all_payload_types() -> Vec<KaspadMessagePayloadType> {
    use KaspadMessagePayloadType as T;
    vec![
        T::Addresses,
        T::Block,
        T::Transaction,
        T::BlockLocator,
        T::RequestAddresses,
        T::RequestRelayBlocks,
        T::RequestTransactions,
        T::IbdBlock,
        T::InvRelayBlock,
        T::InvTransactions,
        T::Ping,
        T::Pong,
        T::Verack,
        T::Version,
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
        T::Ready,
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

fn handshake_payload_types() -> Vec<KaspadMessagePayloadType> {
    use KaspadMessagePayloadType as T;
    vec![T::Version, T::Verack, T::Ready, T::RequestAddresses, T::Addresses]
}

#[test]
fn drain_has_no_duplicates() {
    let drain = drain_payload_types();
    let set: HashSet<_> = drain.iter().copied().collect();
    assert_eq!(set.len(), drain.len(), "drain_payload_types contains duplicates");
}

#[test]
fn drain_does_not_overlap_handshake_routes() {
    let drain: HashSet<_> = drain_payload_types().into_iter().collect();
    for h in handshake_payload_types() {
        assert!(!drain.contains(&h), "drain incorrectly subscribes handshake variant {h:?}");
    }
}

#[test]
fn drain_covers_every_non_handshake_payload_type() {
    let drain: HashSet<_> = drain_payload_types().into_iter().collect();
    let handshake: HashSet<_> = handshake_payload_types().into_iter().collect();
    let all: HashSet<_> = all_payload_types().into_iter().collect();

    // Sanity: our local enum mirror matches the upstream count.
    assert_eq!(all.len(), 49, "rusty-kaspa added/removed a payload variant; update all_payload_types()");

    let union: HashSet<_> = drain.union(&handshake).copied().collect();
    let missing: Vec<_> = all.difference(&union).copied().collect();
    assert!(missing.is_empty(), "drain_payload_types does not cover all non-handshake variants; missing: {missing:?}");
}
