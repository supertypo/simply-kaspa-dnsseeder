//! Probe execution and outcome persistence.
//!
//! Pure functions that drive one [`Probe`] invocation, apply the result to
//! the [`PeerStore`], and seed any advertised peers as stubs for future
//! scheduling. Used by both the worker pool and the web crate's manual
//! `/submit` path.

use std::net::SocketAddr;

use kaspa_utils::networking::IpAddress;
use log::{debug, info, warn};
use simply_kaspa_dnsseeder_common::{canonicalize_ip, now_ms};
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord, PeerStore};

use crate::error::ProbeError;
use crate::metrics::CrawlerMetrics;
use crate::model::{ProbeResult, is_acceptable_address, peer_record_from_version};
use crate::probe::Probe;

/// Probe a peer, record the outcome, and seed any advertised addresses as stubs.
pub(crate) async fn probe_one(
    probe: &dyn Probe,
    store: &PeerStore,
    addr: SocketAddr,
    default_port: u16,
    strict_port: bool,
    metrics: Option<&CrawlerMetrics>,
) {
    match probe.probe(addr).await {
        Ok(result) => {
            if let Some(m) = metrics {
                m.record_ok();
            }
            debug!(
                "crawler: probe {addr} succeeded (protocol={}, ua={:?})",
                result.version.protocol_version, result.version.user_agent
            );
            if let Err(err) = apply_success(store, addr, &result).await {
                warn!("crawler: failed to persist successful probe of {addr}: {err}");
            }
            let agent_label = match PeerRecord::parse_kaspad_version(&result.version.user_agent) {
                Some(v) => format!("kaspad:{v}"),
                None => format!("{:?}", result.version.user_agent),
            };
            let discovered = result.addresses.len();
            let new_stubs = insert_discovered_stubs(store, result.addresses, default_port, strict_port).await;
            info!("crawler: probe {addr} ({agent_label}): received {discovered} address(es), {new_stubs} new");
        }
        Err(err) => {
            if let Some(m) = metrics {
                m.record_failed();
                m.record_failed_kind(&err);
            }
            debug!("crawler: probe {addr} failed: {err}");
            // `last_attempt` was already bumped by the scheduler before dispatch.
        }
    }
}

/// Run a single probe synchronously, used by the web crate to handle HTTP
/// submissions through the same code path as scheduled probes.
pub async fn probe_and_store(probe: &dyn Probe, store: &PeerStore, addr: SocketAddr) -> Result<PeerRecord, ProbeError> {
    match probe.probe(addr).await {
        Ok(result) => apply_success(store, addr, &result)
            .await
            .map_err(|e| ProbeError::Connection(e.to_string())),
        Err(err) => {
            let net = net_from(addr);
            let now = now_ms();
            let _ = store.blocking(move |s| s.record_attempt(&net, now).map(|_| ())).await;
            Err(err)
        }
    }
}

/// Persist a successful probe: upsert the record while preserving prior history.
pub(crate) async fn apply_success(
    store: &PeerStore,
    addr: SocketAddr,
    result: &ProbeResult,
) -> Result<PeerRecord, simply_kaspa_dnsseeder_store::Error> {
    let net = net_from(addr);
    let version = result.version.clone();
    store
        .blocking(move |s| {
            let existing = s.get(&net)?;
            let record = peer_record_from_version(addr, &version, now_ms(), existing.as_ref());
            s.upsert(&record)?;
            Ok(record)
        })
        .await
}

/// Insert routable advertised addresses as stubs and return the count of newly inserted records.
async fn insert_discovered_stubs(store: &PeerStore, addresses: Vec<(IpAddress, u16)>, default_port: u16, strict_port: bool) -> usize {
    let now = now_ms();
    store
        .blocking(move |s| {
            let mut inserted = 0usize;
            for (ip_addr, port) in &addresses {
                let port = if *port == 0 { default_port } else { *port };
                let ip: std::net::IpAddr = (*ip_addr).into();
                let canonical = canonicalize_ip(ip);
                let net = NetAddress { ip: canonical, port };
                if !is_acceptable_address(&net, default_port, strict_port) {
                    continue;
                }
                match s.insert_stub_if_missing(&net, now) {
                    Ok(true) => inserted += 1,
                    Ok(false) => {}
                    Err(err) => warn!("crawler: failed to insert stub for {canonical}:{port}: {err}"),
                }
            }
            inserted
        })
        .await
}

pub(crate) fn net_from(addr: SocketAddr) -> NetAddress {
    NetAddress {
        ip: canonicalize_ip(addr.ip()),
        port: addr.port(),
    }
}
