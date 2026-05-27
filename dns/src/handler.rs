//! [`SeederHandler`] — implements `hickory_server::server::RequestHandler` and
//! serves the apex zone defined by [`crate::DnsConfig`].

use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hickory_proto::op::{Header, MessageType, OpCode, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA, NS, SOA};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use hickory_server::authority::MessageResponseBuilder;
use hickory_server::server::{Request, RequestHandler, ResponseHandler, ResponseInfo};
use log::{debug, trace, warn};
use simply_kaspa_dnsseeder_store::{Family, Filter, PeerStore};

use crate::config::DnsConfig;

/// IPv6 sentinel returned when there are no real AAAA records. musl-libc's
/// resolver treats an empty `AAAA` response as a hard failure and refuses to
/// fall back to A; returning `100::` (a discard-prefix address) keeps it happy
/// while remaining unrouteable.
const MUSL_AAAA_SENTINEL: &str = "100::";

pub struct SeederHandler {
    config: Arc<DnsConfig>,
    store: PeerStore,
    apex: Name,
    nameserver: Name,
    p2p_port: u16,
}

impl SeederHandler {
    /// Build a handler. `dns_host`/`nameserver` are converted to
    /// fully-qualified [`Name`]s once and reused for every query.
    pub fn new(config: DnsConfig, store: PeerStore) -> Result<Self, hickory_proto::ProtoError> {
        let apex = fqdn(&config.dns_host)?;
        let nameserver = fqdn(&config.nameserver)?;
        let p2p_port = config.network_id.default_p2p_port();
        Ok(Self { config: Arc::new(config), store, apex, nameserver, p2p_port })
    }

    fn build_answers(&self, qtype: RecordType) -> Vec<Record> {
        match qtype {
            RecordType::A => self.build_a_records(),
            RecordType::AAAA => self.build_aaaa_records(),
            RecordType::NS => vec![self.ns_record()],
            RecordType::SOA => vec![self.soa_record()],
            _ => Vec::new(),
        }
    }

    fn build_a_records(&self) -> Vec<Record> {
        let filter = Filter {
            now_ms: now_ms(),
            dead_after_ms: i64::MAX,
            stale_good_ms: None,
            family: Some(Family::V4),
            min_protocol_version: None,
            min_user_agent: None,
            default_port: Some(self.p2p_port),
        };
        let mut peers = match self.store.collect_matching(&filter) {
            Ok(v) => v,
            Err(err) => {
                warn!("dns: store lookup failed: {err}");
                return Vec::new();
            }
        };
        peers.sort_by_key(|p| std::cmp::Reverse(p.last_success_ms));
        peers
            .into_iter()
            .take(self.config.max_records)
            .filter_map(|p| match p.address.ip {
                IpAddr::V4(v4) => Some(Record::from_rdata(self.apex.clone(), self.config.ttl_seconds, RData::A(A(v4)))),
                IpAddr::V6(_) => None,
            })
            .collect()
    }

    fn build_aaaa_records(&self) -> Vec<Record> {
        let filter = Filter {
            now_ms: now_ms(),
            dead_after_ms: i64::MAX,
            stale_good_ms: None,
            family: Some(Family::V6),
            min_protocol_version: None,
            min_user_agent: None,
            default_port: Some(self.p2p_port),
        };
        let mut peers = match self.store.collect_matching(&filter) {
            Ok(v) => v,
            Err(err) => {
                warn!("dns: store lookup failed: {err}");
                return Vec::new();
            }
        };
        peers.sort_by_key(|p| std::cmp::Reverse(p.last_success_ms));
        let mut out: Vec<Record> = peers
            .into_iter()
            .take(self.config.max_records)
            .filter_map(|p| match p.address.ip {
                IpAddr::V6(v6) => Some(Record::from_rdata(self.apex.clone(), self.config.ttl_seconds, RData::AAAA(AAAA(v6)))),
                IpAddr::V4(_) => None,
            })
            .collect();
        if out.is_empty() {
            let sentinel = std::net::Ipv6Addr::from_str(MUSL_AAAA_SENTINEL).expect("static sentinel parses");
            out.push(Record::from_rdata(self.apex.clone(), self.config.ttl_seconds, RData::AAAA(AAAA(sentinel))));
        }
        out
    }

    fn ns_record(&self) -> Record {
        Record::from_rdata(self.apex.clone(), self.config.ttl_seconds, RData::NS(NS(self.nameserver.clone())))
    }

    fn soa_record(&self) -> Record {
        let serial = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs() as u32;
        let rdata = SOA::new(
            self.nameserver.clone(),
            Name::from_str("hostmaster.").unwrap().append_domain(&self.apex).expect("apex is valid"),
            serial,
            900,
            300,
            604_800,
            self.config.ttl_seconds,
        );
        Record::from_rdata(self.apex.clone(), self.config.ttl_seconds, RData::SOA(rdata))
    }
}

#[async_trait::async_trait]
impl RequestHandler for SeederHandler {
    async fn handle_request<R: ResponseHandler>(&self, request: &Request, mut response_handle: R) -> ResponseInfo {
        if request.message_type() != MessageType::Query || request.op_code() != OpCode::Query {
            return refuse(request, response_handle).await;
        }

        let Ok(info) = request.request_info() else {
            return refuse(request, response_handle).await;
        };

        let qname: Name = info.query.name().into();
        let qtype = info.query.query_type();
        trace!("dns: query {qname} {qtype:?}");

        if qname != self.apex {
            return refuse(request, response_handle).await;
        }

        let answers = self.build_answers(qtype);
        let mut header = Header::response_from_request(request.header());
        header.set_authoritative(true);
        header.set_response_code(ResponseCode::NoError);

        let soa = if answers.is_empty() && qtype != RecordType::SOA { vec![self.soa_record()] } else { Vec::new() };
        let ns = match qtype {
            RecordType::A | RecordType::AAAA => vec![self.ns_record()],
            _ => Vec::new(),
        };

        let builder = MessageResponseBuilder::from_message_request(request);
        let resp = builder.build(header, answers.iter(), ns.iter(), soa.iter(), [].iter());
        match response_handle.send_response(resp).await {
            Ok(info) => info,
            Err(err) => {
                debug!("dns: failed to send response: {err}");
                serve_failed()
            }
        }
    }
}

async fn refuse<R: ResponseHandler>(request: &Request, mut response_handle: R) -> ResponseInfo {
    let builder = MessageResponseBuilder::from_message_request(request);
    match response_handle.send_response(builder.error_msg(request.header(), ResponseCode::Refused)).await {
        Ok(info) => info,
        Err(_) => serve_failed(),
    }
}

fn serve_failed() -> ResponseInfo {
    let mut header = Header::new();
    header.set_response_code(ResponseCode::ServFail);
    header.into()
}

fn fqdn(host: &str) -> Result<Name, hickory_proto::ProtoError> {
    let trimmed = host.trim_end_matches('.');
    let mut name = Name::from_str(trimmed)?;
    name.set_fqdn(true);
    Ok(name)
}

fn now_ms() -> i64 {
    chrono_like_now_ms()
}

// Avoid pulling chrono into the dns crate for one call.
fn chrono_like_now_ms() -> i64 {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}
