use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hickory_proto::op::{Header, MessageType, OpCode, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA, NS, SOA};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_server::authority::MessageResponseBuilder;
use hickory_server::server::{Request, RequestHandler, ResponseHandler, ResponseInfo};
use log::{debug, trace, warn};
use rand::seq::SliceRandom;
use simply_kaspa_dnsseeder_store::{Family, Filter, PeerStore};

use crate::config::DnsConfig;
use crate::rate_limit::RateLimiter;

// musl-libc treats empty AAAA as a hard failure and refuses A fallback;
// `100::` is from the IETF discard prefix so it's harmless if dialed.
const MUSL_AAAA_SENTINEL: std::net::Ipv6Addr = std::net::Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, 0);

const SOA_REFRESH: i32 = 900;
const SOA_RETRY: i32 = 300;
const SOA_EXPIRE: i32 = 604_800;

pub struct SeederHandler {
    config: Arc<DnsConfig>,
    store: PeerStore,
    apex: Name,
    nameserver: Name,
    hostmaster: Name,
    p2p_port: u16,
    rate_limit: Arc<RateLimiter>,
}

impl SeederHandler {
    pub fn new(config: DnsConfig, store: PeerStore) -> Result<Self, hickory_proto::ProtoError> {
        let apex = fqdn(&config.dns_zone)?;
        let nameserver = fqdn(&config.nameserver)?;
        let hostmaster = Name::from_str("hostmaster.")?.append_domain(&apex)?;
        let p2p_port = config.network_id.default_p2p_port();
        let rate_limit = Arc::new(RateLimiter::new(config.queries_per_ip_per_second, config.rate_limit_window));
        Ok(Self { config: Arc::new(config), store, apex, nameserver, hostmaster, p2p_port, rate_limit })
    }

    fn build_answers(&self, qtype: RecordType) -> Vec<Record> {
        match qtype {
            RecordType::A => self.sample_address_records(Family::V4),
            RecordType::AAAA => {
                let mut out = self.sample_address_records(Family::V6);
                if out.is_empty() {
                    out.push(self.address_record(RData::AAAA(AAAA(MUSL_AAAA_SENTINEL))));
                }
                out
            }
            RecordType::NS => vec![self.ns_record()],
            RecordType::SOA => vec![self.soa_record()],
            _ => Vec::new(),
        }
    }

    fn sample_address_records(&self, family: Family) -> Vec<Record> {
        let stale_good_ms = i64::try_from(self.config.stale_good.as_millis()).unwrap_or(i64::MAX);
        let filter = Filter {
            now_ms: now_ms(),
            dead_after_ms: i64::MAX,
            stale_good_ms: Some(stale_good_ms),
            family: Some(family),
            min_protocol_version: self.config.min_protocol_version,
            min_user_agent: self.config.min_user_agent.clone(),
            default_port: Some(self.p2p_port),
        };
        let peers = match self.store.collect_matching(&filter) {
            Ok(v) => v,
            Err(err) => {
                warn!("dns: store lookup failed: {err}");
                return Vec::new();
            }
        };
        let max = self.config.max_records;
        let mut rng = rand::thread_rng();
        let mut out = Vec::with_capacity(max.min(peers.len()));
        for peer in peers.choose_multiple(&mut rng, max) {
            match peer.address.ip {
                IpAddr::V4(v4) => out.push(self.address_record(RData::A(A(v4)))),
                IpAddr::V6(v6) => out.push(self.address_record(RData::AAAA(AAAA(v6)))),
            }
        }
        out
    }

    fn address_record(&self, data: RData) -> Record {
        Record::from_rdata(self.apex.clone(), self.config.ttl_seconds, data)
    }

    fn ns_record(&self) -> Record {
        Record::from_rdata(self.apex.clone(), self.config.ttl_seconds, RData::NS(NS(self.nameserver.clone())))
    }

    fn soa_record(&self) -> Record {
        // RFC 1982 serial-number arithmetic: truncation to u32 is the expected wrap.
        #[allow(clippy::cast_possible_truncation)]
        let serial = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs()
            & u64::from(u32::MAX)) as u32;
        let rdata = SOA::new(
            self.nameserver.clone(),
            self.hostmaster.clone(),
            serial,
            SOA_REFRESH,
            SOA_RETRY,
            SOA_EXPIRE,
            self.config.ttl_seconds,
        );
        Record::from_rdata(self.apex.clone(), self.config.ttl_seconds, RData::SOA(rdata))
    }
}

#[async_trait::async_trait]
impl RequestHandler for SeederHandler {
    async fn handle_request<R: ResponseHandler>(&self, request: &Request, response_handle: R) -> ResponseInfo {
        let src = request.src();

        if request.message_type() != MessageType::Query || request.op_code() != OpCode::Query {
            trace!("dns: rejecting non-query from {src}: type={:?} op={:?}", request.message_type(), request.op_code());
            return refuse(request, response_handle).await;
        }

        let Ok(info) = request.request_info() else {
            return refuse(request, response_handle).await;
        };

        // Silent drop: emit no bytes so the seeder offers zero amplification.
        if !self.rate_limit.check(src.ip()) {
            debug!("dns: rate-limited query from {}", src.ip());
            return no_response();
        }

        let qclass = info.query.query_class();
        if qclass != DNSClass::IN {
            trace!("dns: refusing non-IN class {qclass:?} from {src}");
            return refuse(request, response_handle).await;
        }

        let qtype = info.query.query_type();
        if !is_allowed_type(qtype) {
            trace!("dns: refusing type {qtype:?} from {src}");
            return refuse(request, response_handle).await;
        }

        let qname: Name = info.query.name().into();
        if qname != self.apex {
            return refuse(request, response_handle).await;
        }

        let answers = self.build_answers(qtype);
        let mut header = Header::response_from_request(request.header());
        header.set_authoritative(true);
        header.set_response_code(ResponseCode::NoError);

        let soa = if answers.is_empty() && qtype != RecordType::SOA { vec![self.soa_record()] } else { Vec::new() };
        let ns = if matches!(qtype, RecordType::A | RecordType::AAAA) { vec![self.ns_record()] } else { Vec::new() };

        send(request, response_handle, header, &answers, &ns, &soa).await
    }
}

const fn is_allowed_type(qtype: RecordType) -> bool {
    matches!(qtype, RecordType::A | RecordType::AAAA | RecordType::NS | RecordType::SOA)
}

async fn send<R: ResponseHandler>(
    request: &Request,
    mut response_handle: R,
    header: Header,
    answers: &[Record],
    ns: &[Record],
    soa: &[Record],
) -> ResponseInfo {
    let builder = MessageResponseBuilder::from_message_request(request);
    let resp = builder.build(header, answers.iter(), ns.iter(), soa.iter(), [].iter());
    match response_handle.send_response(resp).await {
        Ok(info) => info,
        Err(err) => {
            debug!("dns: failed to send response: {err}");
            error_info(ResponseCode::ServFail)
        }
    }
}

async fn refuse<R: ResponseHandler>(request: &Request, mut response_handle: R) -> ResponseInfo {
    let builder = MessageResponseBuilder::from_message_request(request);
    match response_handle.send_response(builder.error_msg(request.header(), ResponseCode::Refused)).await {
        Ok(info) => info,
        Err(_) => error_info(ResponseCode::ServFail),
    }
}

// Returned only for hickory's internal logging; no bytes are transmitted.
fn no_response() -> ResponseInfo {
    error_info(ResponseCode::Refused)
}

fn error_info(code: ResponseCode) -> ResponseInfo {
    let mut header = Header::new();
    header.set_response_code(code);
    header.into()
}

fn fqdn(host: &str) -> Result<Name, hickory_proto::ProtoError> {
    let trimmed = host.trim_end_matches('.');
    let mut name = Name::from_str(trimmed)?;
    name.set_fqdn(true);
    Ok(name)
}

fn now_ms() -> i64 {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
}
