use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hickory_proto::op::{Header, HeaderCounts, MessageType, Metadata, OpCode, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA, NS, SOA};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_server::server::{Request, RequestHandler, RequestInfo, ResponseHandler, ResponseInfo};
use hickory_server::zone_handler::MessageResponseBuilder;
use log::{debug, trace};
use rand::seq::IndexedRandom;
use simply_kaspa_dnsseeder_common::RateLimiter;
use simply_kaspa_dnsseeder_store::Family;

use crate::config::DnsConfig;
use crate::metrics::DnsMetrics;
use crate::serving_cache::ServingCache;

// musl-libc treats an empty AAAA reply as a hard failure (no A fallback).
// `100::` is the IETF discard prefix — harmless if dialed.
const MUSL_AAAA_SENTINEL: std::net::Ipv6Addr = std::net::Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, 0, 0);

const SOA_REFRESH: i32 = 900;
const SOA_RETRY: i32 = 300;
const SOA_EXPIRE: i32 = 604_800;

// Per-record TTLs mirror the Go seeder: short for answer records so clients
// refresh stale peers quickly, long for the NS RRset which rarely changes.
const A_TTL_SECONDS: u32 = 30;
const NS_TTL_SECONDS: u32 = 86_400;

struct QueryPlan {
    qtype: RecordType,
    qname: Name,
}

struct Answer {
    answers: Vec<Record>,
    ns: Vec<Record>,
    soa: Vec<Record>,
}

pub struct SeederHandler {
    config: Arc<DnsConfig>,
    serving: Arc<ServingCache>,
    apex: Name,
    nameserver: Name,
    hostmaster: Name,
    rate_limit: Arc<RateLimiter>,
    metrics: Arc<DnsMetrics>,
}

impl SeederHandler {
    pub fn new(config: DnsConfig, serving: Arc<ServingCache>) -> Result<Self, hickory_proto::ProtoError> {
        Self::with_metrics(config, serving, Arc::new(DnsMetrics::new()))
    }

    pub fn with_metrics(
        config: DnsConfig,
        serving: Arc<ServingCache>,
        metrics: Arc<DnsMetrics>,
    ) -> Result<Self, hickory_proto::ProtoError> {
        let apex = fqdn(&config.dns_zone)?;
        let nameserver = fqdn(&config.nameserver)?;
        let hostmaster = Name::from_str("hostmaster.")?.append_domain(&apex)?;
        let rate_limit = Arc::new(RateLimiter::new(config.queries_per_ip_per_second, config.rate_limit_window));
        Ok(Self {
            config: Arc::new(config),
            serving,
            apex,
            nameserver,
            hostmaster,
            rate_limit,
            metrics,
        })
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<DnsMetrics> {
        self.metrics.clone()
    }

    #[must_use]
    pub fn rate_limiter(&self) -> Arc<RateLimiter> {
        self.rate_limit.clone()
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

    /// Protocol-layer query validation. Records `refused` metrics on failure.
    fn parse_basic<'a>(&self, request: &'a Request) -> Result<RequestInfo<'a>, ()> {
        if request.metadata.message_type != MessageType::Query || request.metadata.op_code != OpCode::Query {
            trace!(
                "dns: rejecting non-query from {}: type={:?} op={:?}",
                request.src(),
                request.metadata.message_type,
                request.metadata.op_code
            );
            self.metrics.record_refused();
            return Err(());
        }
        request.request_info().map_err(|_| {
            self.metrics.record_refused();
        })
    }

    /// Content validation: class/type/name match what this zone serves.
    fn classify(&self, info: &RequestInfo<'_>, src: std::net::SocketAddr) -> Result<QueryPlan, ()> {
        let qclass = info.query.query_class();
        if qclass != DNSClass::IN {
            trace!("dns: refusing non-IN class {qclass:?} from {src}");
            self.metrics.record_refused();
            return Err(());
        }
        let qtype = info.query.query_type();
        if !is_allowed_type(qtype) {
            trace!("dns: refusing type {qtype:?} from {src}");
            self.metrics.record_refused();
            return Err(());
        }
        let qname: Name = info.query.name().into();
        if qname != self.apex {
            self.metrics.record_refused();
            return Err(());
        }
        Ok(QueryPlan { qtype, qname })
    }

    /// Assemble response sections (answers + authority NS + SOA) for a validated plan.
    fn build_answer(&self, qtype: RecordType) -> Answer {
        let answers = self.build_answers(qtype);
        let soa = if answers.is_empty() && qtype != RecordType::SOA {
            vec![self.soa_record()]
        } else {
            Vec::new()
        };
        let ns = if matches!(qtype, RecordType::A | RecordType::AAAA) {
            vec![self.ns_record()]
        } else {
            Vec::new()
        };
        Answer { answers, ns, soa }
    }

    fn sample_address_records(&self, family: Family) -> Vec<Record> {
        let snap = self.serving.load();
        let pool: &[IpAddr] = match family {
            Family::V4 => &snap.v4,
            Family::V6 => &snap.v6,
        };
        let max = self.config.max_records;
        let mut rng = rand::rng();
        pool.sample(&mut rng, max)
            .map(|ip| match *ip {
                IpAddr::V4(v4) => self.address_record(RData::A(A(v4))),
                IpAddr::V6(v6) => self.address_record(RData::AAAA(AAAA(v6))),
            })
            .collect()
    }

    fn address_record(&self, data: RData) -> Record {
        Record::from_rdata(self.apex.clone(), A_TTL_SECONDS, data)
    }

    fn ns_record(&self) -> Record {
        Record::from_rdata(self.apex.clone(), NS_TTL_SECONDS, RData::NS(NS(self.nameserver.clone())))
    }

    fn soa_record(&self) -> Record {
        // RFC 1982 serial-number arithmetic: truncation to u32 is the expected wrap.
        #[allow(clippy::cast_possible_truncation)]
        let serial = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs() & u64::from(u32::MAX)) as u32;
        let rdata = SOA::new(
            self.nameserver.clone(),
            self.hostmaster.clone(),
            serial,
            SOA_REFRESH,
            SOA_RETRY,
            SOA_EXPIRE,
            A_TTL_SECONDS,
        );
        Record::from_rdata(self.apex.clone(), A_TTL_SECONDS, RData::SOA(rdata))
    }
}

#[async_trait::async_trait]
impl RequestHandler for SeederHandler {
    async fn handle_request<R: ResponseHandler, T: hickory_server::net::runtime::Time>(
        &self,
        request: &Request,
        response_handle: R,
    ) -> ResponseInfo {
        let src = request.src();

        let Ok(info) = self.parse_basic(request) else {
            return refuse(request, response_handle).await;
        };

        // Silent drop: emit no bytes so the seeder offers zero amplification.
        if !self.rate_limit.check(src.ip()) {
            debug!("dns: rate-limited query from {}", src.ip());
            self.metrics.record_denied();
            return no_response();
        }

        let Ok(plan) = self.classify(&info, src) else {
            return refuse(request, response_handle).await;
        };

        let answer = self.build_answer(plan.qtype);
        self.metrics
            .record_answered(plan.qtype == RecordType::A, plan.qtype == RecordType::AAAA, answer.answers.len());
        debug!(
            "dns: answered {:?} for {} from {src} with {} record(s)",
            plan.qtype,
            plan.qname,
            answer.answers.len()
        );

        let mut metadata = Metadata::response_from_request(&request.metadata);
        metadata.authoritative = true;
        metadata.response_code = ResponseCode::NoError;

        send(request, response_handle, metadata, &answer.answers, &answer.ns, &answer.soa).await
    }
}

const fn is_allowed_type(qtype: RecordType) -> bool {
    matches!(qtype, RecordType::A | RecordType::AAAA | RecordType::NS | RecordType::SOA)
}

async fn send<R: ResponseHandler>(
    request: &Request,
    mut response_handle: R,
    metadata: Metadata,
    answers: &[Record],
    ns: &[Record],
    soa: &[Record],
) -> ResponseInfo {
    let builder = MessageResponseBuilder::from_message_request(request);
    let resp = builder.build(metadata, answers.iter(), ns.iter(), soa.iter(), [].iter());
    response_handle.send_response(resp).await.unwrap_or_else(|err| {
        debug!("dns: failed to send response: {err}");
        error_info(ResponseCode::ServFail)
    })
}

async fn refuse<R: ResponseHandler>(request: &Request, mut response_handle: R) -> ResponseInfo {
    let builder = MessageResponseBuilder::from_message_request(request);
    response_handle
        .send_response(builder.error_msg(&request.metadata, ResponseCode::Refused))
        .await
        .unwrap_or_else(|_| error_info(ResponseCode::ServFail))
}

// Returned only for hickory's internal logging; no bytes are transmitted.
fn no_response() -> ResponseInfo {
    error_info(ResponseCode::Refused)
}

fn error_info(code: ResponseCode) -> ResponseInfo {
    let mut metadata = Metadata::new(0, MessageType::Response, OpCode::Query);
    metadata.response_code = code;
    Header {
        metadata,
        counts: HeaderCounts::default(),
    }
    .into()
}

fn fqdn(host: &str) -> Result<Name, hickory_proto::ProtoError> {
    let trimmed = host.trim_end_matches('.');
    let mut name = Name::from_str(trimmed)?;
    name.set_fqdn(true);
    Ok(name)
}
