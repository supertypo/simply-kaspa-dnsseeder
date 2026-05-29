//! Renders a stats block into a Vec<String> of info-log lines.

use std::time::Duration;

use kaspa_consensus_core::network::NetworkId;
use simply_kaspa_dnsseeder_crawler::CrawlerSnapshot;
use simply_kaspa_dnsseeder_dns::DnsSnapshot;
use simply_kaspa_dnsseeder_web::WebSnapshot;

use super::format::{count, uptime};

const RULE_TOP: &str = "=========================================================================================================";
const RULE_MID: &str = "  ---------------------------------------------------------------------------------------------------";

pub(super) struct Block {
    pub uptime: Duration,
    pub network: NetworkId,
    pub version: &'static str,
    pub summary_good: u64,
    pub summary_filtered: u64,
    pub summary_stale: u64,
    pub summary_failed: u64,
    pub summary_v4: u64,
    pub summary_v6: u64,
    pub crawler: CrawlerSnapshot,
    pub dns: DnsSnapshot,
    pub web: WebSnapshot,
}

pub(super) fn render(b: &Block) -> Vec<String> {
    let mut out = Vec::with_capacity(16);
    out.push(RULE_TOP.to_string());
    out.push(row(
        "node",
        "up",
        &uptime(b.uptime),
        "network",
        &b.network.to_string(),
        "version",
        b.version,
    ));
    out.push(RULE_MID.to_string());
    out.push(row(
        "peers",
        "good",
        &count(b.summary_good),
        "filtered",
        &count(b.summary_filtered),
        "stale",
        &count(b.summary_stale),
    ));
    out.push(row(
        "",
        "ipv4",
        &count(b.summary_v4),
        "ipv6",
        &count(b.summary_v6),
        "failed",
        &count(b.summary_failed),
    ));
    out.push(RULE_MID.to_string());
    out.push(row(
        "crawler",
        "ok",
        &count(b.crawler.ok),
        "failed",
        &count(b.crawler.failed),
        "in-flight",
        &count(b.crawler.in_flight),
    ));
    out.push(RULE_MID.to_string());
    out.push(row(
        "dns",
        "answered",
        &count(b.dns.answered),
        "empty",
        &count(b.dns.empty),
        "refused",
        &count(b.dns.refused),
    ));
    out.push(row(
        "",
        "A",
        &count(b.dns.a),
        "AAAA",
        &count(b.dns.aaaa),
        "denied",
        &count(b.dns.denied),
    ));
    out.push(RULE_MID.to_string());
    out.push(row(
        "web",
        "requests",
        &count(b.web.requests),
        "accepted",
        &count(b.web.accepted),
        "rejected",
        &count(b.web.rejected),
    ));
    out.push(RULE_TOP.to_string());
    out
}

/// Render a single stats row. `label` is the leftmost section column (blank on continuation rows).
fn row(label: &str, k1: &str, v1: &str, k2: &str, v2: &str, k3: &str, v3: &str) -> String {
    format!("  {label:<8}{k1:<9} {v1:<17} \u{2502} {k2:<9} {v2:<17} \u{2502} {k3:<9} {v3}")
}
