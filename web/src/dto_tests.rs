use std::net::{IpAddr, Ipv4Addr};

use serde_json::Value;
use simply_kaspa_dnsseeder_store::{NetAddress, PeerRecord};

use crate::dto::PeerDto;

const DEFAULT_PORT: u16 = 16111;

fn rec_with_ua(ua: &str) -> PeerRecord {
    PeerRecord {
        id: [0x11; 16],
        protocol_version: 7,
        timestamp_ms: 0,
        address: NetAddress {
            ip: IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
            port: 16111,
        },
        user_agent: ua.to_string(),
        subnetwork_id: None,
        first_seen_ms: 1_700_000_000_000,
        last_attempt_ms: 1_700_000_010_000,
        last_success_ms: 1_700_000_020_000,
        last_seen_ms: 1_700_000_030_000,
    }
}

#[test]
fn public_view_exposes_only_anonymous_fields() {
    let rec = rec_with_ua("/kaspad:1.1.0/");
    let dto = PeerDto::from_record(&rec, false, DEFAULT_PORT);
    let json: Value = serde_json::to_value(&dto).unwrap();
    let obj = json.as_object().expect("object");

    let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
    let expected: std::collections::BTreeSet<&str> = [
        "protocolVersion",
        "userAgent",
        "kaspadVersion",
        "port",
        "defaultPort",
        "lastSeenMs",
        "lastSeen",
    ]
    .into_iter()
    .collect();
    assert_eq!(keys, expected);
    assert_eq!(obj["port"], 16111);
    assert_eq!(obj["protocolVersion"], 7);
    assert_eq!(obj["defaultPort"], true);
}

#[test]
fn full_view_exposes_all_fields() {
    let rec = rec_with_ua("/kaspad:1.1.0/");
    let dto = PeerDto::from_record(&rec, true, DEFAULT_PORT);
    let json: Value = serde_json::to_value(&dto).unwrap();
    let obj = json.as_object().expect("object");

    for k in [
        "id",
        "protocolVersion",
        "userAgent",
        "kaspadVersion",
        "ip",
        "port",
        "defaultPort",
        "firstSeenMs",
        "lastSeenMs",
        "lastAttemptMs",
        "lastSuccessMs",
        "firstSeen",
        "lastSeen",
        "lastAttempt",
        "lastSuccess",
    ] {
        assert!(obj.contains_key(k), "missing key {k}");
    }
    assert_eq!(obj["ip"], "1.2.3.4");
    assert_eq!(obj["id"], "11111111111111111111111111111111");
    assert_eq!(obj["defaultPort"], true);
}

#[test]
fn default_port_is_false_when_port_differs() {
    let mut rec = rec_with_ua("/kaspad:1.1.0/");
    rec.address.port = 16112;
    let json: Value = serde_json::to_value(PeerDto::from_record(&rec, true, DEFAULT_PORT)).unwrap();
    assert_eq!(json["defaultPort"], false);
    assert_eq!(json["port"], 16112);
}

#[test]
fn iso_timestamps_use_seconds_precision_with_z_suffix() {
    let rec = rec_with_ua("/kaspad:1.1.0/");
    let dto = PeerDto::from_record(&rec, true, DEFAULT_PORT);
    let json: Value = serde_json::to_value(&dto).unwrap();
    for field in ["firstSeen", "lastSeen", "lastAttempt", "lastSuccess"] {
        let s = json[field].as_str().unwrap();
        assert!(s.ends_with('Z'), "{field} = {s} (expected trailing Z)");
        assert!(!s.contains('.'), "{field} = {s} (expected seconds precision, no fractional)");
        assert_eq!(s.len(), 20, "{field} = {s} (expected `YYYY-MM-DDTHH:MM:SSZ`)");
    }
}

#[test]
fn kaspad_version_preserves_prerelease_suffix() {
    let rec = rec_with_ua("/kaspad:1.2.1-toc.3/");
    let json: Value = serde_json::to_value(PeerDto::from_record(&rec, false, DEFAULT_PORT)).unwrap();
    assert_eq!(json["kaspadVersion"], "1.2.1-toc.3");
}

#[test]
fn kaspad_version_is_null_when_unparseable() {
    let rec = rec_with_ua("/something-else/");
    let json: Value = serde_json::to_value(PeerDto::from_record(&rec, true, DEFAULT_PORT)).unwrap();
    assert_eq!(json["kaspadVersion"], Value::Null);
}
