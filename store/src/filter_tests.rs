use crate::filter::{Family, Filter};
use crate::record::{NetAddress, PeerRecord};
use semver::Version;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

fn rec(now_ms: i64, ua: &str, port: u16, ipv6: bool, pv: u32) -> PeerRecord {
    PeerRecord {
        id: [1; 16],
        protocol_version: pv,
        network: "kaspa-mainnet".into(),
        services: 0,
        timestamp_ms: now_ms,
        address: NetAddress {
            ip: if ipv6 {
                IpAddr::V6(Ipv6Addr::LOCALHOST)
            } else {
                IpAddr::V4(Ipv4Addr::LOCALHOST)
            },
            port,
        },
        user_agent: ua.into(),
        disable_relay_tx: false,
        subnetwork_id: None,
        first_seen_ms: now_ms,
        last_attempt_ms: now_ms,
        last_success_ms: now_ms,
        last_seen_ms: now_ms,
    }
}

#[test]
fn parses_kaspad_version_basic() {
    let v = PeerRecord::parse_kaspad_version("/kaspad:1.1.0/kaspad:1.1.0(FluxCloud)/").unwrap();
    assert_eq!(v, Version::new(1, 1, 0));
}

#[test]
fn parses_kaspad_version_with_comment() {
    let v = PeerRecord::parse_kaspad_version("/kaspad:0.12.17(MyNode)/").unwrap();
    assert_eq!(v, Version::new(0, 12, 17));
}

#[test]
fn unparseable_user_agent() {
    assert!(PeerRecord::parse_kaspad_version("/foo/bar/").is_none());
}

#[test]
fn rejects_dead_peer() {
    let f = Filter {
        now_ms: 1_000_000,
        dead_after_ms: 100,
        stale_good_ms: None,
        family: None,
        min_protocol_version: None,
        min_user_agent: None,
        default_port: None,
    };
    let mut r = rec(0, "/kaspad:1.0.0/", 16111, false, 7);
    r.last_seen_ms = 0;
    assert!(!f.matches(&r));
}

#[test]
fn stale_good_filter_only_affects_dns() {
    let now = 1_000_000;
    let mut r = rec(now, "/kaspad:1.0.0/", 16111, false, 7);
    r.last_success_ms = now - 1_000;
    let f = Filter {
        now_ms: now,
        dead_after_ms: 7 * 24 * 60 * 60 * 1000,
        stale_good_ms: Some(500),
        family: Some(Family::V4),
        min_protocol_version: None,
        min_user_agent: None,
        default_port: Some(16111),
    };
    assert!(!f.matches(&r), "should fail stale_good");
    let f_http = Filter { stale_good_ms: None, family: None, default_port: None, ..f };
    assert!(f_http.matches(&r));
}

#[test]
fn family_filter() {
    let now = 1_000_000;
    let r_v4 = rec(now, "/kaspad:1.0.0/", 16111, false, 7);
    let r_v6 = rec(now, "/kaspad:1.0.0/", 16111, true, 7);
    let base = Filter {
        now_ms: now,
        dead_after_ms: 1_000_000_000,
        stale_good_ms: None,
        family: Some(Family::V4),
        min_protocol_version: None,
        min_user_agent: None,
        default_port: None,
    };
    assert!(base.matches(&r_v4));
    assert!(!base.matches(&r_v6));
    let f6 = Filter { family: Some(Family::V6), ..base.clone() };
    assert!(f6.matches(&r_v6));
    assert!(!f6.matches(&r_v4));
}

#[test]
fn min_protocol_version_filter() {
    let now = 1_000_000;
    let r = rec(now, "/kaspad:1.0.0/", 16111, false, 7);
    let f = Filter {
        now_ms: now,
        dead_after_ms: 1_000_000_000,
        stale_good_ms: None,
        family: None,
        min_protocol_version: Some(8),
        min_user_agent: None,
        default_port: None,
    };
    assert!(!f.matches(&r));
    let f_ok = Filter { min_protocol_version: Some(7), ..f };
    assert!(f_ok.matches(&r));
}

#[test]
fn min_user_agent_filter() {
    let now = 1_000_000;
    let r = rec(now, "/kaspad:1.1.0/", 16111, false, 7);
    let f = Filter {
        now_ms: now,
        dead_after_ms: 1_000_000_000,
        stale_good_ms: None,
        family: None,
        min_protocol_version: None,
        min_user_agent: Some(Version::new(1, 2, 0)),
        default_port: None,
    };
    assert!(!f.matches(&r));
    let f_ok = Filter { min_user_agent: Some(Version::new(1, 0, 0)), ..f };
    assert!(f_ok.matches(&r));
}

#[test]
fn default_port_only_filter() {
    let now = 1_000_000;
    let r = rec(now, "/kaspad:1.0.0/", 16112, false, 7);
    let f = Filter {
        now_ms: now,
        dead_after_ms: 1_000_000_000,
        stale_good_ms: None,
        family: None,
        min_protocol_version: None,
        min_user_agent: None,
        default_port: Some(16111),
    };
    assert!(!f.matches(&r));
}
