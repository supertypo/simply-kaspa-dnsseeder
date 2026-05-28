use crate::filter::Filter;
use crate::peer_store::{PeerStore, UNKNOWN_PEER_ID};
use crate::record::{NetAddress, PeerRecord};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tempfile::tempdir;

fn make_rec(id_byte: u8, ip: Ipv4Addr, port: u16, last_seen_ms: i64) -> PeerRecord {
    PeerRecord {
        id: [id_byte; 16],
        protocol_version: 7,
        timestamp_ms: last_seen_ms,
        address: NetAddress { ip: IpAddr::V4(ip), port },
        user_agent: "/kaspad:1.0.0/".into(),
        subnetwork_id: None,
        first_seen_ms: last_seen_ms,
        last_attempt_ms: last_seen_ms,
        last_success_ms: last_seen_ms,
        last_seen_ms,
    }
}

fn open_temp_store() -> (tempfile::TempDir, PeerStore) {
    let dir = tempdir().unwrap();
    let store = PeerStore::open(dir.path().join("peers.redb")).unwrap();
    (dir, store)
}

#[test]
fn upsert_and_get_roundtrip() {
    let (_dir, store) = open_temp_store();
    let r = make_rec(1, Ipv4Addr::new(1, 2, 3, 4), 16111, 100);
    store.upsert(&r).unwrap();
    let got = store.get(&r.address).unwrap().unwrap();
    assert_eq!(got, r);
}

#[test]
fn upsert_overwrites_id_for_same_address() {
    let (_dir, store) = open_temp_store();
    let r1 = make_rec(2, Ipv4Addr::new(1, 1, 1, 1), 16111, 100);
    store.upsert(&r1).unwrap();
    let r2 = make_rec(3, Ipv4Addr::new(1, 1, 1, 1), 16111, 200);
    store.upsert(&r2).unwrap();
    let got = store.get(&r1.address).unwrap().unwrap();
    assert_eq!(got.id, [3; 16]);
    assert_eq!(got.last_seen_ms, 200);
    assert_eq!(store.len().unwrap(), 1);
}

#[test]
fn upsert_distinguishes_by_address() {
    let (_dir, store) = open_temp_store();
    let r1 = make_rec(2, Ipv4Addr::new(1, 1, 1, 1), 16111, 100);
    let r2 = make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16112, 200);
    store.upsert(&r1).unwrap();
    store.upsert(&r2).unwrap();
    assert_eq!(store.len().unwrap(), 2);
    assert_eq!(store.get(&r1.address).unwrap().unwrap().last_seen_ms, 100);
    assert_eq!(store.get(&r2.address).unwrap().unwrap().last_seen_ms, 200);
}

#[test]
fn delete_removes_only_one() {
    let (_dir, store) = open_temp_store();
    let r1 = make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 100);
    let r2 = make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16111, 100);
    store.upsert(&r1).unwrap();
    store.upsert(&r2).unwrap();
    assert!(store.delete(&r1.address).unwrap());
    assert!(store.get(&r1.address).unwrap().is_none());
    assert!(store.get(&r2.address).unwrap().is_some());
    assert_eq!(store.len().unwrap(), 1);
}

#[test]
fn prune_dead_removes_old_seen() {
    let (_dir, store) = open_temp_store();
    let r1 = make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 100);
    let r2 = make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16111, 500);
    store.upsert(&r1).unwrap();
    store.upsert(&r2).unwrap();
    let removed = store.prune_dead(200).unwrap();
    assert_eq!(removed, 1);
    assert!(store.get(&r1.address).unwrap().is_none());
    assert!(store.get(&r2.address).unwrap().is_some());
}

#[test]
fn prune_dead_keeps_recent_never_seen() {
    let (_dir, store) = open_temp_store();
    let addr = NetAddress {
        ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        port: 16111,
    };
    store.record_attempt(&addr, 1_000).unwrap();
    let removed = store.prune_dead(500).unwrap();
    assert_eq!(removed, 0);
    assert!(store.get(&addr).unwrap().is_some());
}

#[test]
fn prune_dead_removes_old_never_seen() {
    let (_dir, store) = open_temp_store();
    let addr = NetAddress {
        ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        port: 16111,
    };
    store.record_attempt(&addr, 100).unwrap();
    let removed = store.prune_dead(500).unwrap();
    assert_eq!(removed, 1);
    assert!(store.get(&addr).unwrap().is_none());
}

#[test]
fn record_attempt_creates_stub() {
    let (_dir, store) = open_temp_store();
    let addr = NetAddress {
        ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        port: 16111,
    };
    let rec = store.record_attempt(&addr, 1234).unwrap();
    assert_eq!(rec.id, UNKNOWN_PEER_ID);
    assert_eq!(rec.address, addr);
    assert_eq!(rec.first_seen_ms, 1234);
    assert_eq!(rec.last_attempt_ms, 1234);
    assert_eq!(rec.last_success_ms, 0);
    assert_eq!(rec.last_seen_ms, 0);
}

#[test]
fn record_attempt_preserves_existing_success_fields() {
    let (_dir, store) = open_temp_store();
    let r = make_rec(1, Ipv4Addr::new(1, 2, 3, 4), 16111, 100);
    store.upsert(&r).unwrap();
    let rec = store.record_attempt(&r.address, 999).unwrap();
    assert_eq!(rec.id, [1; 16]);
    assert_eq!(rec.first_seen_ms, 100);
    assert_eq!(rec.last_success_ms, 100);
    assert_eq!(rec.last_seen_ms, 100);
    assert_eq!(rec.last_attempt_ms, 999);
}

#[test]
fn insert_stub_if_missing_creates_then_noops() {
    let (_dir, store) = open_temp_store();
    let addr = NetAddress {
        ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)),
        port: 16111,
    };

    assert!(store.insert_stub_if_missing(&addr, 100).unwrap());
    let r1 = store.get(&addr).unwrap().expect("stub present");
    assert_eq!(r1.id, UNKNOWN_PEER_ID);
    assert_eq!(r1.first_seen_ms, 100);
    assert_eq!(r1.last_seen_ms, 100);
    assert_eq!(r1.last_attempt_ms, 0);
    assert_eq!(r1.last_success_ms, 0);

    assert!(!store.insert_stub_if_missing(&addr, 999).unwrap());
    let r2 = store.get(&addr).unwrap().expect("stub still present");
    assert_eq!(r2, r1, "existing record must be untouched");
}

#[test]
fn insert_stub_if_missing_does_not_touch_existing_record() {
    let (_dir, store) = open_temp_store();
    let r = make_rec(1, Ipv4Addr::new(7, 7, 7, 7), 16111, 100);
    store.upsert(&r).unwrap();
    assert!(!store.insert_stub_if_missing(&r.address, 999).unwrap());
    let got = store.get(&r.address).unwrap().unwrap();
    assert_eq!(got, r);
}

#[test]
fn collect_matching_applies_filter() {
    let (_dir, store) = open_temp_store();
    store.upsert(&make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 100)).unwrap();
    store.upsert(&make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16111, 500)).unwrap();
    let f = Filter {
        now_ms: 600,
        dead_after_ms: 200,
        stale_good_ms: None,
        family: None,
        min_protocol_version: None,
        min_user_agent: None,
        default_port: None,
    };
    let mut recs = store.collect_matching(&f).unwrap();
    recs.sort_by_key(|r| r.id);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].id, [2; 16]);
}

#[test]
fn is_empty_initially() {
    let (_dir, store) = open_temp_store();
    assert!(store.is_empty().unwrap());
    store.upsert(&make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 100)).unwrap();
    assert!(!store.is_empty().unwrap());
}

#[test]
fn summary_v4_v6_counts_only_good_subset() {
    let (_dir, store) = open_temp_store();
    // Two v4 peers within stale_good window.
    let mut r1 = make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 500);
    r1.last_success_ms = 500;
    let mut r2 = make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16111, 500);
    r2.last_success_ms = 500;
    // One v4 peer that is stale (success too old).
    let mut r3 = make_rec(3, Ipv4Addr::new(3, 3, 3, 3), 16111, 50);
    r3.last_success_ms = 50;
    // One v4 peer that never succeeded.
    let mut r4 = make_rec(4, Ipv4Addr::new(4, 4, 4, 4), 16111, 500);
    r4.last_success_ms = 0;
    // One v6 peer within window.
    let mut r6 = make_rec(5, Ipv4Addr::UNSPECIFIED, 16111, 500);
    r6.address.ip = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
    r6.last_success_ms = 500;
    for r in [&r1, &r2, &r3, &r4, &r6] {
        store.upsert(r).unwrap();
    }
    // stale_good = 200 → success_ms in (now-200 ..= now) is "good".
    let s = store.summary(600, 200).unwrap();
    assert_eq!(s.total, 5);
    assert_eq!(s.good, 3, "good = 2 v4 + 1 v6 within window");
    assert_eq!(s.v4, 2, "v4 counts only good subset");
    assert_eq!(s.v6, 1, "v6 counts only good subset");
    assert_eq!(s.failed, 2, "stale-good + never-succeeded both count as failed");
}

#[test]
fn due_for_probe_returns_oldest_first_and_respects_max() {
    let (_dir, store) = open_temp_store();
    // 5 peers, all "good class" (last_success_ms > 0), different last_attempt_ms.
    let mut recs = Vec::new();
    for (i, last_attempt) in [50_i64, 10, 100, 30, 70].into_iter().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let mut r = make_rec(i as u8 + 1, Ipv4Addr::new(10, 0, 0, i as u8 + 1), 16111, 1_000);
        r.last_attempt_ms = last_attempt;
        r.last_success_ms = last_attempt;
        store.upsert(&r).unwrap();
        recs.push(r);
    }
    // now=10_000, stale_good=100 → eligible iff last_attempt_ms <= 9_900.
    // All five qualify; ordering should be ascending by last_attempt_ms: 10, 30, 50, 70, 100.
    let out = store.due_for_probe(10_000, 100, 1_000, 0, 3).unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].last_attempt_ms, 10);
    assert_eq!(out[1].last_attempt_ms, 30);
    assert_eq!(out[2].last_attempt_ms, 50);
}

#[test]
fn due_for_probe_stops_at_threshold() {
    let (_dir, store) = open_temp_store();
    let mut r_old = make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 1_000);
    r_old.last_attempt_ms = 0;
    r_old.last_success_ms = 0; // bad class → needs stale_bad_ms (=500) elapsed
    let mut r_recent = make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16111, 1_000);
    r_recent.last_attempt_ms = 990; // very recent
    r_recent.last_success_ms = 990;
    store.upsert(&r_old).unwrap();
    store.upsert(&r_recent).unwrap();
    // now=1_000, stale_good=100 → recent (since_attempt=10) cannot be eligible.
    let out = store.due_for_probe(1_000, 100, 500, 0, 10).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].address.ip, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
}

#[test]
fn due_for_probe_filters_bad_class_within_stale_bad_window() {
    let (_dir, store) = open_temp_store();
    // Bad-class (never succeeded), attempted 200ms ago.
    let mut r = make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 1_000);
    r.last_attempt_ms = 800;
    r.last_success_ms = 0;
    store.upsert(&r).unwrap();
    // stale_good=100, stale_bad=500, now=1_000 → since_attempt=200, threshold=500 → NOT eligible.
    let out = store.due_for_probe(1_000, 100, 500, 0, 10).unwrap();
    assert!(out.is_empty(), "bad-class peer still inside stale_bad window must not be returned");
}

#[test]
fn record_attempt_updates_index_position() {
    let (_dir, store) = open_temp_store();
    let net = NetAddress { ip: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), port: 16111 };
    // Initial insert via record_attempt at t=0.
    store.record_attempt(&net, 0).unwrap();
    // Promote to good class so future eligibility is based on stale_good only.
    let mut rec = store.get(&net).unwrap().unwrap();
    rec.last_success_ms = 0; // keep bad class for simplicity
    store.upsert(&rec).unwrap();
    // At now=10_000, attempt was at 0, since_attempt=10_000 ≥ stale_bad=500 → eligible.
    let due = store.due_for_probe(10_000, 100, 500, 0, 10).unwrap();
    assert_eq!(due.len(), 1);
    // Bump attempt forward; should drop out of the most-overdue window.
    store.record_attempt(&net, 9_950).unwrap();
    // Now since_attempt=50, below stale_good=100 → no longer eligible for any class.
    let due = store.due_for_probe(10_000, 100, 500, 0, 10).unwrap();
    assert!(due.is_empty(), "peer should disappear from due list after attempt bump");
}

#[test]
fn delete_removes_attempt_index_entry() {
    let (_dir, store) = open_temp_store();
    let net = NetAddress { ip: IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), port: 16111 };
    store.insert_stub_if_missing(&net, 0).unwrap();
    // Stub has last_attempt_ms=0 and is bad-class → eligible when since_attempt ≥ stale_bad.
    let due = store.due_for_probe(10_000, 100, 500, 0, 10).unwrap();
    assert_eq!(due.len(), 1);
    assert!(store.delete(&net).unwrap());
    let due = store.due_for_probe(10_000, 100, 500, 0, 10).unwrap();
    assert!(due.is_empty());
}
