use crate::filter::Filter;
use crate::peer_store::{PeerStore, UNKNOWN_PEER_ID};
use crate::record::{NetAddress, PeerRecord};
use std::net::{IpAddr, Ipv4Addr};
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
