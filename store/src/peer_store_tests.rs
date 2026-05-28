use crate::filter::Filter;
use crate::peer_store::PeerStore;
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
    let got = store.get(&r.id).unwrap().unwrap();
    assert_eq!(got, r);
}

#[test]
fn upsert_overwrites_address_for_same_id() {
    let (_dir, store) = open_temp_store();
    let r1 = make_rec(2, Ipv4Addr::new(1, 1, 1, 1), 16111, 100);
    store.upsert(&r1).unwrap();
    let r2 = make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16112, 200);
    store.upsert(&r2).unwrap();
    let got = store.get(&r1.id).unwrap().unwrap();
    assert_eq!(got.address.ip, IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)));
    assert_eq!(got.address.port, 16112);
    assert_eq!(store.len().unwrap(), 1);
}

#[test]
fn delete_removes_only_one() {
    let (_dir, store) = open_temp_store();
    store.upsert(&make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 100)).unwrap();
    store.upsert(&make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16111, 100)).unwrap();
    assert!(store.delete(&[1; 16]).unwrap());
    assert!(store.get(&[1; 16]).unwrap().is_none());
    assert!(store.get(&[2; 16]).unwrap().is_some());
    assert_eq!(store.len().unwrap(), 1);
}

#[test]
fn prune_dead_removes_old() {
    let (_dir, store) = open_temp_store();
    store.upsert(&make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 100)).unwrap();
    store.upsert(&make_rec(2, Ipv4Addr::new(2, 2, 2, 2), 16111, 500)).unwrap();
    // cutoff at 200: anything with last_seen_ms < 200 is dropped
    let removed = store.prune_dead(200).unwrap();
    assert_eq!(removed, 1);
    assert!(store.get(&[1; 16]).unwrap().is_none());
    assert!(store.get(&[2; 16]).unwrap().is_some());
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
fn reopens_persisting_records() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("p.redb");
    {
        let s = PeerStore::open(&path).unwrap();
        s.upsert(&make_rec(7, Ipv4Addr::new(7, 7, 7, 7), 16111, 1)).unwrap();
    }
    let s2 = PeerStore::open(&path).unwrap();
    assert_eq!(s2.len().unwrap(), 1);
    assert!(s2.get(&[7; 16]).unwrap().is_some());
}

#[test]
fn open_purges_undecodable_records() {
    use redb::Database;
    const PEERS: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("peers");

    let dir = tempdir().unwrap();
    let path = dir.path().join("p.redb");

    // Seed: one valid record + two garbage rows representing an obsolete schema.
    {
        let s = PeerStore::open(&path).unwrap();
        s.upsert(&make_rec(1, Ipv4Addr::new(1, 1, 1, 1), 16111, 100)).unwrap();
    }
    {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(PEERS).unwrap();
            let garbage_a: [u8; 16] = [0xAA; 16];
            let garbage_b: [u8; 16] = [0xBB; 16];
            // Bytes that will not decode as `PeerRecord` under any current layout.
            t.insert(garbage_a.as_slice(), [0x6B, 0x6B, 0x6B, 0x6B, 0x6B].as_slice()).unwrap();
            t.insert(garbage_b.as_slice(), [0xFF; 32].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    // Reopening should silently drop the two undecodable rows.
    let s = PeerStore::open(&path).unwrap();
    assert_eq!(s.len().unwrap(), 1);
    assert!(s.get(&[1; 16]).unwrap().is_some());
    assert!(s.get(&[0xAA; 16]).unwrap().is_none());
    assert!(s.get(&[0xBB; 16]).unwrap().is_none());
}
