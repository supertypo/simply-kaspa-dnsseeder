use crate::error::Error;
use crate::filter::Filter;
use crate::record::{NetAddress, PeerRecord};
use log::{debug, warn};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

/// redb table: key = bincoded `NetAddress` (ip + port), value = bincoded `PeerRecord`.
///
/// The `_v2` suffix distinguishes this from the prior id-keyed schema; opening an
/// old database simply ignores the legacy `peers` table and starts fresh.
const PEERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("peers_v2");

/// Secondary index ordered by `last_attempt_ms` (ascending = oldest first).
///
/// Key layout: `last_attempt_ms` encoded as big-endian `i64` with the sign
/// bit flipped (so the byte order matches the signed numeric order), followed
/// by the bincoded `NetAddress` to make the key unique. Value is empty.
///
/// Rebuilt from `PEERS` on every `open()` so it always stays consistent with
/// the primary table, even after schema changes that purge records.
const ATTEMPT_IDX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("peers_by_attempt_v1");

/// Generic key/value table used for small persisted blobs (e.g. metrics snapshots).
const KV: TableDefinition<&str, &[u8]> = TableDefinition::new("kv_v1");

/// Read-only snapshot of aggregate store state, computed by [`PeerStore::summary`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreSummary {
    pub total: u64,
    pub good: u64,
    pub failed: u64,
    /// Count of good IPv4 peers (subset of `good`).
    pub v4: u64,
    /// Count of good IPv6 peers (subset of `good`).
    pub v6: u64,
    /// Average age in ms of `last_success_ms` across the `good` subset. Zero when `good == 0`.
    pub avg_success_age_ms: u64,
}

/// Sentinel id used for records created before a successful handshake.
pub const UNKNOWN_PEER_ID: [u8; 16] = [0u8; 16];

/// Thread-safe handle to the peer database.
#[derive(Clone)]
pub struct PeerStore {
    db: Arc<Database>,
}

impl PeerStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path.as_ref())?;
        let txn = db.begin_write()?;
        {
            let _ = txn.open_table(PEERS)?;
            let _ = txn.open_table(ATTEMPT_IDX)?;
            let _ = txn.open_table(KV)?;
        }
        txn.commit()?;
        let store = Self { db: Arc::new(db) };
        let purged = store.purge_undecodable()?;
        if purged > 0 {
            warn!("store: purged {purged} record(s) incompatible with the current schema");
        }
        let indexed = store.rebuild_attempt_index()?;
        debug!("store: rebuilt attempt index with {indexed} entries");
        Ok(store)
    }

    /// Wipe and rebuild [`ATTEMPT_IDX`] from [`PEERS`]. Called at startup so
    /// the index is always in lock-step with the primary table, even after
    /// crashes or schema purges.
    fn rebuild_attempt_index(&self) -> Result<usize, Error> {
        let txn = self.db.begin_write()?;
        let mut count = 0usize;
        {
            let mut idx = txn.open_table(ATTEMPT_IDX)?;
            // Clear any stale entries.
            let keys: Vec<Vec<u8>> = idx.iter()?.filter_map(|e| e.ok().map(|(k, _)| k.value().to_vec())).collect();
            for k in &keys {
                idx.remove(k.as_slice())?;
            }
            let peers = txn.open_table(PEERS)?;
            for entry in peers.iter()? {
                let (k, v) = entry?;
                let Ok(rec) = decode_record(v.value()) else { continue };
                let idx_key = attempt_index_key(rec.last_attempt_ms, k.value());
                idx.insert(idx_key.as_slice(), [].as_slice())?;
                count += 1;
            }
        }
        txn.commit()?;
        Ok(count)
    }

    /// Drop every row that fails to decode against the current `PeerRecord`
    /// schema. Returns the number of rows removed. The attempt index will be
    /// rebuilt afterwards by [`rebuild_attempt_index`].
    fn purge_undecodable(&self) -> Result<usize, Error> {
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        {
            let txn = self.db.begin_read()?;
            let t = txn.open_table(PEERS)?;
            for entry in t.iter()? {
                let (k, v) = entry?;
                if decode_record(v.value()).is_err() {
                    to_delete.push(k.value().to_vec());
                }
            }
        }
        if to_delete.is_empty() {
            return Ok(0);
        }
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(PEERS)?;
            for k in &to_delete {
                t.remove(k.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(to_delete.len())
    }

    /// Insert or update a record. Keyed by `rec.address`.
    pub fn upsert(&self, rec: &PeerRecord) -> Result<(), Error> {
        let key = encode_key(&rec.address)?;
        let bytes = encode_record(rec)?;
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_table(ATTEMPT_IDX)?;
            if let Some(old) = t.get(key.as_slice())?
                && let Ok(old_rec) = decode_record(old.value())
                && old_rec.last_attempt_ms != rec.last_attempt_ms
            {
                idx.remove(attempt_index_key(old_rec.last_attempt_ms, &key).as_slice())?;
            }
            t.insert(key.as_slice(), bytes.as_slice())?;
            idx.insert(attempt_index_key(rec.last_attempt_ms, &key).as_slice(), [].as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Look up a record by its address.
    pub fn get(&self, addr: &NetAddress) -> Result<Option<PeerRecord>, Error> {
        let key = encode_key(addr)?;
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        match t.get(key.as_slice())? {
            Some(v) => Ok(Some(decode_record(v.value())?)),
            None => Ok(None),
        }
    }

    /// Delete a record by its address. Returns whether a row was removed.
    pub fn delete(&self, addr: &NetAddress) -> Result<bool, Error> {
        let key = encode_key(addr)?;
        let txn = self.db.begin_write()?;
        let removed = {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_table(ATTEMPT_IDX)?;
            match t.remove(key.as_slice())? {
                Some(v) => {
                    if let Ok(old_rec) = decode_record(v.value()) {
                        idx.remove(attempt_index_key(old_rec.last_attempt_ms, &key).as_slice())?;
                    }
                    true
                }
                None => false,
            }
        };
        txn.commit()?;
        Ok(removed)
    }

    /// Record an attempt at `addr` taken at `now_ms`. Creates a stub record
    /// (`id` = [`UNKNOWN_PEER_ID`], `last_seen_ms` = 0) if none exists, else
    /// only refreshes `last_attempt_ms` on the existing record.
    pub fn record_attempt(&self, addr: &NetAddress, now_ms: i64) -> Result<PeerRecord, Error> {
        let key = encode_key(addr)?;
        let txn = self.db.begin_write()?;
        let rec = {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_table(ATTEMPT_IDX)?;
            let (mut rec, old_attempt) = match t.get(key.as_slice())? {
                Some(v) => {
                    let r = decode_record(v.value())?;
                    let prev = r.last_attempt_ms;
                    (r, Some(prev))
                }
                None => (
                    PeerRecord {
                        id: UNKNOWN_PEER_ID,
                        protocol_version: 0,
                        timestamp_ms: 0,
                        address: *addr,
                        user_agent: String::new(),
                        subnetwork_id: None,
                        first_seen_ms: now_ms,
                        last_attempt_ms: now_ms,
                        last_success_ms: 0,
                        last_seen_ms: 0,
                    },
                    None,
                ),
            };
            rec.last_attempt_ms = now_ms;
            let bytes = encode_record(&rec)?;
            t.insert(key.as_slice(), bytes.as_slice())?;
            if let Some(prev) = old_attempt
                && prev != now_ms
            {
                idx.remove(attempt_index_key(prev, &key).as_slice())?;
            }
            idx.insert(attempt_index_key(now_ms, &key).as_slice(), [].as_slice())?;
            rec
        };
        txn.commit()?;
        Ok(rec)
    }

    /// Insert a stub record for `addr` if none exists. Used by the discovery
    /// path: a peer told us about this address, but we haven't tried it yet,
    /// so we only want to register it for future probing without touching
    /// `last_attempt_ms`. Returns `true` if a new record was created.
    pub fn insert_stub_if_missing(&self, addr: &NetAddress, now_ms: i64) -> Result<bool, Error> {
        let key = encode_key(addr)?;
        let txn = self.db.begin_write()?;
        let inserted = {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_table(ATTEMPT_IDX)?;
            if t.get(key.as_slice())?.is_some() {
                false
            } else {
                let rec = PeerRecord {
                    id: UNKNOWN_PEER_ID,
                    protocol_version: 0,
                    timestamp_ms: 0,
                    address: *addr,
                    user_agent: String::new(),
                    subnetwork_id: None,
                    first_seen_ms: now_ms,
                    last_attempt_ms: 0,
                    last_success_ms: 0,
                    last_seen_ms: now_ms,
                };
                let bytes = encode_record(&rec)?;
                t.insert(key.as_slice(), bytes.as_slice())?;
                idx.insert(attempt_index_key(0, &key).as_slice(), [].as_slice())?;
                true
            }
        };
        txn.commit()?;
        Ok(inserted)
    }

    /// Returns the number of stored records.
    pub fn len(&self) -> Result<u64, Error> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        Ok(t.len()?)
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.len()? == 0)
    }

    /// Read all records, applying `filter` and collecting matches.
    pub fn collect_matching(&self, filter: &Filter) -> Result<Vec<PeerRecord>, Error> {
        let mut out = Vec::new();
        let mut total = 0usize;
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        for entry in t.iter()? {
            total += 1;
            let (_, v) = entry?;
            match decode_record(v.value()) {
                Ok(rec) => {
                    if filter.matches(&rec) {
                        out.push(rec);
                    }
                }
                Err(e) => warn!("store: skipping corrupt record: {e}"),
            }
        }
        debug!(
            "store: collect_matching family={:?} scanned={total} matched={}",
            filter.family,
            out.len()
        );
        Ok(out)
    }

    /// Read all records as-is. Use a `Filter` if you only want a subset.
    pub fn iter_all(&self) -> Result<Vec<PeerRecord>, Error> {
        let mut out = Vec::new();
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        for entry in t.iter()? {
            let (_, v) = entry?;
            match decode_record(v.value()) {
                Ok(rec) => out.push(rec),
                Err(e) => warn!("store: skipping corrupt record: {e}"),
            }
        }
        Ok(out)
    }

    /// Return up to `max` records that are due to be probed, in ascending
    /// `last_attempt_ms` order (most-overdue first).
    ///
    /// Walks the [`ATTEMPT_IDX`] secondary index — never the full `PEERS`
    /// table — and stops as soon as an entry's attempt time is too recent
    /// for **any** eligibility class (i.e. `now - last_attempt < stale_good_ms`,
    /// which is the looser of the two thresholds). Records are still verified
    /// against `is_eligible_for_probe` to handle the bad-class threshold and
    /// the dead cutoff.
    pub fn due_for_probe(
        &self,
        now_ms: i64,
        stale_good_ms: i64,
        stale_bad_ms: i64,
        dead_cutoff_ms: i64,
        max: usize,
    ) -> Result<Vec<PeerRecord>, Error> {
        if max == 0 {
            return Ok(Vec::new());
        }
        // Past this attempt time, no record (good or bad class) can be eligible.
        let attempt_ceiling = now_ms.saturating_sub(stale_good_ms);
        let mut out: Vec<PeerRecord> = Vec::with_capacity(max);
        let txn = self.db.begin_read()?;
        let idx = txn.open_table(ATTEMPT_IDX)?;
        let peers = txn.open_table(PEERS)?;
        for entry in idx.iter()? {
            let (key_bytes, _) = entry?;
            let key = key_bytes.value();
            if key.len() < 8 {
                continue;
            }
            let attempt = decode_attempt(&key[..8]);
            if attempt > attempt_ceiling {
                break;
            }
            let addr_bytes = &key[8..];
            let Some(v) = peers.get(addr_bytes)? else { continue };
            let Ok(rec) = decode_record(v.value()) else { continue };
            if !is_eligible_for_probe(&rec, now_ms, stale_good_ms, stale_bad_ms, dead_cutoff_ms) {
                continue;
            }
            out.push(rec);
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }

    /// Delete every record where both `last_seen_ms` and `first_seen_ms` are
    /// older than `cutoff_ms`. This handles two cases uniformly:
    ///   - peers we used to reach but haven't recently (`last_seen_ms` stale, and
    ///     `first_seen_ms <= last_seen_ms` is therefore also stale).
    ///   - peers we've been trying for a while but never successfully reached
    ///     (`last_seen_ms == 0`, so they go as soon as `first_seen_ms < cutoff_ms`).
    pub fn prune_dead(&self, cutoff_ms: i64) -> Result<usize, Error> {
        debug!("store: prune_dead scan (cutoff_ms={cutoff_ms})");
        let mut to_delete: Vec<(Vec<u8>, i64)> = Vec::new();
        {
            let txn = self.db.begin_read()?;
            let t = txn.open_table(PEERS)?;
            for entry in t.iter()? {
                let (k, v) = entry?;
                if let Ok(rec) = decode_record(v.value())
                    && rec.last_seen_ms < cutoff_ms
                    && rec.first_seen_ms < cutoff_ms
                {
                    to_delete.push((k.value().to_vec(), rec.last_attempt_ms));
                }
            }
        }
        if to_delete.is_empty() {
            return Ok(0);
        }
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_table(ATTEMPT_IDX)?;
            for (k, attempt) in &to_delete {
                t.remove(k.as_slice())?;
                idx.remove(attempt_index_key(*attempt, k).as_slice())?;
            }
        }
        txn.commit()?;
        debug!("store: pruned {} dead peers", to_delete.len());
        Ok(to_delete.len())
    }

    /// Compute an aggregate summary of all stored peers in a single read pass.
    /// A peer is "good" iff `last_success_ms > 0` and `now_ms - last_success_ms <= stale_good_ms`.
    /// `v4` / `v6` count only the "good" subset.
    pub fn summary(&self, now_ms: i64, stale_good_ms: i64) -> Result<StoreSummary, Error> {
        let mut s = StoreSummary::default();
        let mut sum_age_ms: u128 = 0;
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        for entry in t.iter()? {
            let (_, v) = entry?;
            let Ok(rec) = decode_record(v.value()) else { continue };
            s.total += 1;
            if rec.last_success_ms <= 0 {
                s.failed += 1;
                continue;
            }
            let age = now_ms.saturating_sub(rec.last_success_ms);
            if age <= stale_good_ms {
                s.good += 1;
                match rec.address.ip {
                    IpAddr::V4(_) => s.v4 += 1,
                    IpAddr::V6(_) => s.v6 += 1,
                }
                if age > 0 {
                    sum_age_ms += u128::from(u64::try_from(age).unwrap_or(0));
                }
            } else {
                s.failed += 1;
            }
        }
        if s.good > 0 {
            s.avg_success_age_ms = u64::try_from(sum_age_ms / u128::from(s.good)).unwrap_or(u64::MAX);
        }
        Ok(s)
    }

    /// Read a generic blob from the KV table.
    pub fn get_blob(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(KV)?;
        Ok(t.get(key)?.map(|v| v.value().to_vec()))
    }

    /// Write a generic blob to the KV table.
    pub fn put_blob(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(KV)?;
            t.insert(key, value)?;
        }
        txn.commit()?;
        Ok(())
    }
}

fn encode_record(rec: &PeerRecord) -> Result<Vec<u8>, Error> {
    bincode::serde::encode_to_vec(rec, bincode::config::standard()).map_err(|e| Error::Encode(e.to_string()))
}

fn decode_record(bytes: &[u8]) -> Result<PeerRecord, Error> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| Error::Decode(e.to_string()))
}

fn encode_key(addr: &NetAddress) -> Result<Vec<u8>, Error> {
    bincode::serde::encode_to_vec(addr, bincode::config::standard()).map_err(|e| Error::Encode(e.to_string()))
}

/// Build an `ATTEMPT_IDX` key: 8-byte big-endian `i64` (sign bit flipped so byte
/// order matches signed numeric order) followed by the address key bytes.
fn attempt_index_key(attempt_ms: i64, addr_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + addr_key.len());
    out.extend_from_slice(&encode_attempt(attempt_ms));
    out.extend_from_slice(addr_key);
    out
}

fn encode_attempt(attempt_ms: i64) -> [u8; 8] {
    // Flip the sign bit so `i64::MIN` sorts before `i64::MAX` in byte order.
    let biased = attempt_ms.cast_unsigned() ^ 0x8000_0000_0000_0000;
    biased.to_be_bytes()
}

fn decode_attempt(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    let biased = u64::from_be_bytes(buf);
    (biased ^ 0x8000_0000_0000_0000).cast_signed()
}

/// Eligibility predicate used by [`PeerStore::due_for_probe`]. Kept in this
/// module so the index walker can re-check records pulled by primary key.
fn is_eligible_for_probe(
    rec: &PeerRecord,
    now_ms: i64,
    stale_good_ms: i64,
    stale_bad_ms: i64,
    dead_cutoff_ms: i64,
) -> bool {
    if rec.last_seen_ms < dead_cutoff_ms && rec.first_seen_ms < dead_cutoff_ms {
        return false;
    }
    let since_attempt = now_ms.saturating_sub(rec.last_attempt_ms);
    let threshold = if rec.last_success_ms > 0 { stale_good_ms } else { stale_bad_ms };
    since_attempt >= threshold
}
