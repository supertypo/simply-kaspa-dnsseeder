use crate::error::Error;
use crate::filter::Filter;
use crate::record::{NetAddress, PeerRecord};
use log::{debug, warn};
use redb::{Database, Durability, MultimapTableDefinition, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

/// redb table: key = bincoded `NetAddress` (ip + port), value = bincoded `PeerRecord`.
const PEERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("peers_v2");

/// Index by `last_attempt_ms` (oldest first); rebuilt from PEERS on every `open()`.
const ATTEMPT_IDX: MultimapTableDefinition<i64, &[u8]> = MultimapTableDefinition::new("peers_by_attempt_v2");

/// Generic key/value table used for small persisted blobs (e.g. metrics snapshots).
const KV: TableDefinition<&str, &[u8]> = TableDefinition::new("kv_v1");

/// Aggregate store snapshot, computed by [`PeerStore::summary`]. Buckets are mutually
/// exclusive over the peer set: `total == good + filtered + stale + failed + stub`.
/// `good` = passes validity (or all in-window peers when no validity filter is supplied);
/// `filtered` = in-window but fails validity; `v4`/`v6` and `avg_success_age_ms` cover
/// the raw `good + filtered` subset.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreSummary {
    pub total: u64,
    pub good: u64,
    pub filtered: u64,
    pub stale: u64,
    /// Attempted at least once, never succeeded. Stubs (never attempted) live in `stub`.
    pub failed: u64,
    pub stub: u64,
    pub v4: u64,
    pub v6: u64,
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
    /// Run a synchronous DB operation on tokio's blocking pool so it doesn't stall an async worker.
    ///
    /// # Panics
    /// Panics if the underlying `spawn_blocking` join fails (i.e. the closure panics).
    pub async fn blocking<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&PeerStore) -> T + Send + 'static,
        T: Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .expect("PeerStore::blocking task panicked")
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let db = match Database::create(path) {
            Ok(db) => db,
            Err(err) if is_incompatible_db(&err) => {
                warn!(
                    "store: existing database at {} is incompatible ({err}); wiping and recreating",
                    path.display()
                );
                std::fs::remove_file(path)?;
                Database::create(path)?
            }
            Err(err) => return Err(err.into()),
        };
        let txn = db.begin_write()?;
        {
            let _ = txn.open_table(PEERS)?;
            let _ = txn.open_multimap_table(ATTEMPT_IDX)?;
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

    /// Drop and repopulate [`ATTEMPT_IDX`] from [`PEERS`]. Called at startup so
    /// the index is always in lock-step with the primary table, even after
    /// crashes or schema purges. Uses `delete_multimap_table` for an O(1)
    /// drop instead of an entry-by-entry clear loop.
    fn rebuild_attempt_index(&self) -> Result<usize, Error> {
        let txn = self.db.begin_write()?;
        let _ = txn.delete_multimap_table(ATTEMPT_IDX)?;
        let mut count = 0usize;
        {
            let mut idx = txn.open_multimap_table(ATTEMPT_IDX)?;
            let peers = txn.open_table(PEERS)?;
            for entry in peers.iter()? {
                let (k, v) = entry?;
                let Ok(rec) = decode_record(v.value()) else { continue };
                idx.insert(rec.last_attempt_ms, k.value())?;
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

    /// Keyed by `rec.address`.
    pub fn upsert(&self, rec: &PeerRecord) -> Result<(), Error> {
        let key = encode_key(&rec.address)?;
        let bytes = encode_record(rec)?;
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_multimap_table(ATTEMPT_IDX)?;
            if let Some(old) = t.get(key.as_slice())?
                && let Ok(old_rec) = decode_record(old.value())
                && old_rec.last_attempt_ms != rec.last_attempt_ms
            {
                idx.remove(old_rec.last_attempt_ms, key.as_slice())?;
            }
            t.insert(key.as_slice(), bytes.as_slice())?;
            idx.insert(rec.last_attempt_ms, key.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn get(&self, addr: &NetAddress) -> Result<Option<PeerRecord>, Error> {
        let key = encode_key(addr)?;
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        match t.get(key.as_slice())? {
            Some(v) => Ok(Some(decode_record(v.value())?)),
            None => Ok(None),
        }
    }

    /// Returns whether a row was removed.
    pub fn delete(&self, addr: &NetAddress) -> Result<bool, Error> {
        let key = encode_key(addr)?;
        let txn = self.db.begin_write()?;
        let removed = {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_multimap_table(ATTEMPT_IDX)?;
            match t.remove(key.as_slice())? {
                Some(v) => {
                    if let Ok(old_rec) = decode_record(v.value()) {
                        idx.remove(old_rec.last_attempt_ms, key.as_slice())?;
                    }
                    true
                }
                None => false,
            }
        };
        txn.commit()?;
        Ok(removed)
    }

    /// Record an attempt at `addr` taken at `now_ms`. Creates a stub if none
    /// exists; otherwise only refreshes `last_attempt_ms`. Uses
    /// `Durability::None` — losing the latest attempt on crash only
    /// causes a slightly-early re-probe, far cheaper than fsync per probe.
    ///
    /// # Panics
    ///
    /// Panics if `set_durability` is called after a table has been opened in the
    /// transaction; this would indicate a programmer error in this method.
    pub fn record_attempt(&self, addr: &NetAddress, now_ms: i64) -> Result<PeerRecord, Error> {
        let key = encode_key(addr)?;
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::None).expect("durability set before any open_table");
        let rec = {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_multimap_table(ATTEMPT_IDX)?;
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
                idx.remove(prev, key.as_slice())?;
            }
            idx.insert(now_ms, key.as_slice())?;
            rec
        };
        txn.commit()?;
        Ok(rec)
    }

    /// Insert a stub for `addr` if none exists. Discovery-only path —
    /// does not touch `last_attempt_ms`. Returns true when a new record is created.
    pub fn insert_stub_if_missing(&self, addr: &NetAddress, now_ms: i64) -> Result<bool, Error> {
        let key = encode_key(addr)?;
        let txn = self.db.begin_write()?;
        let inserted = {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_multimap_table(ATTEMPT_IDX)?;
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
                idx.insert(0_i64, key.as_slice())?;
                true
            }
        };
        txn.commit()?;
        Ok(inserted)
    }

    pub fn len(&self) -> Result<u64, Error> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        Ok(t.len()?)
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.len()? == 0)
    }

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

    /// Return up to `max` records due for probe, oldest `last_attempt_ms` first.
    /// Walks [`ATTEMPT_IDX`] over a bounded range and re-checks each candidate
    /// against [`is_eligible_for_probe`] for the bad-class threshold and dead cutoff.
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
        let attempt_ceiling = now_ms.saturating_sub(good_probe_threshold(stale_good_ms).min(stale_bad_ms));
        let mut out: Vec<PeerRecord> = Vec::with_capacity(max);
        let txn = self.db.begin_read()?;
        let idx = txn.open_multimap_table(ATTEMPT_IDX)?;
        let peers = txn.open_table(PEERS)?;
        'outer: for entry in idx.range(..=attempt_ceiling)? {
            let (_attempt, values) = entry?;
            for v in values {
                let addr_bytes = v?;
                let Some(rec_bytes) = peers.get(addr_bytes.value())? else {
                    continue;
                };
                let Ok(rec) = decode_record(rec_bytes.value()) else { continue };
                if !is_eligible_for_probe(&rec, now_ms, stale_good_ms, stale_bad_ms, dead_cutoff_ms) {
                    continue;
                }
                out.push(rec);
                if out.len() >= max {
                    break 'outer;
                }
            }
        }
        Ok(out)
    }

    /// Delete every record where both `last_seen_ms` and `first_seen_ms` are
    /// older than `cutoff_ms`. Runs scan + delete in a single write transaction
    /// so a concurrent `record_attempt` cannot leave an orphan index entry.
    pub fn prune_dead(&self, cutoff_ms: i64) -> Result<usize, Error> {
        debug!("store: prune_dead scan (cutoff_ms={cutoff_ms})");
        let txn = self.db.begin_write()?;
        let deleted;
        {
            let mut t = txn.open_table(PEERS)?;
            let mut idx = txn.open_multimap_table(ATTEMPT_IDX)?;
            let mut to_delete: Vec<(Vec<u8>, i64)> = Vec::new();
            for entry in t.iter()? {
                let (k, v) = entry?;
                if let Ok(rec) = decode_record(v.value())
                    && rec.last_seen_ms < cutoff_ms
                    && rec.first_seen_ms < cutoff_ms
                {
                    to_delete.push((k.value().to_vec(), rec.last_attempt_ms));
                }
            }
            for (k, attempt) in &to_delete {
                t.remove(k.as_slice())?;
                idx.remove(*attempt, k.as_slice())?;
            }
            deleted = to_delete.len();
        }
        txn.commit()?;
        debug!("store: pruned {deleted} dead peers");
        Ok(deleted)
    }

    /// One-pass aggregation over every stored peer. `validity` is consulted only on the in-window
    /// subset via [`Filter::passes_validity`] (its own staleness and family fields are ignored).
    pub fn summary(&self, now_ms: i64, stale_good_ms: i64, validity: Option<&Filter>) -> Result<StoreSummary, Error> {
        let mut s = StoreSummary::default();
        let mut sum_age_ms: u128 = 0;
        let mut raw_good: u64 = 0;
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        for entry in t.iter()? {
            let (_, v) = entry?;
            let Ok(rec) = decode_record(v.value()) else { continue };
            s.total += 1;
            if rec.last_success_ms <= 0 {
                if rec.last_attempt_ms <= 0 {
                    s.stub += 1;
                } else {
                    s.failed += 1;
                }
                continue;
            }
            let age = now_ms.saturating_sub(rec.last_success_ms);
            if age <= stale_good_ms {
                raw_good += 1;
                match rec.address.ip {
                    IpAddr::V4(_) => s.v4 += 1,
                    IpAddr::V6(_) => s.v6 += 1,
                }
                if age > 0 {
                    sum_age_ms += u128::from(u64::try_from(age).unwrap_or(0));
                }
                let passes = validity.is_none_or(|f| f.passes_validity(&rec));
                if passes {
                    s.good += 1;
                } else {
                    s.filtered += 1;
                }
            } else {
                s.stale += 1;
            }
        }
        if raw_good > 0 {
            s.avg_success_age_ms = u64::try_from(sum_age_ms / u128::from(raw_good)).unwrap_or(u64::MAX);
        }
        Ok(s)
    }

    pub fn get_blob(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(KV)?;
        Ok(t.get(key)?.map(|v| v.value().to_vec()))
    }

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
    Ok(bincode::serde::encode_to_vec(rec, bincode::config::standard())?)
}

fn decode_record(bytes: &[u8]) -> Result<PeerRecord, Error> {
    let (v, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    Ok(v)
}

fn encode_key(addr: &NetAddress) -> Result<Vec<u8>, Error> {
    Ok(bincode::serde::encode_to_vec(addr, bincode::config::standard())?)
}

/// Eligibility predicate used by [`PeerStore::due_for_probe`]. Exposed for
/// scheduler-side unit tests; production code reaches it through
/// `due_for_probe` rather than calling it directly.
///
/// Good-class peers are eligible at 80% of the `stale_good` window so we
/// re-probe them before they tip into the unservable bracket.
#[must_use]
pub fn is_eligible_for_probe(rec: &PeerRecord, now_ms: i64, stale_good_ms: i64, stale_bad_ms: i64, dead_cutoff_ms: i64) -> bool {
    if rec.last_seen_ms < dead_cutoff_ms && rec.first_seen_ms < dead_cutoff_ms {
        return false;
    }
    let since_attempt = now_ms.saturating_sub(rec.last_attempt_ms);
    let threshold = if rec.last_success_ms > 0 {
        good_probe_threshold(stale_good_ms)
    } else {
        stale_bad_ms
    };
    since_attempt >= threshold
}

#[inline]
const fn good_probe_threshold(stale_good_ms: i64) -> i64 {
    stale_good_ms * 4 / 5
}

const fn is_incompatible_db(err: &redb::DatabaseError) -> bool {
    matches!(err, redb::DatabaseError::UpgradeRequired(_) | redb::DatabaseError::RepairAborted)
}
