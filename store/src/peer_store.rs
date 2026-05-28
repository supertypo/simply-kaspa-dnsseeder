use crate::error::Error;
use crate::filter::Filter;
use crate::record::{NetAddress, PeerRecord};
use log::{debug, warn};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;
use std::sync::Arc;

/// redb table: key = bincoded `NetAddress` (ip + port), value = bincoded `PeerRecord`.
///
/// The `_v2` suffix distinguishes this from the prior id-keyed schema; opening an
/// old database simply ignores the legacy `peers` table and starts fresh.
const PEERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("peers_v2");

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
        }
        txn.commit()?;
        let store = Self { db: Arc::new(db) };
        let purged = store.purge_undecodable()?;
        if purged > 0 {
            warn!("store: purged {purged} record(s) incompatible with the current schema");
        }
        Ok(store)
    }

    /// Drop every row that fails to decode against the current `PeerRecord`
    /// schema. Returns the number of rows removed.
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
            t.insert(key.as_slice(), bytes.as_slice())?;
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
            t.remove(key.as_slice())?.is_some()
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
            let mut rec = match t.get(key.as_slice())? {
                Some(v) => decode_record(v.value())?,
                None => PeerRecord {
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
            };
            rec.last_attempt_ms = now_ms;
            let bytes = encode_record(&rec)?;
            t.insert(key.as_slice(), bytes.as_slice())?;
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
        debug!("store: collect_matching family={:?} scanned={total} matched={}", filter.family, out.len());
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

    /// Delete every record where both `last_seen_ms` and `first_seen_ms` are
    /// older than `cutoff_ms`. This handles two cases uniformly:
    ///   - peers we used to reach but haven't recently (`last_seen_ms` stale, and
    ///     `first_seen_ms <= last_seen_ms` is therefore also stale).
    ///   - peers we've been trying for a while but never successfully reached
    ///     (`last_seen_ms == 0`, so they go as soon as `first_seen_ms < cutoff_ms`).
    pub fn prune_dead(&self, cutoff_ms: i64) -> Result<usize, Error> {
        debug!("store: prune_dead scan (cutoff_ms={cutoff_ms})");
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        {
            let txn = self.db.begin_read()?;
            let t = txn.open_table(PEERS)?;
            for entry in t.iter()? {
                let (k, v) = entry?;
                if let Ok(rec) = decode_record(v.value())
                    && rec.last_seen_ms < cutoff_ms
                    && rec.first_seen_ms < cutoff_ms
                {
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
        debug!("store: pruned {} dead peers", to_delete.len());
        Ok(to_delete.len())
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
