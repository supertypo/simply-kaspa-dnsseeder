use crate::error::Error;
use crate::filter::Filter;
use crate::record::{PeerId, PeerRecord};
use log::{debug, warn};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;
use std::sync::Arc;

/// redb table: key = raw 16-byte peer id, value = bincoded `PeerRecord`.
const PEERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("peers");

/// Thread-safe handle to the peer database.
#[derive(Clone)]
pub struct PeerStore {
    db: Arc<Database>,
}

impl PeerStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        if let Some(parent) = path.as_ref().parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let db = Database::create(path.as_ref())?;
        // Ensure the table exists.
        let txn = db.begin_write()?;
        {
            let _ = txn.open_table(PEERS)?;
        }
        txn.commit()?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Insert or update a record, overwriting any previous record with the same id.
    pub fn upsert(&self, rec: &PeerRecord) -> Result<(), Error> {
        let bytes = encode(rec)?;
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(PEERS)?;
            t.insert(rec.id.as_slice(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn get(&self, id: &PeerId) -> Result<Option<PeerRecord>, Error> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        match t.get(id.as_slice())? {
            Some(v) => Ok(Some(decode(v.value())?)),
            None => Ok(None),
        }
    }

    pub fn delete(&self, id: &PeerId) -> Result<bool, Error> {
        let txn = self.db.begin_write()?;
        let removed = {
            let mut t = txn.open_table(PEERS)?;
            t.remove(id.as_slice())?.is_some()
        };
        txn.commit()?;
        Ok(removed)
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
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        for entry in t.iter()? {
            let (_, v) = entry?;
            match decode(v.value()) {
                Ok(rec) => {
                    if filter.matches(&rec) {
                        out.push(rec);
                    }
                }
                Err(e) => warn!("store: skipping corrupt record: {e}"),
            }
        }
        Ok(out)
    }

    /// Read all records as-is. Use a `Filter` if you only want a subset.
    pub fn iter_all(&self) -> Result<Vec<PeerRecord>, Error> {
        let mut out = Vec::new();
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        for entry in t.iter()? {
            let (_, v) = entry?;
            match decode(v.value()) {
                Ok(rec) => out.push(rec),
                Err(e) => warn!("store: skipping corrupt record: {e}"),
            }
        }
        Ok(out)
    }

    /// Delete every record whose `last_seen_ms` is older than `cutoff_ms`.
    /// Returns the number of records removed.
    pub fn prune_dead(&self, cutoff_ms: i64) -> Result<usize, Error> {
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        {
            let txn = self.db.begin_read()?;
            let t = txn.open_table(PEERS)?;
            for entry in t.iter()? {
                let (k, v) = entry?;
                if let Ok(rec) = decode(v.value()) {
                    if rec.last_seen_ms < cutoff_ms {
                        to_delete.push(k.value().to_vec());
                    }
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

fn encode(rec: &PeerRecord) -> Result<Vec<u8>, Error> {
    bincode::serde::encode_to_vec(rec, bincode::config::standard()).map_err(|e| Error::Encode(e.to_string()))
}

fn decode(bytes: &[u8]) -> Result<PeerRecord, Error> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| Error::Decode(e.to_string()))
}
