//! Tiny TTL cache for `GET /peers` responses, keyed by `(all, expose)`.

use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::Bytes;

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Key {
    pub all: bool,
    pub expose: bool,
}

struct Entry {
    key: Key,
    body: Bytes,
    inserted: Instant,
}

pub struct PeersCache {
    ttl: Duration,
    entries: Mutex<Vec<Entry>>,
}

impl PeersCache {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn get(&self, key: Key) -> Option<Bytes> {
        let now = Instant::now();
        let mut guard = self.entries.lock().expect("peers_cache poisoned");
        guard.retain(|e| now.duration_since(e.inserted) < self.ttl);
        guard.iter().find(|e| e.key == key).map(|e| e.body.clone())
    }

    pub fn put(&self, key: Key, body: Bytes) {
        let mut guard = self.entries.lock().expect("peers_cache poisoned");
        guard.retain(|e| e.key != key);
        guard.push(Entry {
            key,
            body,
            inserted: Instant::now(),
        });
    }

    /// Return the cached body for `key`, or run `compute` to produce it, store
    /// it, and return it. The cache lookup/store-around protocol stays
    /// invisible to callers.
    pub async fn get_or_compute<F, Fut, E>(&self, key: Key, compute: F) -> Result<Bytes, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes, E>>,
    {
        if let Some(hit) = self.get(key) {
            return Ok(hit);
        }
        let body = compute().await?;
        self.put(key, body.clone());
        Ok(body)
    }
}
