#[cfg(feature = "redis-storage")]
mod redis_store;

#[cfg(feature = "redis-storage")]
pub use redis_store::RedisCooperativeStorage;

use super::{PeerState, spatial::distance, wire::RADIUS};
use crate::StorageError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Mutex};

const MAX_NEARBY: usize = 128;
const MAX_PER_BRAND: usize = 4096;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PeerRecord {
    pub donor_id: String,
    pub subject_key: Option<String>,
    pub capture_ms: u64,
    pub received_ms: u64,
    pub state: PeerState,
}

#[derive(Clone, Debug)]
pub struct PeerWrite {
    pub donor_id: String,
    pub subject_key: Option<String>,
    pub capture_ms: u64,
    pub state: Option<PeerState>,
}

#[async_trait]
pub trait CooperativeStorage: Send + Sync {
    /// Atomically enforce ordering and replace or withdraw each peer in this batch.
    async fn publish(
        &self,
        namespace: &str,
        brand: &str,
        updates: &[PeerWrite],
        now: u64,
        freshness_ms: u64,
    ) -> Result<Vec<bool>, StorageError>;

    /// None means the candidate set is incomplete; the caller must not match.
    async fn nearby(
        &self,
        namespace: &str,
        brand: &str,
        center: (f64, f64),
        now: u64,
        freshness_ms: u64,
    ) -> Result<Option<Vec<PeerRecord>>, StorageError>;

    async fn clear(&self, namespace: &str, brand: &str) -> Result<(), StorageError>;
}

type Brand = HashMap<String, (u64, Option<PeerRecord>)>;

#[derive(Default)]
struct Bucket {
    last_now: u64,
    peers: Brand,
}

#[derive(Default)]
pub struct InMemoryCooperativeStorage {
    buckets: Mutex<HashMap<(String, String), Bucket>>,
}

fn unavailable() -> StorageError {
    StorageError::UnexpectedResponse
}

#[async_trait]
impl CooperativeStorage for InMemoryCooperativeStorage {
    async fn publish(
        &self,
        namespace: &str,
        brand: &str,
        updates: &[PeerWrite],
        now: u64,
        freshness_ms: u64,
    ) -> Result<Vec<bool>, StorageError> {
        let mut buckets = self.buckets.lock().map_err(|_| unavailable())?;
        let bucket = buckets.entry((namespace.into(), brand.into())).or_default();
        if now < bucket.last_now {
            return Ok(vec![false; updates.len()]);
        }
        bucket.last_now = now;
        bucket
            .peers
            .retain(|_, (capture, _)| capture.saturating_add(freshness_ms) > now);
        Ok(updates
            .iter()
            .map(|update| {
                let previous = bucket.peers.get(&update.donor_id);
                if !super::spatial::fresh(update.capture_ms, now, freshness_ms)
                    || previous.is_some_and(|(ts, _)| *ts >= update.capture_ms)
                    || (previous.is_none() && bucket.peers.len() == MAX_PER_BRAND)
                {
                    return false;
                }
                let record = update.state.clone().map(|state| PeerRecord {
                    donor_id: update.donor_id.clone(),
                    subject_key: update.subject_key.clone(),
                    capture_ms: update.capture_ms,
                    received_ms: now,
                    state,
                });
                bucket
                    .peers
                    .insert(update.donor_id.clone(), (update.capture_ms, record));
                true
            })
            .collect())
    }

    async fn nearby(
        &self,
        namespace: &str,
        brand: &str,
        center: (f64, f64),
        now: u64,
        freshness_ms: u64,
    ) -> Result<Option<Vec<PeerRecord>>, StorageError> {
        let mut buckets = self.buckets.lock().map_err(|_| unavailable())?;
        let Some(bucket) = buckets.get_mut(&(namespace.into(), brand.into())) else {
            return Ok(Some(Vec::new()));
        };
        if now < bucket.last_now {
            return Ok(None);
        }
        bucket
            .peers
            .retain(|_, (capture, _)| capture.saturating_add(freshness_ms) > now);
        let result: Vec<_> = bucket
            .peers
            .values()
            .filter_map(|(_, record)| record.as_ref())
            .filter(|r| distance(center, (r.state.latitude, r.state.longitude)) <= RADIUS)
            .take(MAX_NEARBY + 1)
            .cloned()
            .collect();
        Ok((result.len() <= MAX_NEARBY).then_some(result))
    }

    async fn clear(&self, namespace: &str, brand: &str) -> Result<(), StorageError> {
        self.buckets
            .lock()
            .map_err(|_| unavailable())?
            .remove(&(namespace.into(), brand.into()));
        Ok(())
    }
}
