use super::{AppendResult, CoopNavStorage, StorageError, StoragePartition, UnixMillisClock};
use crate::model::TrackPoint;
use async_trait::async_trait;
use dashmap::DashMap;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

struct StoredPoint {
    point: TrackPoint,
    expires_at_ms: u64,
}

#[derive(Default)]
struct PartitionData {
    ids: HashMap<String, StoredPoint>,
    stream: Vec<TrackPoint>,
}

pub struct InMemoryCoopNavStorage {
    partitions: DashMap<StoragePartition, Arc<Mutex<PartitionData>>>,
    clock: Arc<dyn UnixMillisClock>,
    dedup_ttl: Duration,
}

impl InMemoryCoopNavStorage {
    pub fn new(clock: Arc<dyn UnixMillisClock>, dedup_ttl: Duration) -> Result<Self, StorageError> {
        if dedup_ttl.is_zero() {
            return Err(StorageError::Configuration(
                "deduplication TTL must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            partitions: DashMap::new(),
            clock,
            dedup_ttl,
        })
    }

    pub fn development_default() -> Self {
        Self::new(
            Arc::new(super::SystemClock),
            Duration::from_secs(7 * 24 * 60 * 60),
        )
        .expect("the built-in deduplication TTL is valid")
    }

    pub fn stream_snapshot(&self, partition: &StoragePartition) -> Vec<TrackPoint> {
        self.partitions
            .get(partition)
            .map(|state| {
                state
                    .lock()
                    .expect("in-memory storage lock poisoned")
                    .stream
                    .clone()
            })
            .unwrap_or_default()
    }
}

impl Default for InMemoryCoopNavStorage {
    fn default() -> Self {
        Self::development_default()
    }
}

#[async_trait]
impl CoopNavStorage for InMemoryCoopNavStorage {
    async fn append_point(
        &self,
        partition: &StoragePartition,
        point: &TrackPoint,
    ) -> Result<AppendResult, StorageError> {
        let state = self
            .partitions
            .entry(partition.clone())
            .or_insert_with(|| Arc::new(Mutex::new(PartitionData::default())))
            .clone();
        let mut state = state
            .lock()
            .map_err(|_| StorageError::Configuration("in-memory storage lock poisoned".into()))?;
        let now = self.clock.now_millis();

        if let Some(existing) = state.ids.get(&point.id) {
            if existing.expires_at_ms > now {
                return Ok(if existing.point == *point {
                    AppendResult::AlreadyPresent
                } else {
                    AppendResult::Conflict
                });
            }
            state.ids.remove(&point.id);
        }

        let expires_at_ms =
            now.saturating_add(self.dedup_ttl.as_millis().min(u64::MAX as u128) as u64);
        state.ids.insert(
            point.id.clone(),
            StoredPoint {
                point: point.clone(),
                expires_at_ms,
            },
        );
        state.stream.push(point.clone());
        Ok(AppendResult::Inserted)
    }
}
