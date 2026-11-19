mod memory;
#[cfg(feature = "redis-storage")]
mod redis_store;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

pub use memory::InMemoryCoopNavStorage;
#[cfg(feature = "redis-storage")]
pub use redis_store::RedisCoopNavStorage;

use crate::model::TrackPoint;

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoragePartition {
    pub brand_id: String,
    pub driver_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendResult {
    Inserted,
    AlreadyPresent,
    Conflict,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[cfg(feature = "redis-storage")]
    #[error("Redis storage failed")]
    Redis(#[source] redis::RedisError),
    #[error("storage configuration is invalid: {0}")]
    Configuration(String),
    #[error("stored point could not be serialized: {0}")]
    Serialization(String),
    #[error("the idempotency key conflicts with an existing point")]
    Conflict,
    #[error("Redis returned an unexpected response")]
    UnexpectedResponse,
    #[error("Redis grant conflicts with an existing session grant")]
    GrantConflict,
}

impl StorageError {
    #[cfg(feature = "redis-storage")]
    pub(crate) fn redis(error: redis::RedisError) -> Self {
        Self::Redis(error)
    }
}

#[async_trait]
pub trait CoopNavStorage: Send + Sync {
    async fn append_point(
        &self,
        partition: &StoragePartition,
        point: &TrackPoint,
    ) -> Result<AppendResult, StorageError>;
}

pub trait UnixMillisClock: Send + Sync {
    fn now_millis(&self) -> u64;
}

#[derive(Clone, Copy, Default)]
pub struct SystemClock;

impl UnixMillisClock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }
}

#[derive(Default)]
pub struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
        }
    }

    pub fn set(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }

    pub fn advance(&self, duration: Duration) {
        let delta = duration.as_millis().min(u64::MAX as u128) as u64;
        self.now_ms.fetch_add(delta, Ordering::SeqCst);
    }
}

impl UnixMillisClock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
