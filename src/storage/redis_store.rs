use super::{AppendResult, CoopNavStorage, StorageError, StoragePartition};
use crate::model::TrackPoint;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::time::Duration;

const APPEND_POINT_LUA: &str = r#"
local dedup_type = redis.call('TYPE', KEYS[1]).ok
local stream_type = redis.call('TYPE', KEYS[2]).ok
if dedup_type ~= 'none' and dedup_type ~= 'string' then
    return redis.error_reply('deduplication key has the wrong Redis type')
end
if stream_type ~= 'none' and stream_type ~= 'stream' then
    return redis.error_reply('track stream key has the wrong Redis type')
end

local previous = redis.call('GET', KEYS[1])
if previous then
    if previous == ARGV[1] then
        return { 'duplicate' }
    end
    return { 'conflict' }
end

local stream_id = redis.call('XADD', KEYS[2], '*',
    'point_id', ARGV[2], 'payload', ARGV[1])
local stored = redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3], 'NX')
if not stored then
    return redis.error_reply('deduplication write failed after stream append')
end
return { 'inserted', stream_id }
"#;

#[derive(Clone)]
pub struct RedisCoopNavStorage {
    connection: redis::aio::ConnectionManager,
    key_prefix: String,
    dedup_ttl: Duration,
}

impl RedisCoopNavStorage {
    pub async fn connect(
        redis_url: &str,
        key_prefix: impl Into<String>,
        dedup_ttl: Duration,
    ) -> Result<Self, StorageError> {
        if dedup_ttl.is_zero() || dedup_ttl.as_secs() == 0 || dedup_ttl.as_secs() > i64::MAX as u64
        {
            return Err(StorageError::Configuration(
                "deduplication TTL must be at least one second".to_owned(),
            ));
        }
        let key_prefix = key_prefix.into();
        if key_prefix.trim().is_empty() {
            return Err(StorageError::Configuration(
                "Redis key prefix must not be empty".to_owned(),
            ));
        }
        let client = redis::Client::open(redis_url).map_err(StorageError::redis)?;
        let connection = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(StorageError::redis)?;
        Ok(Self {
            connection,
            key_prefix,
            dedup_ttl,
        })
    }

    fn keys(&self, partition: &StoragePartition, point_id: &str) -> (String, String) {
        let scope = format!("{}\0{}", partition.brand_id, partition.driver_id);
        let tag = hex::encode(Sha256::digest(scope.as_bytes()));
        let base = format!("{}:{{{tag}}}", self.key_prefix);
        (format!("{base}:dedup:{point_id}"), format!("{base}:stream"))
    }
}

#[async_trait]
impl CoopNavStorage for RedisCoopNavStorage {
    async fn append_point(
        &self,
        partition: &StoragePartition,
        point: &TrackPoint,
    ) -> Result<AppendResult, StorageError> {
        let (dedup_key, stream_key) = self.keys(partition, &point.id);
        let payload = serde_json::to_string(point)
            .map_err(|error| StorageError::Serialization(error.to_string()))?;
        let ttl = self.dedup_ttl.as_secs().to_string();
        let script = redis::Script::new(APPEND_POINT_LUA);
        let mut connection = self.connection.clone();
        let result: Vec<String> = script
            .key(dedup_key)
            .key(stream_key)
            .arg(payload)
            .arg(&point.id)
            .arg(ttl)
            .invoke_async(&mut connection)
            .await
            .map_err(StorageError::redis)?;
        match result.first().map(String::as_str) {
            Some("inserted") => Ok(AppendResult::Inserted),
            Some("duplicate") => Ok(AppendResult::AlreadyPresent),
            Some("conflict") => Ok(AppendResult::Conflict),
            _ => Err(StorageError::UnexpectedResponse),
        }
    }
}
