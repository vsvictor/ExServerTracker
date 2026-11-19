use super::{CooperativeStorage, MAX_NEARBY, MAX_PER_BRAND, PeerRecord, PeerWrite};
use crate::StorageError;
use async_trait::async_trait;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::time::Duration;

// The per-brand ordering ZSET retains withdrawals until their captures are no longer admissible.
// The scripts run on one standalone Redis primary; all keys also share a hash tag.
const PRUNE: &str = r#"
local function prune(geo, order, state_prefix, cutoff)
    local expired = redis.call('ZRANGEBYSCORE', order, '-inf', cutoff)
    for i = 1, #expired, 128 do
        local batch = {}
        local keys = {}
        for j = i, math.min(i + 127, #expired) do
            table.insert(batch, expired[j])
            table.insert(keys, state_prefix .. expired[j])
        end
        redis.call('ZREM', geo, unpack(batch))
        redis.call('ZREM', order, unpack(batch))
        redis.call('DEL', unpack(keys))
    end
end
"#;

const PUBLISH: &str = r#"
local now, limit, cap = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local previous_now = tonumber(redis.call('GET', KEYS[3]) or '0')
local results = {}
if now < previous_now then
    for i = 5, #ARGV, 5 do table.insert(results, 0) end
    return results
end
prune(KEYS[1], KEYS[2], ARGV[1], now - limit)
local size = redis.call('ZCARD', KEYS[2])
local function within(epoch) return epoch > 0 and epoch <= now and epoch > now - limit end
for i = 5, #ARGV, 5 do
    local id, capture = ARGV[i], tonumber(ARGV[i + 1])
    local prior = redis.call('ZSCORE', KEYS[2], id)
    if within(capture) and ((prior and tonumber(prior) < capture) or (not prior and size < cap)) then
        if ARGV[i + 2] == '' then
            redis.call('ZREM', KEYS[1], id)
            redis.call('DEL', ARGV[1] .. id)
        else
            redis.call('GEOADD', KEYS[1], ARGV[i + 3], ARGV[i + 4], id)
            redis.call('SET', ARGV[1] .. id, ARGV[i + 2], 'PX', limit)
        end
        redis.call('ZADD', KEYS[2], capture, id)
        if not prior then size = size + 1 end
        table.insert(results, 1)
    else
        table.insert(results, 0)
    end
end
redis.call('PEXPIRE', KEYS[1], 2 * limit)
redis.call('PEXPIRE', KEYS[2], 2 * limit)
redis.call('SET', KEYS[3], ARGV[2], 'PX', 2 * limit)
return results
"#;

const NEARBY: &str = r#"
local now, limit, count = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[6])
if now < tonumber(redis.call('GET', KEYS[3]) or '0') then return {0} end
prune(KEYS[1], KEYS[2], ARGV[1], now - limit)
local ids = redis.call('GEOSEARCH', KEYS[1], 'FROMLONLAT', ARGV[4], ARGV[5],
    'BYRADIUS', 200, 'm', 'COUNT', count + 1)
if #ids > count then return {0} end
if #ids == 0 then return {1, {}} end
for i, id in ipairs(ids) do ids[i] = ARGV[1] .. id end
return {1, redis.call('MGET', unpack(ids))}
"#;

const CLEAR: &str = r#"
prune(KEYS[1], KEYS[2], ARGV[1], '+inf')
return redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
"#;

#[derive(Clone)]
pub struct RedisCooperativeStorage {
    connection: ConnectionManager,
    prefix: String,
    publish_script: redis::Script,
    nearby_script: redis::Script,
    clear_script: redis::Script,
}

impl RedisCooperativeStorage {
    pub async fn connect(
        redis_url: &str,
        key_prefix: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let prefix = key_prefix.into();
        if prefix.trim().is_empty() || prefix.contains(['{', '}']) {
            return Err(StorageError::Configuration(
                "invalid cooperative Redis prefix".into(),
            ));
        }
        let client = redis::Client::open(redis_url).map_err(StorageError::redis)?;
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Duration::from_secs(2))
            .set_response_timeout(Duration::from_millis(250));
        let connection = ConnectionManager::new_with_config(client, config)
            .await
            .map_err(StorageError::redis)?;
        Ok(Self {
            connection,
            prefix,
            publish_script: redis::Script::new(&format!("{PRUNE}{PUBLISH}")),
            nearby_script: redis::Script::new(&format!("{PRUNE}{NEARBY}")),
            clear_script: redis::Script::new(&format!("{PRUNE}{CLEAR}")),
        })
    }

    fn keys(&self, namespace: &str, brand: &str) -> (String, String, String, String) {
        let tag = super::super::opaque(&[namespace, brand]);
        let base = format!("{}:peers:{{{tag}}}", self.prefix);
        (
            format!("{base}:geo"),
            format!("{base}:ordering"),
            format!("{base}:clock"),
            format!("{base}:state:"),
        )
    }
}

#[async_trait]
impl CooperativeStorage for RedisCooperativeStorage {
    async fn publish(
        &self,
        namespace: &str,
        brand: &str,
        updates: &[PeerWrite],
        now: u64,
        freshness_ms: u64,
    ) -> Result<Vec<bool>, StorageError> {
        let (geo, order, clock, states) = self.keys(namespace, brand);
        let mut script = self.publish_script.prepare_invoke();
        script
            .key(geo)
            .key(order)
            .key(clock)
            .arg(states)
            .arg(now)
            .arg(freshness_ms)
            .arg(MAX_PER_BRAND);
        for update in updates {
            let (body, lon, lat) = if let Some(state) = &update.state {
                if !state.valid() {
                    return Err(StorageError::UnexpectedResponse);
                }
                let record = PeerRecord {
                    donor_id: update.donor_id.clone(),
                    subject_key: update.subject_key.clone(),
                    capture_ms: update.capture_ms,
                    received_ms: now,
                    state: state.clone(),
                };
                (
                    serde_json::to_string(&record).map_err(|_| StorageError::UnexpectedResponse)?,
                    state.longitude,
                    state.latitude,
                )
            } else {
                (String::new(), 0.0, 0.0)
            };
            script
                .arg(&update.donor_id)
                .arg(update.capture_ms)
                .arg(body)
                .arg(lon)
                .arg(lat);
        }
        script
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(StorageError::redis)
    }

    async fn nearby(
        &self,
        namespace: &str,
        brand: &str,
        center: (f64, f64),
        now: u64,
        freshness_ms: u64,
    ) -> Result<Option<Vec<PeerRecord>>, StorageError> {
        let (geo, order, clock, states) = self.keys(namespace, brand);
        // An empty/overflow reply intentionally stays distinct from a complete empty snapshot.
        let result: Vec<redis::Value> = self
            .nearby_script
            .key(geo)
            .key(order)
            .key(clock)
            .arg(states)
            .arg(now)
            .arg(freshness_ms)
            .arg(center.1)
            .arg(center.0)
            .arg(MAX_NEARBY)
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(StorageError::redis)?;
        match result.as_slice() {
            [redis::Value::Int(0)] => Ok(None),
            [redis::Value::Int(1), redis::Value::Array(raw)] => {
                let mut records = Vec::with_capacity(raw.len());
                for value in raw {
                    match value {
                        redis::Value::Nil => (),
                        redis::Value::BulkString(bytes) => records.push(
                            serde_json::from_slice(bytes)
                                .map_err(|_| StorageError::UnexpectedResponse)?,
                        ),
                        _ => return Err(StorageError::UnexpectedResponse),
                    }
                }
                Ok(Some(records))
            }
            _ => Err(StorageError::UnexpectedResponse),
        }
    }

    async fn clear(&self, namespace: &str, brand: &str) -> Result<(), StorageError> {
        let (geo, order, clock, states) = self.keys(namespace, brand);
        let _: u32 = self
            .clear_script
            .key(geo)
            .key(order)
            .key(clock)
            .arg(states)
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(StorageError::redis)?;
        Ok(())
    }
}
