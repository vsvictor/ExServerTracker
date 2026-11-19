use super::super::{AppendResult, CoopNavStorage, RedisCoopNavStorage, StoragePartition};
use crate::model::{PointSource, TrackPoint};
use crate::{ConfirmedSession, RedisSessionAuthorizer, SessionAuthorizer, VerifiedPrincipal};
use redis::AsyncCommands;
use std::{
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

struct EphemeralRedis {
    child: Child,
    url: String,
    port: u16,
}

impl Drop for EphemeralRedis {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl EphemeralRedis {
    async fn start() -> Option<Self> {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).ok()?;
        let port = listener.local_addr().ok()?.port();
        drop(listener);
        let child = Command::new("redis-server")
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "yes",
                "--loglevel",
                "warning",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let url = format!("redis://127.0.0.1:{port}/");
        let client = redis::Client::open(url.as_str()).ok()?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if tokio::time::Instant::now() >= deadline {
                let mut child = child;
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            if let Ok(mut connection) = redis::aio::ConnectionManager::new(client.clone()).await {
                let ping: redis::RedisResult<String> =
                    redis::cmd("PING").query_async(&mut connection).await;
                if matches!(ping, Ok(ref response) if response == "PONG") {
                    return Some(Self { child, url, port });
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn point(id: &str, latitude: f64) -> TrackPoint {
    TrackPoint {
        id: id.to_owned(),
        raw_latitude: None,
        raw_longitude: None,
        raw_accuracy_meters: None,
        smoothed_latitude: latitude,
        smoothed_longitude: 30.52,
        smoothed_accuracy_meters: 5.0,
        speed_mps: None,
        bearing_degrees: None,
        device_time_ms: 1_763_500_000_000,
        observed_at_ms: 1_763_500_000_000,
        source: PointSource::Fused,
        is_mock: false,
        flags: vec![],
    }
}

#[tokio::test]
async fn redis_stream_deduplication_conflicts_and_type_errors_are_atomic_per_point() {
    let Some(redis) = EphemeralRedis::start().await else {
        eprintln!("redis-server not installed; skipping this ephemeral Redis test");
        return;
    };
    let prefix = format!(
        "coopnav:test:{}:{}",
        redis.port,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let storage = RedisCoopNavStorage::connect(&redis.url, &prefix, Duration::from_secs(60))
        .await
        .unwrap();
    let partition = StoragePartition {
        brand_id: "brand".into(),
        driver_id: "driver".into(),
    };
    let point = point("0123456789abcdef0123456789abcdef", 50.45);

    assert_eq!(
        storage.append_point(&partition, &point).await.unwrap(),
        AppendResult::Inserted
    );
    assert_eq!(
        storage.append_point(&partition, &point).await.unwrap(),
        AppendResult::AlreadyPresent
    );
    assert_eq!(
        storage
            .append_point(&partition, &self::point(&point.id, 50.46))
            .await
            .unwrap(),
        AppendResult::Conflict
    );

    let mut connection =
        redis::aio::ConnectionManager::new(redis::Client::open(redis.url.as_str()).unwrap())
            .await
            .unwrap();
    let base = format!("{prefix}:{{");
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{base}*"))
        .query_async(&mut connection)
        .await
        .unwrap();
    let stream = keys.iter().find(|key| key.ends_with(":stream")).unwrap();
    let entries: u64 = connection.xlen(stream).await.unwrap();
    assert_eq!(entries, 1);

    let wrong_type_stream = format!("{prefix}:{{wrong-type}}:stream");
    let wrong_type_dedup = format!("{prefix}:{{wrong-type}}:dedup");
    let _: () = connection
        .set(&wrong_type_stream, "not-a-stream")
        .await
        .unwrap();
    let wrong_script = redis::Script::new(
        "local t=redis.call('TYPE',KEYS[2]).ok; if t~='none' and t~='stream' then return redis.error_reply('wrong type') end; redis.call('XADD',KEYS[2],'*','payload',ARGV[1]); return 'ok'",
    );
    let result: redis::RedisResult<String> = wrong_script
        .key(&wrong_type_dedup)
        .key(wrong_type_stream)
        .arg("payload")
        .invoke_async(&mut connection)
        .await;
    assert!(result.is_err());
    assert!(
        !connection
            .exists::<_, bool>(&wrong_type_dedup)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn redis_session_grants_are_host_provisioned_subject_bound_and_revocable() {
    let Some(redis) = EphemeralRedis::start().await else {
        eprintln!("redis-server not found or failed to start; skipping ephemeral Redis test");
        return;
    };
    let authorizer =
        RedisSessionAuthorizer::connect(&redis.url, format!("coopnav:grant-test:{}", redis.port))
            .await
            .unwrap();
    let session = ConfirmedSession::new("session-1", "driver-1", "brand-1").unwrap();
    authorizer
        .provision_confirmed_session("subject-1", session.clone(), Duration::from_secs(30))
        .await
        .unwrap();
    authorizer
        .provision_confirmed_session("subject-1", session.clone(), Duration::from_secs(30))
        .await
        .unwrap();

    assert_eq!(
        authorizer
            .resolve(
                &VerifiedPrincipal {
                    subject: "subject-1".into()
                },
                "session-1"
            )
            .await
            .unwrap(),
        session
    );
    assert!(
        authorizer
            .resolve(
                &VerifiedPrincipal {
                    subject: "other".into()
                },
                "session-1"
            )
            .await
            .is_err()
    );
    let conflict = authorizer
        .provision_confirmed_session(
            "other",
            ConfirmedSession::new("session-1", "driver-2", "brand-1").unwrap(),
            Duration::from_secs(30),
        )
        .await;
    assert!(matches!(
        conflict,
        Err(super::super::StorageError::GrantConflict)
    ));
    assert!(authorizer.revoke("session-1").await.unwrap());
    assert!(
        authorizer
            .resolve(
                &VerifiedPrincipal {
                    subject: "subject-1".into()
                },
                "session-1"
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn redis_cooperative_geo_batch_keys_withdrawals_and_expiry() {
    use crate::cooperative::{
        CooperativeStorage, PeerState, PeerWrite, RedisCooperativeStorage, VehicleClass,
    };
    let Some(redis) = EphemeralRedis::start().await else {
        eprintln!("redis-server not found or failed to start; skipping ephemeral Redis test");
        return;
    };
    let peers = RedisCooperativeStorage::connect(&redis.url, "cooperative:test")
        .await
        .unwrap();
    let mut update = PeerWrite {
        donor_id: "opaque-donor".into(),
        subject_key: None,
        capture_ms: 10_000,
        state: Some(PeerState {
            latitude: 50.45,
            longitude: 30.52,
            accuracy_meters: 4.0,
            speed_mps: 0.0,
            bearing_degrees: 90.0,
            trust_score: 0.8,
            vehicle_class: VehicleClass::Bus,
            flags: Vec::new(),
        }),
    };
    let range = |namespace, brand, center, now| peers.nearby(namespace, brand, center, now, 2_000);
    let within = (50.45, 30.521);
    assert_eq!(
        peers
            .publish("one", "brand", &[update.clone()], 10_000, 2_000)
            .await
            .unwrap(),
        [true]
    );
    assert_eq!(
        range("one", "brand", within, 10_000)
            .await
            .unwrap()
            .unwrap()[0]
            .donor_id,
        "opaque-donor"
    );
    for (namespace, brand, center) in [
        ("one", "other", within),
        ("two", "brand", within),
        ("one", "brand", (50.45, 30.53)),
    ] {
        assert!(
            range(namespace, brand, center, 10_000)
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }
    let original = update.state.take();
    update.capture_ms = 10_100;
    assert_eq!(
        peers
            .publish("one", "brand", &[update.clone()], 10_100, 2_000)
            .await
            .unwrap(),
        [true]
    );
    assert!(
        range("one", "brand", within, 10_100)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    update.state = original;
    assert_eq!(
        peers
            .publish("one", "brand", &[update.clone()], 10_100, 2_000)
            .await
            .unwrap(),
        [false]
    );
    update.capture_ms = 10_200;
    assert_eq!(
        peers
            .publish("one", "brand", &[update.clone()], 10_200, 2_000)
            .await
            .unwrap(),
        [true]
    );
    assert!(
        range("one", "brand", within, 12_200)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    update.capture_ms = 12_201;
    assert_eq!(
        peers
            .publish("one", "brand", &[update.clone()], 12_201, 2_000)
            .await
            .unwrap(),
        [true]
    );
    peers.clear("one", "brand").await.unwrap();
    assert!(
        range("one", "brand", within, 12_201)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    let mut connection = redis::Client::open(redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("cooperative:test:*")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(keys.is_empty(), "{keys:?}");
    crate::cooperative::tests::v2v_without_gtfs(std::sync::Arc::new(peers)).await;
}
