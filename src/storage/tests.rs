use super::{AppendResult, CoopNavStorage, InMemoryCoopNavStorage, ManualClock, StoragePartition};
use crate::model::{PointSource, TrackPoint};
use std::{sync::Arc, time::Duration};

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
        track_increment_meters: None,
        phantom_increment_meters: None,
    }
}

fn partition(brand_id: &str, driver_id: &str) -> StoragePartition {
    StoragePartition {
        brand_id: brand_id.into(),
        driver_id: driver_id.into(),
    }
}

#[tokio::test]
async fn deduplicates_same_payload_rejects_conflict_and_isolates_driver_brand() {
    let clock = Arc::new(ManualClock::new(1_000));
    let storage = InMemoryCoopNavStorage::new(clock, Duration::from_secs(60)).expect("valid TTL");
    let first = partition("brand-a", "driver-a");
    let second = partition("brand-b", "driver-a");
    let original = point("0123456789abcdef0123456789abcdef", 50.45);

    assert_eq!(
        storage.append_point(&first, &original).await.unwrap(),
        AppendResult::Inserted
    );
    assert_eq!(
        storage.append_point(&first, &original).await.unwrap(),
        AppendResult::AlreadyPresent
    );
    assert_eq!(
        storage
            .append_point(&first, &point(&original.id, 50.46))
            .await
            .unwrap(),
        AppendResult::Conflict
    );
    assert_eq!(
        storage.append_point(&second, &original).await.unwrap(),
        AppendResult::Inserted
    );
    assert_eq!(storage.stream_snapshot(&first).len(), 1);
    assert_eq!(storage.stream_snapshot(&second).len(), 1);
}

#[tokio::test]
async fn deduplication_expiry_uses_injected_clock() {
    let clock = Arc::new(ManualClock::new(5_000));
    let storage =
        InMemoryCoopNavStorage::new(clock.clone(), Duration::from_secs(1)).expect("valid TTL");
    let partition = partition("brand", "driver");
    let point = point("0123456789abcdef0123456789abcdef", 50.45);

    assert_eq!(
        storage.append_point(&partition, &point).await.unwrap(),
        AppendResult::Inserted
    );
    clock.advance(Duration::from_secs(1));
    assert_eq!(
        storage.append_point(&partition, &point).await.unwrap(),
        AppendResult::Inserted
    );
    assert_eq!(storage.stream_snapshot(&partition).len(), 2);
}

#[tokio::test]
async fn concurrent_retries_have_one_append_and_idempotent_acknowledgements() {
    let storage = Arc::new(InMemoryCoopNavStorage::default());
    let partition = partition("brand", "driver");
    let point = point("0123456789abcdef0123456789abcdef", 50.45);
    let (a, b) = tokio::join!(
        storage.append_point(&partition, &point),
        storage.append_point(&partition, &point)
    );
    let results = [a.unwrap(), b.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == AppendResult::Inserted)
            .count(),
        1
    );
    assert_eq!(storage.stream_snapshot(&partition).len(), 1);
}

#[cfg(feature = "redis-storage")]
#[path = "redis/tests.rs"]
mod redis_tests;
