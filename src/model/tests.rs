use super::{IngestParams, PointSource, TrackPoint};
use serde_json::json;

fn point(id: &str) -> TrackPoint {
    TrackPoint {
        id: id.to_owned(),
        raw_latitude: Some(50.45),
        raw_longitude: Some(30.52),
        raw_accuracy_meters: Some(5.0),
        smoothed_latitude: 50.4501,
        smoothed_longitude: 30.5201,
        smoothed_accuracy_meters: 4.0,
        speed_mps: Some(10.0),
        bearing_degrees: Some(180.0),
        device_time_ms: 1_763_500_000_000,
        observed_at_ms: 1_763_500_000_000,
        source: PointSource::Fused,
        is_mock: false,
        track_increment_meters: None,
        phantom_increment_meters: None,
        flags: vec![
            "interference".into(),
            "SUSPECTED_INTERFERENCE".into(),
            "SUSPECTED_SPOOFING".into(),
        ],
    }
}

#[test]
fn mileage_increments_round_trip_and_validate_as_a_pair() {
    let mut point = point("0123456789abcdef0123456789abcdef");
    let old = serde_json::to_value(&point).unwrap();
    assert!(old.get("trackIncrementMeters").is_none());
    let decoded: TrackPoint = serde_json::from_value(old).unwrap();
    assert_eq!(decoded.track_increment_meters, None);
    assert_eq!(decoded.phantom_increment_meters, None);
    assert!(decoded.validate().is_ok());

    point.track_increment_meters = Some(12.5);
    point.phantom_increment_meters = Some(3.0);
    let json = serde_json::to_value(&point).unwrap();
    assert_eq!(json["trackIncrementMeters"], 12.5);
    assert_eq!(json["phantomIncrementMeters"], 3.0);
    let decoded: TrackPoint = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.track_increment_meters, Some(12.5));
    assert_eq!(decoded.phantom_increment_meters, Some(3.0));
    assert!(decoded.validate().is_ok());

    point.phantom_increment_meters = Some(13.0);
    assert!(point.validate().is_err());
    point.phantom_increment_meters = Some(-1.0);
    assert!(point.validate().is_err());
    point.phantom_increment_meters = Some(f64::NAN);
    assert!(point.validate().is_err());
    point.phantom_increment_meters = None;
    assert!(point.validate().is_err());
}

#[test]
fn accepts_wire_source_names_and_bounded_legacy_and_canonical_flags() {
    for (source, expected) in [
        ("fused", PointSource::Fused),
        ("gps", PointSource::Gps),
        ("network", PointSource::Network),
        ("deadReckoning", PointSource::DeadReckoning),
        ("stationary", PointSource::Stationary),
        ("cooperative", PointSource::Cooperative),
    ] {
        let value = json!({
            "id": "0123456789abcdef0123456789abcdef",
            "smoothedLatitude": 50.45,
            "smoothedLongitude": 30.52,
            "smoothedAccuracyMeters": 4.0,
            "deviceTimeMs": 1763500000000_i64,
            "observedAtMs": 1763500000000_i64,
            "source": source,
            "isMock": false,
            "flags": ["interference", "SUSPECTED_INTERFERENCE", "SUSPECTED_SPOOFING"]
        });
        let decoded: TrackPoint = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.source, expected);
        assert!(decoded.validate().is_ok());
    }
}

#[test]
fn rejects_invalid_point_values_and_partial_raw_coordinates() {
    let mut invalid = point("0123456789abcdef0123456789abcdef");
    invalid.raw_longitude = None;
    assert!(invalid.validate().is_err());

    let mut invalid = point("0123456789abcdef0123456789abcdef");
    invalid.smoothed_latitude = f64::NAN;
    assert!(invalid.validate().is_err());

    let mut invalid = point("0123456789abcdef0123456789abcdef");
    invalid.speed_mps = Some(70.0);
    assert!(invalid.validate().is_err());

    let invalid = point("not-a-32-hex-id");
    assert!(invalid.validate().is_err());
}

#[test]
fn permits_stationary_accuracy_growth_and_old_or_out_of_order_positive_times() {
    let mut stationary = point("0123456789abcdef0123456789abcdef");
    stationary.source = PointSource::Stationary;
    stationary.raw_latitude = None;
    stationary.raw_longitude = None;
    stationary.raw_accuracy_meters = None;
    stationary.speed_mps = None;
    stationary.bearing_degrees = None;
    stationary.smoothed_accuracy_meters = 10_000.0;
    stationary.device_time_ms = 10;
    stationary.observed_at_ms = 1;
    assert!(stationary.validate().is_ok());

    let mut network = point("1123456789abcdef0123456789abcdef");
    network.source = PointSource::Network;
    network.speed_mps = None;
    network.bearing_degrees = None;
    assert!(network.validate().is_ok());
}

#[test]
fn stationary_accepts_complete_raw_fix_but_dead_reckoning_rejects_it() {
    let mut stationary = point("0123456789abcdef0123456789abcdef");
    stationary.source = PointSource::Stationary;
    stationary.observed_at_ms = stationary.device_time_ms - 5_000;
    assert!(stationary.validate().is_ok());
    stationary.raw_accuracy_meters = None;
    assert!(stationary.validate().is_err());
    stationary.raw_accuracy_meters = Some(5.0);
    stationary.raw_latitude = Some(f64::NAN);
    assert!(stationary.validate().is_err());

    let mut predicted = point("1123456789abcdef0123456789abcdef");
    predicted.source = PointSource::DeadReckoning;
    assert!(predicted.validate().is_err());
    predicted.raw_latitude = None;
    predicted.raw_longitude = None;
    predicted.raw_accuracy_meters = None;
    assert!(predicted.validate().is_ok());
}

#[test]
fn rejects_entire_batch_before_storage_when_a_late_point_is_invalid_or_conflicting() {
    let valid = point("0123456789abcdef0123456789abcdef");
    let mut bad = point("1123456789abcdef0123456789abcdef");
    bad.observed_at_ms = 0;
    let params = IngestParams {
        session_id: "session".into(),
        driver_id: "driver".into(),
        brand_id: "brand".into(),
        points: vec![valid.clone(), bad],
    };
    assert!(params.validate().is_err());

    let mut conflicting = valid.clone();
    conflicting.smoothed_latitude += 0.01;
    let params = IngestParams {
        session_id: "session".into(),
        driver_id: "driver".into(),
        brand_id: "brand".into(),
        points: vec![valid, conflicting],
    };
    assert!(params.validate().is_err());
}
