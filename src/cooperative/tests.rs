use super::*;
use crate::{ConfirmedSession, ManualClock};
use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

const NOW: u64 = 10_000;

fn state(meters: f64, class: VehicleClass) -> PeerState {
    let (latitude, longitude) = spatial::project(50.45, 30.52, meters, 90.0);
    PeerState {
        latitude,
        longitude,
        vehicle_class: class,
        accuracy_meters: 1.0,
        speed_mps: 0.0,
        bearing_degrees: 90.0,
        trust_score: 1.0,
        flags: Vec::new(),
    }
}

fn observation(id: &str, meters: f64, class: VehicleClass) -> Observation {
    Observation {
        observation_id: id.into(),
        vehicle_class: class,
        distance_meters: meters,
        azimuth_degrees: 90.0,
        relative_speed_mps: None,
        vision_variance_meters2: 4.0,
    }
}

fn params(
    session: &ConfirmedSession,
    time: u64,
    state: PeerState,
    observed_peers: Vec<Observation>,
) -> SyncPeersParams {
    SyncPeersParams {
        session_id: session.session_id.clone(),
        driver_id: session.driver_id.clone(),
        brand_id: session.brand_id.clone(),
        capture_timestamp_ms: time,
        state,
        observed_peers,
    }
}

#[derive(Default)]
struct Source {
    started: Notify,
    release: Notify,
    called: AtomicBool,
}

#[async_trait]
impl TransitSource for Source {
    fn brand_id(&self) -> &str {
        "brand"
    }

    async fn fetch(&self, now: u64, _: u64) -> Result<Vec<TransitFix>, GtfsError> {
        self.called.store(true, Ordering::SeqCst);
        self.started.notify_one();
        self.release.notified().await;
        Ok(vec![TransitFix {
            vehicle_id: "transit".into(),
            capture_ms: now,
            state: state(20.0, VehicleClass::Trolleybus),
        }])
    }
}

fn controls(
    store: Arc<dyn CooperativeStorage>,
    source: Arc<Source>,
    clock: Arc<ManualClock>,
) -> SectionControls {
    SectionControls::new(
        clock,
        Some(
            CooperativeConfig::new(
                store,
                Some(source),
                Duration::from_secs(2),
                Duration::from_millis(100),
            )
            .unwrap(),
        ),
    )
}

#[tokio::test]
async fn sections_fail_closed_and_cancel_fetch_with_generation_fencing() {
    let storage = Arc::new(InMemoryCooperativeStorage::default());
    let clock = Arc::new(ManualClock::new(NOW));
    let source = Arc::new(Source::default());
    let unconfigured = SectionControls::new(clock.clone(), None);
    assert_eq!(
        unconfigured.section(2, true).await,
        Err(SectionError::Unconfigured)
    );
    let sections = controls(storage.clone(), source.clone(), clock);
    assert_eq!(sections.gtfs_status().await, GtfsStatus::Disabled);
    assert!(sections.section_enabled(1).await.unwrap());
    assert!(!sections.section_enabled(2).await.unwrap());
    assert!(!sections.section_enabled(3).await.unwrap());
    assert_eq!(
        sections.section(3, true).await,
        Err(SectionError::StageThree)
    );
    let handle = sections.maintenance_handle();
    let task = handle.maintenance()[0].clone();
    task.run().await.unwrap();
    assert!(!source.called.load(Ordering::SeqCst));
    sections.section(2, true).await.unwrap();
    assert_eq!(sections.gtfs_status().await, GtfsStatus::Enabled);
    let old = sections.generation().unwrap();
    let poll = tokio::spawn(task.run());
    source.started.notified().await;
    sections.section(1, false).await.unwrap();
    assert_eq!(
        sections.section(2, true).await,
        Err(SectionError::RequiresStageOne)
    );
    sections.section(1, true).await.unwrap();
    assert!(!sections.section_enabled(2).await.unwrap());
    sections.section(2, true).await.unwrap();
    let new = sections.generation().unwrap();
    assert_ne!(old, new);
    poll.await.unwrap().unwrap();
    let caller = ConfirmedSession::new("one", "one", "brand").unwrap();
    assert!(matches!(
        sections
            .sync(
                &old,
                "one",
                &caller,
                params(&caller, NOW, state(0.0, VehicleClass::Car), Vec::new())
            )
            .await,
        Err(SectionError::CooperativeDisabled)
    ));

    source.release.notify_one();
    task.run().await.unwrap();
    let center = (50.45, 30.52);
    let donors = storage
        .nearby(&new, "brand", center, NOW, 2000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(donors.len(), 1);
    assert_eq!(donors[0].state.vehicle_class, VehicleClass::Trolleybus);
    sections.section(2, false).await.unwrap();
    assert!(
        storage
            .nearby(&new, "brand", center, NOW, 2000)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn scope_by_brand_and_subject_and_withdraw_unsafe_peers() {
    v2v_without_gtfs(Arc::new(InMemoryCooperativeStorage::default())).await;
}

pub(crate) async fn v2v_without_gtfs(storage: Arc<dyn CooperativeStorage>) {
    let clock = Arc::new(ManualClock::new(NOW));
    let config = CooperativeConfig::new(
        storage.clone(),
        None,
        Duration::from_secs(2),
        Duration::ZERO,
    )
    .unwrap();
    let sections = SectionControls::new(clock.clone(), Some(config));
    sections.section(2, true).await.unwrap();
    assert_eq!(
        serde_json::to_value(sections.gtfs_status().await).unwrap(),
        "gtfs-unconfigured"
    );
    assert!(sections.maintenance_handle().maintenance().is_empty());
    let epoch = sections.generation().unwrap();
    let caller = ConfirmedSession::new("caller-session", "caller", "brand").unwrap();
    let donor = ConfirmedSession::new("donor-session", "donor", "brand").unwrap();
    let outside = ConfirmedSession::new("third-session", "third", "other-brand").unwrap();
    let class = VehicleClass::Bus;
    let mut donor_state = state(40.0, class);
    donor_state.speed_mps = 10.0;
    donor_state.flags.push("gnssTimeUnverified".into());
    let reply = |time| {
        let mut requester = state(0.0, VehicleClass::Car);
        requester.trust_score = 0.0;
        requester.flags = [
            "gnssTimeUnverified",
            "deviceTimeMismatch",
            "SUSPECTED_SPOOFING",
            "SUSPECTED_INTERFERENCE",
            "predicted",
            "positionStale",
            "gnssStale",
            "cooperative",
        ]
        .map(str::to_owned)
        .into();
        params(
            &caller,
            time,
            requester,
            vec![observation("view", 50.0, class)],
        )
    };
    assert!(reply(NOW).valid());
    sections
        .sync(
            &epoch,
            "third",
            &outside,
            params(&outside, NOW, state(50.0, class), vec![]),
        )
        .await
        .unwrap();
    sections
        .sync(
            &epoch,
            "donor-subject",
            &donor,
            params(&donor, NOW - 1_000, donor_state.clone(), vec![]),
        )
        .await
        .unwrap();
    let found = sections
        .sync(&epoch, "caller-subject", &caller, reply(NOW))
        .await
        .unwrap();
    assert_eq!(found.matched_donors.len(), 1);
    let confirmed = &found.matched_donors[0];
    assert_eq!(confirmed.capture_timestamp_ms, NOW);
    assert_eq!(confirmed.trust_score, 0.8);
    assert!(confirmed.accuracy_meters > 3.0);
    assert!(
        (spatial::distance((50.45, 30.52), (confirmed.latitude, confirmed.longitude)) - 50.0).abs()
            < 0.05
    );
    assert_eq!(confirmed.donor_id.len(), 32);
    assert!(matches!(
        sections
            .sync(&epoch, "caller-subject", &caller, reply(NOW))
            .await,
        Err(SectionError::Timestamp)
    ));
    assert!(matches!(
        sections
            .sync(&epoch, "caller-subject", &caller, reply(NOW + 1))
            .await,
        Err(SectionError::Timestamp)
    ));
    clock.set(NOW + 20);
    let own = sections
        .sync(&epoch, "donor-subject", &caller, reply(NOW + 1))
        .await
        .unwrap();
    assert!(own.matched_donors.is_empty());
    donor_state.flags.push("SUSPECTED_SPOOFING".into());
    sections
        .sync(
            &epoch,
            "donor-subject",
            &donor,
            params(&donor, NOW + 2, donor_state, vec![]),
        )
        .await
        .unwrap();
    let withdrawn = sections
        .sync(&epoch, "caller-subject", &caller, reply(NOW + 3))
        .await
        .unwrap();
    assert!(withdrawn.matched_donors.is_empty());
    assert!(matches!(
        sections
            .sync(&epoch, "caller-subject", &caller, reply(NOW - 2000))
            .await,
        Err(SectionError::Timestamp)
    ));
    sections.section(2, false).await.unwrap();
    assert!(
        storage
            .nearby(&epoch, "brand", (50.45, 30.52), NOW + 20, 2000)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    sections.section(2, true).await.unwrap();
    assert_ne!(sections.generation().unwrap(), epoch);
    assert!(sections.maintenance_handle().maintenance().is_empty());
}

#[tokio::test]
async fn configured_transit_failure_is_returned_to_the_host() {
    struct Failing;
    #[async_trait]
    impl TransitSource for Failing {
        fn brand_id(&self) -> &str {
            "brand"
        }
        async fn fetch(&self, _: u64, _: u64) -> Result<Vec<TransitFix>, GtfsError> {
            Err(GtfsError::Fetch)
        }
    }
    let config = CooperativeConfig::new(
        Arc::new(InMemoryCooperativeStorage::default()),
        Some(Arc::new(Failing)),
        Duration::from_secs(2),
        Duration::from_millis(100),
    )
    .unwrap();
    let sections = SectionControls::new(Arc::new(ManualClock::new(NOW)), Some(config));
    sections.section(2, true).await.unwrap();
    let error = sections.maintenance_handle().maintenance()[0]
        .run()
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<GtfsError>().unwrap().to_string(),
        "GTFS request failed"
    );
}

#[test]
fn match_is_one_to_one_and_ambiguity_is_not_resolved_by_weighting() {
    let session = ConfirmedSession::new("a", "a", "brand").unwrap();
    let class = VehicleClass::Car;
    let mut p = params(
        &session,
        NOW,
        state(0.0, class),
        vec![observation("a", 30.0, class)],
    );
    let candidate = |id: &str, meters, class| PeerRecord {
        donor_id: id.into(),
        subject_key: None,
        capture_ms: NOW,
        received_ms: NOW,
        state: state(meters, class),
    };
    let match_peers = |p: &SyncPeersParams, records| {
        spatial::matched(p, records, "self", "self", NOW, 2000).matched_donors
    };
    assert!(
        match_peers(
            &p,
            vec![candidate("a", 30.0, class), candidate("b", 40.0, class)]
        )
        .is_empty()
    );
    assert_eq!(
        match_peers(
            &p,
            vec![
                candidate("a", 30.0, class),
                candidate("b", 30.0, VehicleClass::Bus)
            ]
        )
        .len(),
        1
    );
    assert!(match_peers(&p, vec![candidate("self", 30.0, class)]).is_empty());
    p.observed_peers.push(observation("b", 32.0, class));
    assert!(match_peers(&p, vec![candidate("a", 30.0, class)]).is_empty());
    p.observed_peers = (1..=4)
        .map(|i| observation(&i.to_string(), (i * 40) as f64, class))
        .collect();
    assert_eq!(
        match_peers(
            &p,
            (1..=4)
                .map(|i| candidate(&i.to_string(), (i * 40) as f64, class))
                .collect()
        )
        .len(),
        3
    );
}

fn record(id: &str, peer: PeerState, time: u64) -> PeerRecord {
    PeerRecord {
        donor_id: id.into(),
        subject_key: None,
        capture_ms: time,
        received_ms: time,
        state: peer,
    }
}

#[test]
fn low_trust_or_range_beyond_two_hundred_meters_yields_no_donor() {
    let session = ConfirmedSession::new("a", "a", "brand").unwrap();
    let class = VehicleClass::Car;
    let near = params(
        &session,
        NOW,
        state(0.0, class),
        vec![observation("near", 30.0, class)],
    );
    let mut weak = state(30.0, class);
    weak.trust_score = 0.69;
    assert!(
        spatial::matched(
            &near,
            vec![record("weak", weak, NOW)],
            "self",
            "self",
            NOW,
            2000
        )
        .matched_donors
        .is_empty()
    );

    let far = params(
        &session,
        NOW,
        state(0.0, class),
        vec![observation("far", 200.0, class)],
    );
    assert!(
        spatial::matched(
            &far,
            vec![record("distant", state(201.0, class), NOW)],
            "self",
            "self",
            NOW,
            2000
        )
        .matched_donors
        .is_empty()
    );
}

#[test]
fn extrapolated_unique_pair_within_twelve_meters_is_returned() {
    let session = ConfirmedSession::new("a", "a", "brand").unwrap();
    let class = VehicleClass::Bus;
    let request = params(
        &session,
        NOW,
        state(0.0, class),
        vec![observation("moving", 50.0, class)],
    );
    let mut moving = state(30.0, class);
    moving.speed_mps = 10.0;
    let captured = NOW - 1_500;
    let matched = spatial::matched(
        &request,
        vec![record("moving", moving.clone(), captured)],
        "self",
        "self",
        NOW,
        3_000,
    );
    assert_eq!(matched.matched_donors.len(), 1);
    let own = (50.45, 30.52);
    let returned = (
        matched.matched_donors[0].latitude,
        matched.matched_donors[0].longitude,
    );
    let travelled = spatial::distance(own, returned);
    assert!(
        (travelled - 45.0).abs() < 1.0,
        "extrapolated donor was {travelled} m from the requester"
    );
    moving.speed_mps = 0.0;
    assert!(
        spatial::matched(
            &request,
            vec![record("still", moving, captured)],
            "self",
            "self",
            NOW,
            3_000
        )
        .matched_donors
        .is_empty()
    );
}

#[tokio::test]
async fn gtfs_bus_and_trolleybus_match_for_one_brand_and_car_stays_out_of_the_index() {
    struct Feed;

    #[async_trait]
    impl TransitSource for Feed {
        fn brand_id(&self) -> &str {
            "brand"
        }

        async fn fetch(&self, now: u64, _: u64) -> Result<Vec<TransitFix>, GtfsError> {
            Ok(vec![
                TransitFix {
                    vehicle_id: "bus-1".into(),
                    capture_ms: now,
                    state: state(30.0, VehicleClass::Bus),
                },
                TransitFix {
                    vehicle_id: "wire-1".into(),
                    capture_ms: now,
                    state: state(80.0, VehicleClass::Trolleybus),
                },
                TransitFix {
                    vehicle_id: "car-1".into(),
                    capture_ms: now,
                    state: state(50.0, VehicleClass::Car),
                },
            ])
        }
    }

    let storage = Arc::new(InMemoryCooperativeStorage::default());
    let config = CooperativeConfig::new(
        storage.clone(),
        Some(Arc::new(Feed)),
        Duration::from_secs(2),
        Duration::from_millis(100),
    )
    .unwrap();
    let sections = SectionControls::new(Arc::new(ManualClock::new(NOW)), Some(config));
    sections.section(2, true).await.unwrap();
    sections.maintenance_handle().maintenance()[0]
        .run()
        .await
        .unwrap();
    let epoch = sections.generation().unwrap();
    let mut indexed: Vec<_> = storage
        .nearby(&epoch, "brand", (50.45, 30.52), NOW, 2000)
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|peer| peer.state.vehicle_class)
        .collect();
    indexed.sort_by_key(|class| format!("{class:?}"));
    assert_eq!(indexed, vec![VehicleClass::Bus, VehicleClass::Trolleybus]);

    let caller = ConfirmedSession::new("caller-session", "caller", "brand").unwrap();
    let found = sections
        .sync(
            &epoch,
            "caller-subject",
            &caller,
            params(
                &caller,
                NOW,
                state(0.0, VehicleClass::Car),
                vec![
                    observation("bus", 30.0, VehicleClass::Bus),
                    observation("wire", 80.0, VehicleClass::Trolleybus),
                    observation("car", 50.0, VehicleClass::Car),
                ],
            ),
        )
        .await
        .unwrap();
    let mut matched: Vec<_> = found
        .matched_donors
        .iter()
        .map(|donor| donor.vehicle_class)
        .collect();
    matched.sort_by_key(|class| format!("{class:?}"));
    assert_eq!(matched, vec![VehicleClass::Bus, VehicleClass::Trolleybus]);
}

#[cfg(feature = "redis-storage")]
#[tokio::test]
#[ignore = "set COOPNAV_REDIS_URL; times NEARBY only, not QUIC or RPC JSON"]
async fn redis_nearby_p99() {
    let Ok(url) = std::env::var("COOPNAV_REDIS_URL") else {
        eprintln!("COOPNAV_REDIS_URL is unset; nearby p99 was not measured");
        return;
    };
    if url.trim().is_empty() {
        eprintln!("COOPNAV_REDIS_URL is empty; nearby p99 was not measured");
        return;
    }
    let storage = RedisCooperativeStorage::connect(&url, "coopnav-nearby-p99")
        .await
        .unwrap();
    let namespace = "p99";
    let brand = "brand";
    storage.clear(namespace, brand).await.unwrap();
    let center = (50.45, 30.52);
    let updates: Vec<_> = (0..50)
        .map(|index| {
            let (latitude, longitude) = spatial::project(
                center.0,
                center.1,
                5.0 + index as f64 * 3.0,
                index as f64 * 7.2,
            );
            PeerWrite {
                donor_id: format!("peer-{index}"),
                subject_key: None,
                capture_ms: NOW,
                state: Some(PeerState {
                    latitude,
                    longitude,
                    accuracy_meters: 3.0,
                    speed_mps: 0.0,
                    bearing_degrees: 0.0,
                    trust_score: 0.9,
                    vehicle_class: VehicleClass::Car,
                    flags: Vec::new(),
                }),
            }
        })
        .collect();
    storage
        .publish(namespace, brand, &updates, NOW, 2_000)
        .await
        .unwrap();
    // Eight observations are matched after this script. QUIC and RPC JSON are outside the budget.
    for _ in 0..10 {
        let warmed = storage
            .nearby(namespace, brand, center, NOW, 2_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(warmed.len(), 50);
    }
    let mut samples = Vec::with_capacity(100);
    for _ in 0..100 {
        let started = std::time::Instant::now();
        let found = storage
            .nearby(namespace, brand, center, NOW, 2_000)
            .await
            .unwrap()
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(found.len(), 50);
        samples.push(elapsed);
    }
    storage.clear(namespace, brand).await.unwrap();
    samples.sort();
    let p99 = samples[98];
    eprintln!("nearby p99 {p99:?}");
    assert!(
        p99 <= std::time::Duration::from_millis(5),
        "nearby p99 {p99:?} exceeds 5 ms"
    );
}

fn shared_config(
    storage: Arc<dyn CooperativeStorage>,
    epoch: &str,
    brand: &str,
) -> CooperativeConfig {
    CooperativeConfig::new(storage, None, Duration::from_secs(2), Duration::ZERO)
        .unwrap()
        .with_shared_epoch(epoch)
        .unwrap()
        .with_shared_brand(brand)
        .unwrap()
}

fn enabled_pair(
    storage: Arc<dyn CooperativeStorage>,
    epoch: &str,
    brand: &str,
) -> (SectionControls, SectionControls) {
    let clock = Arc::new(ManualClock::new(NOW));
    let left = SectionControls::new(
        clock.clone(),
        Some(shared_config(storage.clone(), epoch, brand)),
    );
    let right = SectionControls::new(clock, Some(shared_config(storage, epoch, brand)));
    (left, right)
}

#[tokio::test]
async fn shared_epoch_and_brand_are_visible_across_section_controls() {
    let storage = Arc::new(InMemoryCooperativeStorage::default());
    let (left, right) = enabled_pair(storage.clone(), "deploy-epoch", "city");
    left.section(2, true).await.unwrap();
    right.section(2, true).await.unwrap();
    let epoch = left.generation().unwrap();
    assert_eq!(right.generation().unwrap(), epoch);

    let donor = ConfirmedSession::new("donor-session", "donor", "city").unwrap();
    let caller = ConfirmedSession::new("caller-session", "caller", "city").unwrap();
    left.sync(
        &epoch,
        "donor-subject",
        &donor,
        params(&donor, NOW, state(40.0, VehicleClass::Bus), Vec::new()),
    )
    .await
    .unwrap();
    let found = right
        .sync(
            &epoch,
            "caller-subject",
            &caller,
            params(
                &caller,
                NOW,
                state(0.0, VehicleClass::Car),
                vec![observation("bus", 40.0, VehicleClass::Bus)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(found.matched_donors.len(), 1);
    assert_eq!(found.matched_donors[0].observation_id, "bus");
}

#[tokio::test]
async fn disabling_one_shared_epoch_leaves_the_other_index_intact() {
    let storage = Arc::new(InMemoryCooperativeStorage::default());
    let (left, right) = enabled_pair(storage.clone(), "deploy-epoch", "city");
    left.section(2, true).await.unwrap();
    right.section(2, true).await.unwrap();
    let epoch = left.generation().unwrap();
    let donor = ConfirmedSession::new("donor-session", "donor", "city").unwrap();
    left.sync(
        &epoch,
        "donor-subject",
        &donor,
        params(&donor, NOW, state(40.0, VehicleClass::Bus), Vec::new()),
    )
    .await
    .unwrap();

    left.section(2, false).await.unwrap();
    assert!(!left.section_enabled(2).await.unwrap());
    let still_there = storage
        .nearby(&epoch, "city", (50.45, 30.52), NOW, 2000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(still_there.len(), 1);
    let caller = ConfirmedSession::new("caller-session", "caller", "city").unwrap();
    let found = right
        .sync(
            &epoch,
            "caller-subject",
            &caller,
            params(
                &caller,
                NOW,
                state(0.0, VehicleClass::Car),
                vec![observation("bus", 40.0, VehicleClass::Bus)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(found.matched_donors.len(), 1);

    left.section(2, true).await.unwrap();
    assert_eq!(left.generation().unwrap(), epoch);
    left.section(1, false).await.unwrap();
    assert!(!left.section_enabled(2).await.unwrap());
    let after_stage_one = storage
        .nearby(&epoch, "city", (50.45, 30.52), NOW, 2000)
        .await
        .unwrap()
        .unwrap();
    assert!(
        after_stage_one
            .iter()
            .any(|peer| peer.state.vehicle_class == VehicleClass::Bus)
    );
    assert_eq!(after_stage_one.len(), 2);
    left.section(1, true).await.unwrap();
    assert!(!left.section_enabled(2).await.unwrap());
}

#[tokio::test]
async fn personal_session_brand_stays_while_nearby_uses_the_shared_brand() {
    let storage = Arc::new(InMemoryCooperativeStorage::default());
    let sections = SectionControls::new(
        Arc::new(ManualClock::new(NOW)),
        Some(shared_config(storage.clone(), "deploy-epoch", "city")),
    );
    sections.section(2, true).await.unwrap();
    let epoch = sections.generation().unwrap();
    let personal = ConfirmedSession::new("sess", "ada", "personal:ada").unwrap();
    sections
        .sync(
            &epoch,
            "ada",
            &personal,
            params(&personal, NOW, state(10.0, VehicleClass::Car), Vec::new()),
        )
        .await
        .unwrap();
    assert_eq!(personal.brand_id, "personal:ada");
    assert!(
        storage
            .nearby(&epoch, "personal:ada", (50.45, 30.52), NOW, 2000)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        storage
            .nearby(&epoch, "city", (50.45, 30.52), NOW, 2000)
            .await
            .unwrap()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn gtfs_brand_other_than_the_shared_donor_brand_is_not_configured() {
    struct Feed(&'static str);
    #[async_trait]
    impl TransitSource for Feed {
        fn brand_id(&self) -> &str {
            self.0
        }
        async fn fetch(&self, _: u64, _: u64) -> Result<Vec<TransitFix>, GtfsError> {
            Ok(Vec::new())
        }
    }
    let storage = Arc::new(InMemoryCooperativeStorage::default());
    let mismatched = CooperativeConfig::new(
        storage.clone(),
        Some(Arc::new(Feed("other-brand"))),
        Duration::from_secs(2),
        Duration::from_millis(100),
    )
    .unwrap();
    assert!(matches!(
        mismatched.with_shared_brand("city"),
        Err(SectionError::Configuration)
    ));
    let matched = CooperativeConfig::new(
        storage,
        Some(Arc::new(Feed("city"))),
        Duration::from_secs(2),
        Duration::from_millis(100),
    )
    .unwrap()
    .with_shared_brand("city");
    assert!(matched.is_ok());
}

#[test]
fn shared_mode_without_an_epoch_id_is_a_configuration_error() {
    let config = CooperativeConfig::new(
        Arc::new(InMemoryCooperativeStorage::default()),
        None,
        Duration::from_secs(2),
        Duration::ZERO,
    )
    .unwrap();
    assert!(matches!(
        config.with_shared_epoch(""),
        Err(SectionError::Configuration)
    ));
}
