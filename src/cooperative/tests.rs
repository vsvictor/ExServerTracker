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
