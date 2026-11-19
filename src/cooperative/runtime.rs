use super::{
    CooperativeStorage, PeerWrite, SyncPeersParams, SyncPeersResult, TransitSource, VehicleClass,
    opaque, spatial,
};
use crate::{ConfirmedSession, UnixMillisClock};
use json_rpc_2_server::PluginHandle;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, RwLock, RwLockReadGuard, watch};

const MAX_BRANDS: usize = 128;
const MAX_TRANSIT_BATCH: usize = 512;

pub struct CooperativeConfig {
    storage: Arc<dyn CooperativeStorage>,
    transit: Option<Arc<dyn TransitSource>>,
    freshness_ms: u64,
    poll_interval: Duration,
}

impl CooperativeConfig {
    pub fn new(
        storage: Arc<dyn CooperativeStorage>,
        transit: Option<Arc<dyn TransitSource>>,
        freshness: Duration,
        poll_interval: Duration,
    ) -> Result<Self, SectionError> {
        if !(1..=2000).contains(&freshness.as_millis())
            || !freshness.subsec_nanos().is_multiple_of(1_000_000)
            || transit.as_ref().is_some_and(|source| {
                !(Duration::from_millis(100)..=Duration::from_secs(30)).contains(&poll_interval)
                    || crate::model::validate_identifier(source.brand_id()).is_err()
            })
        {
            return Err(SectionError::Configuration);
        }
        Ok(Self {
            storage,
            transit,
            freshness_ms: freshness.as_millis() as u64,
            poll_interval,
        })
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SectionError {
    #[error("unknown CoopNav stage")]
    UnknownStage,
    #[error("stage 1 ingest is disabled")]
    IngestDisabled,
    #[error("stage 2 cooperative positioning is disabled")]
    CooperativeDisabled,
    #[error("stage 2 requires stage 1 to be enabled")]
    RequiresStageOne,
    #[error("stage 2 requires injected cooperative storage")]
    Unconfigured,
    #[error(
        "invalid stage 2 configuration: require 1–2000 ms freshness, 100 ms–30 s polling interval, and valid GTFS brand"
    )]
    Configuration,
    #[error("stage 3 is forbidden and not implemented")]
    StageThree,
    #[error("stage 2 storage is unavailable or at capacity")]
    Storage,
    #[error("stage 2 capture timestamp is future, stale, or out of order")]
    Timestamp,
}

struct Epoch {
    id: String,
    brands: Mutex<HashSet<String>>,
}

impl Epoch {
    fn touch(&self, brand: &str) -> Result<(), SectionError> {
        let mut brands = self.brands.lock().map_err(|_| SectionError::Storage)?;
        if brands.len() == MAX_BRANDS && !brands.contains(brand) {
            return Err(SectionError::Storage);
        }
        brands.insert(brand.into());
        Ok(())
    }
}

pub(crate) struct Stages {
    ingest: bool,
    epoch: Option<Epoch>,
}

struct Inner {
    config: Option<CooperativeConfig>,
    clock: Arc<dyn UnixMillisClock>,
    stages: RwLock<Stages>,
    generation: watch::Sender<Option<String>>,
    transition: AsyncMutex<()>,
    poll: AsyncMutex<()>,
}

/// Clone this handle into host administration; it is not a JSON-RPC endpoint.
#[derive(Clone)]
pub struct SectionControls(Arc<Inner>);

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub enum GtfsStatus {
    #[serde(rename = "gtfs-unconfigured")]
    Unconfigured,
    #[serde(rename = "gtfs-disabled")]
    Disabled,
    #[serde(rename = "gtfs-enabled")]
    Enabled,
}

impl SectionControls {
    pub(crate) fn new(clock: Arc<dyn UnixMillisClock>, config: Option<CooperativeConfig>) -> Self {
        Self(Arc::new(Inner {
            config,
            clock,
            stages: RwLock::new(Stages {
                ingest: true,
                epoch: None,
            }),
            generation: watch::channel(None).0,
            transition: AsyncMutex::new(()),
            poll: AsyncMutex::new(()),
        }))
    }

    pub async fn section(&self, stage: i32, enabled: bool) -> Result<(), SectionError> {
        let _transition = self.0.transition.lock().await;
        let mut stages = self.0.stages.write().await;
        match (stage, enabled) {
            (1, true) => stages.ingest = true,
            (1, false) => stages.ingest = false,
            (2, true) => {
                if !stages.ingest {
                    return Err(SectionError::RequiresStageOne);
                }
                if self.0.config.is_none() {
                    return Err(SectionError::Unconfigured);
                }
                if stages.epoch.is_none() {
                    if self
                        .0
                        .config
                        .as_ref()
                        .is_some_and(|config| config.transit.is_none())
                    {
                        tracing::warn!(
                            status = "gtfs-unconfigured",
                            "stage 2 enabled in V2V-only mode"
                        );
                    }
                    let id = hex::encode(rand::random::<[u8; 16]>());
                    self.0.generation.send_replace(Some(id.clone()));
                    stages.epoch = Some(Epoch {
                        id,
                        brands: Mutex::new(HashSet::new()),
                    });
                }
            }
            (2, false) | (3, false) => (),
            (3, true) => return Err(SectionError::StageThree),
            _ => return Err(SectionError::UnknownStage),
        }
        let old = if !enabled && matches!(stage, 1 | 2) {
            self.0.generation.send_replace(None);
            stages.epoch.take()
        } else {
            None
        };
        drop(stages);
        if let Some(old) = old {
            // Let the canceled fetch drop before completing disable; don't hold the stage write lock.
            let _poll = self.0.poll.lock().await;
            let config = self.0.config.as_ref().ok_or(SectionError::Unconfigured)?;
            let brands = old.brands.into_inner().map_err(|_| SectionError::Storage)?;
            let mut failed = false;
            for brand in brands {
                failed |= config.storage.clear(&old.id, &brand).await.is_err();
            }
            if failed {
                return Err(SectionError::Storage);
            }
        }
        Ok(())
    }

    pub async fn section_enabled(&self, stage: i32) -> Result<bool, SectionError> {
        let stages = self.0.stages.read().await;
        match stage {
            1 => Ok(stages.ingest),
            2 => Ok(stages.epoch.is_some()),
            3 => Ok(false),
            _ => Err(SectionError::UnknownStage),
        }
    }

    /// Host-only configuration and enablement status, not a feed reachability check.
    pub async fn gtfs_status(&self) -> GtfsStatus {
        if self
            .0
            .config
            .as_ref()
            .and_then(|config| config.transit.as_ref())
            .is_none()
        {
            GtfsStatus::Unconfigured
        } else if self.0.stages.read().await.epoch.is_some() {
            GtfsStatus::Enabled
        } else {
            GtfsStatus::Disabled
        }
    }

    pub(crate) async fn ingest_guard(&self) -> Result<RwLockReadGuard<'_, Stages>, SectionError> {
        let stages = self.0.stages.read().await;
        if !stages.ingest {
            return Err(SectionError::IngestDisabled);
        }
        Ok(stages)
    }

    pub(crate) fn generation(&self) -> Result<String, SectionError> {
        if self.0.config.is_none() {
            return Err(SectionError::Unconfigured);
        }
        self.0
            .generation
            .borrow()
            .clone()
            .ok_or(SectionError::CooperativeDisabled)
    }

    pub(crate) async fn sync(
        &self,
        generation: &str,
        subject: &str,
        session: &ConfirmedSession,
        params: SyncPeersParams,
    ) -> Result<SyncPeersResult, SectionError> {
        let config = self.0.config.as_ref().ok_or(SectionError::Unconfigured)?;
        let stages = self.0.stages.read().await;
        let epoch = stages
            .epoch
            .as_ref()
            .filter(|epoch| epoch.id == generation)
            .ok_or(SectionError::CooperativeDisabled)?;
        let now = self.0.clock.now_millis();
        if !spatial::fresh(params.capture_timestamp_ms, now, config.freshness_ms) {
            return Err(SectionError::Timestamp);
        }
        let brand = &session.brand_id;
        epoch.touch(brand)?;
        let donor_id = opaque(&[generation, brand, "driver", &session.driver_id]);
        let subject_key = opaque(&[generation, brand, "subject", subject]);
        let write = PeerWrite {
            donor_id: donor_id.clone(),
            subject_key: Some(subject_key.clone()),
            capture_ms: params.capture_timestamp_ms,
            state: params.state.as_client_donor(),
        };
        let changed = config
            .storage
            .publish(generation, brand, &[write], now, config.freshness_ms)
            .await
            .map_err(|_| SectionError::Storage)?;
        if changed != [true] {
            return Err(SectionError::Timestamp);
        }
        let records = if params.observed_peers.is_empty() {
            Vec::new()
        } else {
            config
                .storage
                .nearby(
                    generation,
                    brand,
                    (params.state.latitude, params.state.longitude),
                    now,
                    config.freshness_ms,
                )
                .await
                .map_err(|_| SectionError::Storage)?
                .unwrap_or_default()
        };
        let finished = self.0.clock.now_millis();
        if finished < now
            || !spatial::fresh(params.capture_timestamp_ms, finished, config.freshness_ms)
        {
            return Err(SectionError::Timestamp);
        }
        Ok(spatial::matched(
            &params,
            records,
            &donor_id,
            &subject_key,
            finished,
            config.freshness_ms,
        ))
    }

    /// `BackendBuilder` schedules this automatically through the plugin handle.
    /// A custom host should schedule the returned tasks and stop them on shutdown.
    pub fn maintenance_handle(&self) -> PluginHandle {
        let handle = PluginHandle::new();
        let Some(config) = self
            .0
            .config
            .as_ref()
            .filter(|config| config.transit.is_some())
        else {
            return handle;
        };
        handle.with_maintenance(config.poll_interval, self.clone(), |stages| async move {
            stages.poll_transit().await
        })
    }

    async fn poll_transit(&self) -> anyhow::Result<()> {
        let Some(config) = &self.0.config else {
            return Ok(());
        };
        let Some(transit) = &config.transit else {
            return Ok(());
        };
        let Ok(_one_poll) = self.0.poll.try_lock() else {
            return Ok(());
        };
        let mut watcher = self.0.generation.subscribe();
        let Some(generation) = watcher.borrow_and_update().clone() else {
            return Ok(());
        };
        let fixes = tokio::select! {
            biased;
            _ = watcher.changed() => return Ok(()),
            result = tokio::time::timeout(
                Duration::from_secs(4),
                transit.fetch(self.0.clock.now_millis(), config.freshness_ms),
            ) => result??,
        };
        anyhow::ensure!(
            fixes.len() <= MAX_TRANSIT_BATCH,
            "cooperative transit batch exceeded limit"
        );
        let stages = self.0.stages.read().await;
        let Some(epoch) = stages.epoch.as_ref().filter(|epoch| epoch.id == generation) else {
            return Ok(());
        };
        let now = self.0.clock.now_millis();
        let brand = transit.brand_id();
        let updates: Vec<_> = fixes
            .into_iter()
            .filter_map(|fix| {
                let state = fix.state;
                if !state.valid()
                    || state.trust_score < super::wire::MIN_TRUST
                    || !state.flags.is_empty()
                    || !matches!(
                        state.vehicle_class,
                        VehicleClass::Bus | VehicleClass::Trolleybus
                    )
                    || crate::model::validate_identifier(&fix.vehicle_id).is_err()
                    || !spatial::fresh(fix.capture_ms, now, config.freshness_ms)
                {
                    return None;
                }
                Some(PeerWrite {
                    donor_id: opaque(&[&generation, brand, "transit", &fix.vehicle_id]),
                    subject_key: None,
                    capture_ms: fix.capture_ms,
                    state: Some(state),
                })
            })
            .collect();
        if !updates.is_empty() {
            epoch.touch(brand)?;
            config
                .storage
                .publish(&generation, brand, &updates, now, config.freshness_ms)
                .await
                .map_err(|_| SectionError::Storage)?;
        }
        Ok(())
    }
}
