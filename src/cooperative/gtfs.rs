use super::{PeerState, VehicleClass, spatial::fresh, wire::MIN_TRUST};
use async_trait::async_trait;
use prost::Message;
use reqwest::{Client, Url};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use thiserror::Error;

const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_FIXES: usize = 512;

/// A feed scoped to one confirmed brand. Configure only verified transit routes.
pub struct GtfsConfig {
    pub url: String,
    pub brand_id: String,
    pub route_classes: HashMap<String, VehicleClass>,
    pub accuracy_meters: f64,
    pub trust_score: f64,
}

#[derive(Debug, Error)]
pub enum GtfsError {
    #[error(
        "GTFS requires an HTTPS URL without userinfo (literal loopback HTTP is allowed in tests)"
    )]
    Url,
    #[error(
        "GTFS requires a brand, verified bus/trolleybus routes, accuracy in (0, 20] and trust in [0.7, 1]"
    )]
    Configuration,
    #[error("GTFS request failed")]
    Fetch,
    #[error("GTFS body exceeds the allowed size")]
    BodyLimit,
    #[error("GTFS feed is malformed, unsupported or stale")]
    Feed,
}

pub struct TransitFix {
    pub vehicle_id: String,
    pub capture_ms: u64,
    pub state: PeerState,
}

#[async_trait]
pub trait TransitSource: Send + Sync {
    fn brand_id(&self) -> &str;
    async fn fetch(&self, now: u64, freshness_ms: u64) -> Result<Vec<TransitFix>, GtfsError>;
}

pub struct GtfsRealtimeSource {
    client: Client,
    url: Url,
    brand: String,
    routes: HashMap<String, VehicleClass>,
    accuracy: f64,
    trust: f64,
}

impl GtfsRealtimeSource {
    pub fn new(config: GtfsConfig) -> Result<Self, GtfsError> {
        let url = Url::parse(&config.url).map_err(|_| GtfsError::Url)?;
        let loopback = cfg!(test)
            && match url.host() {
                Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                _ => false,
            };
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
            || url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(GtfsError::Url);
        }
        if crate::model::validate_identifier(&config.brand_id).is_err()
            || config.route_classes.is_empty()
            || config.route_classes.len() > MAX_FIXES
            || !config.route_classes.iter().all(|(id, class)| {
                crate::model::validate_identifier(id).is_ok()
                    && matches!(class, VehicleClass::Bus | VehicleClass::Trolleybus)
            })
            || !(config.accuracy_meters > 0.0 && config.accuracy_meters <= 20.0)
            || !(MIN_TRUST..=1.0).contains(&config.trust_score)
        {
            return Err(GtfsError::Configuration);
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(3))
            .build()
            .map_err(|_| GtfsError::Fetch)?;
        Ok(Self {
            client,
            url,
            brand: config.brand_id,
            routes: config.route_classes,
            accuracy: config.accuracy_meters,
            trust: config.trust_score,
        })
    }

    fn decode(&self, bytes: &[u8], now: u64, freshness: u64) -> Result<Vec<TransitFix>, GtfsError> {
        let feed = FeedMessage::decode(bytes).map_err(|_| GtfsError::Feed)?;
        let header = feed.header.ok_or(GtfsError::Feed)?;
        let timestamp = header
            .timestamp
            .and_then(|ts| ts.checked_mul(1000))
            .ok_or(GtfsError::Feed)?;
        if !matches!(header.version.as_str(), "1.0" | "2.0")
            || header.incrementality.unwrap_or(0) != 0
            || !fresh(timestamp, now, freshness)
        {
            return Err(GtfsError::Feed);
        }
        let mut fixes: HashMap<_, TransitFix> = HashMap::new();
        let mut duplicates = HashSet::new();
        for entity in feed.entity {
            if entity.deleted.unwrap_or(false) {
                continue;
            }
            let Some(vehicle) = entity.vehicle else {
                continue;
            };
            let Some(class) = vehicle
                .trip
                .and_then(|trip| trip.route_id)
                .and_then(|route| self.routes.get(&route).copied())
            else {
                continue;
            };
            let Some(vehicle_id) = vehicle.descriptor.and_then(|descriptor| descriptor.id) else {
                continue;
            };
            let Some(capture_ms) = vehicle.timestamp.and_then(|ts| ts.checked_mul(1000)) else {
                continue;
            };
            let Some(position) = vehicle.position else {
                continue;
            };
            let (Some(latitude), Some(longitude), Some(speed)) =
                (position.latitude, position.longitude, position.speed)
            else {
                continue;
            };
            let Some(bearing) = position.bearing.or((speed == 0.0).then_some(0.0)) else {
                continue;
            };
            let state = PeerState {
                latitude: latitude.into(),
                longitude: longitude.into(),
                speed_mps: speed.into(),
                bearing_degrees: bearing.into(),
                accuracy_meters: self.accuracy,
                trust_score: self.trust,
                vehicle_class: class,
                flags: Vec::new(),
            };
            if !fresh(capture_ms, now, freshness)
                || capture_ms > timestamp
                || !state.valid()
                || crate::model::validate_identifier(&vehicle_id).is_err()
                || duplicates.contains(&vehicle_id)
            {
                continue;
            }
            // A full snapshot must not publish conflicting entries for one vehicle.
            if fixes.remove(&vehicle_id).is_some() {
                duplicates.insert(vehicle_id);
            } else {
                fixes.insert(
                    vehicle_id.clone(),
                    TransitFix {
                        vehicle_id,
                        capture_ms,
                        state,
                    },
                );
            }
            if fixes.len() > MAX_FIXES {
                return Err(GtfsError::BodyLimit);
            }
        }
        Ok(fixes.into_values().collect())
    }
}

#[async_trait]
impl TransitSource for GtfsRealtimeSource {
    fn brand_id(&self) -> &str {
        &self.brand
    }

    async fn fetch(&self, now: u64, freshness_ms: u64) -> Result<Vec<TransitFix>, GtfsError> {
        let started = tokio::time::Instant::now();
        let mut response = self
            .client
            .get(self.url.clone())
            .send()
            .await
            .map_err(|_| GtfsError::Fetch)?;
        if !response.status().is_success() {
            return Err(GtfsError::Fetch);
        }
        if response
            .content_length()
            .is_some_and(|len| len > MAX_BODY_BYTES as u64)
        {
            return Err(GtfsError::BodyLimit);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| GtfsError::Fetch)? {
            if body.len() + chunk.len() > MAX_BODY_BYTES {
                return Err(GtfsError::BodyLimit);
            }
            body.extend_from_slice(&chunk);
        }
        self.decode(
            &body,
            now.saturating_add(started.elapsed().as_millis() as u64),
            freshness_ms,
        )
    }
}

// Wire-compatible subset of the official GTFS-Realtime v2 proto2 schema; prost
// skips all other fields. Optional scalar types let us reject missing data explicitly.
#[derive(Clone, Message)]
struct FeedMessage {
    #[prost(message, optional, tag = "1")]
    header: Option<FeedHeader>,
    #[prost(message, repeated, tag = "2")]
    entity: Vec<FeedEntity>,
}

#[derive(Clone, Message)]
struct FeedHeader {
    #[prost(string, tag = "1")]
    version: String,
    #[prost(int32, optional, tag = "2")]
    incrementality: Option<i32>,
    #[prost(uint64, optional, tag = "3")]
    timestamp: Option<u64>,
}

#[derive(Clone, Message)]
struct FeedEntity {
    #[prost(bool, optional, tag = "2")]
    deleted: Option<bool>,
    #[prost(message, optional, tag = "4")]
    vehicle: Option<VehiclePosition>,
}

#[derive(Clone, Message)]
struct VehiclePosition {
    #[prost(message, optional, tag = "1")]
    trip: Option<TripDescriptor>,
    #[prost(message, optional, tag = "2")]
    position: Option<Position>,
    #[prost(uint64, optional, tag = "5")]
    timestamp: Option<u64>,
    #[prost(message, optional, tag = "8")]
    descriptor: Option<VehicleDescriptor>,
}

#[derive(Clone, Message)]
struct TripDescriptor {
    #[prost(string, optional, tag = "5")]
    route_id: Option<String>,
}

#[derive(Clone, Message)]
struct VehicleDescriptor {
    #[prost(string, optional, tag = "1")]
    id: Option<String>,
}

#[derive(Clone, Message)]
struct Position {
    #[prost(float, optional, tag = "1")]
    latitude: Option<f32>,
    #[prost(float, optional, tag = "2")]
    longitude: Option<f32>,
    #[prost(float, optional, tag = "3")]
    bearing: Option<f32>,
    #[prost(float, optional, tag = "5")]
    speed: Option<f32>,
}

#[cfg(test)]
#[path = "gtfs/tests.rs"]
mod tests;
