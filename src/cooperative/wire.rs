use crate::model::validate_identifier;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;

pub(super) const MAX_OBSERVATIONS: usize = 8;
pub(super) const MAX_LAT: f64 = 85.0;
pub(super) const MAX_SPEED: f64 = 220.0 / 3.6;
pub(super) const RADIUS: f64 = 200.0;
pub(super) const MIN_TRUST: f64 = 0.7;
pub(super) const CHI68: f64 = 2.278_868_566_376_73;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VehicleClass {
    Car,
    Truck,
    Bus,
    Trolleybus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PeerState {
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_meters: f64,
    pub speed_mps: f64,
    pub bearing_degrees: f64,
    pub trust_score: f64,
    pub vehicle_class: VehicleClass,
    pub flags: Vec<String>,
}

impl PeerState {
    pub(super) fn donor_flags_allowed(&self) -> bool {
        self.flags.iter().all(|flag| {
            flag.eq_ignore_ascii_case("gnssTimeUnverified")
                || flag.eq_ignore_ascii_case("gnss_time_unverified")
        })
    }

    pub(super) fn valid(&self) -> bool {
        (-MAX_LAT..=MAX_LAT).contains(&self.latitude)
            && (-180.0..=180.0).contains(&self.longitude)
            && self.accuracy_meters > 0.0
            && self.accuracy_meters <= 200.0
            && (0.0..=MAX_SPEED).contains(&self.speed_mps)
            && (0.0..360.0).contains(&self.bearing_degrees)
            && (0.0..=1.0).contains(&self.trust_score)
            && self.flags.len() <= 16
            && self
                .flags
                .iter()
                .all(|f| !f.trim().is_empty() && f.len() <= 64)
    }

    pub(super) fn as_client_donor(&self) -> Option<Self> {
        if !self.donor_flags_allowed()
            || self.accuracy_meters > 12.0
            || self.trust_score < MIN_TRUST
        {
            return None;
        }
        let mut state = self.clone();
        state.accuracy_meters = state.accuracy_meters.max(3.0);
        state.trust_score = state.trust_score.min(if state.accuracy_meters <= 6.0 {
            0.8
        } else {
            MIN_TRUST
        });
        Some(state)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Observation {
    pub observation_id: String,
    pub vehicle_class: VehicleClass,
    pub distance_meters: f64,
    pub azimuth_degrees: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relative_speed_mps: Option<f64>,
    pub vision_variance_meters2: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SyncPeersParams {
    pub session_id: String,
    pub driver_id: String,
    pub brand_id: String,
    pub capture_timestamp_ms: u64,
    pub state: PeerState,
    pub observed_peers: Vec<Observation>,
}

impl SyncPeersParams {
    pub(crate) fn valid(&self) -> bool {
        let mut ids = HashSet::with_capacity(MAX_OBSERVATIONS);
        [&self.session_id, &self.driver_id, &self.brand_id]
            .into_iter()
            .all(|s| validate_identifier(s).is_ok())
            && self.capture_timestamp_ms > 0
            && self.state.valid()
            && self.observed_peers.len() <= MAX_OBSERVATIONS
            && self.observed_peers.iter().all(|o| {
                !o.observation_id.trim().is_empty()
                    && o.observation_id.encode_utf16().count() <= 64
                    && ids.insert(&o.observation_id)
                    && (0.1..=RADIUS).contains(&o.distance_meters)
                    && (0.0..360.0).contains(&o.azimuth_degrees)
                    && o.relative_speed_mps
                        .is_none_or(|v| (-122.3..=122.3).contains(&v))
                    && (0.01..=10_000.0).contains(&o.vision_variance_meters2)
            })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Donor {
    pub observation_id: String,
    pub donor_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_meters: f64,
    pub speed_mps: f64,
    pub bearing_degrees: f64,
    pub trust_score: f64,
    pub vehicle_class: VehicleClass,
    pub capture_timestamp_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPeersResult {
    pub capture_timestamp_ms: u64,
    pub matched_donors: Vec<Donor>,
}

pub(crate) fn schema() -> Value {
    let class = json!({"enum": ["car", "truck", "bus", "trolleybus"]});
    let id = json!({"type": "string", "minLength": 1, "maxLength": 256});
    let bearing = json!({"type": "number", "minimum": 0, "exclusiveMaximum": 360});
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["sessionId", "driverId", "brandId", "captureTimestampMs", "state", "observedPeers"],
        "properties": {
            "sessionId": id, "driverId": id, "brandId": id,
            "access_token": {"type": "string", "minLength": 1},
            "authorization": {"type": "string", "pattern": "^Bearer "},
            "captureTimestampMs": {"type": "integer", "minimum": 1},
            "state": {
                "type": "object", "additionalProperties": false,
                "required": ["latitude", "longitude", "accuracyMeters", "speedMps", "bearingDegrees", "trustScore", "vehicleClass", "flags"],
                "properties": {
                    "latitude": {"type": "number", "minimum": -MAX_LAT, "maximum": MAX_LAT},
                    "longitude": {"type": "number", "minimum": -180, "maximum": 180},
                    "accuracyMeters": {"type": "number", "exclusiveMinimum": 0, "maximum": 200},
                    "speedMps": {"type": "number", "minimum": 0, "maximum": MAX_SPEED},
                    "bearingDegrees": bearing,
                    "trustScore": {"type": "number", "minimum": 0, "maximum": 1},
                    "vehicleClass": class,
                    "flags": {"type": "array", "maxItems": 16, "items": {"type": "string", "minLength": 1, "maxLength": 64}}
                }
            },
            "observedPeers": {
                "type": "array", "maxItems": MAX_OBSERVATIONS,
                "items": {
                    "type": "object", "additionalProperties": false,
                    "required": ["observationId", "vehicleClass", "distanceMeters", "azimuthDegrees", "visionVarianceMeters2"],
                    "properties": {
                        "observationId": {"type": "string", "minLength": 1, "maxLength": 64},
                        "vehicleClass": class,
                        "distanceMeters": {"type": "number", "minimum": 0.1, "maximum": RADIUS},
                        "azimuthDegrees": bearing,
                        "relativeSpeedMps": {"type": ["number", "null"], "minimum": -122.3, "maximum": 122.3},
                        "visionVarianceMeters2": {"type": "number", "minimum": 0.01, "maximum": 10000}
                    }
                }
            }
        },
        "oneOf": [
            {"required": ["access_token"], "not": {"required": ["authorization"]}},
            {"required": ["authorization"], "not": {"required": ["access_token"]}}
        ]
    })
}
