use serde::{Deserialize, Serialize};

pub const MAX_BATCH_POINTS: usize = 100;
const MAX_IDENTITY_LENGTH: usize = 256;
const MAX_SPEED_MPS: f64 = 220.0 / 3.6;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PointSource {
    Fused,
    Gps,
    Network,
    DeadReckoning,
    Stationary,
    Cooperative,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TrackPoint {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_latitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_longitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_accuracy_meters: Option<f64>,
    pub smoothed_latitude: f64,
    pub smoothed_longitude: f64,
    pub smoothed_accuracy_meters: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed_mps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearing_degrees: Option<f64>,
    pub device_time_ms: i64,
    pub observed_at_ms: i64,
    pub source: PointSource,
    pub is_mock: bool,
    pub flags: Vec<String>,
}

impl TrackPoint {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.id.len() != 32 || !self.id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("point id must contain exactly 32 hexadecimal characters");
        }
        validate_coordinate(self.smoothed_latitude, -90.0, 90.0)?;
        validate_coordinate(self.smoothed_longitude, -180.0, 180.0)?;
        validate_accuracy(self.smoothed_accuracy_meters)?;

        match (
            self.raw_latitude,
            self.raw_longitude,
            self.raw_accuracy_meters,
        ) {
            (Some(latitude), Some(longitude), Some(accuracy)) => {
                validate_coordinate(latitude, -90.0, 90.0)?;
                validate_coordinate(longitude, -180.0, 180.0)?;
                validate_accuracy(accuracy)?;
            }
            (None, None, None) => {}
            _ => return Err("raw coordinates and accuracy must be supplied together"),
        }
        if let Some(speed) = self.speed_mps
            && (!speed.is_finite() || !(0.0..=MAX_SPEED_MPS).contains(&speed))
        {
            return Err("speed must be finite and between 0 and 220 km/h");
        }
        if let Some(bearing) = self.bearing_degrees
            && (!bearing.is_finite() || !(0.0..360.0).contains(&bearing))
        {
            return Err("bearing must be finite and in [0, 360)");
        }
        match self.source {
            PointSource::Network if self.speed_mps.is_some() || self.bearing_degrees.is_some() => {
                return Err("network points cannot include speed or bearing");
            }
            PointSource::DeadReckoning | PointSource::Stationary if self.raw_latitude.is_some() => {
                return Err("dead-reckoning and stationary points cannot include raw coordinates");
            }
            PointSource::Fused
            | PointSource::Gps
            | PointSource::Network
            | PointSource::DeadReckoning
            | PointSource::Stationary
            | PointSource::Cooperative => {}
        }
        if self.device_time_ms <= 0 || self.observed_at_ms <= 0 {
            return Err("timestamps must be positive Unix milliseconds");
        }
        if self.flags.len() > 32
            || self
                .flags
                .iter()
                .any(|flag| flag.trim().is_empty() || flag.len() > 64)
        {
            return Err("flags must be non-empty strings of at most 64 bytes");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TrackingSessionParams {
    pub session_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IngestParams {
    pub session_id: String,
    pub driver_id: String,
    pub brand_id: String,
    pub points: Vec<TrackPoint>,
}

impl IngestParams {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_identifier(&self.session_id)?;
        validate_identifier(&self.driver_id)?;
        validate_identifier(&self.brand_id)?;
        if self.points.is_empty() || self.points.len() > MAX_BATCH_POINTS {
            return Err("points must contain between 1 and 100 items");
        }

        let mut ids = std::collections::HashMap::with_capacity(self.points.len());
        for point in &self.points {
            point.validate()?;
            if let Some(previous) = ids.insert(&point.id, point)
                && previous != point
            {
                return Err("a point id cannot have multiple payloads in one batch");
            }
        }
        Ok(())
    }
}

pub fn validate_identifier(value: &str) -> Result<(), &'static str> {
    if value.trim().is_empty() || value.len() > MAX_IDENTITY_LENGTH || value.trim() != value {
        return Err("identifiers must be non-empty, trimmed, and at most 256 bytes");
    }
    Ok(())
}

fn validate_coordinate(value: f64, minimum: f64, maximum: f64) -> Result<(), &'static str> {
    if !value.is_finite() || !(minimum..=maximum).contains(&value) {
        return Err("coordinates must be finite and within latitude/longitude bounds");
    }
    Ok(())
}

fn validate_accuracy(value: f64) -> Result<(), &'static str> {
    if !value.is_finite() || value < 0.0 {
        return Err("accuracy must be finite and non-negative");
    }

    Ok(())
}

#[cfg(test)]
#[path = "model/tests.rs"]
mod tests;
