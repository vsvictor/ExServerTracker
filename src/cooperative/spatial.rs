use super::wire::{CHI68, MAX_LAT, MIN_TRUST, RADIUS};
use super::{Donor, PeerRecord, SyncPeersParams, SyncPeersResult};

const EARTH: f64 = 6_371_000.0;
const TOLERANCE: f64 = 12.0;

pub(super) fn project(lat: f64, lon: f64, distance: f64, bearing: f64) -> (f64, f64) {
    let (sin, cos) = lat.to_radians().sin_cos();
    let (sin_d, cos_d) = (distance / EARTH).sin_cos();
    let (sin_b, cos_b) = bearing.to_radians().sin_cos();
    let dest_lat = (sin * cos_d + cos * sin_d * cos_b).clamp(-1.0, 1.0).asin();
    let d_lon = (sin_b * sin_d * cos).atan2(cos_d - sin * dest_lat.sin());
    (
        dest_lat.to_degrees(),
        (lon + d_lon.to_degrees() + 180.0).rem_euclid(360.0) - 180.0,
    )
}

pub(super) fn distance(a: (f64, f64), b: (f64, f64)) -> f64 {
    let h = ((a.0 - b.0).to_radians() / 2.0).sin().powi(2)
        + a.0.to_radians().cos()
            * b.0.to_radians().cos()
            * ((a.1 - b.1).to_radians() / 2.0).sin().powi(2);
    2.0 * EARTH * h.clamp(0.0, 1.0).sqrt().asin()
}

pub(super) fn fresh(timestamp: u64, now: u64, limit: u64) -> bool {
    timestamp > 0 && now.checked_sub(timestamp).is_some_and(|age| age < limit)
}

pub(super) fn matched(
    params: &SyncPeersParams,
    records: Vec<PeerRecord>,
    caller_id: &str,
    caller_subject: &str,
    now: u64,
    freshness: u64,
) -> SyncPeersResult {
    let own = (params.state.latitude, params.state.longitude);
    let candidates: Vec<_> = records
        .into_iter()
        .filter_map(|record| {
            let state = &record.state;
            if record.donor_id == caller_id
                || record.subject_key.as_deref() == Some(caller_subject)
                || !fresh(record.capture_ms, now, freshness)
                || !fresh(record.received_ms, now, freshness)
                || record.capture_ms > record.received_ms
                || !state.valid()
                || state.trust_score < MIN_TRUST
                || !state.donor_flags_allowed()
                || distance(own, (state.latitude, state.longitude)) > RADIUS
            {
                return None;
            }
            let delta = params.capture_timestamp_ms.abs_diff(record.capture_ms);
            if delta >= freshness {
                return None;
            }
            let dt = delta as f64 / 1000.0;
            let displacement = if params.capture_timestamp_ms >= record.capture_ms {
                dt
            } else {
                -dt
            };
            let p = project(
                state.latitude,
                state.longitude,
                state.speed_mps * displacement,
                state.bearing_degrees,
            );
            if p.0.abs() > MAX_LAT || distance(own, p) > RADIUS {
                return None;
            }
            // Conservatively add motion and clock-alignment error to the 68% radius.
            let motion_margin = 3.0 * dt + 0.5 * 4.0 * dt.powi(2);
            let clock_margin = 0.2 * (state.speed_mps + 3.0);
            let radius = state.accuracy_meters + motion_margin + clock_margin;
            if radius > 100.0 {
                return None;
            }
            Some((record, p, radius))
        })
        .collect();

    let mut observation_edges = vec![Vec::new(); params.observed_peers.len()];
    let mut donor_degree = vec![0; candidates.len()];
    for (obs, edges) in params.observed_peers.iter().zip(&mut observation_edges) {
        let perceived = project(own.0, own.1, obs.distance_meters, obs.azimuth_degrees);
        for (index, (record, predicted, radius)) in candidates.iter().enumerate() {
            if obs.vehicle_class == record.state.vehicle_class {
                let residual = distance(perceived, *predicted);
                if residual <= TOLERANCE {
                    donor_degree[index] += 1;
                    let variance = radius.powi(2) / CHI68
                        + params.state.accuracy_meters.powi(2) / CHI68
                        + obs.vision_variance_meters2.max(1.0);
                    edges.push((index, residual.powi(2) / variance));
                }
            }
        }
    }

    let mut unambiguous: Vec<_> = observation_edges
        .iter()
        .enumerate()
        .filter_map(|(obs, edges)| match edges.as_slice() {
            [(donor, score)] if donor_degree[*donor] == 1 && *score <= 13.8155 => {
                Some((obs, *donor, *score))
            }
            _ => None,
        })
        .collect();
    unambiguous.sort_by(|a, b| a.2.total_cmp(&b.2));

    SyncPeersResult {
        capture_timestamp_ms: params.capture_timestamp_ms,
        matched_donors: unambiguous
            .into_iter()
            .take(3)
            .map(|(obs, donor, _)| {
                let (record, (latitude, longitude), accuracy) = &candidates[donor];
                Donor {
                    observation_id: params.observed_peers[obs].observation_id.clone(),
                    donor_id: record.donor_id.clone(),
                    latitude: *latitude,
                    longitude: *longitude,
                    accuracy_meters: *accuracy,
                    speed_mps: record.state.speed_mps,
                    bearing_degrees: record.state.bearing_degrees,
                    trust_score: record.state.trust_score,
                    vehicle_class: record.state.vehicle_class,
                    capture_timestamp_ms: params.capture_timestamp_ms,
                }
            })
            .collect(),
    }
}
