mod gtfs;
mod runtime;
mod spatial;
mod store;
mod wire;

pub use gtfs::{GtfsConfig, GtfsError, GtfsRealtimeSource, TransitFix, TransitSource};
pub use runtime::{CooperativeConfig, GtfsStatus, SectionControls, SectionError};
#[cfg(feature = "redis-storage")]
pub use store::RedisCooperativeStorage;
pub use store::{CooperativeStorage, InMemoryCooperativeStorage, PeerRecord, PeerWrite};
pub use wire::{Donor, Observation, PeerState, SyncPeersParams, SyncPeersResult, VehicleClass};

pub(crate) use wire::schema;

use sha2::{Digest, Sha256};

fn opaque(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    hex::encode(&digest.finalize()[..16])
}

#[cfg(test)]
pub(crate) mod tests;
