use async_trait::async_trait;
use dashmap::DashMap;
#[cfg(feature = "redis-storage")]
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
#[cfg(feature = "redis-storage")]
use sha2::{Digest, Sha256};
use std::sync::Arc;
#[cfg(feature = "redis-storage")]
use std::time::Duration;
use thiserror::Error;

#[cfg(feature = "redis-storage")]
use crate::storage::StorageError;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedPrincipal {
    pub subject: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmedSession {
    pub session_id: String,
    pub driver_id: String,
    pub brand_id: String,
}

impl ConfirmedSession {
    pub fn new(
        session_id: impl Into<String>,
        driver_id: impl Into<String>,
        brand_id: impl Into<String>,
    ) -> Result<Self, SessionAuthorizationError> {
        let session = Self {
            session_id: session_id.into(),
            driver_id: driver_id.into(),
            brand_id: brand_id.into(),
        };
        for value in [&session.session_id, &session.driver_id, &session.brand_id] {
            crate::model::validate_identifier(value)
                .map_err(|_| SessionAuthorizationError::InvalidGrant)?;
        }
        Ok(session)
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SessionAuthorizationError {
    #[error("the principal is not authorized for this confirmed session")]
    Denied,
    #[error("the host-provisioned session grant is invalid")]
    InvalidGrant,
    #[error("a different confirmed session grant already exists")]
    Conflict,
    #[error("the session grant resolver is unavailable")]
    Unavailable,
}

#[async_trait]
pub trait SessionAuthorizer: Send + Sync {
    async fn resolve(
        &self,
        principal: &VerifiedPrincipal,
        session_id: &str,
    ) -> Result<ConfirmedSession, SessionAuthorizationError>;
}

#[derive(Clone, Default)]
pub struct InMemorySessionAuthorizer {
    grants: Arc<DashMap<String, Grant>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Grant {
    principal: String,
    session: ConfirmedSession,
}

impl InMemorySessionAuthorizer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn provision_confirmed_session(
        &self,
        principal: impl Into<String>,
        session: ConfirmedSession,
    ) -> Result<(), SessionAuthorizationError> {
        let principal = principal.into();
        crate::model::validate_identifier(&principal)
            .map_err(|_| SessionAuthorizationError::InvalidGrant)?;
        for value in [&session.session_id, &session.driver_id, &session.brand_id] {
            crate::model::validate_identifier(value)
                .map_err(|_| SessionAuthorizationError::InvalidGrant)?;
        }
        let grant = Grant { principal, session };
        match self.grants.entry(grant.session.session_id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(grant);
                Ok(())
            }
            dashmap::mapref::entry::Entry::Occupied(entry) if *entry.get() == grant => Ok(()),
            dashmap::mapref::entry::Entry::Occupied(_) => Err(SessionAuthorizationError::Conflict),
        }
    }

    pub fn revoke(&self, session_id: &str) -> bool {
        self.grants.remove(session_id).is_some()
    }
}

#[async_trait]
impl SessionAuthorizer for InMemorySessionAuthorizer {
    async fn resolve(
        &self,
        principal: &VerifiedPrincipal,
        session_id: &str,
    ) -> Result<ConfirmedSession, SessionAuthorizationError> {
        let grant = self
            .grants
            .get(session_id)
            .ok_or(SessionAuthorizationError::Denied)?;
        if grant.principal != principal.subject {
            return Err(SessionAuthorizationError::Denied);
        }
        Ok(grant.session.clone())
    }
}

#[derive(Clone)]
#[cfg(feature = "redis-storage")]
pub struct RedisSessionAuthorizer {
    connection: redis::aio::ConnectionManager,
    prefix: String,
}

#[cfg(feature = "redis-storage")]
impl RedisSessionAuthorizer {
    pub async fn connect(
        redis_url: &str,
        key_prefix: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let client = redis::Client::open(redis_url).map_err(StorageError::redis)?;
        let connection = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(StorageError::redis)?;
        let prefix = key_prefix.into();
        if prefix.trim().is_empty() {
            return Err(StorageError::Configuration(
                "Redis grant key prefix must not be empty".to_owned(),
            ));
        }
        Ok(Self { connection, prefix })
    }

    pub async fn provision_confirmed_session(
        &self,
        principal: impl Into<String>,
        session: ConfirmedSession,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let principal = principal.into();
        crate::model::validate_identifier(&principal)
            .map_err(|message| StorageError::Configuration(message.to_owned()))?;
        for value in [&session.session_id, &session.driver_id, &session.brand_id] {
            crate::model::validate_identifier(value)
                .map_err(|message| StorageError::Configuration(message.to_owned()))?;
        }
        if ttl.is_zero() || ttl.as_secs() == 0 || ttl.as_secs() > i64::MAX as u64 {
            return Err(StorageError::Configuration(
                "grant TTL must be at least one second".to_owned(),
            ));
        }
        let grant = Grant { principal, session };
        let serialized = serde_json::to_vec(&grant)
            .map_err(|error| StorageError::Serialization(error.to_string()))?;
        let key = self.key(&grant.session.session_id);
        let mut connection = self.connection.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(
                String::from_utf8(serialized.clone())
                    .map_err(|error| StorageError::Serialization(error.to_string()))?,
            )
            .arg("EX")
            .arg(ttl.as_secs())
            .arg("NX")
            .query_async(&mut connection)
            .await
            .map_err(StorageError::redis)?;
        if result.is_none() {
            let existing: Option<Vec<u8>> =
                connection.get(&key).await.map_err(StorageError::redis)?;
            if existing.as_deref() != Some(serialized.as_slice()) {
                return Err(StorageError::GrantConflict);
            }
            let _: () = connection
                .expire(&key, ttl.as_secs() as i64)
                .await
                .map_err(StorageError::redis)?;
        }
        Ok(())
    }

    pub async fn revoke(&self, session_id: &str) -> Result<bool, StorageError> {
        let mut connection = self.connection.clone();
        let deleted: u64 = connection
            .del(self.key(session_id))
            .await
            .map_err(StorageError::redis)?;
        Ok(deleted != 0)
    }

    fn key(&self, session_id: &str) -> String {
        let digest = hex::encode(Sha256::digest(session_id.as_bytes()));
        format!("{}:session-grant:{digest}", self.prefix)
    }
}

#[cfg(feature = "redis-storage")]
#[async_trait]
impl SessionAuthorizer for RedisSessionAuthorizer {
    async fn resolve(
        &self,
        principal: &VerifiedPrincipal,
        session_id: &str,
    ) -> Result<ConfirmedSession, SessionAuthorizationError> {
        let mut connection = self.connection.clone();
        let value: Option<Vec<u8>> = connection
            .get(self.key(session_id))
            .await
            .map_err(|_| SessionAuthorizationError::Unavailable)?;
        let Some(value) = value else {
            return Err(SessionAuthorizationError::Denied);
        };
        let grant: Grant =
            serde_json::from_slice(&value).map_err(|_| SessionAuthorizationError::Unavailable)?;
        if grant.principal != principal.subject || grant.session.session_id != session_id {
            return Err(SessionAuthorizationError::Denied);
        }
        Ok(grant.session)
    }
}

#[cfg(test)]
#[path = "session/tests.rs"]
mod tests;
