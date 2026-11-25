use crate::{
    ConfirmedSession, CoopNavStorage, IngestParams, MAX_BATCH_POINTS, SectionControls,
    SectionError, SessionAuthorizer, StoragePartition, SystemClock, TrackingSessionParams,
    UnixMillisClock, VerifiedPrincipal,
    cooperative::{CooperativeConfig, SyncPeersParams},
    model::validate_identifier,
};
use json_rpc_2_oauth2::{
    AccessTokenClaims, AuthError, AuthPlugin, JsonRpcAuthServer, RpcError, authorize,
};
use json_rpc_2_server::{PluginHandle, RegistrationError, RpcServer, ServerPlugin};
use serde_json::{Value, json};
use std::{collections::HashSet, marker::PhantomData, sync::Arc};

pub const INGEST_PERMISSION: &str = "tracking.ingest";
pub const SERVER_TIME_PERMISSION: &str = "tracking.server_time";
pub const COOP_SYNC_PERMISSION: &str = "coop.sync_peers";
const APP_ERROR: i64 = -32010;
const AUTHORIZATION_ERROR: i64 = -32011;

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerTimeResult {
    pub server_time_ms: u64,
}

pub struct CoopNavPlugin<A> {
    auth: AuthPlugin<A>,
    authorizer: Arc<dyn SessionAuthorizer>,
    storage: Arc<dyn CoopNavStorage>,
    clock: Arc<dyn UnixMillisClock>,
    sections: SectionControls,
    marker: PhantomData<fn() -> A>,
}

impl<A: Clone> Clone for CoopNavPlugin<A> {
    fn clone(&self) -> Self {
        Self {
            auth: self.auth.clone(),
            authorizer: Arc::clone(&self.authorizer),
            storage: Arc::clone(&self.storage),
            clock: Arc::clone(&self.clock),
            sections: self.sections.clone(),
            marker: PhantomData,
        }
    }
}

impl<A> CoopNavPlugin<A>
where
    A: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
{
    pub fn new(
        auth: AuthPlugin<A>,
        authorizer: Arc<dyn SessionAuthorizer>,
        storage: Arc<dyn CoopNavStorage>,
    ) -> Self {
        Self::with_clock(auth, authorizer, storage, Arc::new(SystemClock))
    }

    pub fn with_clock(
        auth: AuthPlugin<A>,
        authorizer: Arc<dyn SessionAuthorizer>,
        storage: Arc<dyn CoopNavStorage>,
        clock: Arc<dyn UnixMillisClock>,
    ) -> Self {
        let sections = SectionControls::new(clock.clone(), None);
        Self {
            auth,
            authorizer,
            storage,
            clock,
            sections,
            marker: PhantomData,
        }
    }

    /// Inject cooperative dependencies before installing the plugin or sharing its controls.
    pub fn with_cooperative(mut self, config: CooperativeConfig) -> Self {
        self.sections = SectionControls::new(self.clock.clone(), Some(config));
        self
    }

    pub fn sections(&self) -> SectionControls {
        self.sections.clone()
    }

    pub async fn section(&self, stage: i32, enabled: bool) -> Result<(), SectionError> {
        self.sections.section(stage, enabled).await
    }

    pub fn install_methods(&self, server: &RpcServer) -> Result<(), RegistrationError> {
        let ingest_context = IngestRpcContext {
            auth: crate::RpcAuthContext::new(self.auth.clone()),
            authorizer: Arc::clone(&self.authorizer),
            storage: Arc::clone(&self.storage),
            sections: self.sections.clone(),
        };
        server.register_with(
            "tracking.ingest",
            ingest_context,
            |auth, params| async move { ingest::<A>(auth, params).await },
        )?;

        let time_context = ServerTimeRpcContext {
            auth: crate::RpcAuthContext::new(self.auth.clone()),
            authorizer: Arc::clone(&self.authorizer),
            clock: Arc::clone(&self.clock),
        };
        server.register_with("serverTime", time_context, |auth, params| async move {
            server_time::<A>(auth, params).await
        })?;

        let coop_context = CoopRpcContext {
            auth: crate::RpcAuthContext::new(self.auth.clone()),
            authorizer: Arc::clone(&self.authorizer),
            sections: self.sections.clone(),
        };
        server.register_with_context(COOP_SYNC_PERMISSION, move |params, transport| {
            let auth = coop_context.clone();
            async move {
                if !transport
                    .is_some_and(|ctx| ctx.transport == "quic" && !ctx.connection_id.is_empty())
                {
                    return Err(RpcError::new(
                        -32014,
                        "Cooperative positioning requires QUIC",
                    ));
                }
                sync_peers::<A>(auth, params).await
            }
        })?;

        server.document_method(
            "tracking.ingest",
            ingest_schema(),
            json!({
                "access_token": "PASETO v4.local access token",
                "sessionId": "confirmed-session-id",
                "driverId": "confirmed-driver-id",
                "brandId": "confirmed-brand-id",
                "points": [{
                    "id": "0123456789abcdef0123456789abcdef",
                    "smoothedLatitude": 50.4501,
                    "smoothedLongitude": 30.5234,
                    "smoothedAccuracyMeters": 5.0,
                    "deviceTimeMs": 1763500000000_i64,
                    "observedAtMs": 1763500000000_i64,
                    "source": "fused",
                    "isMock": false,
                    "flags": []
                }]
            }),
        )?;
        server.document_method(
            "serverTime",
            session_schema(),
            json!({
                "access_token": "PASETO v4.local access token",
                "sessionId": "confirmed-session-id"
            }),
        )?;
        server.document_method(COOP_SYNC_PERMISSION, crate::cooperative::schema(), json!({
            "access_token": "PASETO v4.local access token",
            "sessionId": "confirmed-session-id", "driverId": "confirmed-driver-id", "brandId": "confirmed-brand-id",
            "captureTimestampMs": 1763500000000_i64,
            "state": {
                "latitude": 50.4501, "longitude": 30.5234, "accuracyMeters": 4.0,
                "speedMps": 5.0, "bearingDegrees": 90.0, "trustScore": 0.8, "vehicleClass": "car", "flags": []
            },
            "observedPeers": [{
                "observationId": "obs-1", "vehicleClass": "bus", "distanceMeters": 20.0,
                "azimuthDegrees": 90.0, "visionVarianceMeters2": 4.0
            }]
        }))?;
        Ok(())
    }
}

#[derive(Clone)]
struct IngestRpcContext<A> {
    auth: crate::RpcAuthContext<A>,
    authorizer: Arc<dyn SessionAuthorizer>,
    storage: Arc<dyn CoopNavStorage>,
    sections: SectionControls,
}

struct AuthorizedIngestRequest {
    claims: AccessTokenClaims,
    params: Value,
    authorizer: Arc<dyn SessionAuthorizer>,
    storage: Arc<dyn CoopNavStorage>,
    sections: SectionControls,
}

impl<A> IngestRpcContext<A>
where
    A: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
{
    fn authorize(
        &self,
        params: Value,
        permissions: &[&str],
    ) -> Result<AuthorizedIngestRequest, RpcError> {
        let request = self.auth.authorize(params, permissions)?;
        Ok(AuthorizedIngestRequest {
            claims: request.claims,
            params: request.params,
            authorizer: Arc::clone(&self.authorizer),
            storage: Arc::clone(&self.storage),
            sections: self.sections.clone(),
        })
    }
}

#[derive(Clone)]
struct ServerTimeRpcContext<A> {
    auth: crate::RpcAuthContext<A>,
    authorizer: Arc<dyn SessionAuthorizer>,
    clock: Arc<dyn UnixMillisClock>,
}

struct AuthorizedServerTimeRequest {
    claims: AccessTokenClaims,
    params: Value,
    authorizer: Arc<dyn SessionAuthorizer>,
    clock: Arc<dyn UnixMillisClock>,
}

impl<A> ServerTimeRpcContext<A>
where
    A: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
{
    fn authorize(
        &self,
        params: Value,
        permissions: &[&str],
    ) -> Result<AuthorizedServerTimeRequest, RpcError> {
        let request = self.auth.authorize(params, permissions)?;
        Ok(AuthorizedServerTimeRequest {
            claims: request.claims,
            params: request.params,
            authorizer: Arc::clone(&self.authorizer),
            clock: Arc::clone(&self.clock),
        })
    }
}

impl<A> ServerPlugin for CoopNavPlugin<A>
where
    A: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
{
    const NAME: &'static str = "coopnav-tracking";

    async fn install(self, server: &RpcServer) -> anyhow::Result<PluginHandle> {
        self.install_methods(server)?;
        Ok(self.sections.maintenance_handle())
    }
}

#[authorize(scopes = "tracking.ingest")]
async fn ingest<A>(
    auth: IngestRpcContext<A>,
    request: AuthorizedIngestRequest,
) -> Result<Value, RpcError>
where
    A: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
{
    let params: IngestParams =
        serde_json::from_value(request.params).map_err(|_| RpcError::invalid_params())?;
    params.validate().map_err(|_| RpcError::invalid_params())?;

    let principal = VerifiedPrincipal {
        subject: request.claims.subject,
    };
    let confirmed = resolve_owned(
        &*request.authorizer,
        &principal,
        &params.session_id,
        &params.driver_id,
        &params.brand_id,
    )
    .await?;
    store_points(&request.sections, &*request.storage, &confirmed, &params).await
}

pub(crate) async fn store_points(
    sections: &SectionControls,
    storage: &dyn CoopNavStorage,
    confirmed: &ConfirmedSession,
    params: &IngestParams,
) -> Result<Value, RpcError> {
    let _stage = sections.ingest_guard().await.map_err(section_error)?;

    let partition = StoragePartition {
        brand_id: confirmed.brand_id.clone(),
        driver_id: confirmed.driver_id.clone(),
    };
    let mut accepted = Vec::with_capacity(params.points.len());
    let mut seen = HashSet::with_capacity(params.points.len());
    for point in &params.points {
        if !seen.insert(point.id.as_str()) {
            continue;
        }
        match storage
            .append_point(&partition, point)
            .await
            .map_err(|error| {
                tracing::error!(%error, "tracking point persistence failed");
                storage_error()
            })? {
            crate::AppendResult::Inserted | crate::AppendResult::AlreadyPresent => {
                accepted.push(point.id.clone());
            }
            crate::AppendResult::Conflict => return Err(storage_error()),
        }
    }
    Ok(json!({ "acceptedIds": accepted }))
}

#[derive(Clone)]
struct CoopRpcContext<A> {
    auth: crate::RpcAuthContext<A>,
    authorizer: Arc<dyn SessionAuthorizer>,
    sections: SectionControls,
}

struct AuthorizedSyncRequest {
    auth: crate::AuthorizedCoopRequest,
    authorizer: Arc<dyn SessionAuthorizer>,
    sections: SectionControls,
}

impl<A> CoopRpcContext<A>
where
    A: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
{
    fn authorize(
        &self,
        params: Value,
        permissions: &[&str],
    ) -> Result<AuthorizedSyncRequest, RpcError> {
        Ok(AuthorizedSyncRequest {
            auth: self.auth.authorize(params, permissions)?,
            authorizer: self.authorizer.clone(),
            sections: self.sections.clone(),
        })
    }
}

#[authorize(scopes = "coop.sync_peers")]
async fn sync_peers<A>(
    auth: CoopRpcContext<A>,
    request: AuthorizedSyncRequest,
) -> Result<Value, RpcError>
where
    A: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
{
    let generation = request.sections.generation();
    let params: SyncPeersParams =
        serde_json::from_value(request.auth.params).map_err(|_| RpcError::invalid_params())?;
    if !params.valid() {
        return Err(RpcError::invalid_params());
    }
    let principal = VerifiedPrincipal {
        subject: request.auth.claims.subject,
    };
    let confirmed = resolve_owned(
        &*request.authorizer,
        &principal,
        &params.session_id,
        &params.driver_id,
        &params.brand_id,
    )
    .await?;
    let result = request
        .sections
        .sync(
            &generation.map_err(section_error)?,
            &principal.subject,
            &confirmed,
            params,
        )
        .await
        .map_err(section_error)?;
    Ok(json!(result))
}

async fn resolve_owned(
    authorizer: &dyn SessionAuthorizer,
    principal: &VerifiedPrincipal,
    session_id: &str,
    driver_id: &str,
    brand_id: &str,
) -> Result<ConfirmedSession, RpcError> {
    let confirmed = authorizer
        .resolve(principal, session_id)
        .await
        .map_err(|_| denied())?;
    if confirmed.session_id != session_id
        || confirmed.driver_id != driver_id
        || confirmed.brand_id != brand_id
    {
        return Err(denied());
    }
    Ok(confirmed)
}

pub(crate) fn section_error(error: SectionError) -> RpcError {
    let code = match error {
        SectionError::Timestamp => -32013,
        SectionError::Storage => -32010,
        _ => -32012,
    };
    RpcError::new(code, error.to_string())
}

#[authorize(scopes = "tracking.server_time")]
async fn server_time<A>(
    auth: ServerTimeRpcContext<A>,
    request: AuthorizedServerTimeRequest,
) -> Result<Value, RpcError>
where
    A: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
{
    let params: TrackingSessionParams =
        serde_json::from_value(request.params).map_err(|_| RpcError::invalid_params())?;
    validate_identifier(&params.session_id).map_err(|_| RpcError::invalid_params())?;
    let principal = VerifiedPrincipal {
        subject: request.claims.subject,
    };
    request
        .authorizer
        .resolve(&principal, &params.session_id)
        .await
        .map_err(|_| denied())?;
    Ok(json!(ServerTimeResult {
        server_time_ms: request.clock.now_millis(),
    }))
}

fn denied() -> RpcError {
    RpcError::new(AUTHORIZATION_ERROR, "Not authorized for this session")
}

fn storage_error() -> RpcError {
    RpcError::new(
        APP_ERROR,
        "Tracking persistence failed or point id conflicts",
    )
}

pub(crate) fn session_schema() -> Value {
    json!({
        "type": "object",
        "required": ["sessionId"],
        "properties": {
            "sessionId": {"type": "string", "minLength": 1, "maxLength": 256},
            "access_token": {"type": "string", "minLength": 1},
            "authorization": {"type": "string", "pattern": "^Bearer .+"}
        },
        "oneOf": [
            {"required": ["access_token"], "not": {"required": ["authorization"]}},
            {"required": ["authorization"], "not": {"required": ["access_token"]}}
        ],
        "additionalProperties": false
    })
}

pub(crate) fn ingest_schema() -> Value {
    json!({
        "type": "object",
        "required": ["sessionId", "driverId", "brandId", "points"],
        "properties": {
            "sessionId": {"type": "string", "minLength": 1, "maxLength": 256},
            "driverId": {"type": "string", "minLength": 1, "maxLength": 256},
            "brandId": {"type": "string", "minLength": 1, "maxLength": 256},
            "access_token": {"type": "string", "minLength": 1},
            "authorization": {"type": "string", "pattern": "^Bearer .+"},
            "points": {
                "type": "array", "minItems": 1, "maxItems": MAX_BATCH_POINTS,
                "items": {
                    "type": "object",
                    "required": [
                        "id", "smoothedLatitude", "smoothedLongitude",
                        "smoothedAccuracyMeters", "deviceTimeMs", "observedAtMs",
                        "source", "isMock", "flags"
                    ],
                    "properties": {
                        "id": {"type": "string", "pattern": "^[0-9A-Fa-f]{32}$"},
                        "rawLatitude": {"type": "number", "minimum": -90, "maximum": 90},
                        "rawLongitude": {"type": "number", "minimum": -180, "maximum": 180},
                        "rawAccuracyMeters": {"type": "number", "minimum": 0},
                        "smoothedLatitude": {"type": "number", "minimum": -90, "maximum": 90},
                        "smoothedLongitude": {"type": "number", "minimum": -180, "maximum": 180},
                        "smoothedAccuracyMeters": {"type": "number", "minimum": 0},
                        "speedMps": {"type": "number", "minimum": 0, "maximum": 61.1111111112},
                        "bearingDegrees": {"type": "number", "minimum": 0, "exclusiveMaximum": 360},
                        "deviceTimeMs": {"type": "integer", "minimum": 1},
                        "observedAtMs": {"type": "integer", "minimum": 1},
                        "source": {"enum": ["fused", "gps", "network", "deadReckoning", "stationary", "cooperative"]},
                        "isMock": {"type": "boolean"},
                        "flags": {
                            "type": "array", "maxItems": 32,
                            "items": {"type": "string", "minLength": 1, "maxLength": 64}
                        }
                    },
                    "additionalProperties": false
                }
            }
        },
        "oneOf": [
            {"required": ["access_token"], "not": {"required": ["authorization"]}},
            {"required": ["authorization"], "not": {"required": ["access_token"]}}
        ],
        "additionalProperties": false
    })
}

#[cfg(test)]
#[path = "rpc/tests.rs"]
mod tests;
