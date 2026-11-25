use std::sync::Arc;

use async_trait::async_trait;
use json_rpc_2_server::{PluginHandle, RegistrationError, RpcError, RpcServer, ServerPlugin};
use serde_json::{Value, json};

use crate::{
    ConfirmedSession, CoopNavStorage, IngestParams, SectionControls, SectionError,
    SessionAuthorizationError, SystemClock, UnixMillisClock,
    cooperative::{CooperativeConfig, SyncPeersParams},
    rpc::{COOP_SYNC_PERMISSION, section_error, store_points},
};

/// Resolves an existing host-issued session credential to its trusted identity.
#[async_trait]
pub trait HostSessionAuthorizer: Send + Sync {
    async fn resolve(
        &self,
        session_id: &str,
    ) -> Result<ConfirmedSession, SessionAuthorizationError>;
}

#[derive(Clone)]
pub struct HostCoopNavPlugin {
    authorizer: Arc<dyn HostSessionAuthorizer>,
    storage: Arc<dyn CoopNavStorage>,
    clock: Arc<dyn UnixMillisClock>,
    sections: SectionControls,
}

impl HostCoopNavPlugin {
    pub fn new(
        authorizer: Arc<dyn HostSessionAuthorizer>,
        storage: Arc<dyn CoopNavStorage>,
    ) -> Self {
        let clock: Arc<dyn UnixMillisClock> = Arc::new(SystemClock);
        Self {
            authorizer,
            storage,
            sections: SectionControls::new(clock.clone(), None),
            clock,
        }
    }

    pub fn with_cooperative(mut self, config: CooperativeConfig) -> Self {
        self.sections = SectionControls::new(self.clock.clone(), Some(config));
        self
    }

    pub async fn section(&self, stage: i32, enabled: bool) -> Result<(), SectionError> {
        self.sections.section(stage, enabled).await
    }

    async fn session(&self, params: &Value) -> Result<ConfirmedSession, RpcError> {
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|s| crate::model::validate_identifier(s).is_ok())
            .ok_or_else(RpcError::invalid_params)?;
        let session = self
            .authorizer
            .resolve(session_id)
            .await
            .map_err(|error| match error {
                SessionAuthorizationError::Unavailable => {
                    RpcError::new(-32010, "Session storage unavailable")
                }
                _ => RpcError::new(-32011, "Not authorized for this session"),
            })?;
        if session.session_id != session_id {
            return Err(RpcError::new(-32011, "Not authorized for this session"));
        }
        for (key, value) in [
            ("driverId", &session.driver_id),
            ("brandId", &session.brand_id),
        ] {
            if params
                .get(key)
                .is_some_and(|input| input.as_str() != Some(value))
            {
                return Err(RpcError::new(-32011, "Not authorized for this session"));
            }
        }
        Ok(session)
    }

    pub fn install_methods(&self, server: &RpcServer) -> Result<(), RegistrationError> {
        let plugin = self.clone();
        server.register_with(
            "tracking.ingest",
            plugin,
            |plugin, mut params: Value| async move {
                let session = plugin.session(&params).await?;
                let object = params
                    .as_object_mut()
                    .ok_or_else(RpcError::invalid_params)?;
                object.insert("driverId".into(), json!(session.driver_id));
                object.insert("brandId".into(), json!(session.brand_id));
                let params: IngestParams =
                    serde_json::from_value(params).map_err(|_| RpcError::invalid_params())?;
                params.validate().map_err(|_| RpcError::invalid_params())?;
                store_points(&plugin.sections, &*plugin.storage, &session, &params).await
            },
        )?;
        let plugin = self.clone();
        server.register_with("serverTime", plugin, |plugin, params: Value| async move {
            let _: crate::TrackingSessionParams =
                serde_json::from_value(params.clone()).map_err(|_| RpcError::invalid_params())?;
            plugin.session(&params).await?;
            Ok(json!(crate::ServerTimeResult {
                server_time_ms: plugin.clock.now_millis()
            }))
        })?;
        let plugin = self.clone();
        server.register_with_context(
            COOP_SYNC_PERMISSION,
            move |mut params: Value, transport| {
                let plugin = plugin.clone();
                async move {
                    if !transport
                        .is_some_and(|ctx| ctx.transport == "quic" && !ctx.connection_id.is_empty())
                    {
                        return Err(RpcError::new(
                            -32014,
                            "Cooperative positioning requires QUIC",
                        ));
                    }
                    let generation = plugin.sections.generation();
                    let session = plugin.session(&params).await?;
                    let object = params
                        .as_object_mut()
                        .ok_or_else(RpcError::invalid_params)?;
                    object.insert("driverId".into(), json!(session.driver_id));
                    object.insert("brandId".into(), json!(session.brand_id));
                    let params: SyncPeersParams =
                        serde_json::from_value(params).map_err(|_| RpcError::invalid_params())?;
                    if !params.valid() {
                        return Err(RpcError::invalid_params());
                    }
                    let result = plugin
                        .sections
                        .sync(
                            &generation.map_err(section_error)?,
                            &session.session_id,
                            &session,
                            params,
                        )
                        .await
                        .map_err(section_error)?;
                    Ok(json!(result))
                }
            },
        )?;
        server.document_method("tracking.ingest", host_schema(crate::rpc::ingest_schema()), json!({
            "sessionId": "example-session-id",
            "points": [{"id":"0123456789abcdef0123456789abcdef","smoothedLatitude":50.45,"smoothedLongitude":30.52,"smoothedAccuracyMeters":5.0,"deviceTimeMs":1763500000000_i64,"observedAtMs":1763500000000_i64,"source":"fused","isMock":false,"flags":[]}]
        }))?;
        server.document_method(
            "serverTime",
            host_schema(crate::rpc::session_schema()),
            json!({"sessionId":"example-session-id"}),
        )?;
        server.document_method(COOP_SYNC_PERMISSION, host_schema(crate::cooperative::schema()), json!({
            "sessionId":"example-session-id", "captureTimestampMs":1763500000000_i64,
            "state":{"latitude":50.45,"longitude":30.52,"accuracyMeters":4.0,"speedMps":5.0,"bearingDegrees":90.0,"trustScore":0.8,"vehicleClass":"car","flags":[]},
            "observedPeers":[]
        }))?;
        Ok(())
    }
}

fn host_schema(mut schema: Value) -> Value {
    let object = schema.as_object_mut().expect("schema is an object");
    object.remove("oneOf");
    if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
        properties.remove("access_token");
        properties.remove("authorization");
    }
    if let Some(required) = object.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|v| v != "driverId" && v != "brandId");
    }
    schema
}

impl ServerPlugin for HostCoopNavPlugin {
    const NAME: &'static str = "coopnav-tracking";
    async fn install(self, server: &RpcServer) -> anyhow::Result<PluginHandle> {
        self.install_methods(server)?;
        Ok(self.sections.maintenance_handle())
    }
}

#[cfg(test)]
#[path = "host/tests.rs"]
mod tests;
