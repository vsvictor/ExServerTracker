mod auth;
pub mod cooperative;
mod host;
mod model;
mod rpc;
mod session;
pub mod storage;

pub use auth::{AuthorizedCoopRequest, RpcAuthContext};
pub use cooperative::{SectionControls, SectionError};
pub use host::{HostCoopNavPlugin, HostSessionAuthorizer};
pub use model::{IngestParams, MAX_BATCH_POINTS, PointSource, TrackPoint, TrackingSessionParams};
pub use rpc::{
    COOP_SYNC_PERMISSION, CoopNavPlugin, INGEST_PERMISSION, SERVER_TIME_PERMISSION,
    ServerTimeResult,
};
pub use session::{
    ConfirmedSession, InMemorySessionAuthorizer, SessionAuthorizationError, SessionAuthorizer,
    VerifiedPrincipal,
};
pub use storage::{
    AppendResult, CoopNavStorage, InMemoryCoopNavStorage, ManualClock, StorageError,
    StoragePartition, SystemClock, UnixMillisClock,
};

#[cfg(feature = "redis-storage")]
pub use session::RedisSessionAuthorizer;
#[cfg(feature = "redis-storage")]
pub use storage::RedisCoopNavStorage;

#[cfg(feature = "metrics-http1")]
pub async fn serve_health_http1(
    listener: tokio::net::TcpListener,
    health: json_rpc_2_runtime::HealthHandle,
) -> std::io::Result<()> {
    axum::serve(listener, health.router()).await
}

#[cfg(feature = "metrics-http1")]
pub async fn serve_running_health_http1(
    listener: tokio::net::TcpListener,
    running: &json_rpc_2_runtime::RunningServer,
    rpc: std::sync::Arc<json_rpc_2_runtime::RpcServer>,
) -> std::io::Result<()> {
    let health = running.health_handle(rpc);
    axum::serve(listener, health.router()).await
}
