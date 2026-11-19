use anyhow::{Context, Result, ensure};
use json_rpc_2_backend::BackendBuilder;
use json_rpc_2_coopnav::{CoopNavPlugin, RedisCoopNavStorage, RedisSessionAuthorizer};
use json_rpc_2_oauth2::{AuthPlugin, JsonRpcAuthServer, RedisAuthServer, TokenConfig};
use std::{env, sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let paserk_key = env::var("PASETO_LOCAL_KEY").context("PASETO_LOCAL_KEY is required")?;
    let auth_redis_url = env::var("AUTH_REDIS_URL").context("AUTH_REDIS_URL is required")?;
    let tracking_redis_url =
        env::var("TRACKING_REDIS_URL").context("TRACKING_REDIS_URL is required")?;
    ensure!(
        paserk_key.starts_with("k4.local."),
        "PASETO_LOCAL_KEY must be a PASERK-encoded v4.local key"
    );

    let mut auth_storage = RedisAuthServer::new(&paserk_key, TokenConfig::new("coopnav"))?;
    auth_storage.set_db_host(&auth_redis_url);

    let grants = Arc::new(
        RedisSessionAuthorizer::connect(&auth_redis_url, "coopnav:v1")
            .await
            .context("connecting to the session-grant Redis service")?,
    );
    let tracking = Arc::new(
        RedisCoopNavStorage::connect(
            &tracking_redis_url,
            "coopnav:v1",
            Duration::from_secs(7 * 24 * 60 * 60),
        )
        .await
        .context("connecting to the tracking Redis service")?,
    );
    let coopnav = CoopNavPlugin::new(AuthPlugin::new(auth_storage.clone()), grants, tracking);

    let backend = BackendBuilder::from_env()?
        .default_instance_id("coopnav")
        .plugin(AuthPlugin::new(auth_storage))
        .plugin(coopnav)
        .build()
        .await?;
    backend.run_until_signal().await
}
