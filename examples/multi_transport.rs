use anyhow::{Context, Result, ensure};
use json_rpc_2_coopnav::{
    CoopNavPlugin, RedisCoopNavStorage, RedisSessionAuthorizer,
    cooperative::{
        CooperativeConfig, GtfsConfig, GtfsRealtimeSource, GtfsStatus, RedisCooperativeStorage,
        TransitSource, VehicleClass,
    },
};
use json_rpc_2_oauth2::{AuthPlugin, JsonRpcAuthServer, RedisAuthServer, TokenConfig};
use json_rpc_2_runtime::{MultiTransportServer, RpcServer};
use json_rpc_2_server::{JsonRpcProtocol, ServerPlugin};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::{
    collections::HashMap,
    env,
    fs::File,
    io::BufReader,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

#[tokio::main]
async fn main() -> Result<()> {
    let paserk_key = env::var("PASETO_LOCAL_KEY").context("PASETO_LOCAL_KEY is required")?;
    let auth_redis_url = env::var("AUTH_REDIS_URL").context("AUTH_REDIS_URL is required")?;
    let tracking_redis_url =
        env::var("TRACKING_REDIS_URL").context("TRACKING_REDIS_URL is required")?;
    let cert_path =
        env::var("COOPNAV_TLS_CERT_FILE").context("COOPNAV_TLS_CERT_FILE is required")?;
    let key_path = env::var("COOPNAV_TLS_KEY_FILE").context("COOPNAV_TLS_KEY_FILE is required")?;
    ensure!(
        paserk_key.starts_with("k4.local."),
        "PASETO_LOCAL_KEY must be a PASERK-encoded v4.local key"
    );

    let (certificates, key) = load_identity(&cert_path, &key_path)?;
    let auth = redis_auth(&paserk_key, &auth_redis_url)?;
    let grants = Arc::new(
        RedisSessionAuthorizer::connect(&auth_redis_url, "coopnav:v1")
            .await
            .context("connecting to the session-grant Redis service")?,
    );
    let storage = Arc::new(
        RedisCoopNavStorage::connect(
            &tracking_redis_url,
            "coopnav:v1",
            Duration::from_secs(7 * 24 * 60 * 60),
        )
        .await
        .context("connecting to the tracking Redis service")?,
    );

    let rpc = Arc::new(RpcServer::new());
    let mut plugin = CoopNavPlugin::new(AuthPlugin::new(auth), grants, storage);
    let stage_one = configured_bool("COOPNAV_STAGE1", true)?;
    let stage_two = configured_bool("COOPNAV_STAGE2", false)?;
    if stage_two && stage_one {
        plugin = plugin.with_cooperative(cooperative_config(&tracking_redis_url).await?);
    }
    plugin.section(1, stage_one).await?;
    plugin.section(2, stage_two).await?;
    let sections = plugin.sections();
    if stage_two && sections.gtfs_status().await == GtfsStatus::Unconfigured {
        eprintln!("gtfs-unconfigured: stage 2 enabled in V2V-only mode");
    }
    let handle = plugin.install(&rpc).await?;
    let bind_ip: IpAddr = env::var("COOPNAV_BIND_IP")
        .unwrap_or_else(|_| Ipv4Addr::UNSPECIFIED.to_string())
        .parse()
        .context("COOPNAV_BIND_IP must be an IP address")?;
    let quic_port = env::var("COOPNAV_QUIC_PORT")
        .unwrap_or_else(|_| "4433".to_owned())
        .parse::<u16>()
        .context("COOPNAV_QUIC_PORT must be a valid port")?;
    let health_address: SocketAddr = env::var("COOPNAV_HEALTH_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9000".to_owned())
        .parse()
        .context("COOPNAV_HEALTH_ADDR must be a socket address")?;
    let health_listener = tokio::net::TcpListener::bind(health_address)
        .await
        .context("binding the HTTP/1 health listener")?;

    let running = MultiTransportServer::builder(rpc.clone() as Arc<dyn JsonRpcProtocol>)
        .set_bind([bind_ip])
        .set_quic_port(quic_port)
        .set_tls(certificates, key)
        .start()
        .await
        .context("starting the protected QUIC RPC listener")?;
    let health = running.health_handle(rpc);
    let health_task = tokio::spawn(async move {
        axum::serve(health_listener, health.router())
            .await
            .context("serving HTTP/1 health endpoints")
    });
    let mut tasks = tokio::task::JoinSet::new();
    for task in handle.maintenance() {
        let task = task.clone();
        tasks.spawn(async move {
            let mut ticks = tokio::time::interval(task.interval());
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                if let Err(error) = task.run().await {
                    eprintln!("cooperative transit refresh skipped: {error}");
                }
            }
        });
    }

    eprintln!(
        "CoopNav QUIC RPC listening on {} and HTTP/1 health endpoints on {}",
        running
            .listeners()
            .find(|(kind, _)| *kind == json_rpc_2_runtime::TransportKind::Quic)
            .map(|(_, address)| address)
            .context("QUIC listener was not started")?,
        health_address
    );
    let outcome = tokio::select! {
        signal = tokio::signal::ctrl_c() => signal.context("waiting for shutdown signal"),
        worker = tasks.join_next(), if !tasks.is_empty() => {
            Err(anyhow::anyhow!("cooperative maintenance worker exited: {worker:?}"))
        }
    };
    let cleanup = sections.section(2, false).await;
    tasks.shutdown().await;
    health_task.abort();
    running.close().await?;
    outcome?;
    cleanup?;
    Ok(())
}

fn configured_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be true or false")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(_) => anyhow::bail!("{name} must be UTF-8"),
    }
}

async fn cooperative_config(redis_url: &str) -> Result<CooperativeConfig> {
    let transit: Option<Arc<dyn TransitSource>> = match env::var("COOPNAV_GTFS_URL") {
        Ok(url) => Some(Arc::new(configure_gtfs(url)?)),
        Err(env::VarError::NotPresent) => None,
        Err(_) => anyhow::bail!("COOPNAV_GTFS_URL must be UTF-8"),
    };
    Ok(CooperativeConfig::new(
        Arc::new(RedisCooperativeStorage::connect(redis_url, "coopnav:v1").await?),
        transit,
        Duration::from_secs(2),
        Duration::from_millis(500),
    )?)
}

fn configure_gtfs(url: String) -> Result<GtfsRealtimeSource> {
    let brand_id =
        env::var("COOPNAV_GTFS_BRAND_ID").context("GTFS requires COOPNAV_GTFS_BRAND_ID")?;
    let routes = env::var("COOPNAV_GTFS_ROUTES").context("GTFS requires COOPNAV_GTFS_ROUTES")?;
    let mut route_classes = HashMap::new();
    for item in routes.split(',') {
        let (class, id) = item
            .split_once(':')
            .context("GTFS route mapping must be class:routeId")?;
        let class = match class {
            "bus" => VehicleClass::Bus,
            "trolleybus" => VehicleClass::Trolleybus,
            _ => anyhow::bail!("GTFS route mapping supports bus and trolleybus only"),
        };
        ensure!(
            route_classes.insert(id.to_owned(), class).is_none(),
            "duplicate GTFS route mapping"
        );
    }
    let accuracy_meters = env::var("COOPNAV_GTFS_ACCURACY_METERS")
        .context("GTFS requires COOPNAV_GTFS_ACCURACY_METERS")?
        .parse()
        .map_err(|_| anyhow::anyhow!("COOPNAV_GTFS_ACCURACY_METERS must be numeric"))?;
    let trust_score = env::var("COOPNAV_GTFS_TRUST_SCORE")
        .context("GTFS requires COOPNAV_GTFS_TRUST_SCORE")?
        .parse()
        .map_err(|_| anyhow::anyhow!("COOPNAV_GTFS_TRUST_SCORE must be numeric"))?;
    Ok(GtfsRealtimeSource::new(GtfsConfig {
        url,
        brand_id,
        route_classes,
        accuracy_meters,
        trust_score,
    })?)
}

fn redis_auth(paserk_key: &str, redis_url: &str) -> Result<RedisAuthServer> {
    let mut auth = RedisAuthServer::new(paserk_key, TokenConfig::new("coopnav"))?;
    auth.set_db_host(redis_url);
    Ok(auth)
}

fn load_identity(
    cert_path: &str,
    key_path: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certificates = rustls_pemfile::certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("reading TLS certificate chain")?;
    ensure!(!certificates.is_empty(), "TLS certificate chain is empty");
    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(key_path)?))?
        .context("TLS private key file contains no supported key")?;
    Ok((certificates, key))
}
