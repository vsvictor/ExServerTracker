use json_rpc_2_client::RpcClient;
use json_rpc_2_coopnav::{
    COOP_SYNC_PERMISSION, ConfirmedSession, CoopNavPlugin, InMemoryCoopNavStorage,
    InMemorySessionAuthorizer, ManualClock, SessionAuthorizer, StoragePartition,
    cooperative::{CooperativeConfig, GtfsStatus, InMemoryCooperativeStorage},
};
use json_rpc_2_oauth2::{
    AuthPlugin, JsonRpcAuthServer, LoginRequest, RegisterRequest, RpcServer, SqliteAuthServer,
    TokenConfig,
};
use json_rpc_2_quic::client::transports::quic::QuicClientTransport;
use json_rpc_2_runtime::{MultiTransportServer, QUIC_ALPN, TransportKind};
use json_rpc_2_server::JsonRpcProtocol;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::{Value, json};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const TEST_KEY: &str = "k4.local.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const SESSION: &str = "confirmed-session-123";
const DRIVER: &str = "driver-123";
const BRAND: &str = "brand-123";

#[tokio::test]
async fn quic_tracking_and_host_http1_health_endpoints_work_together() {
    let mut auth = SqliteAuthServer::new(TEST_KEY, TokenConfig::new("coopnav-host-test")).unwrap();
    auth.set_db_host(":memory:");
    auth.initialize_storage().await.unwrap();
    let user = auth
        .handle_register(RegisterRequest {
            email: "host-test@example.test".into(),
            password: "test-password-123".into(),
            profile: HashMap::new(),
        })
        .await
        .unwrap();
    auth.handle_add_role(user.id.clone(), "tracker".into())
        .await
        .unwrap();
    for permission in ["tracking.ingest", "tracking.server_time"] {
        auth.handle_add_permission("tracker".into(), permission.into())
            .await
            .unwrap();
    }
    let tokens = auth
        .handle_login(LoginRequest {
            email: "host-test@example.test".into(),
            password: "test-password-123".into(),
            grant_type: "password".into(),
        })
        .await
        .unwrap();
    auth.handle_add_permission("tracker".into(), COOP_SYNC_PERMISSION.into())
        .await
        .unwrap();
    let tokens_with_coop = auth
        .handle_login(LoginRequest {
            email: "host-test@example.test".into(),
            password: "test-password-123".into(),
            grant_type: "password".into(),
        })
        .await
        .unwrap();
    let donor = auth
        .handle_register(RegisterRequest {
            email: "donor@example.test".into(),
            password: "test-password-123".into(),
            profile: HashMap::new(),
        })
        .await
        .unwrap();
    auth.handle_add_role(donor.id.clone(), "tracker".into())
        .await
        .unwrap();
    let donor_token = auth
        .handle_login(LoginRequest {
            email: "donor@example.test".into(),
            password: "test-password-123".into(),
            grant_type: "password".into(),
        })
        .await
        .unwrap()
        .access_token;

    let grants = Arc::new(InMemorySessionAuthorizer::new());
    grants
        .provision_confirmed_session(
            user.id,
            ConfirmedSession::new(SESSION, DRIVER, BRAND).unwrap(),
        )
        .unwrap();
    grants
        .provision_confirmed_session(
            donor.id,
            ConfirmedSession::new("donor-session", "donor", BRAND).unwrap(),
        )
        .unwrap();
    let clock = Arc::new(ManualClock::new(1_763_500_000_123));
    let storage =
        Arc::new(InMemoryCoopNavStorage::new(clock.clone(), Duration::from_secs(60)).unwrap());
    let rpc = Arc::new(RpcServer::new());
    let plugin = CoopNavPlugin::with_clock(
        AuthPlugin::new(auth),
        grants as Arc<dyn SessionAuthorizer>,
        storage.clone(),
        clock,
    )
    .with_cooperative(
        CooperativeConfig::new(
            Arc::new(InMemoryCooperativeStorage::default()),
            None,
            Duration::from_secs(2),
            Duration::from_millis(100),
        )
        .unwrap(),
    );
    plugin.install_methods(&rpc).unwrap();
    assert_eq!(
        plugin.sections().gtfs_status().await,
        GtfsStatus::Unconfigured
    );
    assert!(
        plugin
            .sections()
            .maintenance_handle()
            .maintenance()
            .is_empty()
    );
    let direct = serde_json::to_vec(
        &json!({"jsonrpc":"2.0", "id":1, "method": COOP_SYNC_PERMISSION, "params":{}}),
    )
    .unwrap();
    let rejected: Value =
        serde_json::from_slice(&rpc.handle_bytes(&direct).await.unwrap()).unwrap();
    assert_eq!(rejected["error"]["code"], -32014);

    let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let certificate: CertificateDer<'static> = generated.cert.into();
    let key = PrivateKeyDer::Pkcs8(generated.key_pair.serialize_der().into());
    let running = MultiTransportServer::builder(rpc.clone() as Arc<dyn JsonRpcProtocol>)
        .set_bind(["127.0.0.1".parse().unwrap()])
        .set_quic_port(0)
        .set_tls(vec![certificate.clone()], key)
        .start()
        .await
        .unwrap();
    let quic_address = running
        .listeners()
        .find(|(kind, _)| *kind == TransportKind::Quic)
        .map(|(_, address)| address)
        .unwrap();

    let health_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health_address = health_listener.local_addr().unwrap();
    let health = running.health_handle(rpc);
    let health_task =
        tokio::spawn(async move { axum::serve(health_listener, health.router()).await });

    let endpoint = client_endpoint(quic_address, &certificate);
    let transport = QuicClientTransport::connect(endpoint, quic_address, "127.0.0.1")
        .await
        .unwrap();
    let client = RpcClient::new(transport);
    let time: Value = client
        .request(
            "serverTime",
            json!({"sessionId": SESSION, "access_token": tokens.access_token}),
        )
        .await
        .unwrap();
    assert_eq!(time["serverTimeMs"], 1_763_500_000_123_u64);

    let point_id = "0123456789abcdef0123456789abcdef";
    let ingest: Value = client
        .request(
            "tracking.ingest",
            json!({
                "sessionId": SESSION,
                "driverId": DRIVER,
                "brandId": BRAND,
                "access_token": tokens.access_token,
                "points": [{
                    "id": point_id,
                    "smoothedLatitude": 50.4501,
                    "smoothedLongitude": 30.5234,
                    "smoothedAccuracyMeters": 5.0,
                    "deviceTimeMs": 1763500000000_i64,
                    "observedAtMs": 1763500000000_i64,
                    "source": "cooperative",
                    "isMock": false,
                    "flags": []
                }]
            }),
        )
        .await
        .unwrap();
    assert_eq!(ingest["acceptedIds"], json!([point_id]));
    assert_eq!(
        storage
            .stream_snapshot(&StoragePartition {
                brand_id: BRAND.into(),
                driver_id: DRIVER.into(),
            })
            .len(),
        1
    );

    let coop_params = json!({
        "sessionId": SESSION, "driverId": DRIVER, "brandId": BRAND,
        "captureTimestampMs": 1763500000123_u64,
        "access_token": tokens_with_coop.access_token,
        "state": {
            "latitude": 50.45, "longitude": 30.52, "accuracyMeters": 4.0,
            "speedMps": 0.0, "bearingDegrees": 90.0, "trustScore": 0.8, "vehicleClass": "car", "flags": []
        },
        "observedPeers": [{
            "observationId": "view-1", "vehicleClass": "bus", "distanceMeters": 22.0,
            "azimuthDegrees": 90.0, "visionVarianceMeters2": 4.0
        }]
    });
    let disabled = client
        .request::<_, Value>(COOP_SYNC_PERMISSION, coop_params.clone())
        .await
        .unwrap_err();
    assert!(
        disabled
            .to_string()
            .contains("stage 2 cooperative positioning is disabled"),
        "{disabled}"
    );
    plugin.section(2, true).await.unwrap();
    let mut no_scope = coop_params.clone();
    no_scope["access_token"] = json!(tokens.access_token);
    assert!(
        client
            .request::<_, Value>(COOP_SYNC_PERMISSION, no_scope)
            .await
            .is_err()
    );
    let mut wrong_brand = coop_params.clone();
    wrong_brand["brandId"] = json!("foreign-brand");
    let unauthorized = client
        .request::<_, Value>(COOP_SYNC_PERMISSION, wrong_brand)
        .await
        .unwrap_err();
    assert!(
        unauthorized
            .to_string()
            .contains("Not authorized for this session"),
        "{unauthorized}"
    );
    let mut wrong_subject = coop_params.clone();
    wrong_subject["access_token"] = json!(donor_token);
    assert!(
        client
            .request::<_, Value>(COOP_SYNC_PERMISSION, wrong_subject)
            .await
            .unwrap_err()
            .to_string()
            .contains("Not authorized for this session")
    );
    let mut donor_params = coop_params.clone();
    donor_params["access_token"] = json!(donor_token);
    donor_params["driverId"] = json!("donor");
    donor_params["sessionId"] = json!("donor-session");
    donor_params["state"]["longitude"] = json!(30.52031);
    donor_params["state"]["vehicleClass"] = json!("bus");
    donor_params["observedPeers"] = json!([]);
    client
        .request::<_, Value>(COOP_SYNC_PERMISSION, donor_params)
        .await
        .unwrap();
    let matches: Value = client
        .request(COOP_SYNC_PERMISSION, coop_params.clone())
        .await
        .unwrap();
    assert_eq!(matches["matchedDonors"][0]["observationId"], "view-1");
    assert_eq!(matches["matchedDonors"][0]["vehicleClass"], "bus");
    assert_eq!(
        matches["matchedDonors"][0]["captureTimestampMs"],
        matches["captureTimestampMs"]
    );
    assert!(serde_json::to_vec(&matches).unwrap().len() < 1200);
    plugin.section(1, false).await.unwrap();
    assert!(
        client
            .request::<_, Value>(COOP_SYNC_PERMISSION, coop_params)
            .await
            .unwrap_err()
            .to_string()
            .contains("stage 2 cooperative positioning is disabled")
    );
    let disabled_ingest = client
        .request::<_, Value>(
            "tracking.ingest",
            json!({
                "sessionId": SESSION, "driverId": DRIVER, "brandId": BRAND,
                "access_token": tokens_with_coop.access_token,
                "points": [{
                    "id": "1123456789abcdef0123456789abcdef", "smoothedLatitude": 50.4501,
                    "smoothedLongitude": 30.5234, "smoothedAccuracyMeters": 5.0,
                    "deviceTimeMs": 1763500000000_i64, "observedAtMs": 1763500000000_i64,
                    "source": "fused", "isMock": false, "flags": []
                }]
            }),
        )
        .await
        .unwrap_err();
    assert!(
        disabled_ingest
            .to_string()
            .contains("stage 1 ingest is disabled")
    );
    let still_time: Value = client
        .request(
            "serverTime",
            json!({
                "sessionId": SESSION, "access_token": tokens_with_coop.access_token
            }),
        )
        .await
        .unwrap();
    assert_eq!(still_time["serverTimeMs"], time["serverTimeMs"]);
    client.close().await.unwrap();

    let health_response = http_get(health_address, "/healthz").await;
    assert!(
        health_response.starts_with("HTTP/1.1 200"),
        "{health_response}"
    );
    let metrics_json = http_get(health_address, "/metrics.json").await;
    assert!(metrics_json.starts_with("HTTP/1.1 200"), "{metrics_json}");
    let body = metrics_json.split_once("\r\n\r\n").unwrap().1;
    let metrics: Value = serde_json::from_str(body).unwrap();
    assert!(
        metrics["server"]["rpc_calls_total"]
            .as_u64()
            .unwrap_or_default()
            >= 2,
        "{metrics}"
    );
    let prometheus = http_get(health_address, "/metrics").await;
    assert!(prometheus.starts_with("HTTP/1.1 200"), "{prometheus}");
    assert!(
        prometheus.contains("json_rpc_2_server_rpc_calls_total"),
        "{prometheus}"
    );

    health_task.abort();
    running.close().await.unwrap();
}

fn client_endpoint(address: SocketAddr, certificate: &CertificateDer<'static>) -> quinn::Endpoint {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate.clone()).unwrap();
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![QUIC_ALPN.to_vec()];
    let mut endpoint = quinn::Endpoint::client(SocketAddr::new(address.ip(), 0)).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    )));
    endpoint
}

async fn http_get(address: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}
