use super::CoopNavPlugin;
use crate::{
    ConfirmedSession, InMemoryCoopNavStorage, InMemorySessionAuthorizer, ManualClock,
    SessionAuthorizer,
};
use json_rpc_2_oauth2::{
    AuthPlugin, JsonRpcAuthServer, LoginRequest, RegisterRequest, RpcServer, SqliteAuthServer,
    TokenConfig,
};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};

const TEST_KEY: &str = "k4.local.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const SESSION: &str = "confirmed-session-123";
const DRIVER: &str = "driver-123";
const BRAND: &str = "brand-123";

async fn setup() -> (
    RpcServer,
    String,
    Arc<InMemoryCoopNavStorage>,
    Arc<InMemorySessionAuthorizer>,
) {
    let mut identity = SqliteAuthServer::new(TEST_KEY, TokenConfig::new("coopnav-tests")).unwrap();
    identity.set_db_host(":memory:");
    identity.initialize_storage().await.unwrap();
    let user = identity
        .handle_register(RegisterRequest {
            email: "driver@example.test".into(),
            password: "test-password-123".into(),
            profile: HashMap::new(),
        })
        .await
        .unwrap();
    identity
        .handle_add_role(user.id.clone(), "tracker".into())
        .await
        .unwrap();
    for permission in ["tracking.ingest", "tracking.server_time"] {
        identity
            .handle_add_permission("tracker".into(), permission.into())
            .await
            .unwrap();
    }
    let tokens = identity
        .handle_login(LoginRequest {
            email: "driver@example.test".into(),
            password: "test-password-123".into(),
            grant_type: "password".into(),
        })
        .await
        .unwrap();

    let session_authorizer = Arc::new(InMemorySessionAuthorizer::new());
    session_authorizer
        .provision_confirmed_session(
            user.id,
            ConfirmedSession::new(SESSION, DRIVER, BRAND).unwrap(),
        )
        .unwrap();
    let clock = Arc::new(ManualClock::new(1_763_500_000_123));
    let storage =
        Arc::new(InMemoryCoopNavStorage::new(clock.clone(), Duration::from_secs(60)).unwrap());
    let server = RpcServer::new();
    let plugin = CoopNavPlugin::with_clock(
        AuthPlugin::new(identity),
        session_authorizer.clone() as Arc<dyn SessionAuthorizer>,
        storage.clone(),
        clock,
    );
    plugin.install_methods(&server).unwrap();
    (server, tokens.access_token, storage, session_authorizer)
}

fn point(id: &str, latitude: f64) -> Value {
    json!({
        "id": id,
        "rawLatitude": 50.45,
        "rawLongitude": 30.52,
        "rawAccuracyMeters": 5.0,
        "smoothedLatitude": latitude,
        "smoothedLongitude": 30.5201,
        "smoothedAccuracyMeters": 4.0,
        "speedMps": 10.0,
        "bearingDegrees": 180.0,
        "deviceTimeMs": 1763500000000_i64,
        "observedAtMs": 1763500000000_i64,
        "source": "fused",
        "isMock": false,
        "flags": ["interference", "SUSPECTED_INTERFERENCE", "SUSPECTED_SPOOFING"]
    })
}

async fn call(server: &RpcServer, method: &str, params: Value) -> Value {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    });
    let bytes = server
        .handle_bytes(&serde_json::to_vec(&request).unwrap())
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn auth_params(token: &str, mut business: Value) -> Value {
    business
        .as_object_mut()
        .unwrap()
        .insert("access_token".to_owned(), json!(token));
    business
}

fn ingest_params(token: &str, points: Vec<Value>) -> Value {
    auth_params(
        token,
        json!({
            "sessionId": SESSION,
            "driverId": DRIVER,
            "brandId": BRAND,
            "points": points
        }),
    )
}

#[tokio::test]
async fn ingest_authenticates_checks_session_grant_persists_and_retries_idempotently() {
    let (server, token, storage, _) = setup().await;
    let partition = crate::StoragePartition {
        brand_id: BRAND.into(),
        driver_id: DRIVER.into(),
    };
    let payload = point("0123456789abcdef0123456789abcdef", 50.4501);
    let first = call(
        &server,
        "tracking.ingest",
        ingest_params(&token, vec![payload.clone()]),
    )
    .await;
    assert_eq!(
        first["result"]["acceptedIds"],
        json!(["0123456789abcdef0123456789abcdef"])
    );
    assert_eq!(storage.stream_snapshot(&partition).len(), 1);

    let retry = call(
        &server,
        "tracking.ingest",
        ingest_params(&token, vec![payload]),
    )
    .await;
    assert_eq!(
        retry["result"]["acceptedIds"],
        json!(["0123456789abcdef0123456789abcdef"])
    );
    assert_eq!(storage.stream_snapshot(&partition).len(), 1);

    let conflict = call(
        &server,
        "tracking.ingest",
        ingest_params(
            &token,
            vec![point("0123456789abcdef0123456789abcdef", 50.451)],
        ),
    )
    .await;
    assert!(conflict.get("error").is_some());
    assert_eq!(storage.stream_snapshot(&partition).len(), 1);
}

#[tokio::test]
async fn ingest_rejects_bad_late_point_without_storing_anything() {
    let (server, token, storage, _) = setup().await;
    let partition = crate::StoragePartition {
        brand_id: BRAND.into(),
        driver_id: DRIVER.into(),
    };
    let mut bad = point("1123456789abcdef0123456789abcdef", 50.45);
    bad["smoothedLatitude"] = json!(91.0);
    let response = call(
        &server,
        "tracking.ingest",
        ingest_params(
            &token,
            vec![point("0123456789abcdef0123456789abcdef", 50.45), bad],
        ),
    )
    .await;
    assert_eq!(response["error"]["code"], -32602);
    assert!(storage.stream_snapshot(&partition).is_empty());
}

#[tokio::test]
async fn client_identity_cannot_override_the_confirmed_session_grant() {
    let (server, token, storage, _) = setup().await;
    let mut params = ingest_params(
        &token,
        vec![point("0123456789abcdef0123456789abcdef", 50.45)],
    );
    params["driverId"] = json!("attacker-selected-driver");
    let response = call(&server, "tracking.ingest", params).await;
    assert_eq!(response["error"]["code"], -32011);
    assert!(
        storage
            .stream_snapshot(&crate::StoragePartition {
                brand_id: BRAND.into(),
                driver_id: DRIVER.into()
            })
            .is_empty()
    );
}

#[tokio::test]
async fn server_time_is_session_authorized_and_method_schemas_include_auth_fields() {
    let (server, token, _, _) = setup().await;
    let response = call(
        &server,
        "serverTime",
        auth_params(&token, json!({"sessionId": SESSION})),
    )
    .await;
    assert_eq!(response["result"]["serverTimeMs"], 1_763_500_000_123_u64);

    let methods = call(&server, "rpc.list_methods", json!({"describe": true})).await;
    let descriptions = methods["result"].as_array().unwrap();
    for method in ["tracking.ingest", "serverTime"] {
        let description = descriptions
            .iter()
            .find(|description| description["name"] == method)
            .unwrap();
        let properties = &description["params_schema"]["properties"];
        assert!(properties.get("access_token").is_some());
        assert!(properties.get("authorization").is_some());
    }
}

#[tokio::test]
async fn every_method_requires_authentication_and_its_own_scope() {
    let (server, _, _, _) = setup().await;
    let response = call(&server, "serverTime", json!({"sessionId": SESSION})).await;
    assert!(response.get("error").is_some());
}
