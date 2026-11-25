use super::*;
use crate::{InMemoryCoopNavStorage, StoragePartition};
use std::time::Duration;

struct Sessions;

#[async_trait]
impl HostSessionAuthorizer for Sessions {
    async fn resolve(
        &self,
        session_id: &str,
    ) -> Result<ConfirmedSession, SessionAuthorizationError> {
        if session_id != "valid-session" {
            return Err(SessionAuthorizationError::Denied);
        }
        ConfirmedSession::new(session_id, "driver", "brand")
    }
}

async fn call(rpc: &RpcServer, method: &str, params: Value) -> Value {
    let request = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
    serde_json::from_slice(
        &rpc.handle_bytes(&serde_json::to_vec(&request).unwrap())
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn uses_host_session_and_derives_driver_identity() {
    let storage = Arc::new(
        InMemoryCoopNavStorage::new(Arc::new(SystemClock), Duration::from_secs(60)).unwrap(),
    );
    let plugin = HostCoopNavPlugin::new(Arc::new(Sessions), storage.clone());
    plugin.section(1, true).await.unwrap();
    let server = RpcServer::new();
    plugin.install_methods(&server).unwrap();
    let point = json!({"id":"0123456789abcdef0123456789abcdef","smoothedLatitude":50.45,"smoothedLongitude":30.52,"smoothedAccuracyMeters":5.0,"deviceTimeMs":1763500000000_i64,"observedAtMs":1763500000000_i64,"source":"fused","isMock":false,"flags":[]});
    let result = call(
        &server,
        "tracking.ingest",
        json!({"sessionId":"valid-session","points":[point.clone()]}),
    )
    .await;
    assert!(result.get("result").is_some(), "{result}");
    assert_eq!(
        storage
            .stream_snapshot(&StoragePartition {
                brand_id: "brand".into(),
                driver_id: "driver".into()
            })
            .len(),
        1
    );
    for params in [
        json!({"sessionId":"wrong","points":[point.clone()]}),
        json!({"sessionId":"valid-session","brandId":"other","points":[point.clone()]}),
        json!({"sessionId":"valid-session","driverId":"other","points":[point.clone()]}),
        json!({"points":[point.clone()]}),
    ] {
        assert!(
            call(&server, "tracking.ingest", params)
                .await
                .get("error")
                .is_some()
        );
    }
    assert!(
        call(&server, "serverTime", json!({"sessionId":"valid-session"}))
            .await
            .get("result")
            .is_some()
    );
    assert!(
        call(&server, "serverTime", json!({"sessionId":"wrong"}))
            .await
            .get("error")
            .is_some()
    );
}
