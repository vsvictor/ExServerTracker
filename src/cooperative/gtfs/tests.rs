use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn entity(id: &str, route: &str, timestamp: u64) -> FeedEntity {
    FeedEntity {
        deleted: None,
        vehicle: Some(VehiclePosition {
            trip: Some(TripDescriptor {
                route_id: Some(route.into()),
            }),
            position: Some(Position {
                latitude: Some(50.45),
                longitude: Some(30.52),
                bearing: Some(90.0),
                speed: Some(5.0),
            }),
            timestamp: Some(timestamp),
            descriptor: Some(VehicleDescriptor {
                id: Some(id.into()),
            }),
        }),
    }
}

fn config(url: String) -> GtfsConfig {
    GtfsConfig {
        url,
        brand_id: "brand".into(),
        accuracy_meters: 4.0,
        trust_score: 0.9,
        route_classes: HashMap::from([
            ("bus".into(), VehicleClass::Bus),
            ("wire".into(), VehicleClass::Trolleybus),
        ]),
    }
}

async fn serve(status: &str, body: Vec<u8>, length: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let status = status.to_owned();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 4096];
        let _bytes = socket.read(&mut request).await.unwrap();
        socket
            .write_all(
                format!(
                    "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {length}\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(&body).await.unwrap();
    });
    format!("http://{addr}/vehicle-positions")
}

#[tokio::test]
async fn retrieves_real_protobuf_and_filters_unmapped_stale_future_or_duplicate_vehicles() {
    let body = FeedMessage {
        header: Some(FeedHeader {
            version: "2.0".into(),
            incrementality: Some(0),
            timestamp: Some(100),
        }),
        entity: vec![
            entity("a", "bus", 100),
            entity("b", "wire", 99),
            entity("stale", "bus", 98),
            entity("future", "bus", 101),
            entity("wrong-route", "train", 100),
            entity("dupe", "bus", 100),
            entity("dupe", "wire", 99),
        ],
    }
    .encode_to_vec();
    let len = body.len();
    let source = GtfsRealtimeSource::new(config(serve("200 OK", body, len).await)).unwrap();
    let mut fixes = source.fetch(100_100, 2_000).await.unwrap();
    fixes.sort_by(|a, b| a.vehicle_id.cmp(&b.vehicle_id));
    assert_eq!(fixes.len(), 2);
    assert_eq!(fixes[0].state.vehicle_class, VehicleClass::Bus);
    assert_eq!(fixes[1].state.vehicle_class, VehicleClass::Trolleybus);
    assert_eq!(fixes[1].capture_ms, 99_000);
    assert_eq!(fixes[1].state.trust_score, 0.9);
}

#[tokio::test]
async fn fetch_errors_are_sanitized_and_reject_invalid_or_oversized_bodies() {
    for (status, body, len, expected) in [
        ("503 Unavailable", vec![], 0, "GTFS request failed"),
        (
            "200 OK",
            vec![],
            MAX_BODY_BYTES + 1,
            "GTFS body exceeds the allowed size",
        ),
        (
            "200 OK",
            vec![255],
            1,
            "GTFS feed is malformed, unsupported or stale",
        ),
    ] {
        let url = format!("{}?key=fixture-secret", serve(status, body, len).await);
        let source = GtfsRealtimeSource::new(config(url)).unwrap();
        assert_eq!(
            source.fetch(1000, 2000).await.err().unwrap().to_string(),
            expected
        );
    }
    assert!(matches!(
        GtfsRealtimeSource::new(config("http://example.test/gtfs".into())),
        Err(GtfsError::Url)
    ));
    assert!(matches!(
        GtfsRealtimeSource::new(config("https://user:pass@example.test/gtfs".into())),
        Err(GtfsError::Url)
    ));
}
