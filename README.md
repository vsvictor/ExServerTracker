# CoopNav Rust backend — stages 1 and 2

Independent Rust plugin project, version `1.0.0`, implementing the Phase 1
single-vehicle tracking ingest, server-clock synchronization, and opt-in
cooperative positioning. Stage 3 (MoQ/video offload) is forbidden and not implemented.

## Wire contract

The plugin registers and documents these protected RPC methods:

| Method | Required PASETO permission | Params | Result |
| --- | --- | --- | --- |
| `tracking.ingest` | `tracking.ingest` | `{sessionId,driverId,brandId,points}` | `{acceptedIds:[...]}` |
| `serverTime` | `tracking.server_time` | `{sessionId}` | `{serverTimeMs}` |
| `coop.sync_peers` | `coop.sync_peers` | `{sessionId,driverId,brandId,captureTimestampMs,state,observedPeers}` | `{captureTimestampMs,matchedDonors}` |

Authentication is the JSON-RPC `params.access_token` or
`params.authorization: "Bearer …"` field. `AuthPlugin` validates the
PASETO v4.local access token and removes that field before business params are
decoded. Both auth forms are advertised in `RpcServer::document_method` /
`rpc.list_methods` schemas. All methods require authentication and their
individual permission; there is no anonymous or default-allow mode.

The ingest `points` payload matches the KMP `RpcTrackingTransport` contract:

```json
{
  "id": "0123456789abcdef0123456789abcdef",
  "rawLatitude": 50.45,
  "rawLongitude": 30.52,
  "rawAccuracyMeters": 5.0,
  "smoothedLatitude": 50.4501,
  "smoothedLongitude": 30.5201,
  "smoothedAccuracyMeters": 4.0,
  "speedMps": 10.0,
  "bearingDegrees": 180.0,
  "deviceTimeMs": 1763500000000,
  "observedAtMs": 1763500000000,
  "source": "fused",
  "isMock": false,
  "flags": ["interference", "SUSPECTED_INTERFERENCE", "SUSPECTED_SPOOFING"]
}
```

`raw*`, `speedMps`, and `bearingDegrees` may be omitted. Raw latitude,
longitude, and accuracy must be present together. Coordinates and accuracies
must be finite and in their physical ranges; accuracies have no arbitrary
upper limit, so long stationary anchors remain representable. Speed is bounded
to 220 km/h. Network points cannot report speed/course; dead-reckoning points
cannot carry raw coordinates. Stationary points can include the last accepted raw
fix and its original observation time. Timestamps must be positive
integer Unix milliseconds. Old/offline points and out-of-order device-clock
timestamps are valid; the server does not sort or enforce freshness. IDs are
32 hexadecimal characters and request order is preserved. Batches must contain
1–100 points and are fully validated before storage begins.
`source: "cooperative"` is accepted as tracking history; ingest never
populates the cooperative donor index.

The server does not choose how often a vehicle reports. It stores every accepted
point and enforces no minimum spacing. ExKMPTracker 1.1.0 decides that before
enqueue: the 25 m / 10° / 30 s gates are the reference at 8 m/s, the schedule is
sparser at low speed and denser at high speed, and turns, departure, a 5 m/s
speed change, GNSS recovery and a large accuracy change record immediately.
Ingest is attempted about every 30 s while slow and every 5 s at highway speed.
A fast vehicle therefore arrives as a small batch, not as one point per GNSS fix.
The 100-point cap still applies when a phone has been offline.

The server resolves `sessionId` through an injected `SessionAuthorizer` using
the verified PASETO subject, then requires its returned confirmed
`sessionId`/`driverId`/`brandId` tuple to exactly match the request. Client
identity fields are never trusted on their own. `serverTime` also requires a
valid principal-to-session grant.

## Stage 2 configuration and wire

The host controls stages through `plugin.section(stage, enabled).await` or
`plugin.sections().section(stage, enabled).await`. Defaults: `1=true`,
`2=false`, `3=false`. Stage 1 off blocks ingest but leaves authenticated
`serverTime` available. Turning it off disables stage 2, cancels its fetches,
and clears the old generation's peers. Turning stage 1 back on does not
re-enable stage 2. Enabling stage 2 requires stage 1 and injected cooperative
storage, but not a GTFS feed. Enabling stage 3 returns an explicit error.
These controls are not RPC methods.

For the QUIC example below, grant `coop.sync_peers` to appropriate roles and
set `COOPNAV_STAGE2=true`. Leave `COOPNAV_GTFS_URL` unset for V2V-only mode.
To also enable GTFS, add the following settings with the real URL and route IDs:

```sh
export COOPNAV_STAGE2=true
export COOPNAV_GTFS_URL='https://transit.example/vehicle-positions.pb'
export COOPNAV_GTFS_BRAND_ID='confirmed-brand-id'
export COOPNAV_GTFS_ROUTES='bus:route-101,trolleybus:route-5'
export COOPNAV_GTFS_ACCURACY_METERS=5
export COOPNAV_GTFS_TRUST_SCORE=0.85
```

In custom hosts, use `CooperativeConfig::new(storage, None, freshness, poll_interval)`
for V2V only (the polling interval is ignored). To include GTFS, pass
`Some(Arc::new(GtfsRealtimeSource::new(gtfs_config)?))` instead of `None`.
Use `RedisCooperativeStorage` (standalone Redis 6.2+) or
`InMemoryCooperativeStorage` for development; inject the config with
`CoopNavPlugin::with_cooperative(...)` before installing the plugin.
`sections().gtfs_status().await` is host-only and serializes to `gtfs-unconfigured`,
`gtfs-disabled`, or `gtfs-enabled` (enablement, not feed reachability).
Unconfigured GTFS emits a warning on stage 2 enable and registers no poll task.
`BackendBuilder` schedules the maintenance
returned by `ServerPlugin::install`; custom hosts must schedule that handle
or `sections().maintenance_handle()`. On shutdown, await `section(2, false)`
to cancel and drain the current fetch and clear its index, then stop and
join the scheduler. Maintenance failures are returned to the scheduler.
No feed is selected by default; a configured but disabled stage never fetches.
Camera model and calibration are client-side inputs.

The feed must be a GTFS-Realtime full `VehiclePositions` protobuf over HTTPS
(no redirects, 3-second timeout, 2 MiB response limit), with a fresh header
and per-vehicle timestamp, a stable vehicle ID, mapped `trip.route_id`, and
position/speed/bearing (a stopped vehicle may omit bearing). The example polls
every 500 ms and only uses captures less than 2 s old, so typical 10–30 s feeds
will not make reliable donors. Failures do not refresh positions; prior fixes
age out. The host sets each feed's brand, 68% accuracy radius, and trust score.

| Field | Contract |
| --- | --- |
| `state` | `{latitude,longitude,accuracyMeters,speedMps,bearingDegrees,trustScore,vehicleClass,flags}`. Accuracy is a 68% radius. Latitude must be within ±85°, inside Redis GEO bounds. |
| `observedPeers` | Up to 8 objects `{observationId,vehicleClass,distanceMeters,azimuthDegrees,relativeSpeedMps?,visionVarianceMeters2}`. IDs are unique, nonblank strings of at most 64 UTF-16 code units. Distance is horizontal (0.1–200 m); azimuth is **absolute true north** clockwise `[0,360)`, aligned by calibrated camera extrinsics and heading. Vision variance is **per axis** (0.01–10,000 m²) and includes angular, range, and calibration uncertainty. `relativeSpeedMps` is radial range-rate (positive when receding), may be null or omitted, and is not used for matching. |
| `matchedDonors` | Up to 3 objects `{observationId,donorId,latitude,longitude,accuracyMeters,speedMps,bearingDegrees,trustScore,vehicleClass,captureTimestampMs}`. Coordinates are projected to the exact echoed request capture time; the returned 68% radius includes allowance for motion and clock-alignment error. |
| `vehicleClass` | `car`, `truck`, `bus`, or `trolleybus`, matched exactly. |

Use secure QUIC RPC (nominally 10 Hz). No plaintext RUDP channel is opened
or accepted for this RPC. A response is limited to three donors; it is not
a UDP datagram and does not have a 1,200-byte transport guarantee. Capture
times use the authenticated server-clock reference. Future, stale, and
non-increasing captures are rejected. The server indexes eligible caller
states by brand, searches within 200 m using Redis `GEOSEARCH`, then reads
candidate states in one `MGET`. A candidate must be unambiguous in both
directions within 12 m. Dense candidate overflow returns no donors.
Each plugin instance has its own generation; route all peers of a brand to
one cooperative instance. A shared Redis endpoint does not itself merge
separate plugin generations.

Client donor eligibility requires confirmed session ownership, accuracy of
at most 12 m, trust at least 0.7, and no flags except
`gnssTimeUnverified` (also `gnss_time_unverified`). A newer ineligible fix
withdraws the previous donor. The server floors client-reported accuracy at
3 m and clamps trust to at most 0.8 (0.7 when accuracy exceeds 6 m). Donor
IDs are opaque and change with the generation. Self/subject aliases are
excluded. Donor trust and flags do not gate the requester: a fresh valid
request with trust zero may still match from its supplied query origin.
Revocation prevents new authenticated updates; an already accepted sample
can remain eligible only until its capture reaches the freshness limit
(at most 2 s). **This is not GNSS attestation**: an authorized client can still
misreport position, flags, or camera observations. Restrict permissions and
select feed operators accordingly. The 5 ms Redis, 4G RTT, and 3–5 m
positioning figures are requirements, not benchmark results.

## Host integration and grants

`HostCoopNavPlugin` provides an alternative for hosts that already issue session
credentials. Inject a `HostSessionAuthorizer` that validates expiry and revocation
and resolves the server-side identity. Requests supply the existing `sessionId`;
optional `driverId` and `brandId` must match that identity. The session ID is a
bearer credential and must be protected in transit. No OAuth token or grant is
required in this mode. Stage 2 still requires QUIC.

`CoopNavPlugin<A>` is a normal `ServerPlugin`. The host supplies its
`AuthPlugin<A>`, `SessionAuthorizer`, and `CoopNavStorage`; all are injected,
with no singleton or permissive production default. `InMemorySessionAuthorizer`
is an explicit development/test resolver. Production hosts can use
`RedisSessionAuthorizer`, and provision/revoke confirmed grants through its
host-only methods. Each grant binds the PASETO subject to the confirmed
session/driver/brand tuple and has a host-selected TTL. No grant-management RPC
is exposed.

The `backend` example uses `BackendBuilder`, Redis-backed PASETO auth, Redis
session grants, and Redis track storage:

```sh
PASETO_LOCAL_KEY='k4.local.<PASERK-encoded-v4-local-key>' \
AUTH_REDIS_URL='redis://127.0.0.1:6379/' \
TRACKING_REDIS_URL='redis://127.0.0.1:6379/' \
BACKEND_TLS_CERT_FILE='/path/to/server-cert.pem' \
BACKEND_TLS_KEY_FILE='/path/to/server-key.pem' \
cargo run --features server-example --example backend
```

`BackendBuilder::from_env` reads `BACKEND_*` process variables only (it does
not load `.env`). Its pinned API requires TCP, UDP, TLS, and QUIC listeners and
requires a TLS identity for TLS/QUIC. Keep the default bind on loopback for
local use. In deployment, expose encrypted TLS/QUIC listeners only and firewall
the plaintext TCP/UDP listener; do not send bearer tokens over plaintext.
Provision account roles/permissions and confirmed session grants from trusted
host-side administration before ingest is allowed. The example deliberately
does not create demo users, grant broad permissions, or provision sessions.

For a host that must expose only protected QUIC RPC and a separate HTTP/1
health listener, use the actual `MultiTransportServer` example instead:

```sh
PASETO_LOCAL_KEY='k4.local.<PASERK-encoded-v4-local-key>' \
AUTH_REDIS_URL='redis://127.0.0.1:6379/' \
TRACKING_REDIS_URL='redis://127.0.0.1:6379/' \
COOPNAV_TLS_CERT_FILE='/path/to/server-cert.pem' \
COOPNAV_TLS_KEY_FILE='/path/to/server-key.pem' \
COOPNAV_BIND_IP='0.0.0.0' \
COOPNAV_QUIC_PORT='4433' \
COOPNAV_HEALTH_ADDR='127.0.0.1:9000' \
cargo run --no-default-features --features multi-transport-example,redis-storage \
  --example multi_transport
```

This host creates one `RpcServer`, installs the protected plugin, starts its
QUIC listener with the supplied TLS identity, obtains
`RunningServer::health_handle(Arc<RpcServer>)`, and serves the real runtime
health router on its host-provided HTTP listener. The health listener defaults
to loopback because these unauthenticated endpoints can disclose operational
metrics; expose it only behind an appropriately restricted network boundary.
The host registers no plaintext RPC listeners and does not hardcode secrets.
The existing `backend` example remains available, but the pinned
`BackendBuilder` internally enables runtime defaults (including plaintext TCP
and UDP) and does not expose its internal `RunningServer` for health-router
attachment.

The client must only remove queued FIFO points whose IDs are in a successful
`acceptedIds` response. A storage error returns a JSON-RPC error rather than
acknowledging a point. Already persisted prefix points are safe to retry.

## Storage and durability

`InMemoryCoopNavStorage` is non-durable and intended only for development/tests.
It uses DashMap partition lookup, per-partition synchronization, and an
injectable Unix-millisecond clock for deterministic dedup-expiry tests.

`RedisCoopNavStorage` partitions streams by SHA-256 of `(brandId,driverId)`.
Each point is appended to a Redis Stream and deduplicated atomically by its ID.
Same-ID/same-payload retries are acknowledged idempotently; same-ID/different-
payload writes fail without replacing the original. The default example dedup
TTL is seven days; after expiry an old ID may be appended again. Streams are
not trimmed automatically, so retention/archival is an explicit operator
policy and trimming must not discard unconsumed tracking data.

Redis accepting an in-memory write is not itself a durable-storage guarantee.
Production must configure and monitor Redis persistence, disk capacity, and
failover to match the required RPO (for the strongest local acknowledgment
semantics, enable AOF with `appendfsync always`). Redis write/script errors are
returned to the RPC caller and never produce `acceptedIds`. Redis Lua checks
both key types before the ordered `XADD` then `SET NX EX` mutations; a Redis
runtime/disk error after `XADD` can leave a stream entry without its dedup key,
so storage errors are not acknowledged and operators must monitor persistence
health. No hidden stream trimming or data-loss policy is applied.

## Metrics and health

The pinned runtime health router exposes `/healthz`, `/metrics.json`, and
Prometheus `/metrics`. The host example above wires all three to a real
listener. The library also exposes `serve_health_http1(listener, health_handle)`
and `serve_running_health_http1(listener, &running_server, rpc_server)` for
hosts that already own the runtime server. HTTP/1 support is behind the
`metrics-http1` Cargo feature; it is not enabled by a no-default-features
library build.

## Build and verification

```sh
cargo fmt --all -- --check
cargo test --all-targets --all-features
cargo test --no-default-features --lib
cargo clippy --all-targets --all-features -- -D warnings
python3 tools/check-architecture.py
sh tools/check-features.sh
```

If `redis-server` is installed, the Redis storage test starts an isolated
loopback-only server on an OS-selected ephemeral port, and kills only the
child process it started. Without it, that specific integration check reports
a skip; all deterministic memory/auth/RPC tests still run.

The host transport integration test binds loopback QUIC and HTTP listeners to
ephemeral ports, exercises all three authenticated methods over QUIC, and
fetches each health endpoint over HTTP/1.

All JSON-RPC crates are pinned to JSON-RPC-2 revision
`af8d04dbbaee05fa1514caa07a37808d5973d100`; OAuth2 is pinned to
`1b0043f27cc445e9d238a643ab32fd45e143ea6e`. The Cargo patch entries for
`h3`/`h3-quinn` match the framework ecosystem.
