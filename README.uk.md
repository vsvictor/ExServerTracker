# Rust backend CoopNav — етапи 1 і 2

Окремий проєкт Rust-плагіна версії `1.0.0` для приймання точок
автономного трекінгу, синхронізації серверного часу та кооперативного
позиціювання. Етап 3 (MoQ/відео) заборонений і не реалізований.

## Контракт JSON-RPC

Плагін реєструє та документує такі захищені методи:

| Метод | Обов’язковий дозвіл PASETO | Параметри | Результат |
| --- | --- | --- | --- |
| `tracking.ingest` | `tracking.ingest` | `{sessionId,driverId,brandId,points}` | `{acceptedIds:[...]}` |
| `serverTime` | `tracking.server_time` | `{sessionId}` | `{serverTimeMs}` |
| `coop.sync_peers` | `coop.sync_peers` | `{sessionId,driverId,brandId,captureTimestampMs,state,observedPeers}` | `{captureTimestampMs,matchedDonors}` |

Автентифікація надходить у JSON-RPC `params.access_token` або
`params.authorization: "Bearer …"`. `AuthPlugin` перевіряє access token
PASETO v4.local і вилучає поле токена перед розбором бізнес-параметрів.
Обидва способи автентифікації відображені у схемах
`RpcServer::document_method` / `rpc.list_methods`. Анонімного або
дозвільного за замовчуванням режиму немає.

Формат `points` відповідає контракту KMP `RpcTrackingTransport`:

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

Поля `raw*`, `speedMps` і `bearingDegrees` необов’язкові. Широта, довгота
та точність сирих координат передаються разом або відсутні всі разом.
Координати й точності мають бути скінченними та в допустимих діапазонах;
для точності немає штучної верхньої межі, тому невизначеність якоря
стаціонарного авто може зростати. Швидкість обмежена 220 км/год. Точки
`network` не можуть містити швидкість/курс; `deadReckoning` і `stationary`
не містять сирих координат. Часові мітки — додатні цілі мілісекунди Unix.
Старі офлайн-точки та порушений порядок міток часу валідні: сервер їх не
сортує і не відкидає за давністю. Авторитетні — унікальні ID та порядок
запиту. Пакет містить 1–100 точок і повністю перевіряється до початку запису.
`source: "cooperative"` також приймається як історія трекінгу; ці точки
не потрапляють до індексу донорів.

Сервер не обирає, як часто авто звітує. Він зберігає кожну прийняту точку і не
вимагає мінімального інтервалу. ExKMPTracker 1.1.0 вирішує це до запису в чергу:
пороги 25 м / 10° / 30 с є еталоном при 8 м/с, на малій швидкості розклад рідший,
на великій — щільніший, а поворот, рушання, зміна швидкості на 5 м/с, повернення
GNSS і велика зміна точності пишуться одразу. Ingest пробується приблизно кожні
30 с на малій швидкості і кожні 5 с на трасі. Швидке авто тому приходить невеликим
пакетом, а не окремою точкою на кожен фікс GNSS. Межа 100 точок лишається для
телефона, який був офлайн.

Сервер шукає `sessionId` через ін’єктований `SessionAuthorizer`, використовуючи
підтверджений subject із PASETO, а тоді вимагає точного збігу повернених
`sessionId`/`driverId`/`brandId` із запитом. Ідентифікаторам клієнта самостійно
не довіряємо. Для `serverTime` також потрібен дозвіл principal-to-session.

## Налаштування й контракт етапу 2

Керування доступне тільки хосту: `plugin.section(stage, enabled).await` або
`plugin.sections().section(stage, enabled).await`. За замовчуванням
`1=true`, `2=false`, `3=false`. Вимкнення етапу 1 забороняє ingest, але не
автентифікований `serverTime`; також вимикає етап 2, скасовує запити до
фіда й очищує попереднє покоління донорів. Повторне ввімкнення етапу 1 не
вмикає етап 2. Для етапу 2 потрібні ввімкнений етап 1 та явно надане
кооперативне сховище. Джерело GTFS необов’язкове. Ввімкнення етапу 3 повертає помилку.

Для QUIC-прикладу нижче надайте ролям дозвіл `coop.sync_peers` і встановіть
`COOPNAV_STAGE2=true`. Для режиму лише V2V залиште `COOPNAV_GTFS_URL`
невстановленою. Щоб додати GTFS, задайте такі змінні з фактичною адресою й ID маршрутів:

```sh
export COOPNAV_STAGE2=true
export COOPNAV_GTFS_URL='https://transit.example/vehicle-positions.pb'
export COOPNAV_GTFS_BRAND_ID='confirmed-brand-id'
export COOPNAV_GTFS_ROUTES='bus:route-101,trolleybus:route-5'
export COOPNAV_GTFS_ACCURACY_METERS=5
export COOPNAV_GTFS_TRUST_SCORE=0.85
```

Для власного хоста використайте `CooperativeConfig::new(storage, None, freshness, poll_interval)`
для режиму лише V2V (інтервал опитування ігнорується). Для GTFS передайте
`Some(Arc::new(GtfsRealtimeSource::new(gtfs_config)?))` замість `None`.
Сховище: `RedisCooperativeStorage` (standalone Redis 6.2+) або
`InMemoryCooperativeStorage` для розробки. Передайте конфігурацію через
`CoopNavPlugin::with_cooperative(...)` перед встановленням плагіна.
`sections().gtfs_status().await` доступний лише хосту й серіалізується як
`gtfs-unconfigured`, `gtfs-disabled` або `gtfs-enabled`; це стан
ввімкнення, а не доступності джерела. Без GTFS ввімкнення етапу 2 видає
попередження й не реєструє задачі опитування. `BackendBuilder` запускає періодичні
задачі з `ServerPlugin::install`; інший хост має запланувати їх самостійно
(або через `sections().maintenance_handle()`). При завершенні дочекайтеся
`section(2, false)` для скасування запиту й очищення індексу, а потім
зупиніть планувальник і дочекайтеся його завершення. Помилки задач
повертаються планувальнику.
За замовчуванням джерело не вибирається; вимкнений етап не робить запитів.
Модель камери та калібрування надаються клієнту.

Потрібен GTFS-Realtime protobuf `VehiclePositions` у режимі повного знімка
через HTTPS (без редиректів, таймаут 3 с, до 2 МіБ). Потрібні свіжі мітки
часу заголовка й кожного ТЗ, сталий ID, налаштований `trip.route_id`,
координати, швидкість і курс (стоянка може не мати курсу). У прикладі
опитування кожні 500 мс і вік фікса менше 2 с: типовий фід з оновленням
раз на 10–30 с не дасть стабільних донорів. Після помилки старі фікси
старіють без поновлення. Бренд, 68%-й радіус і довіру фіда задає хост.

| Поле | Контракт |
| --- | --- |
| `state` | `{latitude,longitude,accuracyMeters,speedMps,bearingDegrees,trustScore,vehicleClass,flags}`. Точність — 68%-й радіус; широта ±85°, у межах Redis GEO. |
| `observedPeers` | До 8 об’єктів `{observationId,vehicleClass,distanceMeters,azimuthDegrees,relativeSpeedMps?,visionVarianceMeters2}`. ID унікальні, непорожні, до 64 кодових одиниць UTF-16. Дистанція горизонтальна, 0,1–200 м. Азимут від істинної півночі за годинниковою стрілкою `[0,360)`, з урахуванням калібрування камери та курсу. Дисперсія **на вісь** (0,01–10 000 м²), включає кутову, дальнісну й калібрувальну похибку. `relativeSpeedMps` — радіальна швидкість зміни дистанції (додатна при віддаленні); можна опустити або передати `null`, у зіставленні не використовується. |
| `matchedDonors` | До 3 об’єктів `{observationId,donorId,latitude,longitude,accuracyMeters,speedMps,bearingDegrees,trustScore,vehicleClass,captureTimestampMs}`. Координати приведені точно до мітки запиту; 68%-й радіус враховує похибку руху та синхронізації часу. |
| `vehicleClass` | `car`, `truck`, `bus`, `trolleybus`, зіставлення лише однакових класів. |

Використовуйте захищений QUIC RPC (цільова частота 10 Гц). Відкритий RUDP
не підтримується; обмеження трьох донорів не є гарантією UDP-розміру 1200
байтів. Час фікса прив’язаний до `serverTime`; майбутні, прострочені та
не зростаючі мітки відхиляються. Redis `GEOSEARCH` шукає в межах 200 м
одного бренду, а `MGET` читає стани одним пакетом. Пара має бути однозначна
в обох напрямках у межах 12 м. При переповненні кандидатів донори не повертаються.
Кожен екземпляр плагіна має власне покоління: спрямовуйте всі запити одного
бренду до одного кооперативного екземпляра. Спільний Redis не об’єднує покоління.

Донор із клієнта потребує підтвердженої сесії, точності до 12 м, довіри
від 0,7 й відсутності прапорців, крім `gnssTimeUnverified` (або
`gnss_time_unverified`). Новий ненадійний стан вилучає донора. Сервер
обмежує точність знизу 3 м, довіру зверху 0,8 (0,7 при точності понад 6 м).
ID непрозорі й змінюються з поколінням; власні стани й псевдоніми того
самого principal не зіставляються. Свіжий валідний запит із нульовою
довірою може шукати донорів зі своїх координат. Відкликання доступу
блокує нові оновлення; уже прийнятий фікс діє лише до закінчення строку
свіжості (не більше 2 с). **Це не атестація GNSS**: навіть
авторизований клієнт може підмінити координати, прапорці або спостереження.
Показники 5 мс Redis, RTT 4G та похибка 3–5 м не підтверджені вимірюваннями.

## Інтеграція та дозволи сесій

`CoopNavPlugin<A>` реалізує `ServerPlugin`. Хост ін’єктує `AuthPlugin<A>`,
`SessionAuthorizer` і `CoopNavStorage`; синглтонів чи permissive production
default немає. `InMemorySessionAuthorizer` призначений для локальної
розробки/тестів. Для production доступний `RedisSessionAuthorizer` із
host-only методами створення та відкликання підтверджених дозволів. Дозвіл
зв’язує subject PASETO з підтвердженим кортежем session/driver/brand та має
заданий хостом TTL. RPC для керування дозволами немає.

Приклад `backend` використовує `BackendBuilder`, Redis PASETO auth, Redis
дозволи сесій і Redis-сховище точок:

```sh
PASETO_LOCAL_KEY='k4.local.<PASERK-ключ-v4.local>' \
AUTH_REDIS_URL='redis://127.0.0.1:6379/' \
TRACKING_REDIS_URL='redis://127.0.0.1:6379/' \
BACKEND_TLS_CERT_FILE='/path/to/server-cert.pem' \
BACKEND_TLS_KEY_FILE='/path/to/server-key.pem' \
cargo run --features server-example --example backend
```

`BackendBuilder::from_env` читає лише змінні процесу `BACKEND_*` і не
завантажує `.env`. Закріплений API обов’язково запускає TCP, UDP, TLS і QUIC
та вимагає TLS identity для TLS/QUIC. Для локального запуску залишайте bind
за замовчуванням на loopback. У production відкривайте лише зашифровані
TLS/QUIC listener-и, а plaintext TCP/UDP блокуйте firewall-ом. Не передавайте
bearer-токени відкритим транспортом. Ролі/дозволи облікового запису й
підтверджені дозволи сесій необхідно створювати надійною адміністративною
частиною до ingest. Приклад навмисно не створює demo-користувачів,
широких дозволів чи дозволів сесій.

Для хоста, якому потрібні лише захищений QUIC RPC і окремий HTTP/1 listener
health, використовуйте приклад на реальному `MultiTransportServer`:

```sh
PASETO_LOCAL_KEY='k4.local.<PASERK-ключ-v4.local>' \
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

Хост створює один `RpcServer`, встановлює захищений plugin, запускає QUIC із
наданою TLS identity, отримує `RunningServer::health_handle(Arc<RpcServer>)`
і обслуговує справжній health router runtime на окремому HTTP listener.
Health listener за замовчуванням доступний лише через loopback, оскільки ці
неавтентифіковані endpoints можуть розкривати операційні метрики; відкривайте
його лише в обмеженій мережевій зоні. Приклад не запускає plaintext RPC і не
містить секретів. Приклад `backend` збережено, але закріплений `BackendBuilder`
вмикає runtime default features (зокрема plaintext TCP/UDP) і не відкриває
внутрішній `RunningServer` для підключення health router.

Клієнт видаляє з FIFO-черги лише точки, ID яких повернуті у успішній відповіді
`acceptedIds`. Помилка сховища повертається як JSON-RPC error без підтвердження
точки. Уже записаний префікс безпечно надсилати повторно.

## Сховище та довговічність

`InMemoryCoopNavStorage` недовговічний і призначений лише для розробки/тестів.
Він використовує DashMap для пошуку партиції, синхронізацію всередині
партиції та ін’єктований Unix-clock у мілісекундах для детермінованих тестів
строку дедуплікації.

`RedisCoopNavStorage` розділяє Streams за SHA-256 від `(brandId,driverId)`.
Кожна точка додається до Redis Stream і дедуплікується за ID атомарно.
Повтор того самого ID/вмісту підтверджується ідемпотентно; той самий ID з
іншим вмістом відхиляється без заміни. Приклад використовує TTL дедуплікації
сім днів; після завершення TTL давній ID може бути доданий знову. Streams
автоматично не обрізаються: retention/архівація є явною політикою оператора,
і не можна видаляти ще не оброблені точки.

Підтвердження запису Redis у пам’ять саме по собі не гарантує стійке
збереження. Production має налаштувати й моніторити Redis persistence,
дисковий простір і failover відповідно до RPO (для найсильнішої локальної
гарантії використовуйте AOF із `appendfsync always`). Помилки Redis/script
повертаються клієнту й не породжують `acceptedIds`. Lua перевіряє типи обох
ключів до послідовних мутацій `XADD`, потім `SET NX EX`; помилка Redis після
`XADD` може залишити запис у stream без ключа дедуплікації. Тому помилку
сховища не підтверджуємо, а persistence слід моніторити. Прихованого обрізання
stream чи політики втрати даних немає.

## Метрики та health

Health router runtime відкриває `/healthz`, `/metrics.json` і Prometheus
`/metrics`; наведений приклад підключає всі три до реального listener.
Бібліотека також надає `serve_health_http1(listener, health_handle)` та
`serve_running_health_http1(listener, &running_server, rpc_server)` для
хостів, які вже керують runtime server. HTTP/1 вмикається Cargo feature
`metrics-http1`; він вимкнений у збірці бібліотеки з `--no-default-features`.

## Перевірки

```sh
cargo fmt --all -- --check
cargo test --all-targets --all-features
cargo test --no-default-features --lib
cargo clippy --all-targets --all-features -- -D warnings
python3 tools/check-architecture.py
sh tools/check-features.sh
```

Якщо встановлено `redis-server`, Redis-тест піднімає ізольований loopback
сервер на OS-selected ephemeral port та завершує лише створений ним дочірній
процес. Без Redis пропускається лише цей інтеграційний тест; детерміновані
тести memory/auth/RPC залишаються.

Інтеграційний тест хоста прив’язує QUIC та HTTP listener-и до loopback на
ephemeral ports, перевіряє всі три автентифіковані методи через QUIC, а
потім викликає три health endpoints через HTTP/1.

Усі JSON-RPC крейти закріплено на JSON-RPC-2
`af8d04dbbaee05fa1514caa07a37808d5973d100`, OAuth2 — на
`1b0043f27cc445e9d238a643ab32fd45e143ea6e`. Cargo `[patch.crates-io]`
для `h3`/`h3-quinn` відповідає екосистемі.
