# Engineering Guide — `realtime_gateway`

Persistent technical memory for the realtime_gateway service. The clean architectural
specification (responsibilities, flows, contracts) lives in `docs/architecture.md`; this
document governs runtime, libraries, container lifecycle, testing, conventions,
configuration, and observability. New decisions append to `## Pinned Decisions`; existing
pins are never silently replaced.

---

## 1. Runtime & Toolchain

| Aspect | Choice |
|---|---|
| Language | Go (module path `realtime_gateway`, initialized with `go mod init realtime_gateway` — never a VCS-hosted module path) |
| Runtime version | Go 1.26 (builder image `golang:1.26-alpine`, distroless static runtime) |
| Package manager | Go modules (`go.mod` + `go.sum`, always kept in sync via `go mod tidy`) |
| Build entrypoint | `make build` (Docker image build) and `go build ./cmd/server` for local binaries |
| Test entrypoint | `make test` → `go test ./... -race -count=1 -parallel 4` |
| Run locally | `make run` → `go run ./cmd/server` |
| Lint | `make lint` → `golangci-lint run ./...` |
| Dependency install | `make install` → `go mod download` (always run first in a cold workspace) |

---

## 2. Libraries & External Provider SDKs

Resolve the latest stable version with `go get <module>@latest` unless a version is pinned in
`## Pinned Decisions`. Verify exact APIs via `technical_research_agent` before first use.

| Concern | Package / Library | Source | Usage Notes |
|---|---|---|---|
| HTTP framework / routing & probes | `github.com/gin-gonic/gin` | language-skill | Serves `/ws` (upgrade handled by the WebSocket library), `/healthz`, `/readyz`; RED middleware (`http_requests_total`, `http_request_duration_seconds`). |
| WebSocket | `github.com/gorilla/websocket` | tech-lead-research | De-facto standard, Autobahn-tested, API-stable (v1.5.x line). Upgrade in the HTTP handler; **exactly one goroutine per direction** (one reader, one writer); `SetPingHandler`/`SetPongHandler` for heartbeats; `WriteControl` for close frames; explicit `CheckOrigin` allow-list from configuration; `HandshakeTimeout` set. |
| JWT verification | `github.com/golang-jwt/jwt/v5` | tech-lead-research | Verify identity_service access tokens locally with configured public keys (asymmetric alg only — reject `none`/HMAC). Validate `exp`, `iss`, `aud`; extract `sub`, `sid`. Never accept tokens signed by symmetric keys. |
| Kafka client | `github.com/IBM/sarama` | language-skill | Broadcast consumer mode: `sarama.NewClient` + per-topic `Consumer.Partitions()` with manual partition assignment, `ConsumePartition(..., sarama.OffsetNewest)`, no offset commits, no consumer group coordination. SASL/TLS configurable via env. |
| Logging | `go.uber.org/zap` | language-skill | Structured JSON production logger; `trace_id`, `event_id`, `event_type` fields on every event-related log. |
| Metrics | `github.com/prometheus/client_golang/prometheus` | language-skill | Register business + technical metrics from architecture §7; expose `/metrics`. |
| Environment loading | `github.com/joho/godotenv` | language-skill | Local development only; production reads plain env vars. `.env` must be gitignored. |
| Testing | standard `testing` + `github.com/stretchr/testify` | language-skill | Table-driven tests, `t.Parallel()` everywhere. |
| Mocking | `github.com/stretchr/testify/mock` | language-skill | Mocks for the scope resolver, token verifier, and Kafka consumer interfaces. |
| HTTP mocking | `github.com/jarcoal/httpmock` | language-skill | Stubs the scope-resolver HTTP transport in tests. |

There are **no external third-party provider SDKs** for this service: it integrates no email,
payment, voice, or AI provider. Its only outbound integration is the internal API-003 call to
`messaging_service` (plain HTTP + service token, not an SDK concern). It has no PostgreSQL and
no S3-compatible storage dependency — do not add database or object-storage drivers.

---

## 3. Container Lifecycle & Probes

- **Statelessness rules:** no local disk writes, no session state, no persisted offsets. All
  connection state is in-memory and per-connection. Safe to run with `replicas > 1`.
- **Signals:** trap `SIGTERM` and `SIGINT` via `signal.NotifyContext`. On signal:
  1. Flip `/readyz` to 503 immediately (load balancer stops routing new connections).
  2. Stop accepting WebSocket upgrades.
  3. Flush pending outbound frames, close sockets with close code `1001` (going away).
  4. Stop Kafka consumers (close partition consumers, then the client).
  5. Exit 0 within the bounded drain window (15–30 s hard deadline).
- **`GET /healthz` (liveness):** lightweight; answers 200 as long as the process is
  responsive. MUST NOT probe Kafka or the scope service — transient downstream outages must
  not restart the container.
- **`GET /readyz` (readiness):** 200 only when the live-event feed is attached (partitions
  assigned for all five topics on this replica) and the scope-resolver client is configured;
  503 during drain. A feed-detached replica stops receiving new connections but keeps its
  live sockets.
- **WebSocket lifecycle guards:** ping/pong heartbeats with read/write deadlines; per-socket
  bounded send buffer; token-expiry sweeper closes connections that never refreshed.

---

## 4. Testing Strategy

- **Unit tests** (majority, small and fast): scope-matching and fan-out logic, scope
  add/drop semantics for EVT-002 / EVT-008, token verification decisions (valid, expired,
  wrong issuer/audience, wrong alg, unsigned), in-band refresh handling, dedupe behavior,
  close-code mapping. No network calls — the scope resolver, token verifier clock, and Kafka
  consumer are interfaces faked in-process.
- **Integration tests** (mocked externals only): end-to-end over an in-process HTTP test
  server — real upgrade path with `httptest`, fake scope resolver (`httpmock`/testify mock),
  fake event feed feeding encoded `{"envelope": ..., "payload": ...}` messages into the
  consumer, asserting the frames delivered on a real WebSocket client connection.
- **Mocks live in `tests/mocks/`** (or co-located `*_mock.go` files); they mimic real error
  behavior (timeouts, 503, malformed envelopes).
- **Determinism:** no `time.Sleep`-based assertions — inject a clock and ticker; unique
  per-test ports/topics; no shared mutable state; all tests `t.Parallel()`.
- **No real infrastructure in tests:** no Kafka broker, no Docker, no network egress.
  Kafka consumers are tested by driving the consumer handler functions directly with fake
  message channels.
- **Coverage:** ≥ 80 %, focused on fan-out, scopes, auth, and lifecycle paths. Run via
  `make test`.

---

## 5. Coding Conventions & Project Structure

Repository layout (Go standard layout per platform conventions):

```
realtime_gateway/
├── cmd/server/main.go            # entrypoint: config, wiring, signal handling
├── internal/
│   ├── config/config.go          # typed Config struct validated at startup
│   ├── handler/                  # /ws upgrade, /healthz, /readyz (thin)
│   ├── service/                  # connection lifecycle, heartbeat, token refresh, fan-out logic
│   ├── gateway/                  # hub/registry, scope matcher, event pipeline (consumer→decoder→dispatcher)
│   ├── contracts/                # envelope + event payload structs mirroring EVENT_CONTRACTS.md
│   └── models/                   # frame, scope, close-code types
├── tests/
│   ├── unit/
│   ├── integration/
│   └── mocks/
├── Makefile
├── Dockerfile
├── go.mod / go.sum
└── docs/
```

Conventions:

- **Layering:** handlers → services (lifecycle, fan-out) → external adapters (scope resolver,
  Kafka consumer). Business logic never imports the WebSocket or Kafka packages directly;
  it depends on narrow interfaces.
- **Concurrency:** exactly one goroutine reads from each socket and one writes to it (the
  WebSocket library requirement); fan-out delivers to per-connection buffered channels, never
  writes directly from consumer goroutines.
- **Errors:** return errors, never `panic` outside init. Malformed events are logged with
  `trace_id` and dropped, never crash the consumer loop. Classify every close path into the
  close-code table from `docs/openapi_spec.yaml`.
- **Contracts as code:** the envelope is a single reusable struct in `internal/contracts`
  (consumed side); event payloads are typed structs matching the JSON Schemas in
  `docs/architecture.md` verbatim; unknown fields tolerated for forward compatibility,
  unknown event types/versions dropped with a WARN log.
- **Typing/docs:** all exported types and functions carry Go doc comments; no commented-out
  or dead code; comments explain why, not what.
- **Security:** the `access_token` query parameter is scrubbed from every log and error
  message; connection ids logged are opaque and non-identifying; origin allow-list enforced
  on upgrade; tokens verified with asymmetric algorithms only.

---

## 6. Configuration & Secrets Management

All configuration is 12-factor environment variables, validated at startup (fail fast with a
structured error log if required variables are missing or malformed). No `.env` in production.

| Variable | Required | Meaning |
|---|---|---|
| `PORT` | yes | HTTP listen port for `/ws`, probes, `/metrics`. |
| `KAFKA_BROKERS` | yes | Comma-separated Kafka bootstrap brokers for the five consumed topics. |
| `KAFKA_SASL_MECHANISM` / `KAFKA_SASL_USER` / `KAFKA_SASL_PASSWORD` / `KAFKA_TLS_ENABLED` | no | Kafka auth when the deployment requires SASL/TLS. |
| `MESSAGING_SERVICE_URL` | yes | Base URL of `messaging_service` for the API-003 scopes call. |
| `INTERNAL_SERVICE_TOKEN` | yes | Per-edge service token sent as `Authorization: Bearer` on API-003. |
| `IDENTITY_JWKS` or `IDENTITY_PUBLIC_KEY_PATH` + `IDENTITY_NEXT_PUBLIC_KEY_PATH` | yes | identity_service public verification keys (current + next for rotation), distributed via deployment configuration — never fetched at request time. |
| `IDENTITY_TOKEN_ISSUER` / `IDENTITY_TOKEN_AUDIENCE` | yes | Expected `iss` / `aud` claims for token verification. |
| `WS_ALLOWED_ORIGINS` | yes | Comma-separated origin allow-list for the WebSocket upgrade `CheckOrigin` policy. |
| `WS_HEARTBEAT_INTERVAL` / `WS_READ_DEADLINE` | no | Heartbeat ping interval and pong deadline (sane defaults, seconds). |
| `WS_SEND_BUFFER_SIZE` | no | Per-connection outbound frame buffer bound (slow-consumer policy). |
| `EVENT_DEDUPE_TTL` | no | Retention window of the in-memory `event_id` dedupe cache. |
| `SHUTDOWN_TIMEOUT` | no | Bounded drain window in seconds (default 20). |

Secrets (`INTERNAL_SERVICE_TOKEN`, SASL password, key material) come from injected
environment or mounted secret files only; they are never logged and never committed.

---

## 7. Observability & Logging Conventions

- **Structured JSON logging** (zap): every log carries `timestamp`, `level`, `service_name`,
  `operation`, `status`; event-related logs add `trace_id`, `event_id`, `event_type`;
  connection logs use an opaque connection id and the user id is never logged. Levels: DEBUG
  dev, INFO normal, WARN degraded (dropped frame, malformed event, rejected refresh), ERROR
  (feed detached, upgrade failures).
- **Trace propagation:** inbound HTTP carries `traceparent` / `X-Trace-Id`; the API-003 call
  propagates them. Consumed events carry `trace_id` in the mandatory envelope, which the
  gateway copies into all downstream log lines for that frame.
- **Mandatory event envelope (consumed side):** every message the gateway consumes is
  `{"envelope": {...}, "payload": {...}}`. The envelope carries `event_id`, `event_type`,
  `event_version`, `occurred_at`, `producer`, `trace_id`, `correlation_id`, `causation_id`,
  and `schema_ref`; `trace_id` is **never empty** (propagated from the originating request or
  minted at the producer's flow edge). A payload MUST NOT carry an envelope field. The
  gateway validates the envelope on consume, dedupes on `event_id`, and drops messages with a
  missing/invalid envelope (logged with whatever correlation data survived). The gateway
  produces no events, so it never mints envelopes — but the envelope struct and validation
  must exist in `internal/contracts` so consumed messages cannot bypass it.
- **Prometheus metrics:** register the business metrics (`realtime_connections_active`,
  `realtime_connections_opened_total{result}`, `realtime_events_delivered_total{event_type}`,
  `realtime_reconnects_total`, `realtime_dropped_frames_total{event_type}`) and the platform
  technical metrics (`http_requests_total{route,method,status_class}`,
  `http_request_duration_seconds{route,method}`, `kafka_consumer_lag{topic,consumer_group}`).
  Labels stay low-cardinality; never user, community, conversation, or message ids. Expose
  `/metrics` on the same port as the probes.

---

## 8. Pinned Decisions

| Dependency / Area | Pin / Constraint | Rationale | Added By | Date |
|---|---|---|---|---|
| WebSocket library | `github.com/gorilla/websocket` | De-facto Go standard, Autobahn-tested, stable API; hub patterns well documented. | tech-lead | 2026-02-11 |
| JWT library | `github.com/golang-jwt/jwt/v5` | Maintained successor of `dgrijalva/jwt-go`; supports asymmetric verification and strict claim validation needed for local token verification. | tech-lead | 2026-02-11 |
| Kafka consumption mode | Broadcast reader — manual partition assignment, `OffsetNewest` start, no commits, no shared consumer group | Required by the platform contract: every replica must see every event, and the gateway must never replay history. | tech-lead | 2026-02-11 |
| No persistence | Service must not take a PostgreSQL or S3 dependency | The gateway is a pure fan-out pipe; adding state would break the stateless scale-out model. | tech-lead | 2026-02-11 |
| Client→server protocol surface | Heartbeats and in-band token refresh only | Keeps the gateway out of the domain write path; typing indicators explicitly out of scope. | tech-lead | 2026-02-11 |

