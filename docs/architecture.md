# Architecture — `realtime_gateway`

WebSocket push gateway for the Community Chat & Voice Platform. It maintains long-lived
browser connections and fans out live platform events to the sockets whose subscription
scopes match. It owns no database and produces no events.

This document is the clean architectural specification: service responsibilities, internal
component boundaries, request/event flows, this service's slice of the inter-service
contracts, the operational model of its stateless containers, and observability metric
definitions. Library, framework, and code-organization choices live in
`docs/engineering_guide.md`.

---

## 1. Service Responsibilities

- **Authenticated live connections.** Terminate one WebSocket per browser tab. Verify the
  access token **locally** (JWT, asymmetric signature, claims `sub`, `sid`, `email_verified`,
  `exp`, `iat`, `iss`, `aud`) using identity's public verification keys distributed through
  deployment configuration (current key + next key, to support rotation). No synchronous call
  to `identity_service` at connect or refresh time.
- **Scope resolution and subscription.** On connect (and on every reconnect), ask
  `messaging_service` (API-003) for the user's subscription scopes: community ids and private
  conversation ids. Subscribe the connection to those scopes plus the user's personal user
  scope. Subscription state lives **only on the connection**; it is rebuilt from scratch on
  reconnect — no sticky cross-request state, no shared in-memory state between replicas.
- **Broadcast event consumption.** Consume `message_events`, `voice_events`,
  `community_events`, `social_events`, `notification_events` as a **broadcast reader**: every
  replica reads every partition independently, starting from latest offsets, without a shared
  consumer group and without committing offsets. The gateway guarantees **no replay**: an
  event is pushed to a given connection at most once.
- **Scope-aware fan-out.** Match each consumed event against the scopes of every locally
  connected socket and push it as a frame `{type, scope, data}`.
- **Live scope maintenance.** Apply membership and conversation events to keep connection
  scopes current without reconnects:
  - `community_member_changed`: on `joined` add the affected user's community scope to their
    connections; on `left` / `removed` / `banned` drop it (and push the member snapshot to
    remaining community subscribers).
  - `conversation_changed`: on `created` / `participants_changed` add and drop conversation
    scopes per `added_user_ids` / `removed_user_ids` and push the conversation snapshot.
- **Connection hygiene.** Heartbeat ping/pong to detect dead connections; in-band token
  refresh; close connections whose token expires without refresh; drop frames for consumers
  too slow to keep up (bounded per-connection send buffer) and count them.
- **Probes.** `GET /healthz` (liveness) and `GET /readyz` (readiness: live-event feed attached,
  scope client configured; 503 once draining).

**Explicitly out of scope:** typing indicators; client→server domain writes (all writes go
through domain REST APIs); media transport (voice/video flows browser ↔ managed voice
provider directly); history storage or replay; token issuance.

---

## 2. Internal Component Boundaries

| Component | Responsibility |
|---|---|
| **WebSocket endpoint** | HTTP handler performing the upgrade; token extraction (query parameter for browsers, Authorization header otherwise); origin policy enforcement; delegates to the connection lifecycle. |
| **Token verifier** | Local JWT verification against the configured public verification keys (current + next). Validates signature, `exp`, `iss`, `aud`; extracts `sub` (user id) and `sid` (session id). |
| **Scope resolver** | Synchronous client for API-003 (`messaging_service`): sends the user id, receives community and conversation ids; applies the 2 s timeout with one retry per the platform call policy. |
| **Connection registry (hub)** | Per-replica registry of live sockets. Each connection holds: user id, session id, token expiry, its scope set, and a bounded outbound send buffer. Supports registration/removal and scope-set add/drop on a live connection. |
| **Scope matcher / fan-out** | For each consumed event, computes the target scope(s) and the affected user ids (for scope add/drop), then delivers the frame to every matching connection of this replica. |
| **Broadcast event consumers** | One consumer per topic (`message_events`, `voice_events`, `community_events`, `social_events`, `notification_events`). Manual partition assignment of all partitions, starting at latest offsets; no commits; dedupe on `envelope.event_id` within the consumer's retention window (at-least-once delivery must not produce duplicate frames on one connection). |
| **Event decoder & validator** | Validates the mandatory envelope (see §4) and the payload against the event contract version; unknown event types or versions are logged and dropped, never crash the consumer. |
| **Heartbeat & lifecycle manager** | Ping/pong scheduling, read/write deadlines, in-band `refresh_token` handling, close-code emission, graceful drain. |
| **Probes & metrics** | `/healthz`, `/readyz`, Prometheus metric registration (see §7). |

Layering: handlers (WebSocket upgrade, probes) → lifecycle services (connection, heartbeat,
refresh) → event pipeline (consumer → decoder → matcher → fan-out). No persistence layer
exists.

---

## 3. Request and Event Flows

### 3.1 Connect flow (synchronous)

1. Client opens `GET /ws` with the access token (query parameter or Authorization header).
2. The token is verified locally. On failure the upgrade is refused: `invalid_token` (close
   code 4402) or `token_expired` (4401). No scope call is made.
3. The scope resolver calls API-003 with the user id (2 s timeout, one retry on 5xx/timeout).
   On failure the connection is refused with a **retryable** close code (`service_unavailable`,
   4503) — the client reconnects with back-off.
4. The connection is registered with scopes: `user:{user_id}`, `community:{id}` for each
   returned community id, `conversation:{id}` for each returned conversation id.
5. Heartbeat scheduling starts. The connection is now live. Connect p95 target (including the
   API-003 call): < 300 ms.

### 3.2 Consume-and-fan-out flow (asynchronous)

1. Each replica's consumers receive every event on every partition of the five topics from
   latest offsets.
2. The decoder validates `envelope` and payload; duplicates (`event_id` already processed by
   this consumer) are dropped.
3. The scope matcher resolves the target scope:
   - `message_changed` → `community:{community_id}` when `community_id` is set (channel
     message), otherwise `conversation:{container_id}`.
   - `voice_presence_changed` → `community:{community_id}`.
   - `community_member_changed` → `community:{community_id}`; additionally, the affected
     user's own connections get the community scope added (`joined`, `unbanned` while member)
     or dropped (`left`, `removed`, `banned`).
   - `community_channel_changed` → `community:{community_id}`.
   - `conversation_changed` → `conversation:{conversation_id}`; additionally,
     `added_user_ids` get the conversation scope added and `removed_user_ids` dropped on their
     connections.
   - `notification_created` → `user:{recipient_user_id}`.
4. The frame `{type: <event_type>, scope: <scope>, data: <payload>}` is pushed to every local
   connection holding that scope. A connection whose send buffer is full is treated as slow:
   the frame is dropped and counted (dropped-frame budget < 0.1 %); a connection exceeding its
   buffer repeatedly is closed so the client can reconnect cleanly.
5. Kafka-to-socket delivery p95 target: < 150 ms; consumer lag p95 target: < 500 ms.

### 3.3 Heartbeat and token-refresh flows (client → server)

- **Heartbeat:** the server pings on a fixed interval; a connection missing pongs past the
  read deadline is closed and its state freed. The client may also send `{"type":"heartbeat"}`
  and receives `heartbeat_ack`.
- **In-band refresh:** the client sends `{"type":"refresh_token","access_token":"..."}`. The
  new token is verified like the connect token. On success the connection is re-associated
  with the new token and expiry (`refresh_token_ack`, `ok:true`). On failure the refresh is
  rejected (`ok:false`, `invalid_token`); the connection keeps its previous token and closes
  when that expires. A connection whose token expires without a successful refresh is closed
  with `token_expired` (4401).
- Any other inbound message is a protocol violation → close 4400.

### 3.4 Reconnect flow

1. The connection drops (network, server restart, token expiry).
2. The client shows "reconnecting", reconnects with back-off, and the connect flow (§3.1)
   runs again from scratch — subscriptions are rebuilt from current scopes.
3. The client catches up missed messages via the messaging REST history `after` cursor. The
   gateway replays nothing. Duplicate delivery of the same event to one connection is
   suppressed via `event_id` dedupe.

### 3.5 Shutdown flow

On `SIGTERM`/`SIGINT`: readiness flips to 503, new upgrades are refused, in-flight frames are
flushed, connections are closed with `1001 going away`, feed consumers stop, process exits 0
within the bounded drain window.

---

## Inter-Service Contracts

### Synchronous — API-003 (caller: `realtime_gateway` → provider: `messaging_service`)

`realtime_gateway` calls `POST /internal/realtime/user-scopes` at connect time (and on
reconnect). Contract copied verbatim from `API_CONTRACTS.md`.

General rules for every internal contract:
- Internal endpoints are served under `/internal/*` and are not exposed through the public ingress.
- Authentication: per-edge service token in `Authorization: Bearer <token>`, provisioned through deployment secrets. Requests without a valid service token get `401`.
- Tracing: callers propagate `traceparent` (and `X-Trace-Id`) headers; providers continue the trace.
- Timeouts: callers use a 2 s timeout with at most one retry on `5xx`/timeout. All three operations are read-only checks, so retries are safe.
- Errors use `application/problem+json` with the `shared:ErrorResponse` body.

| Field | Value |
|---|---|
| Provider | `messaging_service` |
| Consumers | `realtime_gateway` |
| Kind | REST |
| Operation | `POST /internal/realtime/user-scopes` |
| Auth | service token, `Authorization: Bearer` |
| Idempotency | not required (read-only, safe to retry) |

**Request** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["user_id"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"}
  }
}
```

**Response 200** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["user_id", "community_ids", "conversation_ids", "generated_at"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"},
    "community_ids": {
      "type": "array",
      "items": {"type": "string", "format": "uuid"},
      "uniqueItems": true,
      "description": "Communities where the user is an active member (muted members included; removed/banned excluded)."
    },
    "conversation_ids": {
      "type": "array",
      "items": {"type": "string", "format": "uuid"},
      "uniqueItems": true,
      "description": "Private conversations (direct and group) where the user is a current participant."
    },
    "generated_at": {"type": "string", "format": "date-time"}
  }
}
```

**Errors** — `400` (malformed body), `401` (missing or invalid service token), `500`, `503`, all `application/problem+json` referencing `shared:ErrorResponse`. An unknown user returns `200` with empty lists.

The `shared:ErrorResponse` body:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["error"],
  "properties": {
    "error": {"type": "string", "description": "Stable machine-readable error code, snake_case."},
    "detail": {"type": "string", "description": "Human-readable explanation."}
  }
}
```

### Asynchronous — events consumed by `realtime_gateway`

`realtime_gateway` consumes the topics below as a **broadcast reader**: each replica reads all
partitions independently from the latest offset and does not replay history (no shared
consumer-group offsets, no offset commits). Delivery is at-least-once; the gateway dedupes on
`envelope.event_id` so a connection receives each event at most once.

The gateway consumes; it never produces. `trace_id` from the consumed envelope is propagated
into the connection's logs for correlation.

#### Envelope (mandatory, every event)

Every message on every topic is a JSON object with exactly two top-level keys: `envelope` and `payload`.

| Field | Type | Required | Rule |
|---|---|---|---|
| `event_id` | string (uuid) | yes | Unique per event. Consumer deduplication key. |
| `event_type` | string (snake_case) | yes | Must match a registered contract. |
| `event_version` | integer | yes | >= 1. A breaking payload change bumps it. |
| `occurred_at` | string (RFC3339 UTC) | yes | Producer clock at emit time. |
| `producer` | string (snake_case) | yes | Must equal the registered producer. |
| `trace_id` | string (32 lowercase hex) | yes | **Never empty.** Propagated from the originating request; minted at the edge when absent. |
| `correlation_id` | string (uuid) | yes | Groups every event of one business flow. |
| `causation_id` | string (uuid) or null | yes | `event_id` of the causing event. `null` only for the flow's first event. |
| `schema_ref` | string | yes | `EVENT_CONTRACTS.md#<event_type>-v<version>`. |

Rules:
- The envelope is not optional and not per-topic. A topic needing a field the envelope lacks adds it to `payload`, never to the envelope.
- A payload schema MUST NOT declare an envelope field name at its top level.
- `trace_id` is mandatory even when no HTTP request originated the flow: the producer mints one at the edge of its own flow. This covers scheduled jobs such as mute expiry, LiveKit-webhook-triggered presence changes, and audit purge.
- When a consumer emits an event because of another event (for example, `community_service` emitting `community_member_changed` after applying `member_sanction_changed`), it copies `trace_id` and `correlation_id` from the causing event and sets `causation_id` to the causing `event_id`.
- The envelope is also mirrored into Kafka message headers (`trace_id`, `event_type`) for routing and observability. The JSON envelope remains the source of truth.
- Delivery is at-least-once. Every consumer dedupes on `envelope.event_id` and applies changes idempotently. Projections keep an entity version and ignore stale versions.

Envelope schema (the `envelope` object):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["event_id", "event_type", "event_version", "occurred_at", "producer", "trace_id", "correlation_id", "causation_id", "schema_ref"],
  "properties": {
    "event_id": {"type": "string", "format": "uuid"},
    "event_type": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "event_version": {"type": "integer", "minimum": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "producer": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "trace_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    "correlation_id": {"type": "string", "format": "uuid"},
    "causation_id": {"type": ["string", "null"], "format": "uuid"},
    "schema_ref": {"type": "string", "pattern": "^EVENT_CONTRACTS\\.md#[a-z][a-z0-9_]*-v[0-9]+$"}
  }
}
```

#### EVT-002 — `community_member_changed` v1 (topic `community_events`)

Emitted whenever a membership's state changes. The authoritative member state (active/removed, banned, mute, roles) is always included as a snapshot. Sanction-driven changes carry the originating `sanction_id`.

| Field | Value |
|---|---|
| Producer | `community_service` |
| Consumers | `messaging_service`, `voice_service`, `realtime_gateway` |
| Topic | `community_events` |
| Key | `community_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `member_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "user_id", "change", "status", "is_banned", "muted_until", "role_ids", "sanction_id", "member_version"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["joined", "left", "removed", "banned", "unbanned", "muted", "unmuted", "roles_changed"]},
    "status": {"type": "string", "enum": ["active", "removed"]},
    "is_banned": {"type": "boolean"},
    "muted_until": {"type": ["string", "null"], "format": "date-time"},
    "role_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "sanction_id": {"type": ["string", "null"], "format": "uuid"},
    "member_version": {"type": "integer", "minimum": 1}
  }
}
```

Gateway handling: push the snapshot on `community:{community_id}`; add the community scope to
the affected user's connections on `joined` (and on `unbanned` when the snapshot shows the
member active); drop the scope on `left`, `removed`, `banned`.

#### EVT-003 — `community_channel_changed` v1 (topic `community_events`)

Emitted when a text or voice channel is created, updated (name, position, capacity), or deleted.

| Field | Value |
|---|---|
| Producer | `community_service` |
| Consumers | `realtime_gateway`, `messaging_service`, `voice_service` |
| Topic | `community_events` |
| Key | `community_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `channel_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "channel_id", "change", "kind", "name", "position", "capacity", "channel_version"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "channel_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["created", "updated", "deleted"]},
    "kind": {"type": "string", "enum": ["text", "voice"]},
    "name": {"type": "string", "minLength": 1, "maxLength": 100},
    "position": {"type": "integer", "minimum": 0},
    "capacity": {"type": ["integer", "null"], "minimum": 1, "maximum": 10, "description": "Voice channels only; null for text channels."},
    "channel_version": {"type": "integer", "minimum": 1}
  }
}
```

Gateway handling: push the snapshot on `community:{community_id}`. No scope changes.

#### EVT-008 — `conversation_changed` v1 (topic `social_events`)

Emitted when a private conversation (direct or group) is created, its participants change, or it is renamed. The payload always carries the full current participant list, plus a delta.

| Field | Value |
|---|---|
| Producer | `social_service` |
| Consumers | `messaging_service`, `realtime_gateway` |
| Topic | `social_events` |
| Key | `conversation_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `conversation_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["conversation_id", "kind", "change", "name", "participant_user_ids", "added_user_ids", "removed_user_ids", "conversation_version"],
  "properties": {
    "conversation_id": {"type": "string", "format": "uuid"},
    "kind": {"type": "string", "enum": ["direct", "group"]},
    "change": {"type": "string", "enum": ["created", "participants_changed", "renamed"]},
    "name": {"type": ["string", "null"], "maxLength": 100},
    "participant_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "minItems": 1, "maxItems": 10, "uniqueItems": true},
    "added_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "removed_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "conversation_version": {"type": "integer", "minimum": 1}
  }
}
```

Gateway handling: push the snapshot on `conversation:{conversation_id}`; add the conversation
scope for `added_user_ids`, drop it for `removed_user_ids`.

#### EVT-009 — `message_changed` v1 (topic `message_events`)

Emitted for every message creation, edit, deletion (by the author or by automod), and reaction change. The payload carries a message snapshot. On deletion, `content` is null and `attachments` is empty. Attachment download URLs are short-lived presigned URLs, and clients refresh them through message history.

| Field | Value |
|---|---|
| Producer | `messaging_service` |
| Consumers | `realtime_gateway`, `notification_service` |
| Topic | `message_events` |
| Key | `container_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; ordering guaranteed per container; apply only if `message_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["message_id", "change", "container_type", "container_id", "community_id", "author_user_id", "thread_root_id", "content", "attachments", "mentioned_user_ids", "reactions", "deletion_reason", "created_at", "edited_at", "message_version"],
  "properties": {
    "message_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["created", "edited", "deleted", "reactions_changed"]},
    "container_type": {"type": "string", "enum": ["channel", "conversation"]},
    "container_id": {"type": "string", "format": "uuid", "description": "channel_id or conversation_id."},
    "community_id": {"type": ["string", "null"], "format": "uuid", "description": "Set for channel messages; null for conversation messages."},
    "author_user_id": {"type": "string", "format": "uuid"},
    "thread_root_id": {"type": ["string", "null"], "format": "uuid"},
    "content": {"type": ["string", "null"], "maxLength": 4000},
    "attachments": {
      "type": "array",
      "maxItems": 10,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["attachment_id", "content_type", "size_bytes", "download_url", "url_expires_at"],
        "properties": {
          "attachment_id": {"type": "string", "format": "uuid"},
          "content_type": {"type": "string", "enum": ["image/png", "image/jpeg", "image/gif", "image/webp"]},
          "size_bytes": {"type": "integer", "minimum": 1, "maximum": 10485760},
          "download_url": {"type": "string", "format": "uri"},
          "url_expires_at": {"type": "string", "format": "date-time"}
        }
      }
    },
    "mentioned_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "reactions": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["emoji", "count"],
        "properties": {
          "emoji": {"type": "string", "minLength": 1, "maxLength": 32},
          "count": {"type": "integer", "minimum": 1}
        }
      }
    },
    "deletion_reason": {"type": ["string", "null"], "enum": ["author", "automod", null]},
    "created_at": {"type": "string", "format": "date-time"},
    "edited_at": {"type": ["string", "null"], "format": "date-time"},
    "message_version": {"type": "integer", "minimum": 1}
  }
}
```

Gateway handling: push on `community:{community_id}` when `community_id` is non-null
(`container_type = channel`), otherwise on `conversation:{container_id}`. No scope changes.

#### EVT-010 — `voice_presence_changed` v1 (topic `voice_events`)

Emitted when a participant joins or leaves a voice channel, or toggles camera or screen share. The trigger is a LiveKit webhook, an explicit leave, a moderation removal, or reconciliation. For webhook-triggered flows, the producer mints the `trace_id`.

| Field | Value |
|---|---|
| Producer | `voice_service` |
| Consumers | `realtime_gateway` |
| Topic | `voice_events` |
| Key | `channel_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id` |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "channel_id", "user_id", "change", "camera_enabled", "screen_share_enabled", "publishing_allowed", "participant_count", "capacity"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "channel_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["joined", "left", "media_changed"]},
    "camera_enabled": {"type": "boolean"},
    "screen_share_enabled": {"type": "boolean"},
    "publishing_allowed": {"type": "boolean", "description": "False when the member is muted (listen-only)."},
    "participant_count": {"type": "integer", "minimum": 0, "maximum": 10},
    "capacity": {"type": "integer", "minimum": 1, "maximum": 10}
  }
}
```

Gateway handling: push on `community:{community_id}`. No scope changes.

#### EVT-011 — `notification_created` v1 (topic `notification_events`)

Emitted when an in-app notification is stored for a user, so open sessions can show it immediately.

| Field | Value |
|---|---|
| Producer | `notification_service` |
| Consumers | `realtime_gateway` |
| Topic | `notification_events` |
| Key | `recipient_user_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id` |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["notification_id", "recipient_user_id", "kind", "title", "summary", "link", "unread_count"],
  "properties": {
    "notification_id": {"type": "string", "format": "uuid"},
    "recipient_user_id": {"type": "string", "format": "uuid"},
    "kind": {"type": "string", "enum": ["mention", "friend_request_received", "friend_request_accepted", "moderation_notice", "appeal_decided"]},
    "title": {"type": "string", "maxLength": 200},
    "summary": {"type": "string", "maxLength": 500},
    "link": {"type": "string", "description": "In-app deep link path, e.g. /channels/{channel_id}/messages/{message_id}."},
    "unread_count": {"type": "integer", "minimum": 0}
  }
}
```

Gateway handling: push on `user:{recipient_user_id}`. No scope changes.

---

## 5. Data Ownership & PostgreSQL Schema

`realtime_gateway` owns **no database and no persistent domain data**. All state is
connection-bound (scope sets, send buffers, token expiry, per-consumer dedupe caches) and is
reconstructed on reconnect or restart. There is no PostgreSQL schema for this service, and it
must not be given one: connection state must never be persisted.

In-memory state and its lifecycle:

| State | Scope | Lifetime |
|---|---|---|
| Connection record (user id, session id, token expiry, scope set, send buffer) | per connection | connect → close |
| Event dedupe cache (`event_id` → seen) | per consumer, per replica | bounded retention window (recent entries only) |
| Consumer offsets | none persisted | start at latest on every (re)start |

---

## 6. Stateless Container Operational Model

- **Statelessness:** every replica is interchangeable. Subscription state lives on the
  connection; the event feed is a broadcast read from latest offsets. Scaling out adds fan-out
  capacity without coordination; scaling in simply drops that replica's sockets (clients
  reconnect).
- **Probes:**
  - `/healthz` — liveness: process responsive; never fails on transient dependency hiccups.
  - `/readyz` — readiness: live-event feed attached (partitions assigned for all five topics
    on this replica), scope client configured; 503 once shutdown starts. A replica whose feed
    is detached stops receiving new connections.
- **Graceful shutdown:** trap `SIGTERM`/`SIGINT`; mark not ready; stop accepting upgrades;
  flush outbound frames; close connections with `1001 going away`; stop consumers; exit 0
  within the bounded drain window (15–30 s).
- **Failure modes & resilience:**
  - *API-003 unavailable at connect* → refuse with retryable `service_unavailable` (4503);
    client backs off. Never serve a connection with unverified or guessed scopes.
  - *API-003 slow* → 2 s timeout, one retry; the read-only call is safe to retry.
  - *Kafka broker unavailability* → consumer keeps the feed detached, readiness 503, existing
    sockets stay open (they are already subscribed); on reattach, resume from latest offsets
    (no replay, consistent with the no-replay guarantee).
  - *Malformed or unknown-version events* → log with `trace_id`, drop the event, keep the
    consumer alive.
  - *Slow sockets* → bounded send buffer; drop frames for that socket and count them; close
    persistently slow sockets so the client reconnects cleanly.
  - *Token expiry mid-connection* → in-band refresh accepted; otherwise close 4401.
  - *Dedup window eviction* → treated the same as first sight; the no-replay guarantee comes
    from latest-offset consumption, dedupe only closes the at-least-once gap.

---

## 7. Observability

**Business metrics**

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `realtime_connections_active` | gauge | — | Live sockets on this replica. |
| `realtime_connections_opened_total` | counter | `result` (`accepted`\|`rejected_auth`\|`rejected_scopes`\|`rejected_not_ready`) | Upgrade outcomes. |
| `realtime_events_delivered_total` | counter | `event_type` | Frames actually pushed to ≥ 1 socket, per event type. |
| `realtime_reconnects_total` | counter | — | Client reconnects observed (connection opened shortly after a disconnect of the same user on this replica). |
| `realtime_dropped_frames_total` | counter | `event_type` | Frames dropped for slow consumers. Alert budget: < 0.1 % of deliveries. |

**Technical metrics (platform standard)**

| Metric | Labels |
|---|---|
| `http_requests_total` | `route`, `method`, `status_class` |
| `http_request_duration_seconds` | `route`, `method` |
| `kafka_consumer_lag` | `topic`, `consumer_group` |

All labels are low cardinality — never user, community, conversation, or message ids.

**SLOs:** Kafka-to-socket delivery p95 < 150 ms; consumer lag p95 < 500 ms; connect
(including the API-003 call) p95 < 300 ms; dropped-frame rate < 0.1 %.

**Tracing:** every HTTP request carries `traceparent` / `X-Trace-Id`; the outbound API-003
call propagates it. Consumed events carry `trace_id` in the mandatory envelope; the gateway
puts that `trace_id` in every log line for the frame it produces. Logs are structured JSON
with `trace_id`, `event_type`, `event_id`, an opaque non-identifying connection id, and the
outcome.
