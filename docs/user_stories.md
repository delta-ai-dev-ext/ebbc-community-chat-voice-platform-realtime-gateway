# User Stories — `realtime_gateway`

Derived from the platform architecture (`SYSTEM_DESIGN.md` §2.8) and the inter-service contracts
(API-003, EVT-002, EVT-003, EVT-008, EVT-009, EVT-010, EVT-011). The gateway's only users are the
browser client (on behalf of a signed-in member) and the platform operator.

---

## US-001: Open an authenticated live event stream

**As a** signed-in member,
**I want to** open a single live connection to the platform,
**So that** I receive chat, presence, and notification updates instantly without polling.

### Acceptance Criteria
- Given I present a valid, unexpired access token when connecting, when the connection is established, then I start receiving event frames `{type, scope, data}` for every scope I am entitled to, and nothing else.
- Given I present a missing, malformed, or expired access token, when I attempt to connect, then the connection is refused before any event is delivered and I receive a machine-readable close reason (`invalid_token` / `token_expired`).
- Given the user's email is not verified but the token is otherwise valid, when I connect, then the connection is accepted (verification is enforced by other services, not by the gateway).

### Out of Scope
- Issuing or refreshing tokens (that is `identity_service`'s job; the gateway only verifies).
- Typing indicators — client-to-server traffic other than heartbeats and token refresh is rejected.

---

## US-002: Receive live messages for communities I belong to

**As a** member of a community,
**I want to** see new channel messages, edits, deletions, and reaction changes appear immediately,
**So that** the conversation feels live without manual refresh.

### Acceptance Criteria
- Given I am connected and an active member of community C, when any message event (`created`/`edited`/`deleted`/`reactions_changed`) occurs in a text channel of C, then I receive a frame scoped to that community containing the message snapshot.
- Given I am connected but was removed from community C (left, kicked, or banned), when a message event occurs in C after my removal has been applied, then I receive no frame for C.
- Given I am a muted member, when messages are posted in a channel of my community, then I still receive them (mutes restrict posting, not reading).

### Out of Scope
- History retrieval — catching up on missed messages is done through the messaging REST history endpoint, not through the gateway.
- Sending messages — all writes go through the domain REST APIs.

---

## US-003: Receive live messages for my private conversations

**As a** participant in a direct or group conversation,
**I want to** see new messages in that conversation immediately,
**So that** private chats behave like community chats.

### Acceptance Criteria
- Given I am connected and a current participant of conversation X, when a message event occurs in X, then I receive a frame scoped to that conversation.
- Given I was added to a group conversation while connected, when the participant change reaches me, then subsequent messages in that conversation are delivered to me without reconnecting.
- Given I left (or was removed from) a group conversation while connected, when the participant change reaches me, then I receive no further frames for that conversation.

### Out of Scope
- Conversation membership decisions — the gateway follows the conversation participant events; it never decides membership itself.

---

## US-004: See who is present in voice channels in real time

**As a** community member,
**I want to** see voice-channel presence (joins, leaves, camera and screen-share flags, seat usage) update live,
**So that** I know who is talking before I join.

### Acceptance Criteria
- Given I am connected with a community scope, when a participant joins, leaves, or toggles media in any voice channel of that community, then I receive a presence frame including the current participant count and capacity.
- Given I have no scope for community C, when a presence event occurs in C, then I receive no frame for it.
- Given the presence event carries `publishing_allowed: false` (muted member), then the frame still reaches community members (the flag itself tells clients the participant is listen-only).

### Out of Scope
- Carrying any voice/video media — media flows directly between browsers and the managed voice provider, never through the gateway.

---

## US-005: Keep member and channel lists current

**As a** community member,
**I want to** see member join/leave/mute/role changes and channel create/rename/delete/reorder update live,
**So that** my sidebar and member list stay accurate without a reload.

### Acceptance Criteria
- Given I am connected with a community scope, when a member change event occurs in that community, then I receive a frame with the member snapshot (status, ban flag, mute expiry, role ids).
- Given I am connected with a community scope, when a channel is created, updated, or deleted in that community, then I receive a frame with the channel snapshot.
- Given my own membership in community C is removed or I am banned, when that member change reaches my connection, then the community scope for C is dropped from my connection and no further C events are delivered to me.
- Given I join a community while connected, when my join event reaches my connection, then the community scope for C is added and I begin receiving C events without reconnecting.

### Out of Scope
- Rendering or diffing member/channel lists — the gateway forwards snapshots; the client owns presentation.

---

## US-006: Receive my notifications the moment they are created

**As a** signed-in member,
**I want to** see in-app notifications (mentions, friend requests, moderation notices, appeal outcomes) pop up immediately,
**So that** I never have to refresh to learn something needs my attention.

### Acceptance Criteria
- Given I am connected, when a notification is created for me, then I receive a frame on my personal scope containing the notification payload and the running unread count.
- Given a notification is created for a different user, when it is published, then I receive no frame for it.

### Out of Scope
- Notification inbox management (read/unread) — handled by the notification REST API.

---

## US-007: Keep my conversation list current

**As a** signed-in member,
**I want to** see new, renamed, and changed conversations appear live in my sidebar,
**So that** my DM list matches reality.

### Acceptance Criteria
- Given I am connected, when a conversation I participate in is created or renamed, then I receive a frame with the conversation snapshot on that conversation's scope.
- Given I am added to a group conversation while connected, when the participant change reaches me, then the conversation scope is added to my connection and I receive subsequent events for it.
- Given I am removed from a group conversation while connected, when the participant change reaches me, then the conversation scope is dropped and I receive no further events for it.

### Out of Scope
- Creating, renaming, or leaving conversations — done through the social REST API.

---

## US-008: Reconnect and catch up without missing or duplicating messages

**As a** member whose connection dropped,
**I want to** reconnect and catch up on what I missed,
**So that** a network hiccup never loses or duplicates a conversation.

### Acceptance Criteria
- Given my connection dropped and I reconnect successfully, then the gateway does not replay any events that occurred while I was away; I fetch the gap through the messaging history `after` cursor myself.
- Given the same underlying event is delivered to the gateway more than once (at-least-once delivery), when the duplicate arrives while I am connected, then I receive the event at most once per connection.
- Given I reconnect, when the connection is established, then my subscriptions are rebuilt from my current scopes (nothing sticky from the old connection is reused).

### Out of Scope
- Server-side history buffering per user; the gateway is a live pipe only.

---

## US-009: Keep the connection healthy with heartbeats

**As a** member with an unreliable network,
**I want to** the gateway to detect dead connections quickly,
**So that** resources are freed and the client can show a "reconnecting" indicator promptly.

### Acceptance Criteria
- Given my connection is alive, when the gateway sends a heartbeat ping and I answer with a pong within the timeout, then the connection stays open.
- Given my connection is dead (network gone, tab frozen), when I fail to answer heartbeats within the configured timeout, then the gateway closes the connection and frees its state.
- Given my client sends anything other than a heartbeat or a token refresh, when it arrives on the socket, then it is ignored (or the connection is closed for protocol violation) and never treated as a domain request.

### Out of Scope
- Client-side reconnect strategy specifics (the client reconnects with back-off; the gateway does not care how).

---

## US-010: Stay connected across access-token expiry

**As a** long-lived member,
**I want to** refresh my access token without dropping my connection,
**So that** browsing for hours doesn't keep interrupting me.

### Acceptance Criteria
- Given my access token is close to expiry, when I send an in-band token refresh with a valid new token, then the connection stays open and is re-associated with the new token.
- Given my access token expires and I do not refresh it, when the expiry passes, then the gateway closes my connection with a `token_expired` close reason.
- Given I send an invalid token in an in-band refresh, when the gateway verifies it, then the refresh is rejected and the connection keeps its previous (still valid) token; if the previous token is already expired the connection closes.

### Out of Scope
- Minting refresh tokens or cookies — `identity_service` owns token issuance.

---

## US-011: Degrade gracefully when the platform is unhealthy

**As a** member,
**I want to** get clear, retryable feedback instead of a silent dead connection,
**So that** my client can recover automatically.

### Acceptance Criteria
- Given the subscription-scope service cannot answer when I connect, when my upgrade is attempted, then the connection is refused with a retryable close reason (`service_unavailable`) and the client may retry with back-off.
- Given the gateway's live-event feed is not ready, when I attempt to connect, then the connection is refused with a retryable close reason and the gateway reports itself not ready to the load balancer.
- Given the gateway is shutting down, when new connections arrive, then they are refused and existing connections are drained gracefully rather than dropped mid-frame.

### Out of Scope
- Retrying on the client's behalf — the client owns its back-off policy.

---

## US-012: Observe live delivery health

**As a** platform operator,
**I want to** see how many connections are live, how many events are delivered, and how fresh the event feed is,
**So that** I can detect push-delivery degradation before members notice it.

### Acceptance Criteria
- Given the gateway is running, when I inspect its metrics, then I see the number of active connections, cumulative connection openings with outcome labels, delivered-event counts per event type, reconnect counts, consumer lag per topic, and dropped frames for slow consumers.
- Given a replica's event feed falls behind, when consumer lag exceeds the alert threshold, then the metric reflects it and readiness still holds (readiness is about feed attachment, not lag).
- Given a replica loses its event feed, when that happens, then it reports itself not ready so the load balancer stops sending it new connections.

### Out of Scope
- Any metric label that could identify a user, community, conversation, or message (low-cardinality labels only).
