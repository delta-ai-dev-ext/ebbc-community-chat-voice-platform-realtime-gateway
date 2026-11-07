package integration

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"realtime_gateway/internal/config"
	"realtime_gateway/internal/contracts"
	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/handler"
	"realtime_gateway/internal/service"
)

const (
	integrationIssuer   = "identity_service"
	integrationAudience = "realtime_gateway"
)

// stack is the fully wired gateway running in-process: real router, real hub, real event
// pipeline, real token verification and a fake live-event feed. Only the broker and the
// scope provider are faked, so no Docker/network/broker is involved.
type stack struct {
	t        *testing.T
	cfg      *config.Config
	server   *httptest.Server
	web      *handler.Handler
	hub      *gateway.Hub
	metrics  *gateway.Metrics
	pipeline *gateway.Pipeline
	feed     *stubFeed
	drain    chan struct{}
	key      *rsa.PrivateKey
}

// stubFeed is the in-process stand-in for the Kafka broadcast reader.
type stubFeed struct {
	attached atomic.Bool
}

// Attached reports whether the feed is attached.
func (f *stubFeed) Attached() bool { return f.attached.Load() }

// integrationEnv is a valid 12-factor environment for the wired stack.
func integrationEnv() map[string]string {
	return map[string]string{
		"PORT":                     "8080",
		"KAFKA_BROKERS":            "kafka:9092",
		"MESSAGING_SERVICE_URL":    "http://messaging_service:8080",
		"INTERNAL_SERVICE_TOKEN":   "internal-token",
		"IDENTITY_PUBLIC_KEY_PATH": "/etc/keys/identity.pem",
		"IDENTITY_TOKEN_ISSUER":    integrationIssuer,
		"IDENTITY_TOKEN_AUDIENCE":  integrationAudience,
		"WS_ALLOWED_ORIGINS":       "http://localhost:3000",
	}
}

// stackOptions tunes the stack under test.
type stackOptions struct {
	resolver service.ScopeResolver
	attached bool
}

// newStack wires the gateway from configuration exactly like cmd/server does.
func newStack(t *testing.T, opts stackOptions) *stack {
	t.Helper()

	cfg, err := config.Load(func(key string) string { return integrationEnv()[key] })
	require.NoError(t, err)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	verifier, err := service.NewJWTVerifier(
		service.NewKeySet(service.VerificationKey{KID: "current", Key: &key.PublicKey}),
		service.VerifierOptions{Issuer: integrationIssuer, Audience: integrationAudience},
	)
	require.NoError(t, err)

	registry := prometheus.NewRegistry()
	metrics := gateway.NewMetrics(registry)
	hub := gateway.NewHub(metrics, gateway.HubOptions{})
	pipeline := gateway.NewPipeline(hub, contracts.AllTopics, 5*time.Minute, zap.NewNop(), metrics)

	feed := &stubFeed{}
	feed.attached.Store(opts.attached)

	resolver := opts.resolver
	if resolver == nil {
		resolver = &fixedResolver{
			result: &service.ScopeResult{
				UserID:          userID,
				CommunityIDs:    []string{communityID},
				ConversationIDs: []string{conversationID},
			},
		}
	}

	drain := make(chan struct{})
	web := handler.New(handler.Deps{
		Config: handler.Config{
			AllowedOrigins:    cfg.WSAllowedOrigins,
			SendBufferSize:    cfg.WSSendBufferSize,
			HeartbeatInterval: 500 * time.Millisecond,
			ReadDeadline:      cfg.WSReadDeadline,
			WriteTimeout:      2 * time.Second,
		},
		Verifier: verifier,
		Resolver: resolver,
		Hub:      hub,
		Feed:     feed,
		Metrics:  metrics,
		Registry: registry,
		Logger:   zap.NewNop(),
		Drain:    drain,
	})

	server := httptest.NewServer(web.Router())
	t.Cleanup(func() {
		select {
		case <-drain:
		default:
			close(drain)
		}
		server.Close()
	})

	return &stack{
		t:        t,
		cfg:      cfg,
		server:   server,
		web:      web,
		hub:      hub,
		metrics:  metrics,
		pipeline: pipeline,
		feed:     feed,
		drain:    drain,
		key:      key,
	}
}

// userID, communityID and conversationID are the fixed identities of the integration tests.
const (
	userID         = "11111111-1111-4111-8111-111111111111"
	communityID    = "22222222-2222-4222-8222-222222222222"
	conversationID = "33333333-3333-4333-8333-333333333333"
	otherUserID    = "44444444-4444-4444-8444-444444444444"
)

// fixedResolver returns a fixed scope result.
type fixedResolver struct {
	result *service.ScopeResult
	err    error
}

// Resolve returns the configured scope result.
func (r *fixedResolver) Resolve(_ context.Context, _ service.ScopeRequest) (*service.ScopeResult, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.result, nil
}

// Configured reports a usable resolver.
func (r *fixedResolver) Configured() bool { return true }

// token mints a valid access token for the given user.
func (s *stack) token(user string, expiry time.Time) string {
	s.t.Helper()

	claims := jwt.MapClaims{
		"sub":            user,
		"sid":            "55555555-5555-4555-8555-555555555555",
		"iss":            integrationIssuer,
		"aud":            integrationAudience,
		"iat":            time.Now().Add(-time.Minute).Unix(),
		"exp":            expiry.Unix(),
		"email_verified": true,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "current"
	signed, err := token.SignedString(s.key)
	require.NoError(s.t, err)
	return signed
}

// wsURL builds the WebSocket URL of the stack.
func (s *stack) wsURL(query string) string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http") + "/ws" + query
}

// dial opens an authenticated WebSocket.
func (s *stack) dial(query string, header http.Header) (*websocket.Conn, error) {
	s.t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(s.wsURL(query), header)
	return conn, err
}

// statusOf performs a plain HTTP GET and returns the status code and decoded JSON body.
func (s *stack) statusOf(path string) (int, map[string]any) {
	s.t.Helper()

	resp, err := http.Get(s.server.URL + path)
	require.NoError(s.t, err)
	defer closeBody(resp)

	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// event builds a raw envelope+payload message of the given event type.
func event(t *testing.T, eventType string, eventID string, payload map[string]any) []byte {
	t.Helper()

	envelope := map[string]any{
		"event_id":       eventID,
		"event_type":     eventType,
		"event_version":  1,
		"occurred_at":    "2026-02-11T10:00:00Z",
		"producer":       "messaging_service",
		"trace_id":       "4bf92f3577b34da6a3ce929d0e0e4736",
		"correlation_id": "66666666-6666-4666-8666-666666666666",
		"causation_id":   nil,
		"schema_ref":     fmt.Sprintf("EVENT_CONTRACTS.md#%s-v1", eventType),
	}
	raw, err := json.Marshal(map[string]any{"envelope": envelope, "payload": payload})
	require.NoError(t, err)
	return raw
}

// messagePayload is a valid message_changed payload for a channel message.
func messagePayload() map[string]any {
	return map[string]any{
		"message_id":         "77777777-7777-4777-8777-777777777777",
		"change":             "created",
		"container_type":     "channel",
		"container_id":       "88888888-8888-4888-8888-888888888888",
		"community_id":       communityID,
		"author_user_id":     otherUserID,
		"thread_root_id":     nil,
		"content":            "hello from the integration test",
		"attachments":        []any{},
		"mentioned_user_ids": []any{},
		"reactions":          []any{},
		"deletion_reason":    nil,
		"created_at":         "2026-02-11T10:00:00Z",
		"edited_at":          nil,
		"message_version":    1,
	}
}

// conversationPayload is a valid conversation_changed payload adding a participant.
func conversationPayload() map[string]any {
	return map[string]any{
		"conversation_id":      conversationID,
		"kind":                 "group",
		"change":               "participants_changed",
		"name":                 "Integration",
		"participant_user_ids": []any{userID, otherUserID},
		"added_user_ids":       []any{otherUserID},
		"removed_user_ids":     []any{},
		"conversation_version": 2,
	}
}

// readFrame reads one pushed frame from a client socket.
func readFrame(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, raw, err := conn.ReadMessage()
	require.NoError(t, err)

	var frame map[string]any
	require.NoError(t, json.Unmarshal(raw, &frame))
	return frame
}

// expectNextFrameIs reads the next frame and asserts its type. It is used as the sentinel of
// a negative assertion: because a socket delivers frames in order, seeing the sentinel first
// proves nothing else was pushed to this connection in between.
func expectNextFrameIs(t *testing.T, conn *websocket.Conn, wantType string) map[string]any {
	t.Helper()

	frame := readFrame(t, conn)
	require.Equal(t, wantType, frame["type"], "expected the sentinel frame %q, got %v", wantType, frame["type"])
	return frame
}

// notificationPayload builds a notification payload for the connected user; it doubles as the
// ordering sentinel of the negative assertions.
func notificationPayload() map[string]any {
	return map[string]any{
		"notification_id":   "99999999-9999-4999-8999-999999999999",
		"recipient_user_id": userID,
		"kind":              "mention",
		"title":             "You were mentioned",
		"summary":           "in #general",
		"link":              "/channels/88888888-8888-4888-8888-888888888888/messages/77777777-7777-4777-8777-777777777777",
		"unread_count":      1,
	}
}

// expectCloseCode reads until the socket closes and asserts the close code.
func expectCloseCode(t *testing.T, conn *websocket.Conn, want int) {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			var closeErr *websocket.CloseError
			require.ErrorAs(t, err, &closeErr, "expected a close frame, got %v", err)
			require.Equal(t, want, closeErr.Code)
			return
		}
	}
}

// closeSocket closes a client socket, ignoring the (irrelevant) close error.
func closeSocket(conn *websocket.Conn) {
	if conn != nil {
		_ = conn.Close()
	}
}

// closeBody closes an HTTP response body, ignoring the error.
func closeBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}
