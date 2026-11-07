package unit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/models"
	"realtime_gateway/internal/service"
)

// stubVerifier resolves tokens from in-memory maps so the session tests never touch crypto.
type stubVerifier struct {
	claims map[string]*service.Claims
	errs   map[string]error
}

// Verify returns the configured claims/error for a token.
func (v stubVerifier) Verify(_ context.Context, token string) (*service.Claims, error) {
	if err, ok := v.errs[token]; ok {
		return nil, err
	}
	if claims, ok := v.claims[token]; ok {
		return claims, nil
	}
	return nil, service.ErrInvalidToken
}

// sessionHarness runs a real WebSocket session against an in-process HTTP test server.
type sessionHarness struct {
	client *websocket.Conn
	conn   *gateway.Connection
	hub    *gateway.Hub
	drain  chan struct{}
	stop   chan struct{}
}

// close tears the harness down.
func (h *sessionHarness) close() {
	_ = h.client.Close()
	select {
	case <-h.drain:
	default:
		close(h.drain)
	}
	<-h.stop
}

// harnessOptions tunes the harness.
type harnessOptions struct {
	cfg      service.SessionConfig
	verifier service.TokenVerifier
	clock    func() time.Time
	expiry   time.Time
	scopes   models.ScopeSet
	sendBuf  int
}

// newSessionHarness starts the session over a real socket and returns the connected client.
func newSessionHarness(t *testing.T, opts harnessOptions) *sessionHarness {
	t.Helper()

	if opts.cfg.ReadDeadline == 0 {
		opts.cfg.ReadDeadline = 5 * time.Second
	}
	if opts.cfg.HeartbeatInterval == 0 {
		opts.cfg.HeartbeatInterval = time.Second
	}
	if opts.cfg.WriteTimeout == 0 {
		opts.cfg.WriteTimeout = 2 * time.Second
	}
	if opts.cfg.ExpiryCheckInterval == 0 {
		opts.cfg.ExpiryCheckInterval = 20 * time.Millisecond
	}
	if opts.expiry.IsZero() {
		opts.expiry = time.Now().Add(time.Hour)
	}
	if opts.sendBuf == 0 {
		opts.sendBuf = 8
	}
	if opts.scopes == nil {
		opts.scopes = models.ScopeSet{models.UserScope(uuidAt(13))}
	}

	conn := gateway.NewConnection("test-conn", uuidAt(13), uuidAt(30), opts.scopes, opts.expiry, opts.sendBuf)
	hub := gateway.NewHub(newTestMetrics(t), gateway.HubOptions{})
	hub.Register(conn)

	stop := make(chan struct{})
	drain := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(stop)
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		session := service.NewSession(conn, socket, opts.verifier, opts.cfg, zap.NewNop(), opts.clock)
		session.Run(drain)
	}))
	t.Cleanup(server.Close)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	client, _, err := websocket.DefaultDialer.Dial(url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// The session starts as soon as the upgrade is served; a heartbeat round trip below
	// proves the writer goroutine is running.
	return &sessionHarness{client: client, conn: conn, hub: hub, drain: drain, stop: stop}
}

// readAck reads one control acknowledgement from the socket.
func (h *sessionHarness) readAck(t *testing.T) models.Ack {
	t.Helper()
	require.NoError(t, h.client.SetReadDeadline(time.Now().Add(3*time.Second)))
	var ack models.Ack
	require.NoError(t, h.client.ReadJSON(&ack))
	return ack
}

// send writes one client control message.
func (h *sessionHarness) send(t *testing.T, message models.ClientMessage) {
	t.Helper()
	require.NoError(t, h.client.WriteJSON(message))
}

// expectCloseCode asserts the server closes the socket with the given code.
func (h *sessionHarness) expectCloseCode(t *testing.T, want int) {
	t.Helper()
	require.NoError(t, h.client.SetReadDeadline(time.Now().Add(3*time.Second)))
	for {
		_, _, err := h.client.ReadMessage()
		if err == nil {
			continue
		}
		var closeErr *websocket.CloseError
		require.ErrorAs(t, err, &closeErr, "expected a close frame, got %v", err)
		assert.Equal(t, want, closeErr.Code)
		return
	}
}

func TestSessionHeartbeatAck(t *testing.T) {
	t.Parallel()

	harness := newSessionHarness(t, harnessOptions{verifier: stubVerifier{}})
	defer harness.close()

	harness.send(t, models.ClientMessage{Type: models.ClientMessageHeartbeat})
	ack := harness.readAck(t)

	assert.Equal(t, models.AckHeartbeat, ack.Type)
	assert.True(t, ack.OK)
}

func TestSessionRejectsProtocolViolations(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"unknown type":   `{"type":"typing"}`,
		"domain write":   `{"type":"send_message","content":"hi"}`,
		"not json":       `plain text`,
		"missing type":   `{}`,
		"empty document": ``,
	}

	for name, payload := range cases {
		payload := payload
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			harness := newSessionHarness(t, harnessOptions{verifier: stubVerifier{}})
			defer harness.close()

			require.NoError(t, harness.client.WriteMessage(websocket.TextMessage, []byte(payload)))
			harness.expectCloseCode(t, models.CloseProtocolViolation)
		})
	}
}

func TestSessionAcceptsInBandRefresh(t *testing.T) {
	t.Parallel()

	newExpiry := time.Now().Add(2 * time.Hour)
	verifier := stubVerifier{claims: map[string]*service.Claims{
		"fresh-token": {UserID: uuidAt(13), SessionID: uuidAt(31), ExpiresAt: newExpiry},
	}}
	harness := newSessionHarness(t, harnessOptions{verifier: verifier})
	defer harness.close()

	harness.send(t, models.ClientMessage{Type: models.ClientMessageRefreshToken, AccessToken: "fresh-token"})
	ack := harness.readAck(t)

	assert.Equal(t, models.AckRefreshToken, ack.Type)
	assert.True(t, ack.OK)
	assert.WithinDuration(t, newExpiry, harness.conn.TokenExpiry(), time.Second)
	assert.Equal(t, uuidAt(31), harness.conn.SessionID())

	// The connection stays usable after the refresh.
	harness.send(t, models.ClientMessage{Type: models.ClientMessageHeartbeat})
	assert.True(t, harness.readAck(t).OK)
}

func TestSessionRejectsInvalidRefreshAndKeepsToken(t *testing.T) {
	t.Parallel()

	originalExpiry := time.Now().Add(time.Hour)
	verifier := stubVerifier{errs: map[string]error{
		"garbage":     service.ErrInvalidToken,
		"expired-new": service.ErrTokenExpired,
	}}
	harness := newSessionHarness(t, harnessOptions{verifier: verifier, expiry: originalExpiry})
	defer harness.close()

	harness.send(t, models.ClientMessage{Type: models.ClientMessageRefreshToken, AccessToken: "garbage"})
	ack := harness.readAck(t)
	assert.Equal(t, models.AckRefreshToken, ack.Type)
	assert.False(t, ack.OK)
	assert.Equal(t, models.ReasonInvalidToken, ack.Error)

	harness.send(t, models.ClientMessage{Type: models.ClientMessageRefreshToken, AccessToken: "expired-new"})
	ack = harness.readAck(t)
	assert.False(t, ack.OK)
	assert.Equal(t, models.ReasonTokenExpired, ack.Error)

	// The previous (still valid) token is kept and the socket stays open.
	assert.WithinDuration(t, originalExpiry, harness.conn.TokenExpiry(), time.Second)
	harness.send(t, models.ClientMessage{Type: models.ClientMessageHeartbeat})
	assert.True(t, harness.readAck(t).OK)
}

func TestSessionRejectsRefreshForAnotherUser(t *testing.T) {
	t.Parallel()

	verifier := stubVerifier{claims: map[string]*service.Claims{
		"other-user": {UserID: uuidAt(99), SessionID: uuidAt(31), ExpiresAt: time.Now().Add(time.Hour)},
	}}
	harness := newSessionHarness(t, harnessOptions{verifier: verifier})
	defer harness.close()

	harness.send(t, models.ClientMessage{Type: models.ClientMessageRefreshToken, AccessToken: "other-user"})
	ack := harness.readAck(t)
	assert.False(t, ack.OK)
	assert.Equal(t, models.ReasonInvalidToken, ack.Error)
}

func TestSessionClosesExpiredToken(t *testing.T) {
	t.Parallel()

	harness := newSessionHarness(t, harnessOptions{
		verifier: stubVerifier{},
		expiry:   time.Now().Add(150 * time.Millisecond),
		cfg: service.SessionConfig{
			ExpiryCheckInterval: 10 * time.Millisecond,
			ReadDeadline:        5 * time.Second,
			HeartbeatInterval:   time.Second,
			WriteTimeout:        2 * time.Second,
		},
	})
	defer harness.close()

	harness.expectCloseCode(t, models.CloseTokenExpired)
}

func TestSessionRefreshPostponesExpiryClose(t *testing.T) {
	t.Parallel()

	verifier := stubVerifier{claims: map[string]*service.Claims{
		"fresh-token": {UserID: uuidAt(13), SessionID: uuidAt(31), ExpiresAt: time.Now().Add(time.Hour)},
	}}
	harness := newSessionHarness(t, harnessOptions{
		verifier: verifier,
		expiry:   time.Now().Add(150 * time.Millisecond),
		cfg: service.SessionConfig{
			ExpiryCheckInterval: 10 * time.Millisecond,
			ReadDeadline:        5 * time.Second,
			HeartbeatInterval:   time.Second,
			WriteTimeout:        2 * time.Second,
		},
	})
	defer harness.close()

	harness.send(t, models.ClientMessage{Type: models.ClientMessageRefreshToken, AccessToken: "fresh-token"})
	require.True(t, harness.readAck(t).OK)

	// Well past the original expiry the socket is still serving heartbeats.
	time.Sleep(250 * time.Millisecond)
	harness.send(t, models.ClientMessage{Type: models.ClientMessageHeartbeat})
	assert.True(t, harness.readAck(t).OK)
}

func TestSessionDeliversFannedOutFrames(t *testing.T) {
	t.Parallel()

	community := models.CommunityScope(uuidAt(12))
	harness := newSessionHarness(t, harnessOptions{
		verifier: stubVerifier{},
		scopes:   models.ScopeSet{models.UserScope(uuidAt(13)), community},
	})
	defer harness.close()

	frame, err := models.MarshalFrame("message_changed", community, json.RawMessage(`{"message_id":"`+uuidAt(10)+`"}`))
	require.NoError(t, err)
	require.Equal(t, 1, harness.hub.Deliver(community, "message_changed", frame))

	require.NoError(t, harness.client.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, raw, err := harness.client.ReadMessage()
	require.NoError(t, err)

	var received models.Frame
	require.NoError(t, json.Unmarshal(raw, &received))
	assert.Equal(t, "message_changed", received.Type)
	assert.Equal(t, string(community), received.Scope)
	assert.Contains(t, string(received.Data), uuidAt(10))
}

func TestSessionDrainsWithGoingAway(t *testing.T) {
	t.Parallel()

	community := models.CommunityScope(uuidAt(12))
	harness := newSessionHarness(t, harnessOptions{
		verifier: stubVerifier{},
		scopes:   models.ScopeSet{community},
	})

	// A frame queued right before the drain must still be flushed.
	frame, err := models.MarshalFrame("message_changed", community, json.RawMessage(`{"message_id":"`+uuidAt(10)+`"}`))
	require.NoError(t, err)
	require.Equal(t, 1, harness.hub.Deliver(community, "message_changed", frame))

	close(harness.drain)

	require.NoError(t, harness.client.SetReadDeadline(time.Now().Add(3*time.Second)))
	seenFrame := false
	for {
		messageType, raw, readErr := harness.client.ReadMessage()
		if readErr != nil {
			var closeErr *websocket.CloseError
			require.ErrorAs(t, readErr, &closeErr)
			assert.Equal(t, models.CloseGoingAway, closeErr.Code)
			assert.True(t, seenFrame, "the buffered frame must be flushed before the close")
			_ = harness.client.Close()
			<-harness.stop
			return
		}
		if messageType == websocket.TextMessage && strings.Contains(string(raw), uuidAt(10)) {
			seenFrame = true
		}
	}
}

func TestSessionClosesOnPongTimeout(t *testing.T) {
	t.Parallel()

	harness := newSessionHarness(t, harnessOptions{
		verifier: stubVerifier{},
		cfg: service.SessionConfig{
			// No pong handler on the client side: the read deadline expires and the server
			// stops the session without a close frame.
			ReadDeadline:      120 * time.Millisecond,
			HeartbeatInterval: time.Hour,
			WriteTimeout:      2 * time.Second,
		},
	})
	defer harness.close()

	require.NoError(t, harness.client.SetReadDeadline(time.Now().Add(3*time.Second)))
	for {
		if _, _, err := harness.client.ReadMessage(); err != nil {
			break
		}
	}
	select {
	case <-harness.stop:
	case <-time.After(3 * time.Second):
		t.Fatal("session did not stop after the read deadline")
	}
}

func TestStubVerifierUnknownTokenIsInvalid(t *testing.T) {
	t.Parallel()

	_, err := stubVerifier{}.Verify(context.Background(), "unknown")
	assert.ErrorIs(t, err, service.ErrInvalidToken)
	assert.False(t, errors.Is(err, service.ErrTokenExpired))
}
