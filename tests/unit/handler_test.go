package unit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/handler"
	"realtime_gateway/internal/models"
	"realtime_gateway/internal/service"
)

// stubFeed reports a fixed attachment state.
type stubFeed struct {
	attached atomic.Bool
}

// Attached reports the feed state.
func (f *stubFeed) Attached() bool { return f.attached.Load() }

// stubResolver is a configurable API-003 stand-in.
type stubResolver struct {
	result *service.ScopeResult
	err    error
	calls  atomic.Int32
	last   service.ScopeRequest
}

// Resolve returns the configured result or error.
func (r *stubResolver) Resolve(_ context.Context, req service.ScopeRequest) (*service.ScopeResult, error) {
	r.calls.Add(1)
	r.last = req
	if r.err != nil {
		return nil, r.err
	}
	return r.result, nil
}

// Configured reports whether the stub pretends to be configured.
func (r *stubResolver) Configured() bool { return true }

// unconfiguredResolver simulates a replica without a usable scope client.
type unconfiguredResolver struct{}

// Resolve always fails.
func (unconfiguredResolver) Resolve(context.Context, service.ScopeRequest) (*service.ScopeResult, error) {
	return nil, service.ErrScopeUnavailable
}

// Configured reports false.
func (unconfiguredResolver) Configured() bool { return false }

// handlerHarness runs the full HTTP surface in-process.
type handlerHarness struct {
	server   *httptest.Server
	hub      *gateway.Hub
	metrics  *gateway.Metrics
	feed     *stubFeed
	resolver *stubResolver
	web      *handler.Handler
	drain    chan struct{}
	registry *prometheus.Registry
}

// handlerOptions tunes the harness.
type handlerOptions struct {
	origins  []string
	verifier service.TokenVerifier
	resolver service.ScopeResolver
}

// newHandlerHarness builds the HTTP surface and its test server.
func newHandlerHarness(t *testing.T, opts handlerOptions) *handlerHarness {
	t.Helper()

	registry := prometheus.NewRegistry()
	metrics := gateway.NewMetrics(registry)
	hub := gateway.NewHub(metrics, gateway.HubOptions{})
	feed := &stubFeed{}
	feed.attached.Store(true)

	resolver := &stubResolver{result: &service.ScopeResult{
		UserID:          uuidAt(13),
		CommunityIDs:    []string{uuidAt(12)},
		ConversationIDs: []string{uuidAt(16)},
	}}

	var scopeResolver service.ScopeResolver = resolver
	if opts.resolver != nil {
		scopeResolver = opts.resolver
	}
	verifier := opts.verifier
	if verifier == nil {
		verifier = stubVerifier{}
	}
	origins := opts.origins
	if origins == nil {
		origins = []string{"http://localhost:3000"}
	}

	drain := make(chan struct{})
	web := handler.New(handler.Deps{
		Config: handler.Config{
			AllowedOrigins:    origins,
			SendBufferSize:    8,
			HeartbeatInterval: 500 * time.Millisecond,
			ReadDeadline:      5 * time.Second,
			WriteTimeout:      2 * time.Second,
		},
		Verifier: verifier,
		Resolver: scopeResolver,
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

	return &handlerHarness{
		server:   server,
		hub:      hub,
		metrics:  metrics,
		feed:     feed,
		resolver: resolver,
		web:      web,
		drain:    drain,
		registry: registry,
	}
}

// wsURL builds the WebSocket URL of the test server.
func (h *handlerHarness) wsURL(path string) string {
	return "ws" + strings.TrimPrefix(h.server.URL, "http") + path
}

// dial opens a WebSocket with the given headers.
func (h *handlerHarness) dial(t *testing.T, query string, header http.Header) (*websocket.Conn, error) {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(h.wsURL("/ws"+query), header)
	return conn, err
}

// expectCloseCode reads until the socket closes and asserts the close code.
func expectCloseCode(t *testing.T, conn *websocket.Conn, want int) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			var closeErr *websocket.CloseError
			require.ErrorAs(t, err, &closeErr, "expected a close frame, got %v", err)
			assert.Equal(t, want, closeErr.Code)
			return
		}
	}
}

func TestWebSocketAcceptsAndRegistersScopes(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{
		verifier: stubVerifier{claims: map[string]*service.Claims{
			"good-token": {UserID: uuidAt(13), SessionID: uuidAt(30), ExpiresAt: time.Now().Add(time.Hour)},
		}},
	})

	conn, err := harness.dial(t, "?access_token=good-token", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	require.Eventually(t, func() bool { return harness.hub.Count() == 1 }, 3*time.Second, 5*time.Millisecond)
	assert.InDelta(t, 1, counterValue(t, harness.metrics.ConnectionsOpened.WithLabelValues(gateway.ResultAccepted)), 0.0001)
	assert.Equal(t, uuidAt(13), harness.resolver.last.UserID)
	assert.True(t, harness.resolver.last.TraceID != "")

	// The connection subscribes to user, community and conversation scopes.
	require.Eventually(t, func() bool {
		return harness.hub.UserConnectionCount(uuidAt(13)) == 1
	}, 3*time.Second, 5*time.Millisecond)

	community := models.CommunityScope(uuidAt(12))
	frame, err := models.MarshalFrame("message_changed", community, json.RawMessage(`{}`))
	require.NoError(t, err)
	assert.Equal(t, 1, harness.hub.Deliver(community, "message_changed", frame))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, raw, err := conn.ReadMessage()
	require.NoError(t, err)
	assert.Contains(t, string(raw), "message_changed")
}

func TestWebSocketAcceptsAuthorizationHeader(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{
		verifier: stubVerifier{claims: map[string]*service.Claims{
			"header-token": {UserID: uuidAt(13), SessionID: uuidAt(30), ExpiresAt: time.Now().Add(time.Hour)},
		}},
	})

	header := http.Header{}
	header.Set("Authorization", "Bearer header-token")
	conn, err := harness.dial(t, "", header)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	require.Eventually(t, func() bool { return harness.hub.Count() == 1 }, 3*time.Second, 5*time.Millisecond)
}

func TestWebSocketRejections(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		verifier    service.TokenVerifier
		resolver    service.ScopeResolver
		token       string
		authHeader  string
		wantCode    int
		wantResult  string
		wantNoScope bool
	}{
		{
			name:       "missing token",
			verifier:   stubVerifier{},
			token:      "",
			wantCode:   models.CloseInvalidToken,
			wantResult: gateway.ResultRejectedAuth,
		},
		{
			name:       "invalid token",
			verifier:   stubVerifier{errs: map[string]error{"bad": service.ErrInvalidToken}},
			token:      "bad",
			wantCode:   models.CloseInvalidToken,
			wantResult: gateway.ResultRejectedAuth,
		},
		{
			name:       "expired token",
			verifier:   stubVerifier{errs: map[string]error{"old": service.ErrTokenExpired}},
			token:      "old",
			wantCode:   models.CloseTokenExpired,
			wantResult: gateway.ResultRejectedAuth,
		},
		{
			name:       "malformed authorization header",
			verifier:   stubVerifier{},
			authHeader: "Basic abc",
			wantCode:   models.CloseInvalidToken,
			wantResult: gateway.ResultRejectedAuth,
		},
		{
			name:       "scope resolution failure",
			verifier:   stubVerifier{claims: map[string]*service.Claims{"ok": {UserID: uuidAt(13), SessionID: uuidAt(30), ExpiresAt: time.Now().Add(time.Hour)}}},
			resolver:   &stubResolver{err: service.ErrScopeUnavailable},
			token:      "ok",
			wantCode:   models.CloseServiceUnavailable,
			wantResult: gateway.ResultRejectedScopes,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			harness := newHandlerHarness(t, handlerOptions{verifier: tc.verifier, resolver: tc.resolver})

			header := http.Header{}
			if tc.authHeader != "" {
				header.Set("Authorization", tc.authHeader)
			}
			query := ""
			if tc.token != "" {
				query = "?access_token=" + tc.token
			}

			conn, err := harness.dial(t, query, header)
			require.NoError(t, err, "the handshake is upgraded so the client learns the close code")
			defer func() { _ = conn.Close() }()

			expectCloseCode(t, conn, tc.wantCode)
			assert.InDelta(t, 1, counterValue(t, harness.metrics.ConnectionsOpened.WithLabelValues(tc.wantResult)), 0.0001)
			assert.Eventually(t, func() bool { return harness.hub.Count() == 0 }, 3*time.Second, 5*time.Millisecond)
		})
	}
}

func TestWebSocketRefusesWhenNotReady(t *testing.T) {
	t.Parallel()

	t.Run("feed detached", func(t *testing.T) {
		t.Parallel()

		harness := newHandlerHarness(t, handlerOptions{})
		harness.feed.attached.Store(false)

		conn, err := harness.dial(t, "?access_token=any", nil)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		expectCloseCode(t, conn, models.CloseServiceUnavailable)
		assert.InDelta(t, 1, counterValue(t, harness.metrics.ConnectionsOpened.WithLabelValues(gateway.ResultRejectedNotReady)), 0.0001)
	})

	t.Run("draining", func(t *testing.T) {
		t.Parallel()

		harness := newHandlerHarness(t, handlerOptions{
			verifier: stubVerifier{claims: map[string]*service.Claims{
				"ok": {UserID: uuidAt(13), SessionID: uuidAt(30), ExpiresAt: time.Now().Add(time.Hour)},
			}},
		})
		harness.web.BeginDrain()

		conn, err := harness.dial(t, "?access_token=ok", nil)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		expectCloseCode(t, conn, models.CloseServiceUnavailable)
		assert.Equal(t, 0, harness.hub.Count())
	})
}

func TestWebSocketRejectsDisallowedOrigin(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{
		origins: []string{"http://localhost:3000"},
		verifier: stubVerifier{claims: map[string]*service.Claims{
			"ok": {UserID: uuidAt(13), SessionID: uuidAt(30), ExpiresAt: time.Now().Add(time.Hour)},
		}},
	})

	header := http.Header{}
	header.Set("Origin", "http://evil.example")
	_, resp, err := websocket.DefaultDialer.Dial(harness.wsURL("/ws?access_token=ok"), header)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	// The allow-listed origin is accepted.
	allowed := http.Header{}
	allowed.Set("Origin", "http://localhost:3000")
	conn, _, err := websocket.DefaultDialer.Dial(harness.wsURL("/ws?access_token=ok"), allowed)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
}

func TestWebSocketPlainHTTPRequestGetsStatusCodes(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{
		verifier: stubVerifier{errs: map[string]error{"bad": service.ErrInvalidToken}},
	})
	harness.feed.attached.Store(false)

	get := func(path string) (*http.Response, map[string]any) {
		t.Helper()
		resp, err := http.Get(harness.server.URL + path)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(body, &parsed))
		return resp, parsed
	}

	resp, body := get("/ws")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "service_unavailable", body["error"])

	harness.feed.attached.Store(true)

	resp, body = get("/ws")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "invalid_token", body["error"])

	resp, body = get("/ws?access_token=bad")
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "invalid_token", body["error"])
}

func TestProbes(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{})

	get := func(path string) (int, map[string]any) {
		t.Helper()
		resp, err := http.Get(harness.server.URL + path)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(body, &parsed))
		return resp.StatusCode, parsed
	}

	// Liveness never depends on downstream state.
	harness.feed.attached.Store(false)
	status, body := get("/healthz")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "ok", body["status"])

	status, body = get("/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, "degraded", body["status"])
	assert.Contains(t, body["detail"], "feed")

	harness.feed.attached.Store(true)
	status, body = get("/readyz")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "ok", body["status"])

	harness.web.BeginDrain()
	status, body = get("/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body["detail"], "shutting down")
}

func TestReadyzRequiresConfiguredScopeClient(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{resolver: unconfiguredResolver{}})

	resp, err := http.Get(harness.server.URL + "/readyz")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestMetricsEndpointExposesREDMetrics(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{})

	resp, err := http.Get(harness.server.URL + "/healthz")
	require.NoError(t, err)
	_ = resp.Body.Close()

	resp, err = http.Get(harness.server.URL + "/metrics")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "http_requests_total")
	assert.Contains(t, string(body), "http_request_duration_seconds")
	assert.Contains(t, string(body), "realtime_connections_active")
	assert.Contains(t, string(body), "realtime_events_delivered_total")
	assert.Contains(t, string(body), "kafka_consumer_lag")
	assert.Contains(t, string(body), `route="/healthz"`)
}

func TestWebSocketWaitsForSessionsOnShutdown(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{
		verifier: stubVerifier{claims: map[string]*service.Claims{
			"ok": {UserID: uuidAt(13), SessionID: uuidAt(30), ExpiresAt: time.Now().Add(time.Hour)},
		}},
	})

	conn, err := harness.dial(t, "?access_token=ok", nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return harness.hub.Count() == 1 }, 3*time.Second, 5*time.Millisecond)

	close(harness.drain)
	expectCloseCode(t, conn, models.CloseGoingAway)
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, harness.web.WaitForSessions(ctx))
	assert.Eventually(t, func() bool { return harness.hub.Count() == 0 }, 3*time.Second, 5*time.Millisecond)
}

func TestWebSocketClosesSlowConsumers(t *testing.T) {
	t.Parallel()

	harness := newHandlerHarness(t, handlerOptions{
		verifier: stubVerifier{claims: map[string]*service.Claims{
			"ok": {UserID: uuidAt(13), SessionID: uuidAt(30), ExpiresAt: time.Now().Add(time.Hour)},
		}},
	})
	// Shrink the slow-consumer threshold by dropping the hub's default via fan-out pressure:
	// the connection buffer is 8 and the client never reads, so the socket eventually closes
	// with the slow-consumer code.
	conn, err := harness.dial(t, "?access_token=ok", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.Eventually(t, func() bool { return harness.hub.Count() == 1 }, 3*time.Second, 5*time.Millisecond)

	community := models.CommunityScope(uuidAt(12))
	frame, err := models.MarshalFrame("message_changed", community, json.RawMessage(`{"padding":"`+strings.Repeat("x", 2000)+`"}`))
	require.NoError(t, err)

	// The client stops reading; the writer blocks on the TCP buffer, the send buffer fills
	// and the hub closes the socket as persistently slow.
	for i := 0; i < 200; i++ {
		harness.hub.Deliver(community, "message_changed", frame)
	}

	require.Eventually(t, func() bool {
		return counterValue(t, harness.metrics.DroppedFrames.WithLabelValues("message_changed")) > 0
	}, 5*time.Second, 10*time.Millisecond)

	// Drain whatever is buffered and confirm the socket closes.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		if _, _, readErr := conn.ReadMessage(); readErr != nil {
			var closeErr *websocket.CloseError
			if errors.As(readErr, &closeErr) {
				assert.Equal(t, models.CloseSlowConsumer, closeErr.Code)
			}
			return
		}
	}
}
