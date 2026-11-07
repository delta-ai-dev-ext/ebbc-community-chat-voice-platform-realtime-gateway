package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/models"
	"realtime_gateway/internal/service"
)

// Route templates used as low-cardinality metric labels.
const (
	routeWebSocket   = "/ws"
	routeHealthz     = "/healthz"
	routeReadyz      = "/readyz"
	routeMetrics     = "/metrics"
	routeUnmatched   = "unmatched"
	handshakeTimeout = 5 * time.Second
)

// FeedStatus reports whether the live-event feed is attached on this replica.
type FeedStatus interface {
	Attached() bool
}

// Config is the handler-level configuration.
type Config struct {
	// AllowedOrigins is the WebSocket upgrade origin allow-list ("*" allows every origin).
	AllowedOrigins []string
	// SendBufferSize bounds each connection's outbound frame buffer.
	SendBufferSize int
	// HeartbeatInterval is the server ping interval.
	HeartbeatInterval time.Duration
	// ReadDeadline is the socket read/pong deadline.
	ReadDeadline time.Duration
	// WriteTimeout bounds a single socket write.
	WriteTimeout time.Duration
}

// Deps is the dependency set of the HTTP surface.
type Deps struct {
	Config   Config
	Verifier service.TokenVerifier
	Resolver service.ScopeResolver
	Hub      *gateway.Hub
	Feed     FeedStatus
	Metrics  *gateway.Metrics
	Registry *prometheus.Registry
	Logger   *zap.Logger
	Clock    func() time.Time
	// Drain is closed when the replica starts graceful shutdown.
	Drain <-chan struct{}
}

// Handler serves /ws, /healthz, /readyz and /metrics.
type Handler struct {
	cfg       Config
	verifier  service.TokenVerifier
	resolver  service.ScopeResolver
	hub       *gateway.Hub
	feed      FeedStatus
	metrics   *gateway.Metrics
	httpStats *HTTPMetrics
	registry  *prometheus.Registry
	logger    *zap.Logger
	clock     func() time.Time
	drain     <-chan struct{}

	draining atomic.Bool
	sessions sync.WaitGroup
}

// New builds the handler.
func New(deps Deps) *Handler {
	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	if deps.Config.SendBufferSize < 1 {
		deps.Config.SendBufferSize = 64
	}
	return &Handler{
		cfg:       deps.Config,
		verifier:  deps.Verifier,
		resolver:  deps.Resolver,
		hub:       deps.Hub,
		feed:      deps.Feed,
		metrics:   deps.Metrics,
		httpStats: NewHTTPMetrics(deps.Registry),
		registry:  deps.Registry,
		logger:    logger,
		clock:     clock,
		drain:     deps.Drain,
	}
}

// Router builds the gin engine with the RED middleware, the routes and the metrics endpoint.
func (h *Handler) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	engine := gin.New()
	engine.Use(recovery(h.logger), instrument(h.httpStats, h.logger))

	engine.GET("/ws", h.handleWebSocket)
	engine.GET("/healthz", h.handleHealthz)
	engine.GET("/readyz", h.handleReadyz)

	gatherer := prometheus.Gatherer(prometheus.DefaultGatherer)
	if h.registry != nil {
		gatherer = h.registry
	}
	engine.GET("/metrics", gin.WrapH(promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})))

	return engine
}

// BeginDrain flips readiness to 503 and refuses new upgrades.
func (h *Handler) BeginDrain() { h.draining.Store(true) }

// Draining reports whether the replica stopped accepting upgrades.
func (h *Handler) Draining() bool { return h.draining.Load() }

// WaitForSessions waits until every live socket has closed or ctx expires.
func (h *Handler) WaitForSessions(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		h.sessions.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handleHealthz is the liveness probe: it never fails because of downstream problems.
func (h *Handler) handleHealthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// handleReadyz is the readiness probe: 200 only while the feed is attached, the scope
// resolver is configured and the replica is not draining.
func (h *Handler) handleReadyz(c *gin.Context) {
	if h.draining.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "detail": "shutting down"})
		return
	}
	if h.feed == nil || !h.feed.Attached() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "detail": "live event feed is not attached"})
		return
	}
	if !h.resolverConfigured() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "detail": "subscription scope client is not configured"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// resolverConfigured reports whether the scope resolver is usable.
func (h *Handler) resolverConfigured() bool {
	if h.resolver == nil {
		return false
	}
	if configurable, ok := h.resolver.(interface{ Configured() bool }); ok {
		return configurable.Configured()
	}
	return true
}

// handleWebSocket performs the authenticated upgrade and then serves the socket lifecycle.
func (h *Handler) handleWebSocket(c *gin.Context) {
	trace := service.TraceFromHeader(c.Request.Header)

	if h.draining.Load() {
		h.refuse(c, gateway.ResultRejectedNotReady, http.StatusServiceUnavailable,
			"service_unavailable", "the gateway is shutting down", models.CloseServiceUnavailable)
		return
	}
	if h.feed == nil || !h.feed.Attached() {
		h.refuse(c, gateway.ResultRejectedNotReady, http.StatusServiceUnavailable,
			"service_unavailable", "the live event feed is not attached", models.CloseServiceUnavailable)
		return
	}

	token := tokenFromRequest(c.Request)
	if token == "" {
		h.refuse(c, gateway.ResultRejectedAuth, http.StatusBadRequest,
			"invalid_token", "an access token is required", models.CloseInvalidToken)
		return
	}

	claims, err := h.verifier.Verify(c.Request.Context(), token)
	if err != nil {
		if errors.Is(err, service.ErrTokenExpired) {
			h.refuse(c, gateway.ResultRejectedAuth, http.StatusUnauthorized,
				"token_expired", "the access token has expired", models.CloseTokenExpired)
			return
		}
		h.logger.Warn("access token rejected",
			append(trace.LogFields(), zap.String("status", "rejected_auth"), zap.Error(err))...)
		h.refuse(c, gateway.ResultRejectedAuth, http.StatusUnauthorized,
			"invalid_token", "the access token is invalid", models.CloseInvalidToken)
		return
	}

	scopes, err := h.resolver.Resolve(c.Request.Context(), service.ScopeRequest{
		UserID:      claims.UserID,
		Traceparent: trace.Traceparent,
		TraceID:     trace.TraceID,
	})
	if err != nil {
		h.logger.Warn("subscription scopes unavailable",
			append(trace.LogFields(), zap.String("status", "rejected_scopes"), zap.Error(err))...)
		h.refuse(c, gateway.ResultRejectedScopes, http.StatusServiceUnavailable,
			"service_unavailable", "subscription scopes are temporarily unavailable", models.CloseServiceUnavailable)
		return
	}

	// Authentication and scope resolution happened before the upgrade: a connection is only
	// ever registered with verified scopes.
	socket, err := h.upgrader().Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed",
			append(trace.LogFields(), zap.Error(err))...)
		h.metrics.ConnectionOpened(gateway.ResultRejectedNotReady)
		return
	}

	connID := newConnectionID()
	conn := gateway.NewConnection(
		connID,
		claims.UserID,
		claims.SessionID,
		models.NewScopeSet(claims.UserID, scopes.CommunityIDs, scopes.ConversationIDs),
		claims.ExpiresAt,
		h.cfg.SendBufferSize,
	)

	h.hub.Register(conn)
	h.metrics.ConnectionOpened(gateway.ResultAccepted)
	h.logger.Info("live connection established",
		append(trace.LogFields(),
			zap.String("connection_id", connID),
			zap.Int("scopes", len(conn.Scopes())),
			zap.String("status", "accepted"),
		)...)

	h.sessions.Add(1)
	defer func() {
		h.hub.Unregister(conn)
		h.sessions.Done()
		h.logger.Info("live connection closed",
			zap.String("connection_id", connID),
			zap.String("status", "closed"),
		)
	}()

	session := service.NewSession(conn, socket, h.verifier, service.SessionConfig{
		HeartbeatInterval: h.cfg.HeartbeatInterval,
		ReadDeadline:      h.cfg.ReadDeadline,
		WriteTimeout:      h.cfg.WriteTimeout,
	}, h.logger, h.clock)

	session.Run(h.drain)
}

// refuse rejects a connection attempt. A real WebSocket handshake is upgraded and then
// closed immediately with the machine-readable close code so the client learns the reason
// (4401/4402/4503); a plain HTTP request gets the matching status code and error body.
func (h *Handler) refuse(c *gin.Context, result string, status int, errorCode, detail string, closeCode int) {
	h.metrics.ConnectionOpened(result)

	if isUpgradeRequest(c.Request) {
		if socket, err := h.upgrader().Upgrade(c.Writer, c.Request, nil); err == nil {
			payload := websocket.FormatCloseMessage(closeCode, reasonForCloseCode(closeCode))
			_ = socket.WriteControl(websocket.CloseMessage, payload, time.Now().Add(handshakeTimeout))
			_ = socket.Close()
			return
		}
	}

	c.AbortWithStatusJSON(status, gin.H{"error": errorCode, "detail": detail})
}

// isUpgradeRequest reports whether the request asks for a WebSocket upgrade.
func isUpgradeRequest(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// reasonForCloseCode maps a close code to its stable machine-readable reason.
func reasonForCloseCode(code int) string {
	switch code {
	case models.CloseProtocolViolation:
		return models.ReasonProtocolViolation
	case models.CloseTokenExpired:
		return models.ReasonTokenExpired
	case models.CloseInvalidToken:
		return models.ReasonInvalidToken
	case models.CloseServiceUnavailable:
		return models.ReasonServiceUnavailable
	case models.CloseGoingAway:
		return models.ReasonShutdown
	case models.CloseSlowConsumer:
		return models.ReasonSlowConsumer
	default:
		return models.ReasonClientClosed
	}
}

// tokenFromRequest extracts the access token from the Authorization header or, for browsers
// that cannot set headers on a WebSocket handshake, the access_token query parameter. The
// value is never logged.
func tokenFromRequest(r *http.Request) string {
	if header := r.Header.Get("Authorization"); header != "" {
		scheme, value, ok := strings.Cut(header, " ")
		if !ok || !strings.EqualFold(scheme, "bearer") {
			return ""
		}
		return strings.TrimSpace(value)
	}
	return strings.TrimSpace(r.URL.Query().Get("access_token"))
}

// upgrader builds the WebSocket upgrader with the configured origin allow-list.
func (h *Handler) upgrader() *websocket.Upgrader {
	return &websocket.Upgrader{
		HandshakeTimeout: handshakeTimeout,
		ReadBufferSize:   1024,
		WriteBufferSize:  4096,
		CheckOrigin:      h.checkOrigin,
	}
}

// checkOrigin enforces the WS_ALLOWED_ORIGINS allow-list. Requests without an Origin header
// (native clients) are allowed: browsers always send one.
func (h *Handler) checkOrigin(r *http.Request) bool {
	origin := strings.TrimRight(strings.TrimSpace(r.Header.Get("Origin")), "/")
	if origin == "" {
		return true
	}
	for _, allowed := range h.cfg.AllowedOrigins {
		if allowed == "*" {
			return true
		}
		if strings.EqualFold(strings.TrimRight(allowed, "/"), origin) {
			return true
		}
	}
	h.logger.Warn("websocket origin rejected", zap.String("status", "rejected_origin"))
	return false
}

// newConnectionID returns an opaque, non-identifying connection id.
func newConnectionID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("conn-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

// recovery logs panics as structured errors and answers 500.
func recovery(log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error("panic recovered",
					zap.String("route", c.FullPath()),
					zap.String("method", c.Request.Method),
					zap.Any("panic", recovered),
				)
				c.AbortWithStatusJSON(http.StatusInternalServerError,
					gin.H{"error": "internal_error", "detail": "unexpected server error"})
			}
		}()
		c.Next()
	}
}
