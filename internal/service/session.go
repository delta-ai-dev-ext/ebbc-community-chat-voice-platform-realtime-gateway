package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/models"
)

// Session tuning defaults.
const (
	defaultWriteTimeout        = 10 * time.Second
	defaultExpiryCheckInterval = time.Second
	defaultMaxInboundBytes     = 4096
	defaultHeartbeatInterval   = 25 * time.Second
	defaultSessionReadDeadline = 60 * time.Second
	closeFrameWriteGracePeriod = 5 * time.Second
)

// SessionConfig tunes one socket lifecycle. Zero values fall back to the defaults.
type SessionConfig struct {
	// HeartbeatInterval is the server ping interval.
	HeartbeatInterval time.Duration
	// ReadDeadline is how long the socket may stay silent (no pong, no message).
	ReadDeadline time.Duration
	// WriteTimeout bounds one frame write.
	WriteTimeout time.Duration
	// ExpiryCheckInterval is how often the token expiry is re-evaluated.
	ExpiryCheckInterval time.Duration
	// MaxInboundBytes bounds a client message.
	MaxInboundBytes int64
}

// withDefaults fills unset fields.
func (c SessionConfig) withDefaults() SessionConfig {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.ReadDeadline <= 0 {
		c.ReadDeadline = defaultSessionReadDeadline
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.ExpiryCheckInterval <= 0 {
		c.ExpiryCheckInterval = defaultExpiryCheckInterval
	}
	if c.MaxInboundBytes <= 0 {
		c.MaxInboundBytes = defaultMaxInboundBytes
	}
	return c
}

// Session owns the lifecycle of one upgraded socket: exactly one reader goroutine (this one)
// and exactly one writer goroutine per connection, heartbeat pings, pong-driven read
// deadlines, in-band token refresh and close-code emission. Frames arrive from the hub
// through the connection's bounded send buffer; the consumer goroutines never write to the
// socket.
type Session struct {
	conn     *gateway.Connection
	ws       *websocket.Conn
	verifier TokenVerifier
	cfg      SessionConfig
	logger   *zap.Logger
	clock    func() time.Time

	stop     chan struct{}
	stopOnce sync.Once
}

// NewSession builds the lifecycle manager of one live connection.
func NewSession(conn *gateway.Connection, ws *websocket.Conn, verifier TokenVerifier, cfg SessionConfig, logger *zap.Logger, clock func() time.Time) *Session {
	if logger == nil {
		logger = zap.NewNop()
	}
	if clock == nil {
		clock = time.Now
	}
	return &Session{
		conn:     conn,
		ws:       ws,
		verifier: verifier,
		cfg:      cfg.withDefaults(),
		logger:   logger,
		clock:    clock,
		stop:     make(chan struct{}),
	}
}

// Run serves the socket until it closes for any reason, then returns so the caller can
// unregister the connection. drain is closed when the replica starts shutting down; the
// writer then flushes buffered frames and closes with 1001 going away.
func (s *Session) Run(drain <-chan struct{}) {
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		s.writeLoop(drain)
	}()

	s.readLoop()
	s.requestStop()

	select {
	case <-writerDone:
	case <-time.After(s.cfg.WriteTimeout + closeFrameWriteGracePeriod):
		s.logger.Warn("socket writer did not stop in time",
			zap.String("connection_id", s.conn.ID))
	}
	_ = s.ws.Close()
}

// requestStop tells the writer goroutine that the reader is gone.
func (s *Session) requestStop() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// readLoop is the only goroutine reading from the socket.
func (s *Session) readLoop() {
	s.ws.SetReadLimit(s.cfg.MaxInboundBytes)
	_ = s.ws.SetReadDeadline(time.Now().Add(s.cfg.ReadDeadline))

	s.ws.SetPongHandler(func(string) error {
		return s.ws.SetReadDeadline(time.Now().Add(s.cfg.ReadDeadline))
	})
	// Peers may ping us; WriteControl is the only write allowed outside the writer goroutine.
	s.ws.SetPingHandler(func(data string) error {
		_ = s.ws.SetReadDeadline(time.Now().Add(s.cfg.ReadDeadline))
		return s.ws.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(s.cfg.WriteTimeout))
	})

	for {
		if _, raw, err := s.ws.ReadMessage(); err != nil {
			s.logger.Debug("socket read ended",
				zap.String("connection_id", s.conn.ID),
				zap.Error(err),
			)
			return
		} else if !s.handleClientMessage(raw) {
			return
		}
	}
}

// handleClientMessage applies one client-to-server control message. It returns false when the
// connection must stop; every other inbound message is a protocol violation (close 4400).
func (s *Session) handleClientMessage(raw []byte) bool {
	var message models.ClientMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		s.closeProtocolViolation("message is not a JSON control message")
		return false
	}

	switch message.Type {
	case models.ClientMessageHeartbeat:
		s.sendAck(models.AckHeartbeat, true, "")
		return true
	case models.ClientMessageRefreshToken:
		s.handleRefresh(message.AccessToken)
		return true
	default:
		s.closeProtocolViolation("unsupported client message type")
		return false
	}
}

// handleRefresh verifies a refreshed access token like the connect token. A rejected refresh
// keeps the previous token; the connection closes only once that token expires.
func (s *Session) handleRefresh(token string) {
	claims, err := s.verifier.Verify(context.Background(), token)
	if err == nil && claims.UserID != s.conn.UserID {
		// A token for another user must never be bound to this connection.
		err = ErrInvalidToken
	}
	if err != nil {
		reason := models.ReasonInvalidToken
		if errors.Is(err, ErrTokenExpired) {
			reason = models.ReasonTokenExpired
		}
		s.logger.Warn("in-band token refresh rejected",
			zap.String("connection_id", s.conn.ID),
			zap.String("status", reason),
			zap.Error(err),
		)
		s.sendAck(models.AckRefreshToken, false, reason)
		if s.conn.TokenExpired(s.clock()) {
			s.conn.RequestClose(models.CloseTokenExpired, models.ReasonTokenExpired)
		}
		return
	}

	s.conn.Reassociate(claims.SessionID, claims.ExpiresAt)
	s.logger.Debug("in-band token refresh accepted",
		zap.String("connection_id", s.conn.ID),
	)
	s.sendAck(models.AckRefreshToken, true, "")
}

// sendAck queues a control acknowledgement. Acknowledgements share the bounded send buffer;
// a full buffer drops them (they are not event frames).
func (s *Session) sendAck(ackType string, ok bool, errCode string) {
	encoded, err := json.Marshal(models.Ack{Type: ackType, OK: ok, Error: errCode})
	if err != nil {
		return
	}
	if !s.conn.Enqueue(encoded) {
		s.logger.Warn("control acknowledgement dropped",
			zap.String("connection_id", s.conn.ID),
			zap.String("status", ackType),
		)
	}
}

// closeProtocolViolation closes the socket with code 4400.
func (s *Session) closeProtocolViolation(detail string) {
	s.logger.Warn("protocol violation",
		zap.String("connection_id", s.conn.ID),
		zap.String("status", models.ReasonProtocolViolation),
		zap.String("detail", detail),
	)
	s.conn.RequestClose(models.CloseProtocolViolation, models.ReasonProtocolViolation)
}

// writeLoop is the only goroutine writing application frames to the socket.
func (s *Session) writeLoop(drain <-chan struct{}) {
	ping := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ping.Stop()
	expiry := time.NewTicker(s.cfg.ExpiryCheckInterval)
	defer expiry.Stop()

	for {
		select {
		case frame := <-s.conn.Send():
			if err := s.writeFrame(frame); err != nil {
				return
			}
		case <-ping.C:
			if err := s.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.cfg.WriteTimeout)); err != nil {
				return
			}
		case request := <-s.conn.CloseRequests():
			s.closeWith(request)
			return
		case <-expiry.C:
			if s.conn.TokenExpired(s.clock()) {
				s.logger.Info("closing socket with an expired token",
					zap.String("connection_id", s.conn.ID),
					zap.String("status", models.ReasonTokenExpired),
				)
				s.closeWith(gateway.CloseRequest{Code: models.CloseTokenExpired, Reason: models.ReasonTokenExpired})
				return
			}
		case <-drain:
			s.flushPending()
			s.closeWith(gateway.CloseRequest{Code: models.CloseGoingAway, Reason: models.ReasonShutdown})
			return
		case <-s.stop:
			// The reader stopped (peer closed, read error, protocol violation). Honour a
			// close request if one is pending so the client sees the machine-readable code.
			select {
			case request := <-s.conn.CloseRequests():
				s.closeWith(request)
			default:
			}
			return
		}
	}
}

// writeFrame writes one application frame with a bounded write deadline.
func (s *Session) writeFrame(frame []byte) error {
	if err := s.ws.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
		return err
	}
	return s.ws.WriteMessage(websocket.TextMessage, frame)
}

// flushPending drains the bounded send buffer best effort during graceful shutdown.
func (s *Session) flushPending() {
	for {
		select {
		case frame := <-s.conn.Send():
			if err := s.writeFrame(frame); err != nil {
				return
			}
		default:
			return
		}
	}
}

// closeWith sends the close frame carrying the machine-readable code/reason.
func (s *Session) closeWith(request gateway.CloseRequest) {
	payload := websocket.FormatCloseMessage(request.Code, request.Reason)
	_ = s.ws.WriteControl(websocket.CloseMessage, payload, time.Now().Add(s.cfg.WriteTimeout))
}
