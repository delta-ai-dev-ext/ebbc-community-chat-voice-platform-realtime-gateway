package gateway

import (
	"sync"
	"sync/atomic"
	"time"

	"realtime_gateway/internal/models"
)

// CloseRequest asks the socket writer to send a close frame and stop.
type CloseRequest struct {
	// Code is the WebSocket close code (see internal/models close codes).
	Code int
	// Reason is the stable machine-readable close reason (snake_case).
	Reason string
}

// Connection is one live WebSocket subscription: identity, the scope set it is entitled to,
// its token expiry and a bounded outbound frame buffer. All fields are safe for concurrent
// use; frames are only ever written by the connection's single writer goroutine.
type Connection struct {
	// ID is an opaque, non-identifying connection id used in logs.
	ID string
	// UserID is the authenticated user id. It is never logged.
	UserID string

	send     chan []byte
	closeReq chan CloseRequest
	closing  atomic.Bool
	drops    atomic.Int64

	mu          sync.RWMutex
	sessionID   string
	tokenExpiry time.Time
	scopes      map[models.Scope]struct{}
}

// NewConnection registers a freshly authenticated socket. scopeSet is copied; bufferSize
// bounds the outbound frame buffer (the slow-consumer policy).
func NewConnection(id, userID, sessionID string, scopeSet models.ScopeSet, tokenExpiry time.Time, bufferSize int) *Connection {
	if bufferSize < 1 {
		bufferSize = 1
	}
	conn := &Connection{
		ID:          id,
		UserID:      userID,
		send:        make(chan []byte, bufferSize),
		closeReq:    make(chan CloseRequest, 1),
		sessionID:   sessionID,
		tokenExpiry: tokenExpiry,
		scopes:      make(map[models.Scope]struct{}, len(scopeSet)),
	}
	for _, scope := range scopeSet {
		conn.scopes[scope] = struct{}{}
	}
	return conn
}

// Send returns the outbound frame channel consumed by the writer goroutine.
func (c *Connection) Send() <-chan []byte { return c.send }

// CloseRequests returns the channel carrying at most one pending close instruction.
func (c *Connection) CloseRequests() <-chan CloseRequest { return c.closeReq }

// Scopes returns a snapshot of the connection's scope set.
func (c *Connection) Scopes() []models.Scope {
	c.mu.RLock()
	defer c.mu.RUnlock()

	scopes := make([]models.Scope, 0, len(c.scopes))
	for scope := range c.scopes {
		scopes = append(scopes, scope)
	}
	return scopes
}

// HasScope reports whether the connection currently subscribes to scope.
func (c *Connection) HasScope(scope models.Scope) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.scopes[scope]
	return ok
}

// AddScope subscribes the connection to scope and reports whether it was added.
func (c *Connection) AddScope(scope models.Scope) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.scopes[scope]; ok {
		return false
	}
	c.scopes[scope] = struct{}{}
	return true
}

// DropScope unsubscribes the connection from scope and reports whether it was present.
func (c *Connection) DropScope(scope models.Scope) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.scopes[scope]; !ok {
		return false
	}
	delete(c.scopes, scope)
	return true
}

// SessionID returns the session id of the token currently associated with the connection.
func (c *Connection) SessionID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.sessionID
}

// TokenExpiry returns the expiry of the token currently associated with the connection.
func (c *Connection) TokenExpiry() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.tokenExpiry
}

// Reassociate applies a successfully refreshed token to the connection.
func (c *Connection) Reassociate(sessionID string, expiry time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sessionID = sessionID
	c.tokenExpiry = expiry
}

// TokenExpired reports whether the associated token has expired at now.
func (c *Connection) TokenExpired(now time.Time) bool {
	expiry := c.TokenExpiry()
	return !expiry.IsZero() && !now.Before(expiry)
}

// Enqueue offers one encoded frame to the bounded send buffer without blocking. A full
// buffer means the consumer is too slow: the caller drops and counts the frame.
func (c *Connection) Enqueue(frame []byte) bool {
	if c.closing.Load() {
		return false
	}
	select {
	case c.send <- frame:
		c.drops.Store(0)
		return true
	default:
		return false
	}
}

// RecordDrop counts one dropped frame and returns the current consecutive drop streak.
func (c *Connection) RecordDrop() int64 {
	return c.drops.Add(1)
}

// ResetDrops clears the consecutive drop streak.
func (c *Connection) ResetDrops() { c.drops.Store(0) }

// Closing reports whether a close has already been requested for the connection.
func (c *Connection) Closing() bool { return c.closing.Load() }

// RequestClose asks the writer goroutine to close the socket with the given code/reason. It
// never blocks: only the first request wins, later ones are ignored.
func (c *Connection) RequestClose(code int, reason string) {
	c.closing.Store(true)
	select {
	case c.closeReq <- CloseRequest{Code: code, Reason: reason}:
	default:
	}
}
