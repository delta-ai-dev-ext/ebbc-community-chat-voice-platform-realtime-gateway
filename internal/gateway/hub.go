package gateway

import (
	"sync"
	"time"

	"realtime_gateway/internal/models"
)

// Default tuning of the registry.
const (
	// defaultReconnectWindow is how soon after a disconnect a new connection of the same
	// user counts as a reconnect.
	defaultReconnectWindow = 60 * time.Second
	// defaultSlowDropLimit is how many consecutive dropped frames turn a slow socket into
	// a connection that is closed so the client can reconnect cleanly.
	defaultSlowDropLimit = 50
	// lastSeenPruneThreshold bounds the reconnect bookkeeping map.
	lastSeenPruneThreshold = 4096
)

// HubOptions tunes the registry. Zero values fall back to the defaults above.
type HubOptions struct {
	// Clock is the injected time source used for reconnect bookkeeping.
	Clock func() time.Time
	// ReconnectWindow is the recency window of the reconnect metric.
	ReconnectWindow time.Duration
	// SlowDropLimit is the consecutive drop count that closes a socket.
	SlowDropLimit int
}

// Hub is the per-replica registry of live sockets. It supports registration/removal, frame
// delivery to matching scopes and live scope add/drop for one user's connections.
type Hub struct {
	mu      sync.RWMutex
	conns   map[*Connection]struct{}
	byUser  map[string]map[*Connection]struct{}
	lastOff map[string]time.Time

	metrics         *Metrics
	clock           func() time.Time
	reconnectWindow time.Duration
	slowDropLimit   int
}

// NewHub builds the registry.
func NewHub(metrics *Metrics, opts HubOptions) *Hub {
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	window := opts.ReconnectWindow
	if window <= 0 {
		window = defaultReconnectWindow
	}
	limit := opts.SlowDropLimit
	if limit <= 0 {
		limit = defaultSlowDropLimit
	}
	return &Hub{
		conns:           make(map[*Connection]struct{}),
		byUser:          make(map[string]map[*Connection]struct{}),
		lastOff:         make(map[string]time.Time),
		metrics:         metrics,
		clock:           clock,
		reconnectWindow: window,
		slowDropLimit:   limit,
	}
}

// Register adds a live connection and records a reconnect when the same user was connected
// to this replica shortly before.
func (h *Hub) Register(conn *Connection) {
	h.mu.Lock()
	if last, ok := h.lastOff[conn.UserID]; ok {
		if h.clock().Sub(last) <= h.reconnectWindow {
			h.metrics.ReconnectObserved()
		}
		delete(h.lastOff, conn.UserID)
	}
	h.conns[conn] = struct{}{}
	if h.byUser[conn.UserID] == nil {
		h.byUser[conn.UserID] = make(map[*Connection]struct{})
	}
	h.byUser[conn.UserID][conn] = struct{}{}
	h.mu.Unlock()

	h.metrics.ConnectionActivated()
}

// Unregister removes a connection and remembers when the user went away.
func (h *Hub) Unregister(conn *Connection) {
	h.mu.Lock()
	if _, ok := h.conns[conn]; !ok {
		h.mu.Unlock()
		return
	}
	delete(h.conns, conn)
	if userConns, ok := h.byUser[conn.UserID]; ok {
		delete(userConns, conn)
		if len(userConns) == 0 {
			delete(h.byUser, conn.UserID)
		}
	}
	h.lastOff[conn.UserID] = h.clock()
	h.pruneLastOffLocked()
	h.mu.Unlock()

	h.metrics.ConnectionDeactivated()
}

// pruneLastOffLocked drops expired reconnect bookkeeping. Callers hold the write lock.
func (h *Hub) pruneLastOffLocked() {
	if len(h.lastOff) <= lastSeenPruneThreshold {
		return
	}
	cutoff := h.clock().Add(-h.reconnectWindow)
	for userID, last := range h.lastOff {
		if last.Before(cutoff) {
			delete(h.lastOff, userID)
		}
	}
}

// Count returns the number of live connections on this replica.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.conns)
}

// UserConnectionCount returns the live connection count of one user.
func (h *Hub) UserConnectionCount(userID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.byUser[userID])
}

// Deliver queues the encoded frame on every connection subscribed to scope and returns how
// many sockets accepted it. A full send buffer drops the frame for that socket and counts
// it; a socket that keeps overflowing is closed with the slow-consumer code.
func (h *Hub) Deliver(scope models.Scope, eventType string, frame []byte) int {
	h.mu.RLock()
	targets := make([]*Connection, 0, len(h.conns))
	for conn := range h.conns {
		if conn.HasScope(scope) {
			targets = append(targets, conn)
		}
	}
	h.mu.RUnlock()

	delivered := 0
	for _, conn := range targets {
		if conn.Enqueue(frame) {
			delivered++
			continue
		}
		h.metrics.FrameDropped(eventType)
		if conn.RecordDrop() >= int64(h.slowDropLimit) {
			conn.RequestClose(models.CloseSlowConsumer, models.ReasonSlowConsumer)
		}
	}
	return delivered
}

// AddUserScope subscribes every live connection of userID to scope and returns how many
// connections changed.
func (h *Hub) AddUserScope(userID string, scope models.Scope) int {
	return h.forEachUserConnection(userID, func(conn *Connection) bool {
		return conn.AddScope(scope)
	})
}

// DropUserScope unsubscribes every live connection of userID from scope and returns how many
// connections changed.
func (h *Hub) DropUserScope(userID string, scope models.Scope) int {
	return h.forEachUserConnection(userID, func(conn *Connection) bool {
		return conn.DropScope(scope)
	})
}

// forEachUserConnection applies apply to each live connection of the user.
func (h *Hub) forEachUserConnection(userID string, apply func(*Connection) bool) int {
	h.mu.RLock()
	userConns := make([]*Connection, 0, len(h.byUser[userID]))
	for conn := range h.byUser[userID] {
		userConns = append(userConns, conn)
	}
	h.mu.RUnlock()

	changed := 0
	for _, conn := range userConns {
		if apply(conn) {
			changed++
		}
	}
	return changed
}
