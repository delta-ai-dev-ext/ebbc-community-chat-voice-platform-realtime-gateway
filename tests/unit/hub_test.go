package unit

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/models"
)

// newTestMetrics builds gateway metrics on an isolated registry.
func newTestMetrics(t *testing.T) *gateway.Metrics {
	t.Helper()
	return gateway.NewMetrics(prometheus.NewRegistry())
}

// counterValue reads the current value of a counter child.
func counterValue(t *testing.T, counter interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, counter.Write(metric))
	return metric.GetCounter().GetValue()
}

// newTestConnection builds a registered connection for the given user and scopes.
func newTestConnection(t *testing.T, id, userID string, scopes ...models.Scope) *gateway.Connection {
	t.Helper()
	expiry := testClock.Add(10 * time.Minute)
	conn := gateway.NewConnection(id, userID, uuidAt(30), models.ScopeSet(scopes), expiry, 8)
	require.NotNil(t, conn)
	return conn
}

// readFrame takes the next encoded frame from a connection.
func readFrame(t *testing.T, conn *gateway.Connection) models.Frame {
	t.Helper()
	select {
	case raw := <-conn.Send():
		var frame models.Frame
		require.NoError(t, json.Unmarshal(raw, &frame))
		return frame
	default:
		t.Fatal("expected a frame on the connection")
		return models.Frame{}
	}
}

// expectNoFrame asserts the connection's send buffer is empty.
func expectNoFrame(t *testing.T, conn *gateway.Connection) {
	t.Helper()
	select {
	case raw := <-conn.Send():
		t.Fatalf("unexpected frame: %s", raw)
	default:
	}
}

// expectCloseRequest asserts a close request with the given code is pending.
func expectCloseRequest(t *testing.T, conn *gateway.Connection, code int) {
	t.Helper()
	select {
	case req := <-conn.CloseRequests():
		assert.Equal(t, code, req.Code)
	default:
		t.Fatalf("expected close code %d", code)
	}
}

func TestHubRegisterAndUnregister(t *testing.T) {
	t.Parallel()

	hub := gateway.NewHub(newTestMetrics(t), gateway.HubOptions{Clock: func() time.Time { return testClock }})
	conn := newTestConnection(t, "c1", uuidAt(13), models.UserScope(uuidAt(13)))

	assert.Equal(t, 0, hub.Count())

	hub.Register(conn)
	assert.Equal(t, 1, hub.Count())
	assert.Equal(t, 1, hub.UserConnectionCount(uuidAt(13)))

	hub.Register(conn)
	assert.Equal(t, 1, hub.Count(), "re-registering the same connection is a no-op for the registry")

	hub.Unregister(conn)
	assert.Equal(t, 0, hub.Count())
	assert.Equal(t, 0, hub.UserConnectionCount(uuidAt(13)))

	// Unregistering twice must not underflow the active gauge or the per-user map.
	hub.Unregister(conn)
	assert.Equal(t, 0, hub.Count())
}

func TestHubDeliversOnlyToMatchingScopes(t *testing.T) {
	t.Parallel()

	metrics := newTestMetrics(t)
	hub := gateway.NewHub(metrics, gateway.HubOptions{})

	community := models.CommunityScope(uuidAt(12))
	subscriber := newTestConnection(t, "c1", uuidAt(13), models.UserScope(uuidAt(13)), community)
	outsider := newTestConnection(t, "c2", uuidAt(14), models.UserScope(uuidAt(14)))
	hub.Register(subscriber)
	hub.Register(outsider)

	delivered := hub.Deliver(community, "message_changed", []byte(`{"type":"message_changed","scope":"`+string(community)+`","data":{}}`))
	assert.Equal(t, 1, delivered)

	frame := readFrame(t, subscriber)
	assert.Equal(t, "message_changed", frame.Type)
	assert.Equal(t, string(community), frame.Scope)
	expectNoFrame(t, outsider)

	// A scope nobody holds delivers nothing.
	assert.Equal(t, 0, hub.Deliver(models.CommunityScope(uuidAt(99)), "message_changed", []byte(`{}`)))
}

func TestHubAddAndDropUserScope(t *testing.T) {
	t.Parallel()

	hub := gateway.NewHub(newTestMetrics(t), gateway.HubOptions{})
	community := models.CommunityScope(uuidAt(12))
	first := newTestConnection(t, "c1", uuidAt(13), models.UserScope(uuidAt(13)))
	second := newTestConnection(t, "c2", uuidAt(13), models.UserScope(uuidAt(13)))
	hub.Register(first)
	hub.Register(second)

	assert.Equal(t, 2, hub.AddUserScope(uuidAt(13), community))
	assert.True(t, first.HasScope(community))
	assert.True(t, second.HasScope(community))
	assert.Equal(t, 0, hub.AddUserScope(uuidAt(13), community), "adding twice changes nothing")

	assert.Equal(t, 2, hub.DropUserScope(uuidAt(13), community), "both connections dropped")
	assert.False(t, second.HasScope(community))
	assert.False(t, first.HasScope(community))

	assert.Equal(t, 0, hub.DropUserScope(uuidAt(13), community))
	assert.Equal(t, 0, hub.AddUserScope(uuidAt(44), community), "unknown user has no connections")
}

func TestHubDropsFramesAndClosesSlowConsumers(t *testing.T) {
	t.Parallel()

	metrics := newTestMetrics(t)
	hub := gateway.NewHub(metrics, gateway.HubOptions{SlowDropLimit: 2})

	scope := models.CommunityScope(uuidAt(12))
	conn := gateway.NewConnection("slow", uuidAt(13), uuidAt(30), models.ScopeSet{scope}, testClock.Add(time.Hour), 1)
	hub.Register(conn)

	frame := []byte(`{"type":"message_changed","scope":"` + string(scope) + `","data":{}}`)

	// Fill the single buffer slot, then overflow it twice.
	require.Equal(t, 1, hub.Deliver(scope, "message_changed", frame))
	require.Equal(t, 0, hub.Deliver(scope, "message_changed", frame))
	assert.InDelta(t, 1, counterValue(t, metrics.DroppedFrames.WithLabelValues("message_changed")), 0.0001)

	require.Equal(t, 0, hub.Deliver(scope, "message_changed", frame))
	assert.InDelta(t, 2, counterValue(t, metrics.DroppedFrames.WithLabelValues("message_changed")), 0.0001)
	assert.True(t, conn.Closing(), "persistently slow socket is closed")
	expectCloseRequest(t, conn, models.CloseSlowConsumer)

	// A closing connection refuses further frames instead of blocking the fan-out.
	hub.Unregister(conn)
	assert.False(t, conn.Enqueue(frame))
}

func TestHubCountsReconnects(t *testing.T) {
	t.Parallel()

	now := testClock
	metrics := newTestMetrics(t)
	hub := gateway.NewHub(metrics, gateway.HubOptions{
		Clock:           func() time.Time { return now },
		ReconnectWindow: 30 * time.Second,
	})

	first := newTestConnection(t, "c1", uuidAt(13))
	hub.Register(first)

	now = now.Add(2 * time.Second)
	hub.Unregister(first)

	second := newTestConnection(t, "c2", uuidAt(13))
	hub.Register(second)
	assert.InDelta(t, 1, counterValue(t, metrics.Reconnects), 0.0001)

	// A reconnect outside the window is not counted.
	hub.Unregister(second)
	now = now.Add(10 * time.Minute)
	third := newTestConnection(t, "c3", uuidAt(13))
	hub.Register(third)
	assert.InDelta(t, 1, counterValue(t, metrics.Reconnects), 0.0001)
}

func TestConnectionScopeLifecycle(t *testing.T) {
	t.Parallel()

	community := models.CommunityScope(uuidAt(12))
	conversation := models.ConversationScope(uuidAt(16))
	conn := gateway.NewConnection("c1", uuidAt(13), uuidAt(30),
		models.ScopeSet{models.UserScope(uuidAt(13)), community}, testClock.Add(time.Hour), 2)

	assert.True(t, conn.HasScope(community))
	assert.False(t, conn.HasScope(conversation))

	assert.True(t, conn.AddScope(conversation))
	assert.False(t, conn.AddScope(conversation))
	assert.ElementsMatch(t, []models.Scope{models.UserScope(uuidAt(13)), community, conversation}, conn.Scopes())

	assert.True(t, conn.DropScope(community))
	assert.False(t, conn.DropScope(community))
	assert.False(t, conn.HasScope(community))
}

func TestConnectionTokenAssociation(t *testing.T) {
	t.Parallel()

	conn := gateway.NewConnection("c1", uuidAt(13), uuidAt(30), models.ScopeSet{}, testClock, 1)

	assert.Equal(t, uuidAt(30), conn.SessionID())
	assert.Equal(t, testClock, conn.TokenExpiry())
	assert.True(t, conn.TokenExpired(testClock))
	assert.False(t, conn.TokenExpired(testClock.Add(-time.Second)))

	conn.Reassociate(uuidAt(31), testClock.Add(time.Minute))
	assert.Equal(t, uuidAt(31), conn.SessionID())
	assert.Equal(t, testClock.Add(time.Minute), conn.TokenExpiry())
	assert.False(t, conn.TokenExpired(testClock))
}

func TestConnectionRequestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	conn := gateway.NewConnection("c1", uuidAt(13), uuidAt(30), models.ScopeSet{}, testClock.Add(time.Hour), 1)

	conn.RequestClose(models.CloseTokenExpired, models.ReasonTokenExpired)
	conn.RequestClose(models.CloseProtocolViolation, models.ReasonProtocolViolation)

	assert.True(t, conn.Closing())
	select {
	case req := <-conn.CloseRequests():
		assert.Equal(t, models.CloseTokenExpired, req.Code)
		assert.Equal(t, models.ReasonTokenExpired, req.Reason)
	default:
		t.Fatal("expected the first close request to win")
	}
}

func TestMetricsAreNilSafe(t *testing.T) {
	t.Parallel()

	var metrics *gateway.Metrics
	metrics.ConnectionOpened(gateway.ResultAccepted)
	metrics.ConnectionActivated()
	metrics.ConnectionDeactivated()
	metrics.EventDelivered("message_changed")
	metrics.FrameDropped("message_changed")
	metrics.ReconnectObserved()
	metrics.SetLag("message_events", gateway.ConsumerGroupLabel, 3)
}
