package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"realtime_gateway/internal/contracts"
	"realtime_gateway/internal/models"
)

func TestGracefulShutdownDrainsSockets(t *testing.T) {
	t.Parallel()

	stack := newStack(t, stackOptions{attached: true})

	conn, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(conn)
	require.Eventually(t, func() bool { return stack.hub.UserConnectionCount(userID) == 1 }, 5*time.Second, 5*time.Millisecond)

	// A frame queued just before the drain must still be flushed to the socket.
	stack.push(contracts.EventMessageChanged,
		event(t, contracts.EventMessageChanged, "dddddddd-0000-4000-8000-000000000001", messagePayload()))

	// 1. Readiness flips to 503 as soon as the drain starts.
	stack.web.BeginDrain()
	status, body := stack.statusOf("/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, "degraded", body["status"])
	assert.Contains(t, body["detail"], "shutting down")

	// 2. New upgrades are refused with a retryable close code.
	refused, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(refused)
	expectCloseCode(t, refused, 4503)

	// 3. Existing sockets receive the buffered frame and then 1001 going away.
	close(stack.drain)

	frame := readFrame(t, conn)
	assert.Equal(t, contracts.EventMessageChanged, frame["type"])
	for {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		_, _, readErr := conn.ReadMessage()
		if readErr == nil {
			continue
		}
		var closeErr *websocket.CloseError
		require.ErrorAs(t, readErr, &closeErr, "expected a close frame, got %v", readErr)
		assert.Equal(t, models.CloseGoingAway, closeErr.Code)
		break
	}

	// 4. The handler reports all sockets drained so main can stop the consumers and exit 0.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, stack.web.WaitForSessions(ctx))
}

func TestLivenessSurvivesDownstreamOutage(t *testing.T) {
	t.Parallel()

	stack := newStack(t, stackOptions{attached: false})

	status, body := stack.statusOf("/healthz")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "ok", body["status"])

	status, body = stack.statusOf("/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, "degraded", body["status"])

	// No socket can be established while the feed is detached.
	conn, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(conn)
	expectCloseCode(t, conn, 4503)
	assert.Equal(t, 0, stack.hub.Count())
}
