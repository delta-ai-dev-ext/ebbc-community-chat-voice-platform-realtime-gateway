package integration

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"realtime_gateway/internal/contracts"
	"realtime_gateway/internal/service"
)

// push feeds one raw event through the real pipeline as the Kafka feed would.
func (s *stack) push(eventType string, raw []byte) {
	topic, ok := contracts.TopicForEventType(eventType)
	if !ok {
		s.t.Fatalf("unknown event type %q", eventType)
	}
	s.pipeline.Handle(topic, raw)
}

func TestUpgradeScopeAndFanOutPath(t *testing.T) {
	t.Parallel()

	stack := newStack(t, stackOptions{attached: true})

	conn, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(conn)

	require.Eventually(t, func() bool { return stack.hub.UserConnectionCount(userID) == 1 }, 5*time.Second, 5*time.Millisecond)

	// Community scope: a channel message reaches the socket.
	stack.push(contracts.EventMessageChanged,
		event(t, contracts.EventMessageChanged, "aaaaaaaa-0000-4000-8000-000000000001", messagePayload()))
	frame := readFrame(t, conn)
	assert.Equal(t, contracts.EventMessageChanged, frame["type"])
	assert.Equal(t, "community:"+communityID, frame["scope"])
	data, ok := frame["data"].(map[string]any)
	require.True(t, ok, "frame data carries the event payload")
	assert.Equal(t, "hello from the integration test", data["content"])

	// Duplicate delivery of the same event_id is suppressed on the connection: the sentinel
	// notification arrives next, proving no second message frame was queued.
	stack.push(contracts.EventMessageChanged,
		event(t, contracts.EventMessageChanged, "aaaaaaaa-0000-4000-8000-000000000001", messagePayload()))
	stack.push(contracts.EventNotificationCreated,
		event(t, contracts.EventNotificationCreated, "aaaaaaaa-0000-4000-8000-000000000004", notificationPayload()))
	frame = expectNextFrameIs(t, conn, contracts.EventNotificationCreated)
	assert.Equal(t, "user:"+userID, frame["scope"])

	// Conversation scope: a private message for the resolved conversation reaches the socket.
	conversationMessage := messagePayload()
	conversationMessage["container_type"] = "conversation"
	conversationMessage["container_id"] = conversationID
	conversationMessage["community_id"] = nil
	stack.push(contracts.EventMessageChanged,
		event(t, contracts.EventMessageChanged, "aaaaaaaa-0000-4000-8000-000000000002", conversationMessage))
	frame = readFrame(t, conn)
	assert.Equal(t, "conversation:"+conversationID, frame["scope"])

	// Personal scope: a notification for the connected user reaches the socket.
	stack.push(contracts.EventNotificationCreated,
		event(t, contracts.EventNotificationCreated, "aaaaaaaa-0000-4000-8000-000000000003", notificationPayload()))
	frame = readFrame(t, conn)
	assert.Equal(t, contracts.EventNotificationCreated, frame["type"])
	assert.Equal(t, "user:"+userID, frame["scope"])
}

func TestLiveScopeMaintenanceOverTheSocket(t *testing.T) {
	t.Parallel()

	stack := newStack(t, stackOptions{attached: true})

	conn, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(conn)
	require.Eventually(t, func() bool { return stack.hub.UserConnectionCount(userID) == 1 }, 5*time.Second, 5*time.Millisecond)

	// 1. Banned in the community: the scope is dropped and no further community frames arrive.
	member := map[string]any{
		"community_id":   communityID,
		"user_id":        userID,
		"change":         "banned",
		"status":         "removed",
		"is_banned":      true,
		"muted_until":    nil,
		"role_ids":       []any{},
		"sanction_id":    "bbbbbbbb-0000-4000-8000-000000000001",
		"member_version": 5,
	}
	stack.push(contracts.EventCommunityMemberChanged,
		event(t, contracts.EventCommunityMemberChanged, "bbbbbbbb-0000-4000-8000-000000000010", member))
	// The removed member does not receive the removal snapshot: the sentinel arrives next.
	stack.push(contracts.EventNotificationCreated,
		event(t, contracts.EventNotificationCreated, "bbbbbbbb-0000-4000-8000-000000000020", notificationPayload()))
	expectNextFrameIs(t, conn, contracts.EventNotificationCreated)

	// A community message no longer reaches the removed member.
	stack.push(contracts.EventMessageChanged,
		event(t, contracts.EventMessageChanged, "bbbbbbbb-0000-4000-8000-000000000011", messagePayload()))
	stack.push(contracts.EventNotificationCreated,
		event(t, contracts.EventNotificationCreated, "bbbbbbbb-0000-4000-8000-000000000021", notificationPayload()))
	expectNextFrameIs(t, conn, contracts.EventNotificationCreated)

	// 2. Rejoined: the scope is added back without reconnecting.
	member["change"] = "joined"
	member["status"] = "active"
	member["is_banned"] = false
	member["member_version"] = 6
	stack.push(contracts.EventCommunityMemberChanged,
		event(t, contracts.EventCommunityMemberChanged, "bbbbbbbb-0000-4000-8000-000000000012", member))
	assert.Equal(t, "community:"+communityID, readFrame(t, conn)["scope"])

	stack.push(contracts.EventMessageChanged,
		event(t, contracts.EventMessageChanged, "bbbbbbbb-0000-4000-8000-000000000013", messagePayload()))
	assert.Equal(t, contracts.EventMessageChanged, readFrame(t, conn)["type"])

	// 3. Removed from the conversation: no further conversation frames.
	removal := conversationPayload()
	removal["change"] = "participants_changed"
	removal["added_user_ids"] = []any{}
	removal["removed_user_ids"] = []any{userID}
	removal["participant_user_ids"] = []any{otherUserID}
	removal["conversation_version"] = 3
	stack.push(contracts.EventConversationChanged,
		event(t, contracts.EventConversationChanged, "bbbbbbbb-0000-4000-8000-000000000014", removal))
	stack.push(contracts.EventNotificationCreated,
		event(t, contracts.EventNotificationCreated, "bbbbbbbb-0000-4000-8000-000000000022", notificationPayload()))
	expectNextFrameIs(t, conn, contracts.EventNotificationCreated)

	stack.push(contracts.EventMessageChanged,
		event(t, contracts.EventMessageChanged, "bbbbbbbb-0000-4000-8000-000000000015", conversationOnlyMessage()))
	stack.push(contracts.EventNotificationCreated,
		event(t, contracts.EventNotificationCreated, "bbbbbbbb-0000-4000-8000-000000000023", notificationPayload()))
	expectNextFrameIs(t, conn, contracts.EventNotificationCreated)

	// 4. Added back to the conversation: the snapshot arrives on the conversation scope.
	addition := conversationPayload()
	addition["added_user_ids"] = []any{userID}
	addition["removed_user_ids"] = []any{}
	addition["conversation_version"] = 4
	stack.push(contracts.EventConversationChanged,
		event(t, contracts.EventConversationChanged, "bbbbbbbb-0000-4000-8000-000000000016", addition))
	assert.Equal(t, "conversation:"+conversationID, readFrame(t, conn)["scope"])
}

// conversationOnlyMessage builds a message payload scoped to the fixed conversation.
func conversationOnlyMessage() map[string]any {
	payload := messagePayload()
	payload["container_type"] = "conversation"
	payload["container_id"] = conversationID
	payload["community_id"] = nil
	return payload
}

func TestScopeResolutionFailureRefusesUpgrade(t *testing.T) {
	t.Parallel()

	stack := newStack(t, stackOptions{
		attached: true,
		resolver: &fixedResolver{err: service.ErrScopeUnavailable},
	})

	conn, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(conn)

	expectCloseCode(t, conn, 4503)
	assert.Eventually(t, func() bool { return stack.hub.Count() == 0 }, 5*time.Second, 5*time.Millisecond)
}

func TestRealScopeClientIsUsedWithServiceTokenAndTrace(t *testing.T) {
	t.Parallel()

	transport := httpmock.NewMockTransport()
	var captured *http.Request
	transport.RegisterResponder(http.MethodPost, "http://messaging_service:8080/internal/realtime/user-scopes",
		func(req *http.Request) (*http.Response, error) {
			captured = req.Clone(req.Context())
			responder, err := httpmock.NewJsonResponder(http.StatusOK, map[string]any{
				"user_id":          userID,
				"community_ids":    []string{communityID},
				"conversation_ids": []string{conversationID},
				"generated_at":     "2026-02-11T10:00:00Z",
			})
			require.NoError(t, err)
			return responder(req)
		})

	stack := newStack(t, stackOptions{
		attached: true,
		resolver: service.NewScopeClient("http://messaging_service:8080", "internal-token",
			&http.Client{Transport: transport}, nil),
	})

	header := http.Header{}
	header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

	conn, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), header)
	require.NoError(t, err)
	defer closeSocket(conn)

	require.Eventually(t, func() bool { return stack.hub.UserConnectionCount(userID) == 1 }, 5*time.Second, 5*time.Millisecond)
	require.NotNil(t, captured)
	assert.Equal(t, "Bearer internal-token", captured.Header.Get("Authorization"))
	assert.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", captured.Header.Get("Traceparent"))
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", captured.Header.Get("X-Trace-Id"))
}

func TestFeedDetachedBlocksConnectionsButKeepsSockets(t *testing.T) {
	t.Parallel()

	stack := newStack(t, stackOptions{attached: true})

	conn, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(conn)
	require.Eventually(t, func() bool { return stack.hub.UserConnectionCount(userID) == 1 }, 5*time.Second, 5*time.Millisecond)

	// The feed detaches: readiness flips to 503 and new upgrades are refused.
	stack.feed.attached.Store(false)
	status, body := stack.statusOf("/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body["detail"], "feed")

	refused, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(refused)
	expectCloseCode(t, refused, 4503)

	// Liveness is unaffected and the already subscribed socket keeps working.
	status, _ = stack.statusOf("/healthz")
	assert.Equal(t, http.StatusOK, status)

	stack.push(contracts.EventMessageChanged,
		event(t, contracts.EventMessageChanged, "cccccccc-0000-4000-8000-000000000001", messagePayload()))
	assert.Equal(t, contracts.EventMessageChanged, readFrame(t, conn)["type"])
}

func TestInvalidTokenIsRejectedBeforeScopeResolution(t *testing.T) {
	t.Parallel()

	resolver := &countingResolver{}
	stack := newStack(t, stackOptions{attached: true, resolver: resolver})

	conn, err := stack.dial("?access_token=not-a-jwt", nil)
	require.NoError(t, err)
	defer closeSocket(conn)

	expectCloseCode(t, conn, 4402)
	assert.Equal(t, int32(0), resolver.calls.Load(), "no API-003 call for a rejected token")
}

// countingResolver counts API-003 calls.
type countingResolver struct {
	calls atomic.Int32
}

// Resolve counts the call and returns an empty scope set.
func (r *countingResolver) Resolve(_ context.Context, req service.ScopeRequest) (*service.ScopeResult, error) {
	r.calls.Add(1)
	return &service.ScopeResult{UserID: req.UserID}, nil
}

// Configured reports a usable resolver.
func (r *countingResolver) Configured() bool { return true }

func TestUnsupportedInboundMessageClosesWithProtocolViolation(t *testing.T) {
	t.Parallel()

	stack := newStack(t, stackOptions{attached: true})

	conn, err := stack.dial("?access_token="+stack.token(userID, time.Now().Add(time.Hour)), nil)
	require.NoError(t, err)
	defer closeSocket(conn)

	require.NoError(t, conn.WriteMessage(1, []byte(`{"type":"typing_started"}`)))
	expectCloseCode(t, conn, 4400)
}
