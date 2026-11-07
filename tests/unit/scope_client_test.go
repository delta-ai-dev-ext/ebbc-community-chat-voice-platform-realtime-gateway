package unit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"realtime_gateway/internal/service"
)

const (
	testMessagingURL = "http://messaging_service:8080"
	testScopePath    = testMessagingURL + "/internal/realtime/user-scopes"
	testServiceToken = "internal-service-token"
)

// scopeResponseBody builds a valid API-003 200 body.
func scopeResponseBody(userID string) map[string]any {
	return map[string]any{
		"user_id":          userID,
		"community_ids":    []string{uuidAt(12)},
		"conversation_ids": []string{uuidAt(16)},
		"generated_at":     "2026-02-11T10:00:00Z",
	}
}

// newMockScopeClient builds a scope client backed by an httpmock transport.
func newMockScopeClient(t *testing.T) (*service.ScopeClient, *httpmock.MockTransport) {
	t.Helper()
	transport := httpmock.NewMockTransport()
	client := service.NewScopeClient(testMessagingURL, testServiceToken, &http.Client{Transport: transport}, zap.NewNop())
	return client, transport
}

func TestScopeClientResolveSuccess(t *testing.T) {
	t.Parallel()

	client, transport := newMockScopeClient(t)

	var captured *http.Request
	var capturedBody []byte
	transport.RegisterResponder(http.MethodPost, testScopePath, func(req *http.Request) (*http.Response, error) {
		captured = req
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		capturedBody = body
		resp, err := httpmock.NewJsonResponder(http.StatusOK, scopeResponseBody(uuidAt(13)))
		require.NoError(t, err)
		return resp(req)
	})

	result, err := client.Resolve(context.Background(), service.ScopeRequest{
		UserID:      uuidAt(13),
		Traceparent: "00-" + fixedTraceID + "-00f067aa0ba902b7-01",
		TraceID:     fixedTraceID,
	})
	require.NoError(t, err)

	assert.Equal(t, uuidAt(13), result.UserID)
	assert.Equal(t, []string{uuidAt(12)}, result.CommunityIDs)
	assert.Equal(t, []string{uuidAt(16)}, result.ConversationIDs)
	assert.Equal(t, time.Date(2026, 2, 11, 10, 0, 0, 0, time.UTC), result.GeneratedAt.UTC())

	require.NotNil(t, captured)
	assert.Equal(t, "Bearer "+testServiceToken, captured.Header.Get("Authorization"))
	assert.Equal(t, "application/json", captured.Header.Get("Content-Type"))
	assert.Equal(t, fixedTraceID, captured.Header.Get("X-Trace-Id"))
	assert.Equal(t, "00-"+fixedTraceID+"-00f067aa0ba902b7-01", captured.Header.Get("Traceparent"))
	assert.JSONEq(t, `{"user_id":"`+uuidAt(13)+`"}`, string(capturedBody))
}

func TestScopeClientRetriesOnServerError(t *testing.T) {
	t.Parallel()

	client, transport := newMockScopeClient(t)
	transport.RegisterResponder(http.MethodPost, testScopePath,
		httpmock.NewStringResponder(http.StatusInternalServerError, `{"error":"internal"}`).
			Then(httpmock.NewStringResponder(http.StatusOK, mustJSON(t, scopeResponseBody(uuidAt(13))))))

	result, err := client.Resolve(context.Background(), service.ScopeRequest{UserID: uuidAt(13)})
	require.NoError(t, err)
	assert.Equal(t, uuidAt(13), result.UserID)
	assert.Equal(t, 2, transport.GetTotalCallCount())
}

func TestScopeClientGivesUpAfterOneRetry(t *testing.T) {
	t.Parallel()

	client, transport := newMockScopeClient(t)
	transport.RegisterResponder(http.MethodPost, testScopePath,
		httpmock.NewStringResponder(http.StatusServiceUnavailable, `{"error":"unavailable"}`))

	_, err := client.Resolve(context.Background(), service.ScopeRequest{UserID: uuidAt(13)})
	require.Error(t, err)
	assert.ErrorIs(t, err, service.ErrScopeUnavailable)
	assert.Equal(t, 2, transport.GetTotalCallCount(), "one retry only")
}

func TestScopeClientDoesNotRetryClientErrors(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized} {
		status := status
		client, transport := newMockScopeClient(t)
		transport.RegisterResponder(http.MethodPost, testScopePath,
			httpmock.NewStringResponder(status, `{"error":"invalid_service_token"}`))

		_, err := client.Resolve(context.Background(), service.ScopeRequest{UserID: uuidAt(13)})
		require.Error(t, err)
		assert.ErrorIs(t, err, service.ErrScopeUnavailable)
		assert.Equal(t, 1, transport.GetTotalCallCount(), "4xx must not be retried")
	}
}

func TestScopeClientRetriesTransportErrors(t *testing.T) {
	t.Parallel()

	client, transport := newMockScopeClient(t)
	transport.RegisterResponder(http.MethodPost, testScopePath,
		httpmock.NewErrorResponder(errors.New("connection refused")))

	_, err := client.Resolve(context.Background(), service.ScopeRequest{UserID: uuidAt(13)})
	require.Error(t, err)
	assert.ErrorIs(t, err, service.ErrScopeUnavailable)
	assert.Equal(t, 2, transport.GetTotalCallCount())
}

// timeoutDoer blocks until the per-attempt context is cancelled, simulating a slow provider.
type timeoutDoer struct {
	calls atomic.Int32
}

func (d *timeoutDoer) Do(req *http.Request) (*http.Response, error) {
	d.calls.Add(1)
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func TestScopeClientTimesOutEachAttempt(t *testing.T) {
	t.Parallel()

	doer := &timeoutDoer{}
	client := service.NewScopeClient(testMessagingURL, testServiceToken, doer, zap.NewNop())

	start := time.Now()
	_, err := client.Resolve(context.Background(), service.ScopeRequest{UserID: uuidAt(13)})

	require.Error(t, err)
	assert.ErrorIs(t, err, service.ErrScopeUnavailable)
	// Two attempts of the 2 s platform timeout; the upper bound keeps the test quick but
	// still proves that a timeout triggers exactly one retry.
	assert.Less(t, time.Since(start), 10*time.Second)
	assert.Equal(t, int32(2), doer.calls.Load())
}

func TestScopeClientRejectsMalformedResponses(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"not json":        `{`,
		"foreign user":    mustJSON(t, map[string]any{"user_id": uuidAt(14), "community_ids": []string{}, "conversation_ids": []string{}}),
		"non uuid commun": mustJSON(t, map[string]any{"user_id": uuidAt(13), "community_ids": []string{"community-1"}, "conversation_ids": []string{}}),
		"non uuid conv":   mustJSON(t, map[string]any{"user_id": uuidAt(13), "community_ids": []string{}, "conversation_ids": []string{"dm-1"}}),
	}

	for name, body := range cases {
		body := body
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client, transport := newMockScopeClient(t)
			transport.RegisterResponder(http.MethodPost, testScopePath, httpmock.NewStringResponder(http.StatusOK, body))

			_, err := client.Resolve(context.Background(), service.ScopeRequest{UserID: uuidAt(13)})
			require.Error(t, err)
			assert.ErrorIs(t, err, service.ErrScopeUnavailable)
			assert.Equal(t, 1, transport.GetTotalCallCount(), "a malformed body is not retried")
		})
	}
}

func TestScopeClientValidationGuards(t *testing.T) {
	t.Parallel()

	unconfigured := service.NewScopeClient("", "", nil, nil)
	assert.False(t, unconfigured.Configured())
	_, err := unconfigured.Resolve(context.Background(), service.ScopeRequest{UserID: uuidAt(13)})
	assert.ErrorIs(t, err, service.ErrScopeUnavailable)

	client, _ := newMockScopeClient(t)
	assert.True(t, client.Configured())

	_, err = client.Resolve(context.Background(), service.ScopeRequest{UserID: "  "})
	assert.ErrorIs(t, err, service.ErrScopeUnavailable)
}

func TestScopeClientAcceptsEmptyScopeLists(t *testing.T) {
	t.Parallel()

	client, transport := newMockScopeClient(t)
	body, err := json.Marshal(map[string]any{
		"user_id":          uuidAt(13),
		"community_ids":    []string{},
		"conversation_ids": []string{},
		"generated_at":     "2026-02-11T10:00:00Z",
	})
	require.NoError(t, err)
	transport.RegisterResponder(http.MethodPost, testScopePath, httpmock.NewStringResponder(http.StatusOK, string(body)))

	result, err := client.Resolve(context.Background(), service.ScopeRequest{UserID: uuidAt(13)})
	require.NoError(t, err)
	assert.Empty(t, result.CommunityIDs)
	assert.Empty(t, result.ConversationIDs)
}

func TestScopeClientHonoursContextCancellation(t *testing.T) {
	t.Parallel()

	client, transport := newMockScopeClient(t)
	transport.RegisterResponder(http.MethodPost, testScopePath, httpmock.NewStringResponder(http.StatusOK, "{}"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Resolve(ctx, service.ScopeRequest{UserID: uuidAt(13)})
	require.Error(t, err)
	assert.ErrorIs(t, err, service.ErrScopeUnavailable)
}

func TestTraceFromHeader(t *testing.T) {
	t.Parallel()

	header := http.Header{}
	header.Set("Traceparent", "00-"+fixedTraceID+"-00f067aa0ba902b7-01")
	context := service.TraceFromHeader(header)
	assert.Equal(t, fixedTraceID, context.TraceID)
	assert.NotEmpty(t, context.Traceparent)

	header = http.Header{}
	header.Set("X-Trace-Id", fixedTraceID)
	assert.Equal(t, fixedTraceID, service.TraceFromHeader(header).TraceID)

	// No inbound trace identifiers: the gateway mints one so the field is never empty.
	minted := service.TraceFromHeader(http.Header{})
	assert.Len(t, minted.TraceID, 32)
	assert.NotEqual(t, minted.TraceID, service.TraceFromHeader(http.Header{}).TraceID)

	// A malformed trace id is replaced rather than propagated.
	header = http.Header{}
	header.Set("X-Trace-Id", "not-a-trace-id")
	assert.Len(t, service.TraceFromHeader(header).TraceID, 32)
}
