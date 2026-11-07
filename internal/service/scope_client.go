package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"

	"realtime_gateway/internal/contracts"
)

// defaultScopeTimeout is the platform call policy for internal reads: a 2 s timeout with at
// most one retry on 5xx/timeout. The operation is read-only, so retrying is safe.
const defaultScopeTimeout = 2 * time.Second

// maxScopeAttempts is the total number of attempts (first try + one retry).
const maxScopeAttempts = 2

// maxScopeBodyBytes bounds the response body read from API-003.
const maxScopeBodyBytes = 1 << 20

// ErrScopeUnavailable means the subscription scopes could not be resolved. The connect gate
// maps it to a retryable refusal (close code 4503 / HTTP 503): the gateway never serves a
// connection with guessed scopes.
var ErrScopeUnavailable = errors.New("subscription scopes unavailable")

// ScopeRequest is one API-003 call: the user whose scopes are resolved plus the trace
// identifiers to propagate.
type ScopeRequest struct {
	UserID      string
	Traceparent string
	TraceID     string
}

// ScopeResult is the resolved subscription scope set of one user.
type ScopeResult struct {
	UserID          string
	CommunityIDs    []string
	ConversationIDs []string
	GeneratedAt     time.Time
}

// ScopeResolver resolves the community and conversation ids a user is subscribed to.
type ScopeResolver interface {
	Resolve(ctx context.Context, req ScopeRequest) (*ScopeResult, error)
}

// scopeResponse mirrors the API-003 200 response body.
type scopeResponse struct {
	UserID          string   `json:"user_id"`
	CommunityIDs    []string `json:"community_ids"`
	ConversationIDs []string `json:"conversation_ids"`
	GeneratedAt     string   `json:"generated_at"`
}

// errorResponse mirrors the platform `shared:ErrorResponse` body.
type errorResponse struct {
	Error  string `json:"error"`
	Detail string `json:"detail"`
}

// HTTPDoer performs one HTTP round trip. *http.Client satisfies it.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// ScopeClient is the API-003 client: POST /internal/realtime/user-scopes on
// messaging_service with the per-edge service token.
type ScopeClient struct {
	baseURL  string
	token    string
	doer     HTTPDoer
	logger   *zap.Logger
	timeout  time.Duration
	attempts int
}

// NewScopeClient builds the API-003 client. baseURL must be the messaging_service base URL;
// token is the internal service token sent as `Authorization: Bearer`.
func NewScopeClient(baseURL, token string, doer HTTPDoer, logger *zap.Logger) *ScopeClient {
	if doer == nil {
		doer = &http.Client{Timeout: defaultScopeTimeout}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ScopeClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		token:    token,
		doer:     doer,
		logger:   logger,
		timeout:  defaultScopeTimeout,
		attempts: maxScopeAttempts,
	}
}

// Configured reports whether the client has everything it needs to serve a request. The
// readiness probe uses it.
func (c *ScopeClient) Configured() bool {
	return c != nil && c.baseURL != "" && c.token != ""
}

// Resolve performs the API-003 call with the platform timeout/retry policy.
func (c *ScopeClient) Resolve(ctx context.Context, req ScopeRequest) (*ScopeResult, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("%w: scope client is not configured", ErrScopeUnavailable)
	}
	if strings.TrimSpace(req.UserID) == "" {
		return nil, fmt.Errorf("%w: user id is required", ErrScopeUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScopeUnavailable, err)
	}

	var lastErr error
	for attempt := 1; attempt <= c.attempts; attempt++ {
		result, retryable, err := c.attempt(ctx, req)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !retryable || attempt == c.attempts || ctx.Err() != nil {
			break
		}
		c.logger.Warn("scope resolution attempt failed, retrying",
			append(traceFields(req), zap.Int("attempt", attempt), zap.Error(err))...)
	}
	return nil, fmt.Errorf("%w: %v", ErrScopeUnavailable, lastErr)
}

// traceFields renders the propagated trace identifiers for logs.
func traceFields(req ScopeRequest) []zap.Field {
	fields := make([]zap.Field, 0, 2)
	if req.TraceID != "" {
		fields = append(fields, zap.String("trace_id", req.TraceID))
	}
	if req.Traceparent != "" {
		fields = append(fields, zap.String("traceparent", req.Traceparent))
	}
	return fields
}

// attempt performs one API-003 round trip and reports whether a retry is allowed.
func (c *ScopeClient) attempt(ctx context.Context, req ScopeRequest) (*ScopeResult, bool, error) {
	body, err := json.Marshal(map[string]string{"user_id": req.UserID})
	if err != nil {
		return nil, false, fmt.Errorf("encode request: %w", err)
	}

	endpoint, err := url.JoinPath(c.baseURL, "/internal/realtime/user-scopes")
	if err != nil {
		return nil, false, fmt.Errorf("build endpoint: %w", err)
	}

	attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	if req.Traceparent != "" {
		httpReq.Header.Set("Traceparent", req.Traceparent)
	}
	if req.TraceID != "" {
		httpReq.Header.Set("X-Trace-Id", req.TraceID)
	}

	resp, err := c.doer.Do(httpReq)
	if err != nil {
		// Transport failures and per-attempt timeouts are retryable.
		return nil, true, fmt.Errorf("call API-003: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= http.StatusInternalServerError {
		return nil, true, fmt.Errorf("API-003 returned status %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, maxScopeBodyBytes))
		return nil, false, fmt.Errorf("API-003 returned status %d: %s", resp.StatusCode, summarizeError(detail))
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxScopeBodyBytes))
	if err != nil {
		return nil, true, fmt.Errorf("read API-003 response: %w", err)
	}

	result, err := decodeScopeResponse(raw, req.UserID)
	if err != nil {
		return nil, false, err
	}
	return result, false, nil
}

// summarizeError extracts the platform error code from a problem+json body, if present.
func summarizeError(raw []byte) string {
	var body errorResponse
	if err := json.Unmarshal(raw, &body); err == nil && body.Error != "" {
		return body.Error
	}
	return "unexpected_error"
}

// decodeScopeResponse validates the API-003 200 body.
func decodeScopeResponse(raw []byte, userID string) (*ScopeResult, error) {
	var body scopeResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("decode API-003 response: %w", err)
	}
	if body.UserID != "" && body.UserID != userID {
		return nil, fmt.Errorf("API-003 response user_id %q does not match request", body.UserID)
	}
	for _, id := range body.CommunityIDs {
		if !contracts.IsUUID(id) {
			return nil, errors.New("API-003 response contains a non-uuid community id")
		}
	}
	for _, id := range body.ConversationIDs {
		if !contracts.IsUUID(id) {
			return nil, errors.New("API-003 response contains a non-uuid conversation id")
		}
	}

	result := &ScopeResult{
		UserID:          userID,
		CommunityIDs:    body.CommunityIDs,
		ConversationIDs: body.ConversationIDs,
	}
	if body.GeneratedAt != "" {
		if generatedAt, err := time.Parse(time.RFC3339, body.GeneratedAt); err == nil {
			result.GeneratedAt = generatedAt
		}
	}
	return result, nil
}
