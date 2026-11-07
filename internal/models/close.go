package models

// WebSocket close codes emitted by the gateway (see docs/openapi_spec.yaml CloseReason).
const (
	// CloseProtocolViolation is sent when the client sends anything other than a
	// heartbeat or a token refresh.
	CloseProtocolViolation = 4400
	// CloseTokenExpired is sent when the connection's access token expired without a
	// successful in-band refresh.
	CloseTokenExpired = 4401
	// CloseInvalidToken is sent when the presented access token is missing, malformed,
	// signed with an unexpected algorithm or carries foreign claims.
	CloseInvalidToken = 4402
	// CloseServiceUnavailable is a retryable refusal: the subscription-scope service
	// could not answer, the feed is detached, or the replica is draining.
	CloseServiceUnavailable = 4503
	// CloseGoingAway is sent to every socket during graceful shutdown.
	CloseGoingAway = 1001
	// CloseSlowConsumer is sent when a socket kept overflowing its bounded send buffer;
	// closing lets the client reconnect cleanly.
	CloseSlowConsumer = 1013
	// CloseNormalClosure is the standard normal close code.
	CloseNormalClosure = 1000
	// CloseInternalError is used when the socket cannot continue for server-side reasons.
	CloseInternalError = 1011
)

// Stable machine-readable close reasons (snake_case) paired with the codes above.
const (
	ReasonProtocolViolation  = "protocol_violation"
	ReasonTokenExpired       = "token_expired"
	ReasonInvalidToken       = "invalid_token"
	ReasonServiceUnavailable = "service_unavailable"
	ReasonShutdown           = "going_away"
	ReasonSlowConsumer       = "slow_consumer"
	ReasonClientClosed       = "client_closed"
)
