// Package config loads and validates the 12-factor environment configuration of the
// realtime gateway. Validation is fail-fast: every problem is reported with the name of the
// offending variable so a misconfigured replica never starts serving.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Default values for the optional tuning knobs (all durations are seconds in the
// environment unless they carry an explicit unit suffix).
const (
	defaultWSHeartbeatInterval = 25 * time.Second
	defaultWSReadDeadline      = 60 * time.Second
	defaultWSSendBufferSize    = 64
	defaultEventDedupeTTL      = 10 * time.Minute
	defaultShutdownTimeout     = 20 * time.Second
)

// Config is the typed, validated configuration of the service.
type Config struct {
	// Port is the HTTP listen port for /ws, the probes and /metrics.
	Port int

	// KafkaBrokers is the bootstrap broker list of the consumed event feed.
	KafkaBrokers []string
	// KafkaSASLMechanism, KafkaSASLUser and KafkaSASLPassword enable SASL when present.
	KafkaSASLMechanism string
	KafkaSASLUser      string
	KafkaSASLPassword  string
	// KafkaTLSEnabled turns on TLS for broker connections.
	KafkaTLSEnabled bool

	// MessagingServiceURL is the base URL of messaging_service (API-003 provider).
	MessagingServiceURL string
	// InternalServiceToken is the per-edge service token for API-003.
	InternalServiceToken string

	// IdentityJWKS is inline JWKS JSON (or a file path) of identity's verification keys.
	IdentityJWKS string
	// IdentityPublicKeyPath is a PEM file with the current verification key.
	IdentityPublicKeyPath string
	// IdentityNextPublicKeyPath is a PEM file with the next (rotation) verification key.
	IdentityNextPublicKeyPath string
	// IdentityTokenIssuer and IdentityTokenAudience are the expected iss/aud claims.
	IdentityTokenIssuer   string
	IdentityTokenAudience string

	// WSAllowedOrigins is the Origin allow-list for the WebSocket upgrade ("*" allows all).
	WSAllowedOrigins []string
	// WSHeartbeatInterval is the server ping interval.
	WSHeartbeatInterval time.Duration
	// WSReadDeadline is how long a socket may stay silent before it is closed.
	WSReadDeadline time.Duration
	// WSSendBufferSize bounds the outbound frame buffer of one connection.
	WSSendBufferSize int

	// EventDedupeTTL is the retention window of the event_id dedupe cache.
	EventDedupeTTL time.Duration
	// ShutdownTimeout is the bounded drain window.
	ShutdownTimeout time.Duration
}

// Addr returns the listen address for the HTTP server.
func (c *Config) Addr() string {
	return ":" + strconv.Itoa(c.Port)
}

// Load reads the configuration from the environment, applies defaults for optional
// variables and validates everything. All validation problems are returned together so the
// operator sees every missing or malformed variable in one structured log line.
func Load(getenv func(string) string) (*Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}

	var problems []error
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Errorf(format, args...))
	}

	cfg := &Config{
		IdentityJWKS:              strings.TrimSpace(getenv("IDENTITY_JWKS")),
		IdentityPublicKeyPath:     strings.TrimSpace(getenv("IDENTITY_PUBLIC_KEY_PATH")),
		IdentityNextPublicKeyPath: strings.TrimSpace(getenv("IDENTITY_NEXT_PUBLIC_KEY_PATH")),
		IdentityTokenIssuer:       strings.TrimSpace(getenv("IDENTITY_TOKEN_ISSUER")),
		IdentityTokenAudience:     strings.TrimSpace(getenv("IDENTITY_TOKEN_AUDIENCE")),
		MessagingServiceURL:       strings.TrimSpace(getenv("MESSAGING_SERVICE_URL")),
		InternalServiceToken:      strings.TrimSpace(getenv("INTERNAL_SERVICE_TOKEN")),
		KafkaSASLMechanism:        strings.ToUpper(strings.TrimSpace(getenv("KAFKA_SASL_MECHANISM"))),
		KafkaSASLUser:             getenv("KAFKA_SASL_USER"),
		KafkaSASLPassword:         getenv("KAFKA_SASL_PASSWORD"),
	}

	if port := strings.TrimSpace(getenv("PORT")); port == "" {
		add("PORT is required")
	} else if parsed, err := strconv.Atoi(port); err != nil || parsed < 1 || parsed > 65535 {
		add("PORT must be an integer between 1 and 65535")
	} else {
		cfg.Port = parsed
	}

	cfg.KafkaBrokers = splitList(getenv("KAFKA_BROKERS"))
	if len(cfg.KafkaBrokers) == 0 {
		add("KAFKA_BROKERS is required and must contain at least one broker")
	}

	if raw := strings.TrimSpace(getenv("KAFKA_TLS_ENABLED")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			add("KAFKA_TLS_ENABLED must be a boolean")
		}
		cfg.KafkaTLSEnabled = enabled
	}

	if cfg.KafkaSASLMechanism != "" {
		if cfg.KafkaSASLUser == "" || cfg.KafkaSASLPassword == "" {
			add("KAFKA_SASL_USER and KAFKA_SASL_PASSWORD are required when KAFKA_SASL_MECHANISM is set")
		}
	}

	if cfg.MessagingServiceURL == "" {
		add("MESSAGING_SERVICE_URL is required")
	} else if parsed, err := url.Parse(cfg.MessagingServiceURL); err != nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		add("MESSAGING_SERVICE_URL must be an absolute http(s) URL")
	}

	if cfg.InternalServiceToken == "" {
		add("INTERNAL_SERVICE_TOKEN is required")
	}

	if cfg.IdentityJWKS == "" && cfg.IdentityPublicKeyPath == "" {
		add("IDENTITY_JWKS or IDENTITY_PUBLIC_KEY_PATH is required")
	}

	if cfg.IdentityTokenIssuer == "" {
		add("IDENTITY_TOKEN_ISSUER is required")
	}
	if cfg.IdentityTokenAudience == "" {
		add("IDENTITY_TOKEN_AUDIENCE is required")
	}

	cfg.WSAllowedOrigins = splitList(getenv("WS_ALLOWED_ORIGINS"))
	if len(cfg.WSAllowedOrigins) == 0 {
		add("WS_ALLOWED_ORIGINS is required")
	}
	for _, origin := range cfg.WSAllowedOrigins {
		if origin == "*" {
			continue
		}
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			add("WS_ALLOWED_ORIGINS entry %q must be \"*\" or an absolute http(s) origin", origin)
		}
	}

	cfg.WSHeartbeatInterval = durationValue(getenv("WS_HEARTBEAT_INTERVAL"), defaultWSHeartbeatInterval, "WS_HEARTBEAT_INTERVAL", add)
	cfg.WSReadDeadline = durationValue(getenv("WS_READ_DEADLINE"), defaultWSReadDeadline, "WS_READ_DEADLINE", add)
	cfg.EventDedupeTTL = durationValue(getenv("EVENT_DEDUPE_TTL"), defaultEventDedupeTTL, "EVENT_DEDUPE_TTL", add)
	cfg.ShutdownTimeout = durationValue(getenv("SHUTDOWN_TIMEOUT"), defaultShutdownTimeout, "SHUTDOWN_TIMEOUT", add)

	if cfg.WSReadDeadline <= cfg.WSHeartbeatInterval {
		add("WS_READ_DEADLINE must be greater than WS_HEARTBEAT_INTERVAL")
	}

	if raw := strings.TrimSpace(getenv("WS_SEND_BUFFER_SIZE")); raw == "" {
		cfg.WSSendBufferSize = defaultWSSendBufferSize
	} else if parsed, err := strconv.Atoi(raw); err != nil || parsed < 1 {
		add("WS_SEND_BUFFER_SIZE must be a positive integer")
	} else {
		cfg.WSSendBufferSize = parsed
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("invalid configuration: %w", errors.Join(problems...))
	}
	return cfg, nil
}

// splitList splits a comma separated environment value, trimming blanks.
func splitList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// durationValue parses a duration given either as plain seconds or as a Go duration string.
func durationValue(raw string, fallback time.Duration, name string, add func(string, ...any)) time.Duration {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback
	}
	if seconds, err := strconv.Atoi(trimmed); err == nil {
		if seconds < 1 {
			add("%s must be a positive duration", name)
			return fallback
		}
		return time.Duration(seconds) * time.Second
	}
	parsed, err := time.ParseDuration(trimmed)
	if err != nil || parsed <= 0 {
		add("%s must be a positive duration (seconds or a Go duration such as 30s)", name)
		return fallback
	}
	return parsed
}
