package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"realtime_gateway/internal/config"
)

// validEnv is a complete, valid environment for the gateway.
func validEnv() map[string]string {
	return map[string]string{
		"PORT":                     "8080",
		"KAFKA_BROKERS":            "kafka-1:9092, kafka-2:9092",
		"MESSAGING_SERVICE_URL":    "http://messaging_service:8080",
		"INTERNAL_SERVICE_TOKEN":   "internal-token",
		"IDENTITY_PUBLIC_KEY_PATH": "/etc/keys/identity.pem",
		"IDENTITY_TOKEN_ISSUER":    "identity_service",
		"IDENTITY_TOKEN_AUDIENCE":  "realtime_gateway",
		"WS_ALLOWED_ORIGINS":       "https://app.example.com, https://staging.example.com",
	}
}

// loadEnv runs config.Load over a map based environment.
func loadEnv(env map[string]string) (*config.Config, error) {
	return config.Load(func(key string) string { return env[key] })
}

func TestConfigLoadsValidEnvironment(t *testing.T) {
	t.Parallel()

	cfg, err := loadEnv(validEnv())
	require.NoError(t, err)

	assert.Equal(t, 8080, cfg.Port)
	assert.Equal(t, ":8080", cfg.Addr())
	assert.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "http://messaging_service:8080", cfg.MessagingServiceURL)
	assert.Equal(t, "identity_service", cfg.IdentityTokenIssuer)
	assert.Equal(t, "realtime_gateway", cfg.IdentityTokenAudience)
	assert.Equal(t, []string{"https://app.example.com", "https://staging.example.com"}, cfg.WSAllowedOrigins)
	assert.Equal(t, "/etc/keys/identity.pem", cfg.IdentityPublicKeyPath)

	// Documented defaults for the optional knobs.
	assert.Equal(t, 25*time.Second, cfg.WSHeartbeatInterval)
	assert.Equal(t, 60*time.Second, cfg.WSReadDeadline)
	assert.Equal(t, 64, cfg.WSSendBufferSize)
	assert.Equal(t, 10*time.Minute, cfg.EventDedupeTTL)
	assert.Equal(t, 20*time.Second, cfg.ShutdownTimeout)
}

func TestConfigParsesOptionalOverrides(t *testing.T) {
	t.Parallel()

	env := validEnv()
	env["KAFKA_SASL_MECHANISM"] = "plain"
	env["KAFKA_SASL_USER"] = "svc"
	env["KAFKA_SASL_PASSWORD"] = "secret"
	env["KAFKA_TLS_ENABLED"] = "true"
	env["IDENTITY_JWKS"] = `{"keys":[]}`
	env["WS_HEARTBEAT_INTERVAL"] = "10"
	env["WS_READ_DEADLINE"] = "45s"
	env["WS_SEND_BUFFER_SIZE"] = "256"
	env["EVENT_DEDUPE_TTL"] = "120"
	env["SHUTDOWN_TIMEOUT"] = "30s"

	cfg, err := loadEnv(env)
	require.NoError(t, err)

	assert.Equal(t, "PLAIN", cfg.KafkaSASLMechanism)
	assert.Equal(t, "svc", cfg.KafkaSASLUser)
	assert.Equal(t, "secret", cfg.KafkaSASLPassword)
	assert.True(t, cfg.KafkaTLSEnabled)
	assert.Equal(t, `{"keys":[]}`, cfg.IdentityJWKS)
	assert.Equal(t, 10*time.Second, cfg.WSHeartbeatInterval)
	assert.Equal(t, 45*time.Second, cfg.WSReadDeadline)
	assert.Equal(t, 256, cfg.WSSendBufferSize)
	assert.Equal(t, 2*time.Minute, cfg.EventDedupeTTL)
	assert.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
}

func TestConfigMissingRequiredVariablesAreNamedInTheError(t *testing.T) {
	t.Parallel()

	required := []string{
		"PORT",
		"KAFKA_BROKERS",
		"MESSAGING_SERVICE_URL",
		"INTERNAL_SERVICE_TOKEN",
		"IDENTITY_TOKEN_ISSUER",
		"IDENTITY_TOKEN_AUDIENCE",
		"WS_ALLOWED_ORIGINS",
	}

	for _, variable := range required {
		variable := variable
		t.Run(variable, func(t *testing.T) {
			t.Parallel()

			env := validEnv()
			delete(env, variable)

			_, err := loadEnv(env)
			require.Error(t, err)
			assert.Contains(t, err.Error(), variable)
		})
	}

	// Identity verification keys are required as JWKS or as the current PEM key.
	env := validEnv()
	delete(env, "IDENTITY_PUBLIC_KEY_PATH")
	_, err := loadEnv(env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IDENTITY_JWKS or IDENTITY_PUBLIC_KEY_PATH")

	// Several problems are reported together so one start-up log lists them all.
	_, err = loadEnv(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PORT")
	assert.Contains(t, err.Error(), "KAFKA_BROKERS")
}

func TestConfigRejectsMalformedValues(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		mutate func(map[string]string)
		want   string
	}{
		"port not a number": {func(env map[string]string) { env["PORT"] = "http" }, "PORT"},
		"port out of range": {func(env map[string]string) { env["PORT"] = "70000" }, "PORT"},
		"no broker":         {func(env map[string]string) { env["KAFKA_BROKERS"] = " , " }, "KAFKA_BROKERS"},
		"relative url":      {func(env map[string]string) { env["MESSAGING_SERVICE_URL"] = "/internal" }, "MESSAGING_SERVICE_URL"},
		"wrong scheme":      {func(env map[string]string) { env["MESSAGING_SERVICE_URL"] = "ftp://messaging" }, "MESSAGING_SERVICE_URL"},
		"bad origin":        {func(env map[string]string) { env["WS_ALLOWED_ORIGINS"] = "app.example.com" }, "WS_ALLOWED_ORIGINS"},
		"sasl without user": {func(env map[string]string) { env["KAFKA_SASL_MECHANISM"] = "PLAIN" }, "KAFKA_SASL_USER"},
		"bad tls flag":      {func(env map[string]string) { env["KAFKA_TLS_ENABLED"] = "yes please" }, "KAFKA_TLS_ENABLED"},
		"bad heartbeat":     {func(env map[string]string) { env["WS_HEARTBEAT_INTERVAL"] = "often" }, "WS_HEARTBEAT_INTERVAL"},
		"zero heartbeat":    {func(env map[string]string) { env["WS_HEARTBEAT_INTERVAL"] = "0" }, "WS_HEARTBEAT_INTERVAL"},
		"read deadline too small": {func(env map[string]string) {
			env["WS_HEARTBEAT_INTERVAL"] = "60"
			env["WS_READ_DEADLINE"] = "30"
		}, "WS_READ_DEADLINE"},
		"bad send buffer":      {func(env map[string]string) { env["WS_SEND_BUFFER_SIZE"] = "many" }, "WS_SEND_BUFFER_SIZE"},
		"negative send buffer": {func(env map[string]string) { env["WS_SEND_BUFFER_SIZE"] = "-8" }, "WS_SEND_BUFFER_SIZE"},
		"bad dedupe ttl":       {func(env map[string]string) { env["EVENT_DEDUPE_TTL"] = "forever" }, "EVENT_DEDUPE_TTL"},
		"bad shutdown timeout": {func(env map[string]string) { env["SHUTDOWN_TIMEOUT"] = "-1" }, "SHUTDOWN_TIMEOUT"},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			env := validEnv()
			tc.mutate(env)
			_, err := loadEnv(env)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestConfigAcceptsWildcardOrigin(t *testing.T) {
	t.Parallel()

	env := validEnv()
	env["WS_ALLOWED_ORIGINS"] = "*"

	cfg, err := loadEnv(env)
	require.NoError(t, err)
	assert.Equal(t, []string{"*"}, cfg.WSAllowedOrigins)
}

func TestConfigLoadReadsProcessEnvironment(t *testing.T) {
	for key, value := range validEnv() {
		t.Setenv(key, value)
	}

	cfg, err := config.Load(nil)
	require.NoError(t, err)
	assert.Equal(t, 8080, cfg.Port)
}
