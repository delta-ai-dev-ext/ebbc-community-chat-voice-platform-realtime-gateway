// Command server is the realtime_gateway entrypoint. It loads and validates the 12-factor
// environment, wires the HTTP surface to the broadcast event feed, and runs the process until
// SIGTERM/SIGINT, after which it drains within the bounded shutdown window described in
// docs/architecture.md §3.5 and docs/engineering_guide.md §3.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"realtime_gateway/internal/config"
	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/handler"
	"realtime_gateway/internal/service"
)

const (
	// serviceName is the `service_name` field carried by every structured log line.
	serviceName = "realtime_gateway"

	// writeTimeout bounds a single socket write and the HTTP servers' response write.
	writeTimeout = 10 * time.Second
	// readHeaderTimeout bounds the header read of an inbound HTTP request.
	readHeaderTimeout = 10 * time.Second

	// exitOK and exitFailure are the only process statuses: the drain always exits 0.
	exitOK      = 0
	exitFailure = 1
)

// app is one fully wired gateway process: the HTTP surface, the broadcast feed and the drain
// signal that closes live sockets.
type app struct {
	cfg    *config.Config
	logger *zap.Logger
	server *http.Server
	web    *handler.Handler
	feed   *gateway.KafkaFeed

	// drain is closed once on shutdown; every live session flushes its buffered frames and
	// closes with 1001 going away when it observes the close.
	drain    chan struct{}
	drainOne sync.Once

	// feedStop cancels the feed context and feedDone reports that the consumers exited.
	feedStop context.CancelFunc
	feedDone chan struct{}
}

func main() {
	os.Exit(run())
}

// run wires and serves the gateway, returning the process exit status instead of calling
// os.Exit so that deferred cleanup (logger flush) always runs.
func run() int {
	// Local development convenience only (`.env` is gitignored); production reads plain
	// environment variables. A missing file is the normal production case.
	_ = godotenv.Load()

	logger, err := newLogger(os.Getenv("LOG_LEVEL"))
	if err != nil {
		// No logger yet: report the only failure that cannot be logged structurally.
		return exitFailure
	}
	defer func() { _ = logger.Sync() }()

	// Fail fast: every offending variable is reported together so a misconfigured replica
	// never starts serving.
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		logger.Error("invalid configuration",
			zap.String("operation", "startup"),
			zap.String("status", "invalid_config"),
			zap.Error(err),
		)
		return exitFailure
	}

	wired, err := newApp(cfg, logger)
	if err != nil {
		logger.Error("gateway wiring failed",
			zap.String("operation", "startup"),
			zap.String("status", "failed"),
			zap.Error(err),
		)
		return exitFailure
	}

	// SIGTERM/SIGINT start the graceful drain; a second signal is left to the default
	// handler so an operator can always interrupt a stuck drain.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	return wired.serve(ctx)
}

// newApp builds the dependency graph from configuration. Everything that can fail because of
// configuration or key material fails here, before a single connection is accepted.
func newApp(cfg *config.Config, logger *zap.Logger) (*app, error) {
	keys, err := service.LoadKeySet(cfg.IdentityJWKS, cfg.IdentityPublicKeyPath, cfg.IdentityNextPublicKeyPath)
	if err != nil {
		return nil, err
	}
	verifier, err := service.NewJWTVerifier(keys, service.VerifierOptions{
		Issuer:   cfg.IdentityTokenIssuer,
		Audience: cfg.IdentityTokenAudience,
	})
	if err != nil {
		return nil, err
	}

	// One registry for the business, Kafka and RED metrics: /metrics must expose all of them.
	registry := prometheus.NewRegistry()
	metrics := gateway.NewMetrics(registry)
	hub := gateway.NewHub(metrics, gateway.HubOptions{})
	pipeline := gateway.NewPipeline(hub, gateway.TopicList(), cfg.EventDedupeTTL, logger, metrics)
	feed := gateway.NewKafkaFeed(gateway.FeedConfig{
		Brokers:       cfg.KafkaBrokers,
		Topics:        gateway.TopicList(),
		SASLMechanism: cfg.KafkaSASLMechanism,
		SASLUser:      cfg.KafkaSASLUser,
		SASLPassword:  cfg.KafkaSASLPassword,
		TLSEnabled:    cfg.KafkaTLSEnabled,
	}, pipeline, logger, gateway.FeedDeps{}, metrics)

	// A nil doer keeps the scope client's own transport: a 2 s per-attempt timeout with one
	// retry on 5xx/timeout (API-003 policy).
	scopeClient := service.NewScopeClient(cfg.MessagingServiceURL, cfg.InternalServiceToken, nil, logger)

	drain := make(chan struct{})
	web := handler.New(handler.Deps{
		Config: handler.Config{
			AllowedOrigins:    cfg.WSAllowedOrigins,
			SendBufferSize:    cfg.WSSendBufferSize,
			HeartbeatInterval: cfg.WSHeartbeatInterval,
			ReadDeadline:      cfg.WSReadDeadline,
			WriteTimeout:      writeTimeout,
		},
		Verifier: verifier,
		Resolver: scopeClient,
		Hub:      hub,
		Feed:     feed,
		Metrics:  metrics,
		Registry: registry,
		Logger:   logger,
		Drain:    drain,
	})

	server := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           web.Router(),
		ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout:      writeTimeout,
	}

	return &app{
		cfg:    cfg,
		logger: logger,
		server: server,
		web:    web,
		feed:   feed,
		drain:  drain,
	}, nil
}

// serve attaches the feed, serves HTTP and drains on ctx cancellation. Broker outages never
// terminate the process: the feed stays detached (readiness 503) and retries on its own.
func (a *app) serve(ctx context.Context) int {
	// The feed runs on its own context so it can be stopped after the sockets have drained.
	feedCtx, stopFeed := context.WithCancel(context.Background())
	a.feedStop = stopFeed
	a.feedDone = make(chan struct{})

	go func() {
		defer close(a.feedDone)
		a.feed.Run(feedCtx)
	}()

	serveErr := make(chan error, 1)
	go func() {
		a.logger.Info("gateway listening",
			zap.String("operation", "startup"),
			zap.String("status", "started"),
			zap.String("addr", a.cfg.Addr()),
			zap.Strings("topics", gateway.TopicList()),
		)
		serveErr <- a.server.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		// The listener failed: nothing can be served, so stop the consumers and report.
		stopFeed()
		<-a.feedDone
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Error("http server stopped",
				zap.String("operation", "serve"),
				zap.String("status", "failed"),
				zap.Error(err),
			)
			return exitFailure
		}
		return exitOK
	case <-ctx.Done():
		a.logger.Info("shutdown signal received",
			zap.String("operation", "shutdown"),
			zap.String("status", "draining"),
		)
	}

	// The watchdog guarantees the documented bound even if a socket write or a broker call
	// hangs. The drain window is a hard deadline, so the process still exits 0.
	watchdog := time.AfterFunc(a.cfg.ShutdownTimeout, func() {
		a.logger.Error("shutdown watchdog expired",
			zap.String("operation", "shutdown"),
			zap.String("status", "forced_exit"),
			zap.Duration("shutdown_timeout", a.cfg.ShutdownTimeout),
		)
		_ = a.logger.Sync()
		os.Exit(exitOK)
	})
	defer watchdog.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()

	if err := a.drainNow(ctx); err != nil {
		a.logger.Warn("graceful drain did not complete",
			zap.String("operation", "shutdown"),
			zap.String("status", "incomplete"),
			zap.Error(err),
		)
	}

	a.logger.Info("shutdown complete",
		zap.String("operation", "shutdown"),
		zap.String("status", "stopped"),
	)
	return exitOK
}

// drainNow performs the documented shutdown order: readiness flips to 503 and upgrades are
// refused, buffered frames are flushed and sockets close with 1001 going away, then the feed
// consumers stop and the listener shuts down. Everything is bounded by ctx.
func (a *app) drainNow(ctx context.Context) error {
	// 1 + 2: /readyz starts answering 503 and the handler refuses new upgrades.
	a.web.BeginDrain()

	// 3: every live session observes the closed channel, flushes its buffered frames and
	// closes with 1001 going away. Waiting keeps the listener up so readiness reads 503.
	a.drainOne.Do(func() { close(a.drain) })
	sessionErr := a.web.WaitForSessions(ctx)

	// 4: the sockets are gone, so the consumers can be released.
	a.feedStop()
	var feedErr error
	select {
	case <-a.feedDone:
	case <-ctx.Done():
		feedErr = ctx.Err()
	}

	// 5: stop accepting new connections; the defer'd watchdog bounds the whole sequence.
	shutdownErr := a.server.Shutdown(ctx)

	var errs []error
	for _, err := range []error{sessionErr, feedErr, shutdownErr} {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// newLogger builds the structured JSON logger used by every component: it carries the
// service_name field on every line and takes its level from LOG_LEVEL (DEBUG locally).
func newLogger(level string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.InitialFields = map[string]any{"service_name": serviceName}
	cfg.Level = zap.NewAtomicLevelAt(logLevel(level))
	return cfg.Build()
}

// logLevel maps LOG_LEVEL to a zap level; an unset or unknown value means INFO.
func logLevel(raw string) zapcore.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return zapcore.DebugLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
