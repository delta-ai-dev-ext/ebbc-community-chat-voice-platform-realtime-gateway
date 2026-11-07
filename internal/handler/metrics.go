// Package handler serves the WebSocket upgrade endpoint, the liveness/readiness probes and
// the Prometheus metrics endpoint. Handlers stay thin: they authenticate, resolve scopes and
// delegate the socket lifecycle to the service layer.
package handler

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// HTTPMetrics holds the platform RED metrics of the HTTP surface.
type HTTPMetrics struct {
	// Requests counts requests per route, method and status class.
	Requests *prometheus.CounterVec
	// Duration observes request latency per route and method.
	Duration *prometheus.HistogramVec
}

// NewHTTPMetrics builds the HTTP metrics and registers them on reg (best effort).
func NewHTTPMetrics(reg prometheus.Registerer) *HTTPMetrics {
	metrics := &HTTPMetrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests handled, by route, method and status class.",
		}, []string{"route", "method", "status_class"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds, by route and method.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"route", "method"}),
	}

	if reg != nil {
		_ = reg.Register(metrics.Requests)
		_ = reg.Register(metrics.Duration)
	}
	return metrics
}

// observe records one request. route must be a route template, never a raw URL, to keep the
// label cardinality bounded.
func (m *HTTPMetrics) observe(route, method, statusClass string, duration time.Duration) {
	if m == nil {
		return
	}
	m.Requests.WithLabelValues(route, method, statusClass).Inc()
	m.Duration.WithLabelValues(route, method).Observe(duration.Seconds())
}

// statusClassOf renders the low-cardinality status class label ("2xx", "4xx", ...).
func statusClassOf(status int) string {
	if status < 100 || status > 599 {
		return "unknown"
	}
	return fmt.Sprintf("%dxx", status/100)
}

// instrument is the RED middleware: it records request counts and latencies using the
// matched route template. The raw request URL is never logged, so the access_token query
// parameter cannot leak into logs.
func instrument(metrics *HTTPMetrics, log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		duration := time.Since(start)
		metrics.observe(route, c.Request.Method, statusClassOf(c.Writer.Status()), duration)
		log.Debug("http request completed",
			zap.String("route", route),
			zap.String("method", c.Request.Method),
			zap.Int("status", c.Writer.Status()),
			zap.Int64("duration_ms", duration.Milliseconds()),
		)
	}
}
