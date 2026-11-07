// Package gateway owns the connection registry (hub), the scope matcher, the event pipeline
// (consumer → decoder → matcher → fan-out) and the business metrics of the realtime gateway.
// It never imports the WebSocket or Kafka client packages directly; frames are delivered
// through bounded per-connection channels and the feed is driven through narrow interfaces.
package gateway

import (
	"github.com/prometheus/client_golang/prometheus"

	"realtime_gateway/internal/contracts"
)

// Connection outcome labels of realtime_connections_opened_total{result}.
const (
	// ResultAccepted counts upgrades that became a live subscription.
	ResultAccepted = "accepted"
	// ResultRejectedAuth counts refusals caused by the access token.
	ResultRejectedAuth = "rejected_auth"
	// ResultRejectedScopes counts refusals caused by an unavailable scope resolution.
	ResultRejectedScopes = "rejected_scopes"
	// ResultRejectedNotReady counts refusals because the replica is draining or its feed is
	// detached.
	ResultRejectedNotReady = "rejected_not_ready"
)

// Metrics holds the business and Kafka metrics defined in docs/architecture.md §7.
type Metrics struct {
	ConnectionsActive prometheus.Gauge
	ConnectionsOpened *prometheus.CounterVec
	EventsDelivered   *prometheus.CounterVec
	Reconnects        prometheus.Counter
	DroppedFrames     *prometheus.CounterVec
	KafkaLag          *prometheus.GaugeVec
}

// NewMetrics builds the gateway metrics and registers them on reg. Every method is safe on a
// nil receiver so tests and unit-level wiring can skip instrumentation.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		ConnectionsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "realtime_connections_active",
			Help: "Live WebSocket connections on this replica.",
		}),
		ConnectionsOpened: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "realtime_connections_opened_total",
			Help: "WebSocket upgrade outcomes by result.",
		}, []string{"result"}),
		EventsDelivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "realtime_events_delivered_total",
			Help: "Event frames pushed to at least one socket, by event type.",
		}, []string{"event_type"}),
		Reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "realtime_reconnects_total",
			Help: "Client reconnects observed on this replica.",
		}),
		DroppedFrames: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "realtime_dropped_frames_total",
			Help: "Event frames dropped for slow consumers, by event type.",
		}, []string{"event_type"}),
		KafkaLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Current broadcast consumer lag per topic in messages.",
		}, []string{"topic", "consumer_group"}),
	}

	if reg != nil {
		for _, collector := range []prometheus.Collector{
			metrics.ConnectionsActive,
			metrics.ConnectionsOpened,
			metrics.EventsDelivered,
			metrics.Reconnects,
			metrics.DroppedFrames,
			metrics.KafkaLag,
		} {
			// Registration is best effort: a re-registered collector (tests, re-wiring)
			// must not abort startup.
			_ = reg.Register(collector)
		}
	}

	// Pre-create every bounded label combination so the metric families are always present in
	// the exposition even before the first event arrives (dashboards and alerts depend on it).
	for _, result := range []string{ResultAccepted, ResultRejectedAuth, ResultRejectedScopes, ResultRejectedNotReady} {
		metrics.ConnectionsOpened.WithLabelValues(result)
	}
	for _, eventType := range []string{
		contracts.EventMessageChanged,
		contracts.EventVoicePresenceChanged,
		contracts.EventCommunityMemberChanged,
		contracts.EventCommunityChannelChanged,
		contracts.EventConversationChanged,
		contracts.EventNotificationCreated,
	} {
		metrics.EventsDelivered.WithLabelValues(eventType)
		metrics.DroppedFrames.WithLabelValues(eventType)
	}
	for _, topic := range contracts.AllTopics {
		metrics.KafkaLag.WithLabelValues(topic, ConsumerGroupLabel)
	}
	return metrics
}

// ConnectionOpened records one upgrade outcome.
func (m *Metrics) ConnectionOpened(result string) {
	if m == nil || m.ConnectionsOpened == nil {
		return
	}
	m.ConnectionsOpened.WithLabelValues(result).Inc()
}

// ConnectionActivated records one live socket.
func (m *Metrics) ConnectionActivated() {
	if m == nil || m.ConnectionsActive == nil {
		return
	}
	m.ConnectionsActive.Inc()
}

// ConnectionDeactivated records one closed socket.
func (m *Metrics) ConnectionDeactivated() {
	if m == nil || m.ConnectionsActive == nil {
		return
	}
	m.ConnectionsActive.Dec()
}

// EventDelivered records a frame pushed to at least one socket.
func (m *Metrics) EventDelivered(eventType string) {
	if m == nil || m.EventsDelivered == nil {
		return
	}
	m.EventsDelivered.WithLabelValues(eventType).Inc()
}

// FrameDropped records a frame dropped for a slow consumer.
func (m *Metrics) FrameDropped(eventType string) {
	if m == nil || m.DroppedFrames == nil {
		return
	}
	m.DroppedFrames.WithLabelValues(eventType).Inc()
}

// ReconnectObserved records a reconnect of a user seen shortly before on this replica.
func (m *Metrics) ReconnectObserved() {
	if m == nil || m.Reconnects == nil {
		return
	}
	m.Reconnects.Inc()
}

// SetLag updates kafka_consumer_lag for one topic of the broadcast reader.
func (m *Metrics) SetLag(topic, consumerGroup string, lag float64) {
	if m == nil || m.KafkaLag == nil {
		return
	}
	m.KafkaLag.WithLabelValues(topic, consumerGroup).Set(lag)
}
