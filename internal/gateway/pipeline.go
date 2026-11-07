package gateway

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"realtime_gateway/internal/contracts"
	"realtime_gateway/internal/models"
)

// Subscriber is the fan-out target of the pipeline: the connection registry.
type Subscriber interface {
	// Deliver queues a frame on every connection subscribed to scope and returns how many
	// sockets accepted it.
	Deliver(scope models.Scope, eventType string, frame []byte) int
	// AddUserScope subscribes every live connection of the user to scope.
	AddUserScope(userID string, scope models.Scope) int
	// DropUserScope unsubscribes every live connection of the user from scope.
	DropUserScope(userID string, scope models.Scope) int
}

// Pipeline is the consume-side event pipeline: decoder → dedupe → scope matcher → fan-out.
// One pipeline serves all five topics; each topic gets its own dedupe cache, mirroring the
// per-consumer dedupe state from docs/architecture.md §5.
type Pipeline struct {
	subscriber Subscriber
	dedupers   map[string]*Deduper
	logger     *zap.Logger
	metrics    *Metrics
}

// NewPipeline builds the pipeline for the given topics. A non-positive dedupeTTL disables
// time-based expiry of the dedupe caches.
func NewPipeline(subscriber Subscriber, topics []string, dedupeTTL time.Duration, logger *zap.Logger, metrics *Metrics) *Pipeline {
	if logger == nil {
		logger = zap.NewNop()
	}
	dedupers := make(map[string]*Deduper, len(topics))
	for _, topic := range topics {
		dedupers[topic] = NewDeduper(dedupeTTL, nil)
	}
	return &Pipeline{
		subscriber: subscriber,
		dedupers:   dedupers,
		logger:     logger,
		metrics:    metrics,
	}
}

// Handle processes one raw Kafka message of topic. Malformed messages, unknown event
// types/versions and duplicates are logged and dropped; the consumer loop never fails
// because of a single bad message.
func (p *Pipeline) Handle(topic string, raw []byte) {
	event, err := contracts.Decode(raw)
	if err != nil {
		p.logDrop(topic, raw, err)
		return
	}
	if err := contracts.ValidateTopic(topic, event.Envelope.EventType); err != nil {
		p.logDrop(topic, raw, err)
		return
	}

	deduper := p.deduperFor(topic)
	if deduper.Seen(event.Envelope.EventID) {
		p.logger.Debug("duplicate event dropped",
			zap.String("topic", topic),
			zap.String("event_type", event.Envelope.EventType),
			zap.String("event_id", event.Envelope.EventID),
			zap.String("trace_id", event.Envelope.TraceID),
		)
		return
	}

	route, err := RouteEvent(event)
	if err != nil {
		p.logDrop(topic, raw, err)
		return
	}

	frame, err := models.MarshalFrame(route.EventType, route.Scope, event.Payload)
	if err != nil {
		p.logger.Warn("failed to encode event frame",
			zap.String("topic", topic),
			zap.String("event_type", route.EventType),
			zap.String("event_id", event.Envelope.EventID),
			zap.String("trace_id", event.Envelope.TraceID),
			zap.Error(err),
		)
		return
	}

	eventsDelivered := p.dispatch(route, frame)
	p.logger.Debug("event dispatched",
		zap.String("topic", topic),
		zap.String("event_type", route.EventType),
		zap.String("event_id", event.Envelope.EventID),
		zap.String("trace_id", event.Envelope.TraceID),
		zap.Int("delivered", eventsDelivered),
	)
}

// dispatch applies the routing plan: scope changes and the snapshot broadcast, ordered per
// the plan.
func (p *Pipeline) dispatch(route *Route, frame []byte) int {
	if route.DropFirst {
		p.apply(route.Drop, p.subscriber.DropUserScope)
	}
	p.apply(route.Add, p.subscriber.AddUserScope)
	delivered := p.broadcast(route, frame)
	if !route.DropFirst {
		p.apply(route.Drop, p.subscriber.DropUserScope)
	}
	return delivered
}

// broadcast pushes the frame to every socket holding the scope.
func (p *Pipeline) broadcast(route *Route, frame []byte) int {
	delivered := p.subscriber.Deliver(route.Scope, route.EventType, frame)
	if delivered > 0 {
		p.metrics.EventDelivered(route.EventType)
	}
	return delivered
}

// apply runs one scope change on every connection of each affected user.
func (p *Pipeline) apply(changes []UserScopeChange, fn func(userID string, scope models.Scope) int) {
	for _, change := range changes {
		fn(change.UserID, change.Scope)
	}
}

// deduperFor returns the dedupe cache of a topic, creating one lazily for topics that were
// not known at construction time.
func (p *Pipeline) deduperFor(topic string) *Deduper {
	if deduper, ok := p.dedupers[topic]; ok {
		return deduper
	}
	deduper := NewDeduper(0, nil)
	p.dedupers[topic] = deduper
	return deduper
}

// logDrop records why a message did not produce a frame. The consumed trace id is copied
// into the log line whenever it survived validation.
func (p *Pipeline) logDrop(topic string, raw []byte, err error) {
	fields := []zap.Field{zap.String("topic", topic), zap.Error(err)}
	if traceID := contracts.PeekTraceID(raw); traceID != "" {
		fields = append(fields, zap.String("trace_id", traceID))
	}
	switch {
	case errors.Is(err, contracts.ErrUnknownEvent):
		p.logger.Warn("unknown event dropped", fields...)
	default:
		p.logger.Warn("malformed event dropped", fields...)
	}
}
