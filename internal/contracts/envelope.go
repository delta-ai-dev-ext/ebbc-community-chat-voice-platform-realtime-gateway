// Package contracts mirrors the platform event contract consumed by the realtime gateway:
// the mandatory envelope plus the six event payloads from docs/architecture.md §4.
// Structs are field-for-field copies of the published JSON Schemas; unknown JSON fields are
// tolerated for forward compatibility and unknown event types/versions are rejected by name
// so the caller can log and drop them.
package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Sentinel decode errors. Callers distinguish "malformed" (log WARN with trace id, drop)
// from "unknown event type/version" (log WARN, drop) without crashing the consumer loop.
var (
	// ErrMalformedEvent marks a message whose envelope or payload violates the contract.
	ErrMalformedEvent = errors.New("malformed event")
	// ErrUnknownEvent marks a well-formed message for an event type or version this
	// gateway does not implement.
	ErrUnknownEvent = errors.New("unknown event")
)

// Event type identifiers of the consumed contracts.
const (
	EventMessageChanged          = "message_changed"
	EventVoicePresenceChanged    = "voice_presence_changed"
	EventCommunityMemberChanged  = "community_member_changed"
	EventCommunityChannelChanged = "community_channel_changed"
	EventConversationChanged     = "conversation_changed"
	EventNotificationCreated     = "notification_created"
)

// Kafka topic names consumed by the gateway.
const (
	TopicMessageEvents      = "message_events"
	TopicVoiceEvents        = "voice_events"
	TopicCommunityEvents    = "community_events"
	TopicSocialEvents       = "social_events"
	TopicNotificationEvents = "notification_events"
)

// AllTopics lists the five broadcast topics the gateway consumes on every replica.
var AllTopics = []string{
	TopicMessageEvents,
	TopicVoiceEvents,
	TopicCommunityEvents,
	TopicSocialEvents,
	TopicNotificationEvents,
}

// TopicForEventType maps an event type to the topic that carries it.
func TopicForEventType(eventType string) (string, bool) {
	switch eventType {
	case EventMessageChanged:
		return TopicMessageEvents, true
	case EventVoicePresenceChanged:
		return TopicVoiceEvents, true
	case EventCommunityMemberChanged, EventCommunityChannelChanged:
		return TopicCommunityEvents, true
	case EventConversationChanged:
		return TopicSocialEvents, true
	case EventNotificationCreated:
		return TopicNotificationEvents, true
	default:
		return "", false
	}
}

// Envelope is the mandatory, non-optional envelope of every consumed event.
type Envelope struct {
	EventID       string  `json:"event_id"`
	EventType     string  `json:"event_type"`
	EventVersion  int     `json:"event_version"`
	OccurredAt    string  `json:"occurred_at"`
	Producer      string  `json:"producer"`
	TraceID       string  `json:"trace_id"`
	CorrelationID string  `json:"correlation_id"`
	CausationID   *string `json:"causation_id"`
	SchemaRef     string  `json:"schema_ref"`
}

// envelopeFields lists the envelope keys that must be present (a JSON null still counts as
// present for the nullable causation_id).
var envelopeFields = []string{
	"event_id",
	"event_type",
	"event_version",
	"occurred_at",
	"producer",
	"trace_id",
	"correlation_id",
	"causation_id",
	"schema_ref",
}

var (
	uuidPattern     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	snakePattern    = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	traceIDPattern  = regexp.MustCompile(`^[0-9a-f]{32}$`)
	schemaRefPatern = regexp.MustCompile(`^EVENT_CONTRACTS\.md#[a-z][a-z0-9_]*-v[0-9]+$`)
)

// IsUUID reports whether s is a canonical 8-4-4-4-12 UUID literal.
func IsUUID(s string) bool { return uuidPattern.MatchString(s) }

// PeekTraceID extracts the envelope trace_id of a raw message without validating the rest of
// the contract, so rejected messages can still be logged with their correlation id.
func PeekTraceID(raw []byte) string {
	var outer struct {
		Envelope struct {
			TraceID string `json:"trace_id"`
		} `json:"envelope"`
	}
	if err := json.Unmarshal(raw, &outer); err != nil {
		return ""
	}
	return outer.Envelope.TraceID
}

// IsTraceID reports whether s is a 32 character lowercase hex trace id.
func IsTraceID(s string) bool { return traceIDPattern.MatchString(s) }

// ParseEnvelope validates the mandatory envelope of a consumed message and returns the typed
// value. The raw object is needed as well because `causation_id` must be *present* but may be
// null, which a struct alone cannot distinguish.
func ParseEnvelope(raw json.RawMessage) (Envelope, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Envelope{}, fmt.Errorf("%w: envelope is not a JSON object: %v", ErrMalformedEvent, err)
	}
	for _, name := range envelopeFields {
		if _, ok := fields[name]; !ok {
			return Envelope{}, fmt.Errorf("%w: envelope is missing required field %q", ErrMalformedEvent, name)
		}
	}

	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Envelope{}, fmt.Errorf("%w: envelope decode failed: %v", ErrMalformedEvent, err)
	}

	switch {
	case !IsUUID(env.EventID):
		return Envelope{}, fmt.Errorf("%w: envelope event_id is not a uuid", ErrMalformedEvent)
	case !snakePattern.MatchString(env.EventType):
		return Envelope{}, fmt.Errorf("%w: envelope event_type is not snake_case", ErrMalformedEvent)
	case env.EventVersion < 1:
		return Envelope{}, fmt.Errorf("%w: envelope event_version must be >= 1", ErrMalformedEvent)
	case !snakePattern.MatchString(env.Producer):
		return Envelope{}, fmt.Errorf("%w: envelope producer is not snake_case", ErrMalformedEvent)
	case !IsTraceID(env.TraceID):
		return Envelope{}, fmt.Errorf("%w: envelope trace_id must be 32 lowercase hex characters", ErrMalformedEvent)
	case !IsUUID(env.CorrelationID):
		return Envelope{}, fmt.Errorf("%w: envelope correlation_id is not a uuid", ErrMalformedEvent)
	case env.CausationID != nil && !IsUUID(*env.CausationID):
		return Envelope{}, fmt.Errorf("%w: envelope causation_id is neither null nor a uuid", ErrMalformedEvent)
	case !schemaRefPatern.MatchString(env.SchemaRef):
		return Envelope{}, fmt.Errorf("%w: envelope schema_ref has an unexpected shape", ErrMalformedEvent)
	}

	if _, err := time.Parse(time.RFC3339, env.OccurredAt); err != nil {
		return Envelope{}, fmt.Errorf("%w: envelope occurred_at is not RFC3339", ErrMalformedEvent)
	}

	want := fmt.Sprintf("EVENT_CONTRACTS.md#%s-v%d", env.EventType, env.EventVersion)
	if env.SchemaRef != want {
		return Envelope{}, fmt.Errorf("%w: envelope schema_ref %q does not match event %s v%d",
			ErrMalformedEvent, env.SchemaRef, env.EventType, env.EventVersion)
	}

	return env, nil
}
