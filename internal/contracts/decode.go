package contracts

import (
	"encoding/json"
	"fmt"
)

// Event is one decoded, validated platform event. Payload keeps the payload object exactly
// as produced (it is forwarded inside the pushed frame), Decoded holds the typed payload of
// the matching contract.
type Event struct {
	Envelope Envelope
	Payload  json.RawMessage
	Decoded  any
}

// Decode validates the mandatory envelope and the payload of a raw Kafka message.
//
// A violation of the envelope or payload contract returns an error wrapping
// ErrMalformedEvent. A well-formed message for an unsupported event type or version returns
// an error wrapping ErrUnknownEvent. Neither case is fatal for the consumer loop.
func Decode(raw []byte) (*Event, error) {
	var outer map[string]json.RawMessage
	if err := json.Unmarshal(raw, &outer); err != nil {
		return nil, fmt.Errorf("%w: message is not a JSON object: %v", ErrMalformedEvent, err)
	}
	envelopeRaw, ok := outer["envelope"]
	if !ok {
		return nil, fmt.Errorf("%w: message has no envelope", ErrMalformedEvent)
	}
	payloadRaw, ok := outer["payload"]
	if !ok {
		return nil, fmt.Errorf("%w: message has no payload", ErrMalformedEvent)
	}

	env, err := ParseEnvelope(envelopeRaw)
	if err != nil {
		return nil, err
	}

	decoded, err := decodePayload(env, payloadRaw)
	if err != nil {
		return nil, err
	}

	return &Event{Envelope: env, Payload: payloadRaw, Decoded: decoded}, nil
}

// ValidateTopic reports whether the topic carries the event type. A mismatch means the
// message does not belong to the consumed contract stream.
func ValidateTopic(topic, eventType string) error {
	got, ok := TopicForEventType(eventType)
	if !ok {
		return fmt.Errorf("%w: event type %q has no contract", ErrUnknownEvent, eventType)
	}
	if got != topic {
		return fmt.Errorf("%w: event type %q is not published on %s", ErrUnknownEvent, eventType, topic)
	}
	return nil
}

// decodePayload switches on the registered contract version and type-checks the payload.
func decodePayload(env Envelope, raw json.RawMessage) (any, error) {
	if _, ok := TopicForEventType(env.EventType); !ok {
		return nil, fmt.Errorf("%w: event type %q", ErrUnknownEvent, env.EventType)
	}
	if env.EventVersion != 1 {
		return nil, fmt.Errorf("%w: event %s version %d", ErrUnknownEvent, env.EventType, env.EventVersion)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("%w: payload is not a JSON object: %v", ErrMalformedEvent, err)
	}

	var (
		target   any
		required []string
		spec     *payloadRule
	)
	switch env.EventType {
	case EventMessageChanged:
		p := &MessageChangedPayload{}
		target, spec = p, messageRule
		required = messageRule.required
	case EventVoicePresenceChanged:
		p := &VoicePresenceChangedPayload{}
		target, spec = p, voiceRule
		required = voiceRule.required
	case EventCommunityMemberChanged:
		p := &CommunityMemberChangedPayload{}
		target, spec = p, memberRule
		required = memberRule.required
	case EventCommunityChannelChanged:
		p := &CommunityChannelChangedPayload{}
		target, spec = p, channelRule
		required = channelRule.required
	case EventConversationChanged:
		p := &ConversationChangedPayload{}
		target, spec = p, conversationRule
		required = conversationRule.required
	case EventNotificationCreated:
		p := &NotificationCreatedPayload{}
		target, spec = p, notificationRule
		required = notificationRule.required
	default:
		return nil, fmt.Errorf("%w: event type %q", ErrUnknownEvent, env.EventType)
	}

	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return nil, fmt.Errorf("%w: %s payload is missing required field %q",
				ErrMalformedEvent, env.EventType, name)
		}
	}

	// Unknown fields are tolerated: newer producers may add optional payload members.
	if err := json.Unmarshal(raw, target); err != nil {
		return nil, fmt.Errorf("%w: %s payload decode failed: %v", ErrMalformedEvent, env.EventType, err)
	}
	if err := spec.check(fields); err != nil {
		return nil, fmt.Errorf("%w: %s payload is invalid: %v", ErrMalformedEvent, env.EventType, err)
	}
	return target, nil
}
