package unit

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"realtime_gateway/internal/contracts"
)

// uuidAt returns a deterministic, valid UUID literal for test fixtures.
func uuidAt(n int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", n)
}

// fixedTraceID is a syntactically valid 32 character lowercase hex trace id.
const fixedTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"

// envelopeFor builds a valid envelope map for the given event type/version.
func envelopeFor(eventType string, version int) map[string]any {
	return map[string]any{
		"event_id":       uuidAt(1),
		"event_type":     eventType,
		"event_version":  version,
		"occurred_at":    "2026-02-11T10:00:00Z",
		"producer":       "community_service",
		"trace_id":       fixedTraceID,
		"correlation_id": uuidAt(2),
		"causation_id":   nil,
		"schema_ref":     fmt.Sprintf("EVENT_CONTRACTS.md#%s-v%d", eventType, version),
	}
}

// messagePayload is a valid EVT-009 payload.
func messagePayload() map[string]any {
	return map[string]any{
		"message_id":         uuidAt(10),
		"change":             "created",
		"container_type":     "channel",
		"container_id":       uuidAt(11),
		"community_id":       uuidAt(12),
		"author_user_id":     uuidAt(13),
		"thread_root_id":     nil,
		"content":            "hello",
		"attachments":        []any{},
		"mentioned_user_ids": []any{uuidAt(14)},
		"reactions":          []any{},
		"deletion_reason":    nil,
		"created_at":         "2026-02-11T10:00:00Z",
		"edited_at":          nil,
		"message_version":    3,
	}
}

// voicePayload is a valid EVT-010 payload.
func voicePayload() map[string]any {
	return map[string]any{
		"community_id":         uuidAt(12),
		"channel_id":           uuidAt(15),
		"user_id":              uuidAt(13),
		"change":               "joined",
		"camera_enabled":       true,
		"screen_share_enabled": false,
		"publishing_allowed":   true,
		"participant_count":    2,
		"capacity":             10,
	}
}

// memberPayload is a valid EVT-002 payload.
func memberPayload() map[string]any {
	return map[string]any{
		"community_id":   uuidAt(12),
		"user_id":        uuidAt(13),
		"change":         "joined",
		"status":         "active",
		"is_banned":      false,
		"muted_until":    nil,
		"role_ids":       []any{},
		"sanction_id":    nil,
		"member_version": 2,
	}
}

// channelPayload is a valid EVT-003 payload.
func channelPayload() map[string]any {
	return map[string]any{
		"community_id":    uuidAt(12),
		"channel_id":      uuidAt(15),
		"change":          "created",
		"kind":            "voice",
		"name":            "General",
		"position":        0,
		"capacity":        10,
		"channel_version": 1,
	}
}

// conversationPayload is a valid EVT-008 payload.
func conversationPayload() map[string]any {
	return map[string]any{
		"conversation_id":      uuidAt(16),
		"kind":                 "group",
		"change":               "participants_changed",
		"name":                 "Weekend plans",
		"participant_user_ids": []any{uuidAt(13), uuidAt(14)},
		"added_user_ids":       []any{uuidAt(14)},
		"removed_user_ids":     []any{},
		"conversation_version": 4,
	}
}

// notificationPayload is a valid EVT-011 payload.
func notificationPayload() map[string]any {
	return map[string]any{
		"notification_id":   uuidAt(17),
		"recipient_user_id": uuidAt(13),
		"kind":              "mention",
		"title":             "You were mentioned",
		"summary":           "in #general",
		"link":              "/channels/" + uuidAt(15) + "/messages/" + uuidAt(10),
		"unread_count":      3,
	}
}

// rawEvent marshals an envelope+payload pair exactly as a producer would publish it.
func rawEvent(t *testing.T, envelope, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"envelope": envelope, "payload": payload})
	require.NoError(t, err)
	return raw
}

// decodeEvent builds and decodes one valid event of the given type.
func decodeEvent(t *testing.T, eventType string, payload map[string]any) *contracts.Event {
	t.Helper()
	raw := rawEvent(t, envelopeFor(eventType, 1), payload)
	event, err := contracts.Decode(raw)
	require.NoError(t, err)
	return event
}

func TestDecodeAcceptsEveryContract(t *testing.T) {
	t.Parallel()

	cases := []struct {
		eventType string
		payload   map[string]any
		expect    any
	}{
		{contracts.EventMessageChanged, messagePayload(), &contracts.MessageChangedPayload{}},
		{contracts.EventVoicePresenceChanged, voicePayload(), &contracts.VoicePresenceChangedPayload{}},
		{contracts.EventCommunityMemberChanged, memberPayload(), &contracts.CommunityMemberChangedPayload{}},
		{contracts.EventCommunityChannelChanged, channelPayload(), &contracts.CommunityChannelChangedPayload{}},
		{contracts.EventConversationChanged, conversationPayload(), &contracts.ConversationChangedPayload{}},
		{contracts.EventNotificationCreated, notificationPayload(), &contracts.NotificationCreatedPayload{}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.eventType, func(t *testing.T) {
			t.Parallel()

			event := decodeEvent(t, tc.eventType, tc.payload)

			assert.Equal(t, tc.eventType, event.Envelope.EventType)
			assert.Equal(t, fixedTraceID, event.Envelope.TraceID)
			assert.IsType(t, tc.expect, event.Decoded)
			assert.JSONEq(t, mustJSON(t, tc.payload), string(event.Payload))
		})
	}
}

func TestDecodeUnknownEventTypeIsNotFatal(t *testing.T) {
	t.Parallel()

	envelope := envelopeFor("moderation_sanction_changed", 1)
	raw := rawEvent(t, envelope, map[string]any{"sanction_id": uuidAt(1)})

	_, err := contracts.Decode(raw)
	require.Error(t, err)
	assert.ErrorIs(t, err, contracts.ErrUnknownEvent)
	assert.NotErrorIs(t, err, contracts.ErrMalformedEvent)
}

func TestDecodeUnknownEventVersionIsDropped(t *testing.T) {
	t.Parallel()

	envelope := envelopeFor(contracts.EventMessageChanged, 2)
	raw := rawEvent(t, envelope, messagePayload())

	_, err := contracts.Decode(raw)
	require.Error(t, err)
	assert.ErrorIs(t, err, contracts.ErrUnknownEvent)
}

func TestDecodeEnvelopeFailures(t *testing.T) {
	t.Parallel()

	mutations := map[string]func(map[string]any){
		"missing event_id":     func(e map[string]any) { delete(e, "event_id") },
		"missing causation_id": func(e map[string]any) { delete(e, "causation_id") },
		"event_id not uuid":    func(e map[string]any) { e["event_id"] = "not-a-uuid" },
		"event_type not snake": func(e map[string]any) {
			e["event_type"] = "Message Changed"
			e["schema_ref"] = "EVENT_CONTRACTS.md#message_changed-v1"
		},
		"event_version zero":      func(e map[string]any) { e["event_version"] = 0 },
		"producer not snake":      func(e map[string]any) { e["producer"] = "Community-Service" },
		"trace_id empty":          func(e map[string]any) { e["trace_id"] = "" },
		"trace_id uppercase":      func(e map[string]any) { e["trace_id"] = "4BF92F3577B34DA6A3CE929D0E0E4736" },
		"trace_id wrong length":   func(e map[string]any) { e["trace_id"] = "abc123" },
		"correlation_id not uuid": func(e map[string]any) { e["correlation_id"] = "42" },
		"causation_id not uuid":   func(e map[string]any) { e["causation_id"] = "previous" },
		"schema_ref mismatch":     func(e map[string]any) { e["schema_ref"] = "EVENT_CONTRACTS.md#voice_presence_changed-v1" },
		"schema_ref malformed":    func(e map[string]any) { e["schema_ref"] = "architecture.md#message_changed-v1" },
		"occurred_at not rfc3339": func(e map[string]any) { e["occurred_at"] = "2026-02-11 10:00:00" },
	}

	for name, mutate := range mutations {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			envelope := envelopeFor(contracts.EventMessageChanged, 1)
			mutate(envelope)
			_, err := contracts.Decode(rawEvent(t, envelope, messagePayload()))

			require.Error(t, err)
			assert.ErrorIs(t, err, contracts.ErrMalformedEvent)
		})
	}
}

func TestDecodeCausationIDNullIsAccepted(t *testing.T) {
	t.Parallel()

	envelope := envelopeFor(contracts.EventMessageChanged, 1)
	envelope["causation_id"] = nil
	envelope["event_version"] = 1

	event, err := contracts.Decode(rawEvent(t, envelope, messagePayload()))
	require.NoError(t, err)
	assert.Nil(t, event.Envelope.CausationID)

	causing := uuidAt(9)
	envelope["causation_id"] = causing
	event, err = contracts.Decode(rawEvent(t, envelope, messagePayload()))
	require.NoError(t, err)
	require.NotNil(t, event.Envelope.CausationID)
	assert.Equal(t, causing, *event.Envelope.CausationID)
}

func TestDecodeMessageShapeFailures(t *testing.T) {
	t.Parallel()

	eventType := contracts.EventMessageChanged

	t.Run("not json", func(t *testing.T) {
		t.Parallel()
		_, err := contracts.Decode([]byte("{"))
		assert.ErrorIs(t, err, contracts.ErrMalformedEvent)
	})

	t.Run("missing envelope", func(t *testing.T) {
		t.Parallel()
		raw, err := json.Marshal(map[string]any{"payload": messagePayload()})
		require.NoError(t, err)
		_, err = contracts.Decode(raw)
		assert.ErrorIs(t, err, contracts.ErrMalformedEvent)
	})

	t.Run("missing payload", func(t *testing.T) {
		t.Parallel()
		raw, err := json.Marshal(map[string]any{"envelope": envelopeFor(eventType, 1)})
		require.NoError(t, err)
		_, err = contracts.Decode(raw)
		assert.ErrorIs(t, err, contracts.ErrMalformedEvent)
	})

	t.Run("payload not object", func(t *testing.T) {
		t.Parallel()
		raw, err := json.Marshal(map[string]any{"envelope": envelopeFor(eventType, 1), "payload": "text"})
		require.NoError(t, err)
		_, err = contracts.Decode(raw)
		assert.ErrorIs(t, err, contracts.ErrMalformedEvent)
	})
}

func TestDecodePayloadFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		eventType string
		payload   map[string]any
		mutate    func(map[string]any)
	}{
		{"message missing content", contracts.EventMessageChanged, messagePayload(), func(p map[string]any) { delete(p, "content") }},
		{"message bad change", contracts.EventMessageChanged, messagePayload(), func(p map[string]any) { p["change"] = "restored" }},
		{"message bad container", contracts.EventMessageChanged, messagePayload(), func(p map[string]any) { p["container_type"] = "thread" }},
		{"message bad id", contracts.EventMessageChanged, messagePayload(), func(p map[string]any) { p["message_id"] = "x" }},
		{"message bad version", contracts.EventMessageChanged, messagePayload(), func(p map[string]any) { p["message_version"] = 0 }},
		{"message bad mentions", contracts.EventMessageChanged, messagePayload(), func(p map[string]any) { p["mentioned_user_ids"] = []any{"nope"} }},
		{"voice bad change", contracts.EventVoicePresenceChanged, voicePayload(), func(p map[string]any) { p["change"] = "moved" }},
		{"voice capacity zero", contracts.EventVoicePresenceChanged, voicePayload(), func(p map[string]any) { p["capacity"] = 0 }},
		{"member bad status", contracts.EventCommunityMemberChanged, memberPayload(), func(p map[string]any) { p["status"] = "pending" }},
		{"member missing version", contracts.EventCommunityMemberChanged, memberPayload(), func(p map[string]any) { delete(p, "member_version") }},
		{"member bad sanction", contracts.EventCommunityMemberChanged, memberPayload(), func(p map[string]any) { p["sanction_id"] = "1234" }},
		{"channel bad kind", contracts.EventCommunityChannelChanged, channelPayload(), func(p map[string]any) { p["kind"] = "forum" }},
		{"channel negative position", contracts.EventCommunityChannelChanged, channelPayload(), func(p map[string]any) { p["position"] = -1 }},
		{"conversation bad kind", contracts.EventConversationChanged, conversationPayload(), func(p map[string]any) { p["kind"] = "channel" }},
		{"conversation bad participants", contracts.EventConversationChanged, conversationPayload(), func(p map[string]any) { p["participant_user_ids"] = "nobody" }},
		{"notification bad kind", contracts.EventNotificationCreated, notificationPayload(), func(p map[string]any) { p["kind"] = "promo" }},
		{"notification negative unread", contracts.EventNotificationCreated, notificationPayload(), func(p map[string]any) { p["unread_count"] = -2 }},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			payload := tc.payload
			tc.mutate(payload)
			_, err := contracts.Decode(rawEvent(t, envelopeFor(tc.eventType, 1), payload))

			require.Error(t, err)
			assert.ErrorIs(t, err, contracts.ErrMalformedEvent)
		})
	}
}

func TestDecodeToleratesUnknownPayloadFields(t *testing.T) {
	t.Parallel()

	payload := messagePayload()
	payload["future_field"] = map[string]any{"nested": true}

	event, err := contracts.Decode(rawEvent(t, envelopeFor(contracts.EventMessageChanged, 1), payload))
	require.NoError(t, err)
	assert.Contains(t, string(event.Payload), "future_field")
}

func TestValidateTopic(t *testing.T) {
	t.Parallel()

	require.NoError(t, contracts.ValidateTopic(contracts.TopicMessageEvents, contracts.EventMessageChanged))
	require.NoError(t, contracts.ValidateTopic(contracts.TopicCommunityEvents, contracts.EventCommunityMemberChanged))
	require.NoError(t, contracts.ValidateTopic(contracts.TopicSocialEvents, contracts.EventConversationChanged))

	assert.ErrorIs(t, contracts.ValidateTopic(contracts.TopicMessageEvents, contracts.EventVoicePresenceChanged), contracts.ErrUnknownEvent)
	assert.ErrorIs(t, contracts.ValidateTopic(contracts.TopicMessageEvents, "unknown_type"), contracts.ErrUnknownEvent)
}

func TestTopicForEventTypeCoversAllContracts(t *testing.T) {
	t.Parallel()

	for _, eventType := range []string{
		contracts.EventMessageChanged,
		contracts.EventVoicePresenceChanged,
		contracts.EventCommunityMemberChanged,
		contracts.EventCommunityChannelChanged,
		contracts.EventConversationChanged,
		contracts.EventNotificationCreated,
	} {
		topic, ok := contracts.TopicForEventType(eventType)
		require.True(t, ok, eventType)
		assert.Contains(t, contracts.AllTopics, topic)
	}

	_, ok := contracts.TopicForEventType("nothing")
	assert.False(t, ok)
}
