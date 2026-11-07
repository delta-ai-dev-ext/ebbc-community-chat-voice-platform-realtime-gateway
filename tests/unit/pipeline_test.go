package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"realtime_gateway/internal/contracts"
	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/models"
)

// pipelineFixture wires a hub, a pipeline and the metrics used by the fan-out tests.
type pipelineFixture struct {
	hub      *gateway.Hub
	pipeline *gateway.Pipeline
	metrics  *gateway.Metrics
}

// newPipelineFixture builds a pipeline over a real hub.
func newPipelineFixture(t *testing.T) *pipelineFixture {
	t.Helper()

	metrics := newTestMetrics(t)
	hub := gateway.NewHub(metrics, gateway.HubOptions{
		Clock:         func() time.Time { return testClock },
		SlowDropLimit: 1,
	})
	pipeline := gateway.NewPipeline(hub, contracts.AllTopics, time.Minute, zap.NewNop(), metrics)
	return &pipelineFixture{hub: hub, pipeline: pipeline, metrics: metrics}
}

// handle decodes and dispatches one event of the given type/payload on its topic.
func (f *pipelineFixture) handle(t *testing.T, eventType string, payload map[string]any) {
	t.Helper()
	topic, ok := contracts.TopicForEventType(eventType)
	require.True(t, ok)
	f.pipeline.Handle(topic, rawEvent(t, envelopeFor(eventType, 1), payload))
}

func TestPipelineFansOutEveryEventType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		eventType string
		payload   map[string]any
		scope     models.Scope
	}{
		{"channel message", contracts.EventMessageChanged, messagePayload(), models.CommunityScope(uuidAt(12))},
		{"voice presence", contracts.EventVoicePresenceChanged, voicePayload(), models.CommunityScope(uuidAt(12))},
		{"member change", contracts.EventCommunityMemberChanged, memberPayload(), models.CommunityScope(uuidAt(12))},
		{"channel change", contracts.EventCommunityChannelChanged, channelPayload(), models.CommunityScope(uuidAt(12))},
		{"conversation change", contracts.EventConversationChanged, conversationPayload(), models.ConversationScope(uuidAt(16))},
		{"notification", contracts.EventNotificationCreated, notificationPayload(), models.UserScope(uuidAt(13))},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fixture := newPipelineFixture(t)
			conn := newTestConnection(t, "c1", uuidAt(13), models.UserScope(uuidAt(13)), tc.scope)
			fixture.hub.Register(conn)

			fixture.handle(t, tc.eventType, tc.payload)

			frame := readFrame(t, conn)
			assert.Equal(t, tc.eventType, frame.Type)
			assert.Equal(t, string(tc.scope), frame.Scope)
			assert.JSONEq(t, mustJSON(t, tc.payload), string(frame.Data))
			assert.InDelta(t, 1, counterValue(t, fixture.metrics.EventsDelivered.WithLabelValues(tc.eventType)), 0.0001)
		})
	}
}

func TestPipelineSuppressesDuplicates(t *testing.T) {
	t.Parallel()

	fixture := newPipelineFixture(t)
	community := models.CommunityScope(uuidAt(12))
	conn := newTestConnection(t, "c1", uuidAt(13), community)
	fixture.hub.Register(conn)

	raw := rawEvent(t, envelopeFor(contracts.EventMessageChanged, 1), messagePayload())
	fixture.pipeline.Handle(contracts.TopicMessageEvents, raw)
	fixture.pipeline.Handle(contracts.TopicMessageEvents, raw)

	readFrame(t, conn)
	expectNoFrame(t, conn)
	assert.InDelta(t, 1, counterValue(t, fixture.metrics.EventsDelivered.WithLabelValues(contracts.EventMessageChanged)), 0.0001)
}

func TestPipelineDropsUnknownAndMalformedEvents(t *testing.T) {
	t.Parallel()

	fixture := newPipelineFixture(t)
	community := models.CommunityScope(uuidAt(12))
	conn := newTestConnection(t, "c1", uuidAt(13), community)
	fixture.hub.Register(conn)

	// Unknown event type.
	fixture.pipeline.Handle(contracts.TopicCommunityEvents,
		rawEvent(t, envelopeFor("moderation_sanction_changed", 1), map[string]any{"sanction_id": uuidAt(1)}))
	// Unknown event version.
	fixture.pipeline.Handle(contracts.TopicMessageEvents,
		rawEvent(t, envelopeFor(contracts.EventMessageChanged, 2), messagePayload()))
	// Malformed envelope.
	fixture.pipeline.Handle(contracts.TopicMessageEvents, []byte(`{"envelope":{},"payload":{}}`))
	// Valid event on the wrong topic.
	fixture.pipeline.Handle(contracts.TopicVoiceEvents,
		rawEvent(t, envelopeFor(contracts.EventMessageChanged, 1), messagePayload()))
	// Garbage.
	fixture.pipeline.Handle(contracts.TopicMessageEvents, []byte("not json"))

	expectNoFrame(t, conn)
	assert.InDelta(t, 0, counterValue(t, fixture.metrics.EventsDelivered.WithLabelValues(contracts.EventMessageChanged)), 0.0001)
}

func TestPipelineAddsCommunityScopeBeforeBroadcast(t *testing.T) {
	t.Parallel()

	fixture := newPipelineFixture(t)
	community := models.CommunityScope(uuidAt(12))
	newcomer := newTestConnection(t, "newcomer", uuidAt(14), models.UserScope(uuidAt(14)))
	member := newTestConnection(t, "member", uuidAt(13), community)
	fixture.hub.Register(newcomer)
	fixture.hub.Register(member)

	payload := memberPayload()
	payload["user_id"] = uuidAt(14)
	payload["change"] = "joined"
	fixture.handle(t, contracts.EventCommunityMemberChanged, payload)

	assert.True(t, newcomer.HasScope(community), "joined adds the community scope")
	// The add happens before the broadcast, so the newcomer receives the snapshot too.
	readFrame(t, newcomer)
	readFrame(t, member)

	// Subsequent community events reach the newcomer without a reconnect.
	fixture.handle(t, contracts.EventMessageChanged, messagePayload())
	assert.Equal(t, contracts.EventMessageChanged, readFrame(t, newcomer).Type)
}

func TestPipelineDropsCommunityScopeBeforeRemovalBroadcast(t *testing.T) {
	t.Parallel()

	fixture := newPipelineFixture(t)
	community := models.CommunityScope(uuidAt(12))
	removed := newTestConnection(t, "removed", uuidAt(14), models.UserScope(uuidAt(14)), community)
	remaining := newTestConnection(t, "remaining", uuidAt(13), community)
	fixture.hub.Register(removed)
	fixture.hub.Register(remaining)

	payload := memberPayload()
	payload["user_id"] = uuidAt(14)
	payload["change"] = "banned"
	payload["is_banned"] = true
	payload["status"] = "removed"
	fixture.handle(t, contracts.EventCommunityMemberChanged, payload)

	assert.False(t, removed.HasScope(community))
	expectNoFrame(t, removed)
	assert.Equal(t, contracts.EventCommunityMemberChanged, readFrame(t, remaining).Type)

	// The removed member no longer receives community events.
	fixture.handle(t, contracts.EventMessageChanged, messagePayload())
	expectNoFrame(t, removed)
	readFrame(t, remaining)
}

func TestPipelineMaintainsConversationScopes(t *testing.T) {
	t.Parallel()

	fixture := newPipelineFixture(t)
	conversation := models.ConversationScope(uuidAt(16))
	added := newTestConnection(t, "added", uuidAt(14), models.UserScope(uuidAt(14)))
	dropped := newTestConnection(t, "dropped", uuidAt(19), models.UserScope(uuidAt(19)), conversation)
	existing := newTestConnection(t, "existing", uuidAt(13), conversation)
	fixture.hub.Register(added)
	fixture.hub.Register(dropped)
	fixture.hub.Register(existing)

	payload := conversationPayload()
	payload["added_user_ids"] = []any{uuidAt(14)}
	payload["removed_user_ids"] = []any{uuidAt(19)}
	fixture.handle(t, contracts.EventConversationChanged, payload)

	assert.True(t, added.HasScope(conversation), "added participant is subscribed")
	assert.False(t, dropped.HasScope(conversation), "removed participant is unsubscribed")
	// Added users are subscribed before the broadcast so they receive the snapshot.
	readFrame(t, added)
	readFrame(t, existing)
	expectNoFrame(t, dropped)

	fixture.handle(t, contracts.EventMessageChanged, conversationMessagePayload(uuidAt(16)))
	assert.Equal(t, contracts.EventMessageChanged, readFrame(t, added).Type)
	expectNoFrame(t, dropped)
}

// conversationMessagePayload builds a valid conversation-scoped message payload.
func conversationMessagePayload(conversationID string) map[string]any {
	payload := messagePayload()
	payload["container_type"] = "conversation"
	payload["container_id"] = conversationID
	payload["community_id"] = nil
	return payload
}

func TestPipelineCountsDroppedFramesForSlowSockets(t *testing.T) {
	t.Parallel()

	metrics := newTestMetrics(t)
	hub := gateway.NewHub(metrics, gateway.HubOptions{SlowDropLimit: 5})
	pipeline := gateway.NewPipeline(hub, contracts.AllTopics, time.Minute, zap.NewNop(), metrics)

	community := models.CommunityScope(uuidAt(12))
	slow := gateway.NewConnection("slow", uuidAt(13), uuidAt(30), models.ScopeSet{community}, testClock.Add(time.Hour), 1)
	hub.Register(slow)

	eventType := contracts.EventMessageChanged
	for i := 0; i < 3; i++ {
		payload := messagePayload()
		payload["message_version"] = i + 1
		envelope := envelopeFor(eventType, 1)
		envelope["event_id"] = uuidAt(100 + i)
		pipeline.Handle(contracts.TopicMessageEvents, rawEvent(t, envelope, payload))
	}

	// The single buffer slot took the first frame; the two follow-ups were dropped.
	assert.InDelta(t, 2, counterValue(t, metrics.DroppedFrames.WithLabelValues(eventType)), 0.0001)
	assert.InDelta(t, 1, counterValue(t, metrics.EventsDelivered.WithLabelValues(eventType)), 0.0001)
}

func TestPipelineWorksWithoutSubscribers(t *testing.T) {
	t.Parallel()

	fixture := newPipelineFixture(t)
	// No connection holds the scope: the frame is delivered to nobody and not counted.
	fixture.handle(t, contracts.EventNotificationCreated, notificationPayload())
	assert.InDelta(t, 0, counterValue(t, fixture.metrics.EventsDelivered.WithLabelValues(contracts.EventNotificationCreated)), 0.0001)
}
