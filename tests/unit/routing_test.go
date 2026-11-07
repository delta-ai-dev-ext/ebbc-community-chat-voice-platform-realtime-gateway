package unit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"realtime_gateway/internal/contracts"
	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/models"
)

func TestRouteEventScopeMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		eventType string
		payload   map[string]any
		scope     models.Scope
	}{
		{
			name:      "channel message goes to the community",
			eventType: contracts.EventMessageChanged,
			payload:   messagePayload(),
			scope:     models.CommunityScope(uuidAt(12)),
		},
		{
			name:      "voice presence goes to the community",
			eventType: contracts.EventVoicePresenceChanged,
			payload:   voicePayload(),
			scope:     models.CommunityScope(uuidAt(12)),
		},
		{
			name:      "member change goes to the community",
			eventType: contracts.EventCommunityMemberChanged,
			payload:   memberPayload(),
			scope:     models.CommunityScope(uuidAt(12)),
		},
		{
			name:      "channel change goes to the community",
			eventType: contracts.EventCommunityChannelChanged,
			payload:   channelPayload(),
			scope:     models.CommunityScope(uuidAt(12)),
		},
		{
			name:      "conversation change goes to the conversation",
			eventType: contracts.EventConversationChanged,
			payload:   conversationPayload(),
			scope:     models.ConversationScope(uuidAt(16)),
		},
		{
			name:      "notification goes to the recipient user",
			eventType: contracts.EventNotificationCreated,
			payload:   notificationPayload(),
			scope:     models.UserScope(uuidAt(13)),
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			route, err := gateway.RouteEvent(decodeEvent(t, tc.eventType, tc.payload))
			require.NoError(t, err)
			assert.Equal(t, tc.eventType, route.EventType)
			assert.Equal(t, tc.scope, route.Scope)
		})
	}
}

func TestRouteConversationMessageUsesContainerID(t *testing.T) {
	t.Parallel()

	payload := messagePayload()
	payload["container_type"] = "conversation"
	payload["community_id"] = nil
	payload["container_id"] = uuidAt(16)

	route, err := gateway.RouteEvent(decodeEvent(t, contracts.EventMessageChanged, payload))
	require.NoError(t, err)
	assert.Equal(t, models.ConversationScope(uuidAt(16)), route.Scope)
}

func TestRouteMemberChangeScopeAddDrop(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		change    string
		status    string
		banned    bool
		wantAdd   bool
		wantDrop  bool
		dropFirst bool
	}{
		{name: "joined adds", change: "joined", status: "active", wantAdd: true},
		{name: "unbanned active adds", change: "unbanned", status: "active", wantAdd: true},
		{name: "unbanned but banned adds nothing", change: "unbanned", status: "removed", banned: true},
		{name: "unbanned but removed adds nothing", change: "unbanned", status: "removed"},
		{name: "left drops", change: "left", status: "removed", wantDrop: true, dropFirst: true},
		{name: "removed drops", change: "removed", status: "removed", wantDrop: true, dropFirst: true},
		{name: "banned drops", change: "banned", status: "removed", banned: true, wantDrop: true, dropFirst: true},
		{name: "muted does not change scopes", change: "muted", status: "active"},
		{name: "roles changed does not change scopes", change: "roles_changed", status: "active"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			payload := memberPayload()
			payload["change"] = tc.change
			payload["status"] = tc.status
			payload["is_banned"] = tc.banned

			route, err := gateway.RouteEvent(decodeEvent(t, contracts.EventCommunityMemberChanged, payload))
			require.NoError(t, err)

			scope := models.CommunityScope(uuidAt(12))
			if tc.wantAdd {
				assert.Equal(t, []gateway.UserScopeChange{{UserID: uuidAt(13), Scope: scope}}, route.Add)
			} else {
				assert.Empty(t, route.Add)
			}
			if tc.wantDrop {
				assert.Equal(t, []gateway.UserScopeChange{{UserID: uuidAt(13), Scope: scope}}, route.Drop)
			} else {
				assert.Empty(t, route.Drop)
			}
			assert.Equal(t, tc.dropFirst, route.DropFirst)
		})
	}
}

func TestRouteConversationDeltaScopeChanges(t *testing.T) {
	t.Parallel()

	payload := conversationPayload()
	payload["added_user_ids"] = []any{uuidAt(14), uuidAt(18)}
	payload["removed_user_ids"] = []any{uuidAt(19)}

	route, err := gateway.RouteEvent(decodeEvent(t, contracts.EventConversationChanged, payload))
	require.NoError(t, err)

	scope := models.ConversationScope(uuidAt(16))
	assert.ElementsMatch(t, []gateway.UserScopeChange{
		{UserID: uuidAt(14), Scope: scope},
		{UserID: uuidAt(18), Scope: scope},
	}, route.Add)
	assert.Equal(t, []gateway.UserScopeChange{{UserID: uuidAt(19), Scope: scope}}, route.Drop)
	assert.True(t, route.DropFirst, "removed participants are unsubscribed before the snapshot")
}

func TestRouteEventRejectsUnroutableEvents(t *testing.T) {
	t.Parallel()

	_, err := gateway.RouteEvent(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, contracts.ErrMalformedEvent)

	event := decodeEvent(t, contracts.EventMessageChanged, messagePayload())
	event.Decoded = struct{}{}
	_, err = gateway.RouteEvent(event)
	require.Error(t, err)
	assert.ErrorIs(t, err, contracts.ErrUnknownEvent)
}

func TestRouteMessageWithoutAnyContainerFails(t *testing.T) {
	t.Parallel()

	payload := messagePayload()
	payload["community_id"] = nil
	payload["container_id"] = ""

	event := decodeEvent(t, contracts.EventMessageChanged, messagePayload())
	event.Decoded = &contracts.MessageChangedPayload{ContainerID: ""}

	_, err := gateway.RouteEvent(event)
	require.Error(t, err)
	assert.ErrorIs(t, err, contracts.ErrMalformedEvent)
}
