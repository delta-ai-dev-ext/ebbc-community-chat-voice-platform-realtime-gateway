package gateway

import (
	"fmt"

	"realtime_gateway/internal/contracts"
	"realtime_gateway/internal/models"
)

// UserScopeChange is one subscription change for every live connection of a user.
type UserScopeChange struct {
	UserID string
	Scope  models.Scope
}

// Route is the fan-out plan of one validated event: the broadcast scope the frame is pushed
// on plus the per-user scope changes that keep subscriptions current without a reconnect.
type Route struct {
	// EventType is the frame `type` (the consumed event type).
	EventType string
	// Scope is the subscription scope the frame is delivered on.
	Scope models.Scope
	// Add holds scopes to add to the affected users' connections.
	Add []UserScopeChange
	// Drop holds scopes to drop from the affected users' connections.
	Drop []UserScopeChange
	// DropFirst drops scopes before the snapshot broadcast. Membership and participant
	// removals use it so a removed user stops receiving the scope's snapshots immediately.
	DropFirst bool
}

// RouteEvent computes the routing plan of a decoded event per docs/architecture.md §3.2:
//
//	message_changed            → community:{community_id} (channel) or conversation:{container_id}
//	voice_presence_changed     → community:{community_id}
//	community_member_changed   → community:{community_id} (+ scope add/drop for the member)
//	community_channel_changed  → community:{community_id}
//	conversation_changed       → conversation:{conversation_id} (+ scope add/drop per delta)
//	notification_created       → user:{recipient_user_id}
func RouteEvent(event *contracts.Event) (*Route, error) {
	if event == nil {
		return nil, fmt.Errorf("%w: empty event", contracts.ErrMalformedEvent)
	}

	switch payload := event.Decoded.(type) {
	case *contracts.MessageChangedPayload:
		return routeMessageChanged(event.Envelope, payload)
	case *contracts.VoicePresenceChangedPayload:
		return &Route{
			EventType: event.Envelope.EventType,
			Scope:     models.CommunityScope(payload.CommunityID),
		}, nil
	case *contracts.CommunityMemberChangedPayload:
		return routeCommunityMemberChanged(event.Envelope, payload), nil
	case *contracts.CommunityChannelChangedPayload:
		return &Route{
			EventType: event.Envelope.EventType,
			Scope:     models.CommunityScope(payload.CommunityID),
		}, nil
	case *contracts.ConversationChangedPayload:
		return routeConversationChanged(event.Envelope, payload), nil
	case *contracts.NotificationCreatedPayload:
		return &Route{
			EventType: event.Envelope.EventType,
			Scope:     models.UserScope(payload.RecipientUserID),
		}, nil
	default:
		return nil, fmt.Errorf("%w: %s has no routing rule", contracts.ErrUnknownEvent, event.Envelope.EventType)
	}
}

// routeMessageChanged picks the community scope for channel messages and the conversation
// scope for private messages.
func routeMessageChanged(env contracts.Envelope, payload *contracts.MessageChangedPayload) (*Route, error) {
	route := &Route{EventType: env.EventType}
	if payload.CommunityID != nil && *payload.CommunityID != "" {
		route.Scope = models.CommunityScope(*payload.CommunityID)
		return route, nil
	}
	if payload.ContainerID == "" {
		return nil, fmt.Errorf("%w: message_changed carries no community id and no container id",
			contracts.ErrMalformedEvent)
	}
	route.Scope = models.ConversationScope(payload.ContainerID)
	return route, nil
}

// routeCommunityMemberChanged maps the membership change to a scope add/drop of the affected
// user plus the community snapshot broadcast.
func routeCommunityMemberChanged(env contracts.Envelope, payload *contracts.CommunityMemberChangedPayload) *Route {
	route := &Route{
		EventType: env.EventType,
		Scope:     models.CommunityScope(payload.CommunityID),
	}
	change := UserScopeChange{UserID: payload.UserID, Scope: models.CommunityScope(payload.CommunityID)}

	switch payload.Change {
	case "joined":
		route.Add = append(route.Add, change)
	case "unbanned":
		// The snapshot decides: an unbanned member is only subscribed when the membership
		// is active and not banned.
		if payload.Status == "active" && !payload.IsBanned {
			route.Add = append(route.Add, change)
		}
	case "left", "removed", "banned":
		route.Drop = append(route.Drop, change)
		route.DropFirst = true
	default:
		// muted / unmuted / roles_changed change the snapshot, not the subscription.
	}
	return route
}

// routeConversationChanged maps the participant delta to conversation scope add/drop of the
// affected users plus the conversation snapshot broadcast.
func routeConversationChanged(env contracts.Envelope, payload *contracts.ConversationChangedPayload) *Route {
	scope := models.ConversationScope(payload.ConversationID)
	route := &Route{EventType: env.EventType, Scope: scope}

	for _, userID := range payload.AddedUserIDs {
		route.Add = append(route.Add, UserScopeChange{UserID: userID, Scope: scope})
	}
	for _, userID := range payload.RemovedUserIDs {
		route.Drop = append(route.Drop, UserScopeChange{UserID: userID, Scope: scope})
	}
	if len(route.Drop) > 0 {
		// Removed participants stop receiving the conversation before the snapshot is
		// broadcast: their removal takes effect immediately (US-007).
		route.DropFirst = true
	}
	return route
}
