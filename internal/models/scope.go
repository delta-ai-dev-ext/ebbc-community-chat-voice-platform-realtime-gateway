// Package models holds the shared domain types of the realtime gateway: subscription
// scopes, wire frames, client control messages and close codes.
package models

import "strings"

// Scope identifies one subscription target of a connection. It is encoded on the wire as
// `<scope_type>:<uuid>`, e.g. `community:5f0b8d2e-...`, `conversation:...` or `user:...`.
type Scope string

// Scope kinds used to build and classify scopes.
const (
	ScopeKindCommunity    = "community"
	ScopeKindConversation = "conversation"
	ScopeKindUser         = "user"
)

// UserScope returns the personal scope of a user.
func UserScope(userID string) Scope {
	return Scope(ScopeKindUser + ":" + userID)
}

// CommunityScope returns the subscription scope of a community.
func CommunityScope(communityID string) Scope {
	return Scope(ScopeKindCommunity + ":" + communityID)
}

// ConversationScope returns the subscription scope of a private conversation.
func ConversationScope(conversationID string) Scope {
	return Scope(ScopeKindConversation + ":" + conversationID)
}

// Kind returns the scope type prefix ("community", "conversation" or "user").
func (s Scope) Kind() string {
	idx := strings.IndexByte(string(s), ':')
	if idx < 0 {
		return string(s)
	}
	return string(s)[:idx]
}

// ID returns the identifier part of the scope, or the empty string when the scope is
// malformed.
func (s Scope) ID() string {
	idx := strings.IndexByte(string(s), ':')
	if idx < 0 || idx == len(s)-1 {
		return ""
	}
	return string(s)[idx+1:]
}

// Valid reports whether the scope is a well-formed `<kind>:<id>` scope.
func (s Scope) Valid() bool {
	if s.ID() == "" {
		return false
	}
	switch s.Kind() {
	case ScopeKindCommunity, ScopeKindConversation, ScopeKindUser:
		return true
	default:
		return false
	}
}

// ScopeSet is an ordered, duplicate-free collection of scopes.
type ScopeSet []Scope

// NewScopeSet builds a scope set from the user id plus the resolved community and
// conversation ids. Empty identifiers are ignored.
func NewScopeSet(userID string, communityIDs, conversationIDs []string) ScopeSet {
	set := make(ScopeSet, 0, 1+len(communityIDs)+len(conversationIDs))
	if userID != "" {
		set = append(set, UserScope(userID))
	}
	for _, id := range communityIDs {
		if id != "" {
			set = append(set, CommunityScope(id))
		}
	}
	for _, id := range conversationIDs {
		if id != "" {
			set = append(set, ConversationScope(id))
		}
	}
	return set
}

// Contains reports whether the set contains the scope.
func (s ScopeSet) Contains(scope Scope) bool {
	for _, candidate := range s {
		if candidate == scope {
			return true
		}
	}
	return false
}
