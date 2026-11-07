package contracts

// Attachment is one message attachment in a message_changed snapshot.
type Attachment struct {
	AttachmentID string `json:"attachment_id"`
	ContentType  string `json:"content_type"`
	SizeBytes    int64  `json:"size_bytes"`
	DownloadURL  string `json:"download_url"`
	URLExpiresAt string `json:"url_expires_at"`
}

// Reaction is one emoji reaction aggregate in a message_changed snapshot.
type Reaction struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
}

// MessageChangedPayload is the payload of EVT-009 `message_changed` v1.
type MessageChangedPayload struct {
	MessageID        string       `json:"message_id"`
	Change           string       `json:"change"`
	ContainerType    string       `json:"container_type"`
	ContainerID      string       `json:"container_id"`
	CommunityID      *string      `json:"community_id"`
	AuthorUserID     string       `json:"author_user_id"`
	ThreadRootID     *string      `json:"thread_root_id"`
	Content          *string      `json:"content"`
	Attachments      []Attachment `json:"attachments"`
	MentionedUserIDs []string     `json:"mentioned_user_ids"`
	Reactions        []Reaction   `json:"reactions"`
	DeletionReason   *string      `json:"deletion_reason"`
	CreatedAt        string       `json:"created_at"`
	EditedAt         *string      `json:"edited_at"`
	MessageVersion   int          `json:"message_version"`
}

// VoicePresenceChangedPayload is the payload of EVT-010 `voice_presence_changed` v1.
type VoicePresenceChangedPayload struct {
	CommunityID        string `json:"community_id"`
	ChannelID          string `json:"channel_id"`
	UserID             string `json:"user_id"`
	Change             string `json:"change"`
	CameraEnabled      bool   `json:"camera_enabled"`
	ScreenShareEnabled bool   `json:"screen_share_enabled"`
	PublishingAllowed  bool   `json:"publishing_allowed"`
	ParticipantCount   int    `json:"participant_count"`
	Capacity           int    `json:"capacity"`
}

// CommunityMemberChangedPayload is the payload of EVT-002 `community_member_changed` v1.
type CommunityMemberChangedPayload struct {
	CommunityID   string   `json:"community_id"`
	UserID        string   `json:"user_id"`
	Change        string   `json:"change"`
	Status        string   `json:"status"`
	IsBanned      bool     `json:"is_banned"`
	MutedUntil    *string  `json:"muted_until"`
	RoleIDs       []string `json:"role_ids"`
	SanctionID    *string  `json:"sanction_id"`
	MemberVersion int      `json:"member_version"`
}

// CommunityChannelChangedPayload is the payload of EVT-003 `community_channel_changed` v1.
type CommunityChannelChangedPayload struct {
	CommunityID    string `json:"community_id"`
	ChannelID      string `json:"channel_id"`
	Change         string `json:"change"`
	Kind           string `json:"kind"`
	Name           string `json:"name"`
	Position       int    `json:"position"`
	Capacity       *int   `json:"capacity"`
	ChannelVersion int    `json:"channel_version"`
}

// ConversationChangedPayload is the payload of EVT-008 `conversation_changed` v1.
type ConversationChangedPayload struct {
	ConversationID      string   `json:"conversation_id"`
	Kind                string   `json:"kind"`
	Change              string   `json:"change"`
	Name                *string  `json:"name"`
	ParticipantUserIDs  []string `json:"participant_user_ids"`
	AddedUserIDs        []string `json:"added_user_ids"`
	RemovedUserIDs      []string `json:"removed_user_ids"`
	ConversationVersion int      `json:"conversation_version"`
}

// NotificationCreatedPayload is the payload of EVT-011 `notification_created` v1.
type NotificationCreatedPayload struct {
	NotificationID  string `json:"notification_id"`
	RecipientUserID string `json:"recipient_user_id"`
	Kind            string `json:"kind"`
	Title           string `json:"title"`
	Summary         string `json:"summary"`
	Link            string `json:"link"`
	UnreadCount     int    `json:"unread_count"`
}
