package contracts

import (
	"encoding/json"
	"fmt"
)

// payloadRule is the required-field and value-check table of one payload contract.
type payloadRule struct {
	required []string
	check    func(map[string]json.RawMessage) error
}

// stringField decodes a required payload field as a JSON string.
func stringField(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("missing field %q", name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("field %q is not a string", name)
	}
	return value, nil
}

// uuidField decodes a required payload field as a UUID string.
func uuidField(fields map[string]json.RawMessage, name string) error {
	value, err := stringField(fields, name)
	if err != nil {
		return err
	}
	if !IsUUID(value) {
		return fmt.Errorf("field %q is not a uuid", name)
	}
	return nil
}

// enumField decodes a required payload field and checks it against the allowed values.
func enumField(fields map[string]json.RawMessage, name string, allowed ...string) error {
	value, err := stringField(fields, name)
	if err != nil {
		return err
	}
	for _, candidate := range allowed {
		if value == candidate {
			return nil
		}
	}
	return fmt.Errorf("field %q has unexpected value %q", name, value)
}

// nullableUUIDField decodes an optional payload field that is either null or a UUID.
func nullableUUIDField(fields map[string]json.RawMessage, name string) error {
	raw, ok := fields[name]
	if !ok || string(raw) == "null" {
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("field %q is neither null nor a string", name)
	}
	if !IsUUID(value) {
		return fmt.Errorf("field %q is not a uuid", name)
	}
	return nil
}

// uuidListField decodes a required payload field as an array of UUID strings.
func uuidListField(fields map[string]json.RawMessage, name string) error {
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("missing field %q", name)
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return fmt.Errorf("field %q is not an array of strings", name)
	}
	for _, value := range values {
		if !IsUUID(value) {
			return fmt.Errorf("field %q contains a non-uuid value", name)
		}
	}
	return nil
}

// intField decodes a required payload field as a JSON integer >= min.
func positiveIntField(fields map[string]json.RawMessage, name string, min int) error {
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("missing field %q", name)
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("field %q is not an integer", name)
	}
	if value < min {
		return fmt.Errorf("field %q must be >= %d", name, min)
	}
	return nil
}

// messageRule validates the EVT-009 payload.
var messageRule = &payloadRule{
	required: []string{
		"message_id", "change", "container_type", "container_id", "community_id",
		"author_user_id", "thread_root_id", "content", "attachments", "mentioned_user_ids",
		"reactions", "deletion_reason", "created_at", "edited_at", "message_version",
	},
	check: func(fields map[string]json.RawMessage) error {
		for _, name := range []string{"message_id", "container_id", "author_user_id"} {
			if err := uuidField(fields, name); err != nil {
				return err
			}
		}
		if err := nullableUUIDField(fields, "community_id"); err != nil {
			return err
		}
		if err := nullableUUIDField(fields, "thread_root_id"); err != nil {
			return err
		}
		if err := enumField(fields, "change", "created", "edited", "deleted", "reactions_changed"); err != nil {
			return err
		}
		if err := enumField(fields, "container_type", "channel", "conversation"); err != nil {
			return err
		}
		if err := positiveIntField(fields, "message_version", 1); err != nil {
			return err
		}
		return uuidListField(fields, "mentioned_user_ids")
	},
}

// voiceRule validates the EVT-010 payload.
var voiceRule = &payloadRule{
	required: []string{
		"community_id", "channel_id", "user_id", "change", "camera_enabled",
		"screen_share_enabled", "publishing_allowed", "participant_count", "capacity",
	},
	check: func(fields map[string]json.RawMessage) error {
		for _, name := range []string{"community_id", "channel_id", "user_id"} {
			if err := uuidField(fields, name); err != nil {
				return err
			}
		}
		if err := enumField(fields, "change", "joined", "left", "media_changed"); err != nil {
			return err
		}
		if err := positiveIntField(fields, "participant_count", 0); err != nil {
			return err
		}
		return positiveIntField(fields, "capacity", 1)
	},
}

// memberRule validates the EVT-002 payload.
var memberRule = &payloadRule{
	required: []string{
		"community_id", "user_id", "change", "status", "is_banned", "muted_until",
		"role_ids", "sanction_id", "member_version",
	},
	check: func(fields map[string]json.RawMessage) error {
		for _, name := range []string{"community_id", "user_id"} {
			if err := uuidField(fields, name); err != nil {
				return err
			}
		}
		if err := enumField(fields, "change", "joined", "left", "removed", "banned",
			"unbanned", "muted", "unmuted", "roles_changed"); err != nil {
			return err
		}
		if err := enumField(fields, "status", "active", "removed"); err != nil {
			return err
		}
		if err := nullableUUIDField(fields, "sanction_id"); err != nil {
			return err
		}
		if err := positiveIntField(fields, "member_version", 1); err != nil {
			return err
		}
		return uuidListField(fields, "role_ids")
	},
}

// channelRule validates the EVT-003 payload.
var channelRule = &payloadRule{
	required: []string{
		"community_id", "channel_id", "change", "kind", "name", "position", "capacity",
		"channel_version",
	},
	check: func(fields map[string]json.RawMessage) error {
		for _, name := range []string{"community_id", "channel_id"} {
			if err := uuidField(fields, name); err != nil {
				return err
			}
		}
		if err := enumField(fields, "change", "created", "updated", "deleted"); err != nil {
			return err
		}
		if err := enumField(fields, "kind", "text", "voice"); err != nil {
			return err
		}
		if err := positiveIntField(fields, "position", 0); err != nil {
			return err
		}
		if err := positiveIntField(fields, "channel_version", 1); err != nil {
			return err
		}
		return nil
	},
}

// conversationRule validates the EVT-008 payload.
var conversationRule = &payloadRule{
	required: []string{
		"conversation_id", "kind", "change", "name", "participant_user_ids",
		"added_user_ids", "removed_user_ids", "conversation_version",
	},
	check: func(fields map[string]json.RawMessage) error {
		if err := uuidField(fields, "conversation_id"); err != nil {
			return err
		}
		if err := enumField(fields, "kind", "direct", "group"); err != nil {
			return err
		}
		if err := enumField(fields, "change", "created", "participants_changed", "renamed"); err != nil {
			return err
		}
		if err := positiveIntField(fields, "conversation_version", 1); err != nil {
			return err
		}
		for _, name := range []string{"participant_user_ids", "added_user_ids", "removed_user_ids"} {
			if err := uuidListField(fields, name); err != nil {
				return err
			}
		}
		return nil
	},
}

// notificationRule validates the EVT-011 payload.
var notificationRule = &payloadRule{
	required: []string{
		"notification_id", "recipient_user_id", "kind", "title", "summary", "link",
		"unread_count",
	},
	check: func(fields map[string]json.RawMessage) error {
		for _, name := range []string{"notification_id", "recipient_user_id"} {
			if err := uuidField(fields, name); err != nil {
				return err
			}
		}
		if err := enumField(fields, "kind", "mention", "friend_request_received",
			"friend_request_accepted", "moderation_notice", "appeal_decided"); err != nil {
			return err
		}
		return positiveIntField(fields, "unread_count", 0)
	},
}
