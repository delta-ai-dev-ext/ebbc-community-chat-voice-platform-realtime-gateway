package models

import "encoding/json"

// Frame is one server-pushed live event: `{type, scope, data}`. `data` carries the
// consumed event's payload object unchanged.
type Frame struct {
	Type  string          `json:"type"`
	Scope string          `json:"scope"`
	Data  json.RawMessage `json:"data"`
}

// MarshalFrame encodes a frame for the wire.
func MarshalFrame(eventType string, scope Scope, payload json.RawMessage) ([]byte, error) {
	return json.Marshal(Frame{Type: eventType, Scope: string(scope), Data: payload})
}

// Ack is a server control reply to a client message (`heartbeat_ack`, `refresh_token_ack`).
type Ack struct {
	Type  string `json:"type"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// Ack types accepted on the socket.
const (
	AckHeartbeat    = "heartbeat_ack"
	AckRefreshToken = "refresh_token_ack"
)

// ClientMessageType values accepted from clients. Everything else is a protocol
// violation (close code 4400).
const (
	ClientMessageHeartbeat    = "heartbeat"
	ClientMessageRefreshToken = "refresh_token"
)

// ClientMessage is the client-to-server control surface: heartbeats and in-band token
// refresh only. All domain writes go through the domain REST APIs.
type ClientMessage struct {
	Type        string `json:"type"`
	AccessToken string `json:"access_token,omitempty"`
}
