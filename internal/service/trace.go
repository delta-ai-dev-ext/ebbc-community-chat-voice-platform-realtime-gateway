package service

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"realtime_gateway/internal/contracts"
)

// TraceContext carries the inbound W3C trace identifiers that the gateway propagates to
// API-003 and copies into its log lines.
type TraceContext struct {
	// Traceparent is the inbound W3C `traceparent` header, if any.
	Traceparent string
	// TraceID is the 32 character lowercase hex trace id (`X-Trace-Id` or the traceparent
	// trace-id field, minted at the edge when the caller supplied none).
	TraceID string
}

// TraceFromHeader extracts the trace identifiers from inbound request headers. A trace id is
// always produced: when the caller supplied none the gateway mints one, because every
// propagated trace must be identifiable.
func TraceFromHeader(header http.Header) TraceContext {
	traceparent := strings.TrimSpace(header.Get("Traceparent"))
	traceID := strings.TrimSpace(header.Get("X-Trace-Id"))

	if traceID == "" {
		traceID = traceIDFromTraceparent(traceparent)
	}
	if traceID == "" || !contracts.IsTraceID(traceID) {
		traceID = mintTraceID()
	}
	return TraceContext{Traceparent: traceparent, TraceID: traceID}
}

// traceIDFromTraceparent parses `00-<trace-id>-<parent-id>-<flags>` and returns trace-id.
func traceIDFromTraceparent(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 {
		return ""
	}
	if !contracts.IsTraceID(parts[1]) {
		return ""
	}
	return parts[1]
}

// mintTraceID returns a fresh random 32 character lowercase hex trace id.
func mintTraceID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// rand.Read never fails on supported platforms; fall back to a constant so the
		// field is never empty (trace ids are correlation aids, not secrets).
		return strings.Repeat("0", 32)
	}
	return hex.EncodeToString(buf[:])
}

// LogFields renders the trace context as structured log fields.
func (t TraceContext) LogFields() []zap.Field {
	fields := []zap.Field{zap.String("trace_id", t.TraceID)}
	if t.Traceparent != "" {
		fields = append(fields, zap.String("traceparent", t.Traceparent))
	}
	return fields
}
