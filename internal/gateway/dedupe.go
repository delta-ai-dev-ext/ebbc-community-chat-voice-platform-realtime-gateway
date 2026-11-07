package gateway

import (
	"sync"
	"time"
)

// sweepThreshold is the entry count above which expired dedupe entries are collected.
const sweepThreshold = 1024

// Deduper is the bounded in-memory `event_id` → seen cache of one consumer. It closes the
// at-least-once gap so a socket never receives the same event twice; the no-replay guarantee
// itself comes from consuming at latest offsets.
type Deduper struct {
	mu      sync.Mutex
	ttl     time.Duration
	clock   func() time.Time
	entries map[string]time.Time // event id → expiry
}

// NewDeduper builds a dedupe cache with the given retention window. A non-positive TTL
// disables expiry (entries live until the configured entry threshold is swept).
func NewDeduper(ttl time.Duration, clock func() time.Time) *Deduper {
	if clock == nil {
		clock = time.Now
	}
	return &Deduper{
		ttl:     ttl,
		clock:   clock,
		entries: make(map[string]time.Time),
	}
}

// Seen reports whether eventID was already processed within the retention window and marks
// it as processed when it is new. Empty ids are always treated as new (the envelope
// validator rejects them before this point).
func (d *Deduper) Seen(eventID string) bool {
	if eventID == "" {
		return false
	}
	now := d.clock()

	d.mu.Lock()
	defer d.mu.Unlock()

	if expiry, ok := d.entries[eventID]; ok {
		if d.ttl <= 0 || now.Before(expiry) {
			return true
		}
	}
	expiry := now.Add(d.ttl)
	if d.ttl <= 0 {
		// A non-positive TTL means "retain until swept for size".
		expiry = now.Add(100 * 365 * 24 * time.Hour)
	}
	d.entries[eventID] = expiry
	if len(d.entries) > sweepThreshold {
		d.sweepLocked(now)
	}
	return false
}

// sweepLocked drops expired entries. Callers hold the write lock.
func (d *Deduper) sweepLocked(now time.Time) {
	for eventID, expiry := range d.entries {
		if !now.Before(expiry) {
			delete(d.entries, eventID)
		}
	}
}

// Len returns the number of retained entries (used by tests and diagnostics).
func (d *Deduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.entries)
}
