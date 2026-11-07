package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"realtime_gateway/internal/gateway"
)

func TestDeduperMarksAndDetectsDuplicates(t *testing.T) {
	t.Parallel()

	deduper := gateway.NewDeduper(time.Minute, func() time.Time { return testClock })
	eventID := uuidAt(1)

	assert.False(t, deduper.Seen(eventID), "first sight")
	assert.True(t, deduper.Seen(eventID), "duplicate")
	assert.Equal(t, 1, deduper.Len())

	// A different event id is independent.
	assert.False(t, deduper.Seen(uuidAt(2)))
	assert.Equal(t, 2, deduper.Len())
}

func TestDeduperExpiresAfterTTL(t *testing.T) {
	t.Parallel()

	now := testClock
	deduper := gateway.NewDeduper(30*time.Second, func() time.Time { return now })
	eventID := uuidAt(1)

	assert.False(t, deduper.Seen(eventID))

	now = now.Add(29 * time.Second)
	assert.True(t, deduper.Seen(eventID), "still inside the retention window")

	// Once the window passed the evicted id is treated as first sight: the no-replay
	// guarantee comes from latest-offset consumption, dedupe only closes the at-least-once
	// gap.
	now = now.Add(2 * time.Second)
	assert.False(t, deduper.Seen(eventID))
}

func TestDeduperSweepsExpiredEntries(t *testing.T) {
	t.Parallel()

	now := testClock
	deduper := gateway.NewDeduper(time.Second, func() time.Time { return now })

	for i := 0; i < 1100; i++ {
		assert.False(t, deduper.Seen(uuidAt(i)))
	}
	assert.Equal(t, 1100, deduper.Len())

	// Moving past the TTL and inserting again triggers the sweep of expired entries.
	now = now.Add(time.Hour)
	assert.False(t, deduper.Seen(uuidAt(5000)))
	assert.Equal(t, 1, deduper.Len())
}

func TestDeduperTreatsEmptyIDAsNew(t *testing.T) {
	t.Parallel()

	deduper := gateway.NewDeduper(time.Minute, func() time.Time { return testClock })
	assert.False(t, deduper.Seen(""))
	assert.False(t, deduper.Seen(""))
	assert.Equal(t, 0, deduper.Len())
}

func TestDeduperWithoutTTLRetainsEntries(t *testing.T) {
	t.Parallel()

	now := testClock
	deduper := gateway.NewDeduper(0, func() time.Time { return now })
	eventID := uuidAt(1)

	assert.False(t, deduper.Seen(eventID))
	now = now.Add(24 * time.Hour)
	assert.True(t, deduper.Seen(eventID))
}
