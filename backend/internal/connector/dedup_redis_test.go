package connector

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisDedupeChecker(t *testing.T) {
	mr := miniredis.RunT(t)

	d, err := NewRedisDedupeChecker(context.Background(), mr.Addr(), "", time.Hour)
	if err != nil {
		t.Fatalf("NewRedisDedupeChecker: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	ctx := context.Background()

	// First sighting → not a duplicate, marker is written.
	dup, err := d.IsDuplicate(ctx, "evt-1")
	if err != nil {
		t.Fatalf("IsDuplicate: %v", err)
	}
	if dup {
		t.Fatal("first sighting should not be duplicate")
	}

	// Second sighting → duplicate.
	dup, err = d.IsDuplicate(ctx, "evt-1")
	if err != nil {
		t.Fatalf("IsDuplicate: %v", err)
	}
	if !dup {
		t.Error("second sighting should be duplicate")
	}

	// Different ID → not duplicate.
	dup, _ = d.IsDuplicate(ctx, "evt-2")
	if dup {
		t.Error("evt-2 should not be duplicate")
	}

	// Empty ID is treated as not-a-duplicate so an upstream bug doesn't
	// create a single key that swallows every empty-ID event.
	dup, err = d.IsDuplicate(ctx, "")
	if err != nil || dup {
		t.Errorf("empty ID: dup=%v err=%v; want false, nil", dup, err)
	}

	// MarkSeen is a no-op but must not error.
	if err := d.MarkSeen(ctx, "evt-3"); err != nil {
		t.Errorf("MarkSeen: %v", err)
	}
}

func TestRedisDedupeChecker_TTLExpiry(t *testing.T) {
	mr := miniredis.RunT(t)

	d, err := NewRedisDedupeChecker(context.Background(), mr.Addr(), "", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedisDedupeChecker: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	ctx := context.Background()
	if dup, _ := d.IsDuplicate(ctx, "evt-1"); dup {
		t.Fatal("first should not be duplicate")
	}
	// miniredis controls time explicitly — fast-forward past the TTL.
	mr.FastForward(200 * time.Millisecond)
	if dup, _ := d.IsDuplicate(ctx, "evt-1"); dup {
		t.Error("after TTL expiry, key should be gone and event should be re-acceptable")
	}
}
