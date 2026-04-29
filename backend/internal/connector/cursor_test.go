package connector

import (
	"context"
	"testing"
)

func TestMemoryCursorStore_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryCursorStore()

	got, err := s.GetCursor(ctx, "missing")
	if err != nil {
		t.Fatalf("GetCursor(missing) returned error: %v", err)
	}
	if got != "" {
		t.Errorf("GetCursor(missing) = %q, want empty", got)
	}

	if err := s.SetCursor(ctx, "reddit:r/golang", "t3_abc"); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	got, err = s.GetCursor(ctx, "reddit:r/golang")
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if got != "t3_abc" {
		t.Errorf("GetCursor = %q, want %q", got, "t3_abc")
	}

	if err := s.SetCursor(ctx, "reddit:r/golang", "t3_def"); err != nil {
		t.Fatalf("SetCursor overwrite: %v", err)
	}
	got, _ = s.GetCursor(ctx, "reddit:r/golang")
	if got != "t3_def" {
		t.Errorf("after overwrite GetCursor = %q, want %q", got, "t3_def")
	}
}

func TestMemoryDedupeChecker(t *testing.T) {
	ctx := context.Background()
	d := NewMemoryDedupeChecker()

	dup, err := d.IsDuplicate(ctx, "evt-1")
	if err != nil || dup {
		t.Fatalf("first IsDuplicate(evt-1) = %v, %v; want false, nil", dup, err)
	}
	if err := d.MarkSeen(ctx, "evt-1"); err != nil {
		t.Fatalf("MarkSeen: %v", err)
	}
	dup, _ = d.IsDuplicate(ctx, "evt-1")
	if !dup {
		t.Errorf("after MarkSeen, IsDuplicate(evt-1) = false; want true")
	}
	dup, _ = d.IsDuplicate(ctx, "evt-2")
	if dup {
		t.Errorf("IsDuplicate(evt-2) = true; want false")
	}
}
