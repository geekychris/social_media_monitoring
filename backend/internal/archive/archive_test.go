package archive

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/socialmonitor/backend/internal/model"
)

func TestFilesystemArchive_SaveAndPath(t *testing.T) {
	dir := t.TempDir()
	a, err := NewFilesystem(dir)
	if err != nil {
		t.Fatalf("NewFilesystem: %v", err)
	}

	evt := &model.SocialEvent{
		EventID:    "reddit:t3_abc",
		Platform:   "reddit",
		Timestamps: model.Timestamps{PublishedAt: time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)},
	}
	payload := []byte(`{"hello":"world"}`)

	loc, err := a.Save(context.Background(), evt, payload)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	wantKey := "reddit/2026/04/28/reddit_t3_abc.json"
	if loc.Key != wantKey {
		t.Errorf("Key = %q, want %q", loc.Key, wantKey)
	}
	if loc.Bucket == "" {
		t.Error("Bucket should be set")
	}

	// File exists with the exact bytes we wrote.
	full := filepath.Join(dir, filepath.FromSlash(loc.Key))
	got, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("payload = %q, want %q", got, payload)
	}

	stats := a.Stats()
	if stats.Saved != 1 || stats.Errors != 0 {
		t.Errorf("stats = %+v, want Saved=1 Errors=0", stats)
	}
}

func TestFilesystemArchive_EmptyPayloadIsNoop(t *testing.T) {
	a, _ := NewFilesystem(t.TempDir())
	loc, err := a.Save(context.Background(), &model.SocialEvent{EventID: "x", Platform: "y"}, nil)
	if err != nil {
		t.Fatalf("Save(nil): %v", err)
	}
	if loc != (Location{}) {
		t.Errorf("nil payload should produce zero Location, got %+v", loc)
	}
	if a.Stats().Saved != 0 {
		t.Error("nil payload should not bump Saved")
	}
}

func TestFilesystemArchive_FallsBackToNowWhenTimestampZero(t *testing.T) {
	dir := t.TempDir()
	a, _ := NewFilesystem(dir)
	evt := &model.SocialEvent{EventID: "x", Platform: "p"} // no PublishedAt
	loc, err := a.Save(context.Background(), evt, []byte("x"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Path should still partition into platform/YYYY/MM/DD/...
	parts := strings.Split(loc.Key, "/")
	if len(parts) != 5 || parts[0] != "p" {
		t.Errorf("key shape = %q, want p/YYYY/MM/DD/x.json", loc.Key)
	}
}

func TestSafeFilename(t *testing.T) {
	tests := map[string]string{
		"reddit:t3_abc":               "reddit_t3_abc",
		"discourse:meta.discourse.org:42": "discourse_meta.discourse.org_42",
		"bsky/with/slashes":           "bsky_with_slashes",
		"":                            "_unknown",
		"plain-id_v2.0":               "plain-id_v2.0",
	}
	for in, want := range tests {
		if got := safeFilename(in); got != want {
			t.Errorf("safeFilename(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNoopArchive(t *testing.T) {
	a := NewNoop()
	loc, err := a.Save(context.Background(), &model.SocialEvent{EventID: "x"}, []byte("payload"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if loc != (Location{}) {
		t.Errorf("noop should return empty Location, got %+v", loc)
	}
	if a.Stats().Skipped != 1 {
		t.Errorf("noop Skipped = %d, want 1", a.Stats().Skipped)
	}
}
