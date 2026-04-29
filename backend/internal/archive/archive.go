// Package archive persists raw platform payloads outside of the search
// index. The pipeline calls Save(...) before indexing into OpenSearch,
// records the returned location into SocialEvent.RawRef, and then drops
// the in-memory bytes so the indexed copy stays small.
//
// Two implementations live here:
//
//   - FilesystemArchive writes to a local directory tree, suitable for
//     development and small single-node deployments.
//   - NoopArchive does nothing and is selected when raw archival is
//     disabled.
//
// The interface is intentionally small (one method) so an S3 / GCS /
// MinIO backend can be dropped in without touching call sites. RawRef
// has Bucket+Key fields that already match an object-store mental model.
package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/socialmonitor/backend/internal/model"
)

// Location identifies where a raw payload was persisted. It maps directly
// onto model.RawRef and is what the pipeline records on the indexed event.
type Location struct {
	Bucket string // logical container (an S3 bucket, a root directory, ...)
	Key    string // object key within the bucket
}

// Archive persists raw platform payloads.
type Archive interface {
	// Save writes payload to a deterministic location derived from event
	// metadata and returns a Location pointer that can be used to fetch
	// the payload back later. Implementations should be safe to call
	// concurrently and idempotent: the same event/payload re-saved
	// produces the same Location and overwrites or no-ops cleanly.
	Save(ctx context.Context, event *model.SocialEvent, payload []byte) (Location, error)

	// Stats returns counters for observability.
	Stats() Stats
}

// Stats are exposed via /api/pipeline/status so operators can see whether
// raw archival is keeping up with ingestion.
type Stats struct {
	Saved   int64 `json:"saved"`
	Errors  int64 `json:"errors"`
	Skipped int64 `json:"skipped"`
}

// NoopArchive discards every payload. Use it when archival is disabled
// (e.g. in unit tests or low-disk dev environments).
type NoopArchive struct {
	skipped atomic.Int64
}

func NewNoop() *NoopArchive { return &NoopArchive{} }

func (n *NoopArchive) Save(_ context.Context, _ *model.SocialEvent, _ []byte) (Location, error) {
	n.skipped.Add(1)
	return Location{}, nil
}

func (n *NoopArchive) Stats() Stats {
	return Stats{Skipped: n.skipped.Load()}
}

// FilesystemArchive writes payloads under root in a date-partitioned tree:
//
//	<root>/<platform>/YYYY/MM/DD/<safe-event-id>.json
//
// Date partitioning matches typical lake conventions (so a future S3
// migration is a path-translation, not a re-keying) and keeps directory
// fan-out bounded. Filenames sanitize the event ID so platform-specific
// separators (colons, slashes) don't escape the partition.
type FilesystemArchive struct {
	root   string
	bucket string

	saved  atomic.Int64
	errors atomic.Int64
}

// NewFilesystem creates a FilesystemArchive rooted at root. The directory
// is created (with parents) if it does not exist; an error from
// MkdirAll is fatal because subsequent writes would all fail.
func NewFilesystem(root string) (*FilesystemArchive, error) {
	if root == "" {
		return nil, errors.New("filesystem archive: empty root")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("filesystem archive: abs root: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("filesystem archive: mkdir %s: %w", abs, err)
	}
	return &FilesystemArchive{root: abs, bucket: "raw-social-events"}, nil
}

// Save writes payload to disk and returns the Location that points back
// at it. If payload is nil or empty, the call is a no-op and an empty
// Location is returned — the pipeline treats that as "no archival
// happened" and leaves RawRef zero-valued.
func (f *FilesystemArchive) Save(_ context.Context, event *model.SocialEvent, payload []byte) (Location, error) {
	if event == nil || len(payload) == 0 {
		return Location{}, nil
	}
	t := event.Timestamps.PublishedAt
	if t.IsZero() {
		t = time.Now().UTC()
	}
	t = t.UTC()

	platform := event.Platform
	if platform == "" {
		platform = "unknown"
	}
	key := fmt.Sprintf("%s/%04d/%02d/%02d/%s.json",
		platform,
		t.Year(), int(t.Month()), t.Day(),
		safeFilename(event.EventID),
	)
	full := filepath.Join(f.root, filepath.FromSlash(key))

	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		f.errors.Add(1)
		return Location{}, fmt.Errorf("mkdir: %w", err)
	}
	if err := os.WriteFile(full, payload, 0o644); err != nil {
		f.errors.Add(1)
		return Location{}, fmt.Errorf("write %s: %w", full, err)
	}
	f.saved.Add(1)
	return Location{Bucket: f.bucket, Key: key}, nil
}

func (f *FilesystemArchive) Stats() Stats {
	return Stats{
		Saved:  f.saved.Load(),
		Errors: f.errors.Load(),
	}
}

// safeFilename replaces characters that would create surprise paths or
// confuse filesystems with underscores. The resulting name is still
// reversible if you need to read the original ID elsewhere — we don't
// percent-encode because dedup IDs are short and keeping them
// human-readable is useful for debugging the archive layout.
func safeFilename(s string) string {
	if s == "" {
		return "_unknown"
	}
	mapper := func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '.', r == '_':
			return r
		default:
			return '_'
		}
	}
	return strings.Map(mapper, s)
}
