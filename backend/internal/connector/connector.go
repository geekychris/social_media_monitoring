package connector

import (
	"context"
	"time"

	"github.com/socialmonitor/backend/internal/model"
)

// Connector defines the interface all platform connectors must implement.
type Connector interface {
	// Name returns the platform name (e.g. "bluesky", "reddit", "hackernews").
	Name() string

	// Start begins consuming events from the platform.
	Start(ctx context.Context, out chan<- model.SocialEvent) error

	// Stop gracefully shuts down the connector.
	Stop() error

	// Health returns the connector's current health status.
	Health() HealthStatus
}

type HealthStatus struct {
	Healthy       bool      `json:"healthy"`
	LastEvent     time.Time `json:"last_event"`
	EventsTotal   int64     `json:"events_total"`
	ErrorsTotal   int64     `json:"errors_total"`
	LastError     string    `json:"last_error,omitempty"`
	LastErrorTime time.Time `json:"last_error_time,omitempty"`
}

// CursorStore persists connector cursors for resumption after restarts.
type CursorStore interface {
	GetCursor(ctx context.Context, connectorName string) (string, error)
	SetCursor(ctx context.Context, connectorName string, cursor string) error
}

// DedupeChecker checks if an event has already been processed.
type DedupeChecker interface {
	IsDuplicate(ctx context.Context, eventID string) (bool, error)
	MarkSeen(ctx context.Context, eventID string) error
}

// RateLimiter controls request rates to platform APIs.
type RateLimiter interface {
	Wait(ctx context.Context) error
	SetRate(requestsPerSecond float64)
}
