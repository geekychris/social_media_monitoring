package store

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
	"github.com/socialmonitor/backend/internal/model"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS monitors (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			query TEXT NOT NULL,
			platforms TEXT[] DEFAULT '{}',
			active BOOLEAN DEFAULT true,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS alerts (
			id TEXT PRIMARY KEY,
			monitor_id TEXT REFERENCES monitors(id),
			type TEXT NOT NULL,
			severity TEXT NOT NULL DEFAULT 'medium',
			title TEXT NOT NULL,
			message TEXT,
			fired BOOLEAN DEFAULT false,
			fired_at TIMESTAMPTZ,
			resolved_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS cursors (
			connector_name TEXT PRIMARY KEY,
			cursor_value TEXT NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS seen_events (
			event_id TEXT PRIMARY KEY,
			seen_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		// Index supports periodic pruning by age; cardinality on event_id
		// is already covered by the primary key.
		`CREATE INDEX IF NOT EXISTS seen_events_seen_at_idx ON seen_events (seen_at)`,
	}
	for _, q := range queries {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// Monitor CRUD
func (s *PostgresStore) CreateMonitor(ctx context.Context, m model.Monitor) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO monitors (id, name, query, platforms, active, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		m.ID, m.Name, m.Query, m.Platforms, m.Active, m.CreatedAt, m.UpdatedAt)
	return err
}

func (s *PostgresStore) ListMonitors(ctx context.Context) ([]model.Monitor, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, query, platforms, active, created_at, updated_at FROM monitors ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var monitors []model.Monitor
	for rows.Next() {
		var m model.Monitor
		if err := rows.Scan(&m.ID, &m.Name, &m.Query, &m.Platforms, &m.Active, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		monitors = append(monitors, m)
	}
	return monitors, nil
}

func (s *PostgresStore) DeleteMonitor(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM monitors WHERE id = $1`, id)
	return err
}

// Cursor persistence
func (s *PostgresStore) GetCursor(ctx context.Context, name string) (string, error) {
	var cursor string
	err := s.db.QueryRowContext(ctx,
		`SELECT cursor_value FROM cursors WHERE connector_name = $1`, name).Scan(&cursor)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return cursor, err
}

func (s *PostgresStore) SetCursor(ctx context.Context, name string, cursor string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cursors (connector_name, cursor_value, updated_at)
		 VALUES ($1, $2, NOW())
		 ON CONFLICT (connector_name) DO UPDATE SET cursor_value = $2, updated_at = NOW()`,
		name, cursor)
	return err
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// Dedup persistence
//
// IsDuplicate uses INSERT ... ON CONFLICT DO NOTHING + RETURNING to make
// the check-then-insert atomic. RowsAffected == 0 means another caller
// already inserted the key (i.e. we are a duplicate).
func (s *PostgresStore) IsDuplicate(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, nil
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO seen_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 0, nil
}

// MarkSeen is a no-op: IsDuplicate already wrote the row. Implemented to
// satisfy the DedupeChecker interface so callers can swap implementations.
func (s *PostgresStore) MarkSeen(_ context.Context, _ string) error {
	return nil
}

// PruneSeenEventsOlderThan deletes dedup rows older than cutoff. Call
// periodically from a janitor goroutine to bound table growth.
func (s *PostgresStore) PruneSeenEventsOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM seen_events WHERE seen_at < $1`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
