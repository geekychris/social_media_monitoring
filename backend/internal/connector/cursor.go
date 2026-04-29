package connector

import (
	"context"
	"sync"
)

// MemoryCursorStore is an in-memory cursor store for development.
// Production should use Postgres or Redis.
type MemoryCursorStore struct {
	mu      sync.RWMutex
	cursors map[string]string
}

func NewMemoryCursorStore() *MemoryCursorStore {
	return &MemoryCursorStore{cursors: make(map[string]string)}
}

func (m *MemoryCursorStore) GetCursor(_ context.Context, name string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cursors[name], nil
}

func (m *MemoryCursorStore) SetCursor(_ context.Context, name string, cursor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cursors[name] = cursor
	return nil
}

// MemoryDedupeChecker is an in-memory dedup checker for development.
type MemoryDedupeChecker struct {
	mu   sync.RWMutex
	seen map[string]struct{}
}

func NewMemoryDedupeChecker() *MemoryDedupeChecker {
	return &MemoryDedupeChecker{seen: make(map[string]struct{})}
}

func (m *MemoryDedupeChecker) IsDuplicate(_ context.Context, eventID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.seen[eventID]
	return ok, nil
}

func (m *MemoryDedupeChecker) MarkSeen(_ context.Context, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen[eventID] = struct{}{}
	return nil
}
