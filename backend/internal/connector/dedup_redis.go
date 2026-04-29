package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisDedupeChecker is a DedupeChecker backed by Redis SETNX with a TTL.
//
// Each event ID is stored as a key "dedup:<event_id>" with no value and
// a fixed TTL (default 24h). IsDuplicate is implemented as SETNX which
// atomically returns true when we are the first to mark the key —
// MarkSeen is therefore a no-op (the side effect already happened in
// IsDuplicate). This collapses dedup-check + mark-seen into a single
// round trip and prevents the race where two concurrent events with the
// same ID both pass the IsDuplicate check before either calls MarkSeen.
//
// The 24h TTL is tuned for typical backfill / replay windows. It bounds
// memory growth automatically: at 50 ev/s, a 24h window is ~4M keys
// at <100B each — well under 1 GiB.
type RedisDedupeChecker struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

// NewRedisDedupeChecker connects to Redis and returns a DedupeChecker.
// addr is the host:port (e.g. "redis:6379"); password may be empty.
// If the connection check fails, an error is returned and the caller
// should fall back to a non-persistent dedup.
func NewRedisDedupeChecker(ctx context.Context, addr, password string, ttl time.Duration) (*RedisDedupeChecker, error) {
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     10,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &RedisDedupeChecker{client: client, prefix: "dedup:", ttl: ttl}, nil
}

// IsDuplicate returns true if eventID has been seen recently. The check
// is atomic with the mark-seen side effect: the first caller for a given
// ID gets false, every subsequent caller within ttl gets true.
func (r *RedisDedupeChecker) IsDuplicate(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, nil
	}
	ok, err := r.client.SetNX(ctx, r.prefix+eventID, "1", r.ttl).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}

// MarkSeen is a no-op: IsDuplicate has already inserted the key. The
// method exists to satisfy the DedupeChecker interface so callers can
// swap implementations without changing call sites.
func (r *RedisDedupeChecker) MarkSeen(_ context.Context, _ string) error {
	return nil
}

// Close releases the underlying Redis connection pool.
func (r *RedisDedupeChecker) Close() error {
	if r == nil || r.client == nil {
		return errors.New("redis dedup not initialized")
	}
	return r.client.Close()
}
