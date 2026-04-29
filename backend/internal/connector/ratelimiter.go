package connector

import (
	"context"
	"sync"
	"time"
)

// TokenBucketLimiter implements a token bucket rate limiter.
type TokenBucketLimiter struct {
	mu       sync.Mutex
	tokens   float64
	maxTokens float64
	rate     float64 // tokens per second
	lastTime time.Time
}

func NewTokenBucketLimiter(requestsPerSecond float64) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		tokens:    requestsPerSecond,
		maxTokens: requestsPerSecond * 2,
		rate:      requestsPerSecond,
		lastTime:  time.Now(),
	}
}

func (t *TokenBucketLimiter) Wait(ctx context.Context) error {
	for {
		t.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(t.lastTime).Seconds()
		t.tokens += elapsed * t.rate
		if t.tokens > t.maxTokens {
			t.tokens = t.maxTokens
		}
		t.lastTime = now

		if t.tokens >= 1 {
			t.tokens--
			t.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - t.tokens) / t.rate * float64(time.Second))
		t.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (t *TokenBucketLimiter) SetRate(requestsPerSecond float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rate = requestsPerSecond
	t.maxTokens = requestsPerSecond * 2
}
