# Connector framework

This package defines the contract every platform connector implements and
provides shared infrastructure (cursor persistence, rate limiting, dedup,
HTTP/text helpers). Each platform lives in its own subpackage.

For the broader system design (where connectors sit in the pipeline,
how their output flows through dedup → archive → enrich → index), see
[`docs/architecture.md`](../../../docs/architecture.md).

## Contract

```go
type Connector interface {
    Name() string
    Start(ctx context.Context, out chan<- model.SocialEvent) error
    Stop() error
    Health() HealthStatus
}
```

A connector is a long-lived goroutine that pushes normalized
`model.SocialEvent` values onto `out`. The ingestion pipeline reads `out`,
runs dedup, and indexes into OpenSearch — connectors do not deal with
storage, dedup, or search.

## Built-in connectors

| Package | Source | Mode | Auth |
|---|---|---|---|
| `bluesky` | AT Protocol Jetstream | Streaming WS | None |
| `hackernews` | HN Firebase API | Polling (`maxitem` walk) | None |
| `rss` | RSS / Atom feeds | Polling | None |
| `reddit` | Public subreddit JSON | Polling (`/r/<sub>/new.json`) | None (OAuth optional) |
| `mastodon` | Public + tag timelines | Polling (`/api/v1/timelines/...`) | None (token optional) |
| `discourse` | Forum `/latest.json` | Polling | None |

## Shared infrastructure

| Symbol | Purpose |
|---|---|
| `CursorStore` | Per-connector resume tokens. In-memory or Postgres-backed. |
| `DedupeChecker` | Event dedup. Pipeline-level, not per-connector. |
| `TokenBucketLimiter` | Per-connector rate limiting. |
| `GetJSON` | HTTP GET + JSON decode + non-2xx → error. |
| `FilterEmpty`, `StripHTML` | Normalization helpers. |

## Cursor key conventions

Cursor keys are arbitrary strings; the convention is `<platform>[:<scope>]`
so multiple sub-feeds within one connector each get their own resume point:

| Connector | Key pattern | Example |
|---|---|---|
| Bluesky | `bluesky` | (single firehose) |
| Hacker News | `hackernews` | (single id-walk) |
| Reddit | `reddit:r/<sub>` | `reddit:r/golang` |
| Mastodon | `mastodon:<host>:public` or `mastodon:<host>:tag:<tag>` | `mastodon:fosstodon.org:tag:golang` |
| Discourse | `discourse:<host>` | `discourse:meta.discourse.org` |

## Adding a new connector

1. Create `internal/connector/<platform>/<platform>.go`.
2. Define a `Config` struct; expose a `New(cfg, cursorStore) *Connector`.
3. Implement the four `Connector` methods. Reuse:
   - `connector.GetJSON` for JSON HTTP APIs.
   - `connector.TokenBucketLimiter` for per-host rate limits.
   - `connector.StripHTML` / `FilterEmpty` for content normalization.
4. Write a `normalize` method that takes a parsed platform payload and
   returns `model.SocialEvent`. Test it without network using
   `httptest.NewServer` (see `reddit/reddit_test.go` for the pattern).
5. Pick a unique `EventID` format: `<platform>:<globally-unique-id>`. If
   the platform's ID space is per-instance (Mastodon, Discourse), include
   the host: `<platform>:<host>:<id>`.
6. Wire into:
   - `internal/config/config.go` — add a `<Platform>Config` struct and
     populate it from env vars in `Load()`.
   - `cmd/server/main.go` — instantiate and `pipeline.AddConnector(...)`.
   - `docker-compose.yml` — expose env vars under `backend.environment`.

## Normalize checklist

Every connector's `normalize` should populate at minimum:

- `EventID` — globally unique. This is the dedupe key; collisions silently drop events.
- `Platform`, `SourceType` (`post` / `reply` / `comment` / `article` / `review`).
- `SourceURL` — public, link-shareable URL to the original.
- `Author{ID, Handle}` when available.
- `Content{Text, Language}`. Strip HTML; do not include markup in `Text`.
- `Conversation{ThreadID, RootID}` — set both to the event ID for top-level posts; set `ParentID` and adjust `ThreadID` for replies.
- `Timestamps.PublishedAt` in UTC. The pipeline sets `IngestedAt`.

Optional but useful: `Engagement` counters, `Enrichment.Topics` (tags / categories), `Content.URLs`, `Content.Media`.

## Testing

Each connector ships table-driven `normalize` tests plus a `pollOne` test
that uses `httptest.NewServer` to validate cursor advancement and
filtering without hitting the real API. Run all tests:

```sh
cd backend && go test ./...
```
