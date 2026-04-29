# Architecture

This document explains how the social media monitoring workbench is put
together: the components, the data flow, the storage layer, and the
extension points (connectors, enrichers).

For the product vision and roadmap see
[`social_media_monitoring_workbench_plan.md`](../social_media_monitoring_workbench_plan.md).
For the connector contract see
[`backend/internal/connector/README.md`](../backend/internal/connector/README.md).
For the enrichment plugin contract see
[`backend/internal/enrichment/README.md`](../backend/internal/enrichment/README.md).

## Contents

1. [System context](#1-system-context)
2. [Component view](#2-component-view)
3. [Event lifecycle](#3-event-lifecycle)
4. [Pipeline stages](#4-pipeline-stages)
5. [Connector framework](#5-connector-framework)
6. [Storage layer](#6-storage-layer)
7. [Dedup](#7-dedup)
8. [Raw payload archive](#8-raw-payload-archive)
9. [Enrichment plugins](#9-enrichment-plugins)
10. [Canonical SocialEvent schema](#10-canonical-socialevent-schema)
11. [Frontend](#11-frontend)
12. [Operational notes](#12-operational-notes)
13. [What is intentionally not yet built](#13-what-is-intentionally-not-yet-built)

---

## 1. System context

The workbench ingests public social-media activity from a fixed set of
external platforms, normalizes it into a single canonical event, and
serves it to analysts through a search/dashboard UI. Everything runs in
local Docker today; it's structured to migrate to managed services
(S3, Kafka/Kinesis, OpenSearch Service) without touching application
code.

```mermaid
flowchart LR
    subgraph external["External (no control)"]
        Bsky["Bluesky Jetstream"]
        HN["Hacker News API"]
        RD["Reddit JSON"]
        MS["Mastodon instances"]
        DS["Discourse forums"]
        RSSF["RSS / Atom feeds"]
    end

    subgraph us["Workbench (this repo)"]
        BE["Backend (Go)"]
        FE["Frontend (React/Vite)"]
        ENR["Enricher plugins\n(any language, HTTP)"]
    end

    subgraph storage["Stores"]
        OS[("OpenSearch")]
        PG[("Postgres")]
        RD2[("Redis")]
        FS[("Raw archive\nfilesystem / S3")]
    end

    Bsky --> BE
    HN --> BE
    RD --> BE
    MS --> BE
    DS --> BE
    RSSF --> BE

    BE --> OS
    BE --> PG
    BE --> RD2
    BE --> FS
    BE <--> ENR

    FE --> BE
    User(["Analyst"]) --> FE
```

The boundary between "us" and "external" is important: connectors are
the only code that talks to platforms, and the enrichment chain is the
only code that talks to plugin services. Both are isolated so platform
outages or plugin crashes never cascade into the search/UI tier.

---

## 2. Component view

The deployable surface is one Go backend binary plus the React frontend,
backed by four data stores. Redpanda is provisioned but not yet wired
into the pipeline (see [§13](#13-what-is-intentionally-not-yet-built)).

```mermaid
flowchart TB
    subgraph backend["Backend container (Go)"]
        direction TB
        CONN["Connectors\n(bluesky, hackernews, reddit,\nmastodon, discourse, rss)"]
        CHAN(["Event channel\nbuffered, 1000"])
        PIPE["Pipeline\ndedup → archive → enrich → index"]
        API["HTTP API\n/api/search /api/monitors\n/api/pipeline/status"]
        CONN --> CHAN --> PIPE --> API
    end

    subgraph frontend["Frontend container (nginx)"]
        UI["React app\nDashboard, Search, Monitors"]
    end

    subgraph data["Data stores"]
        OS[("OpenSearch\nsocial_events index")]
        PG[("Postgres\nmonitors, cursors,\nseen_events")]
        RDS[("Redis\ndedup:* keys, 24h TTL")]
        ARC[("Filesystem volume\n/var/lib/raw_archive")]
    end

    PIPE --> OS
    PIPE --> ARC
    PIPE -.optional.-> PG
    PIPE --> RDS
    CONN --> PG

    UI -->|"REST"| API

    subgraph plugins["Enricher plugins (out-of-process)"]
        ELSA["entity-sentiment\n(future ELSA wrapper)"]
        OTHER["…other HTTP services"]
    end

    PIPE -->|"POST /enrich"| ELSA
    PIPE -.->|"POST /enrich"| OTHER
```

Why these specific stores:

| Concern | Store | Why |
|---|---|---|
| Full-text + filtered search over millions of events | OpenSearch | Native lucene + nested object support; the `enrichment.extra` map stays queryable |
| Application metadata (monitors, alerts) | Postgres | Transactional, relational; small volume |
| Connector cursors | Postgres | Persisted across restarts; sub-millisecond |
| Dedup of high-volume firehose events | Redis | Atomic SETNX with TTL handles 50+ events/sec without write amplification |
| Raw immutable payloads | Filesystem (today) | Cheap, simple; same `Archive` interface as a future S3 backend |

---

## 3. Event lifecycle

Every event traverses these stages between hitting a connector and
becoming searchable:

```mermaid
sequenceDiagram
    participant Plat as Platform API
    participant Conn as Connector goroutine
    participant Chan as Event channel
    participant Pipe as Pipeline (single goroutine)
    participant Redis as Redis
    participant FS as Raw archive
    participant Enr as Enricher chain
    participant OS as OpenSearch

    Plat->>Conn: WS frame / HTTP response
    Conn->>Conn: parse + normalize → SocialEvent\n(set RawPayload bytes)
    Conn->>Chan: out <- evt

    Pipe->>Chan: <-chan
    Pipe->>Redis: SETNX dedup:<event_id> 1 EX 86400
    alt key already existed
        Redis-->>Pipe: 0 (duplicate)
        Pipe->>Pipe: stats.Duplicates++; drop event
    else key created
        Redis-->>Pipe: 1 (new)
        Pipe->>FS: Save(event, payload) → Location
        FS-->>Pipe: {bucket, key}
        Pipe->>Pipe: event.RawRef = Location\nevent.RawPayload = nil
        Pipe->>Enr: Run(ctx, &event)
        loop each enricher in order
            Enr->>Enr: HTTP POST /enrich
            Enr->>Enr: merge well-known fields\nstore raw response in Extra[name]
        end
        Pipe->>Pipe: event.IngestedAt = now()
        Pipe->>OS: IndexEvent(event)
        Pipe->>Pipe: stats.Ingested++
    end
```

Three properties of this design matter:

- **Dedup is atomic with mark-seen.** Using `SETNX` collapses
  IsDuplicate + MarkSeen into one round trip and prevents two
  concurrent events from both passing the check before either gets
  recorded.
- **Archive runs before enrichment.** The unmodified platform payload
  is preserved before any code mutates the event, so a buggy enricher
  never corrupts the raw record.
- **Enrichment failures are non-fatal.** Each `Enricher.Enrich` call
  is wrapped: errors are logged and counted in `Chain.Stats()` but the
  event still gets indexed with whatever annotations succeeded.

---

## 4. Pipeline stages

Implemented in [`backend/internal/ingestion/pipeline.go`](../backend/internal/ingestion/pipeline.go).
The pipeline owns one goroutine per connector (each connector's `Start`
runs concurrently) plus a single `processEvents` goroutine that drains
the buffered event channel:

```mermaid
flowchart LR
    C1[bluesky] --> CH[(channel\nbuf=1000)]
    C2[hackernews] --> CH
    C3[reddit] --> CH
    C4[mastodon] --> CH
    C5[discourse] --> CH
    C6[rss] --> CH

    CH --> P[processEvents]
    P --> S1{dedup}
    S1 -- duplicate --> DROP[(drop\nstats.Duplicates++)]
    S1 -- new --> S2[archive]
    S2 --> S3[enrich]
    S3 --> S4[index]
    S4 --> OS[(OpenSearch)]
```

The single-consumer model is deliberate: it serializes index writes
without locks, keeps stats counters lock-free, and matches OpenSearch's
preferred bulk-indexing pattern. If single-threaded indexing becomes a
bottleneck the natural next step is to fan out _after_ archive into N
indexer workers — dedup must stay single-pass.

---

## 5. Connector framework

A connector is a long-lived goroutine that pushes
`model.SocialEvent` values onto a channel. The contract lives in
[`backend/internal/connector/connector.go`](../backend/internal/connector/connector.go):

```go
type Connector interface {
    Name() string
    Start(ctx context.Context, out chan<- model.SocialEvent) error
    Stop() error
    Health() HealthStatus
}
```

There are two implementation patterns. **Streaming** (Bluesky) opens a
WebSocket and reads frames in a loop. **Polling** (HN, Reddit, Mastodon,
Discourse, RSS) wakes on a ticker, fetches a page, advances a cursor.

```mermaid
flowchart TB
    subgraph streaming["Streaming pattern"]
        S1[Connect WS] --> S2[Read frame loop]
        S2 --> S3{filter+normalize}
        S3 --> S4[out <- evt]
        S4 --> S2
        S2 -.error.-> S5[backoff 5s]
        S5 --> S1
    end

    subgraph polling["Polling pattern"]
        P1[Initial poll] --> P2[ticker.C]
        P2 --> P3[for each sub-source]
        P3 --> P4[rate-limit Wait]
        P4 --> P5[GET + JSON parse]
        P5 --> P6[normalize → out <- evt]
        P6 --> P7[advance cursor]
        P7 --> P3
        P3 -- done --> P2
    end
```

### Built-in connectors

| Connector | Pattern | Source | Cursor key |
|---|---|---|---|
| `bluesky` | streaming | AT Proto Jetstream WS | `bluesky` (single firehose, time-us) |
| `hackernews` | polling | Firebase API maxitem walk | `hackernews` (highest item id) |
| `reddit` | polling | `/r/<sub>/new.json` | `reddit:r/<sub>` (fullname `t3_xxx`) |
| `mastodon` | polling | `/api/v1/timelines/{public,tag/<t>}` | `mastodon:<host>:public` or `mastodon:<host>:tag:<t>` |
| `discourse` | polling | `/latest.json` per forum | `discourse:<host>` (highest topic id) |
| `rss` | polling | RSS / Atom feed URLs | (none — uses event-id dedup) |

### Shared infrastructure

```mermaid
classDiagram
    class Connector {
        <<interface>>
        +Name() string
        +Start(ctx, out) error
        +Stop() error
        +Health() HealthStatus
    }
    class CursorStore {
        <<interface>>
        +GetCursor(ctx, name) string
        +SetCursor(ctx, name, value)
    }
    class DedupeChecker {
        <<interface>>
        +IsDuplicate(ctx, id) bool
        +MarkSeen(ctx, id)
    }
    class TokenBucketLimiter {
        +Wait(ctx)
        +SetRate(rps)
    }
    class GetJSON {
        <<helper>>
    }

    Connector --> CursorStore : reads/writes
    Connector --> TokenBucketLimiter : per-host throttle
    Connector --> GetJSON : HTTP fetch
    CursorStore <|.. MemoryCursorStore
    CursorStore <|.. PostgresStore
    DedupeChecker <|.. MemoryDedupeChecker
    DedupeChecker <|.. RedisDedupeChecker
    DedupeChecker <|.. PostgresStore
```

The two `*Store` interfaces are the only places connectors talk to
durable storage; everything else is pure Go.

### Cursor model

Cursors are arbitrary strings keyed by connector. Composite keys let one
connector poll many sub-sources without conflict (a Reddit connector
polling 3 subreddits writes 3 separate cursor rows). The current
implementations:

- `MemoryCursorStore` — fallback when Postgres isn't reachable.
- `PostgresStore` — `cursors(connector_name PRIMARY KEY, cursor_value, updated_at)`.

Streaming connectors save the cursor periodically (every N events for
Bluesky) so a crash loses at most the last batch. Polling connectors
save after each successful page so the worst case is one duplicate
page on restart, which dedup absorbs.

### Rate limiting

`TokenBucketLimiter` is a standard token bucket:

```mermaid
flowchart LR
    REQ[request] --> W{tokens >= 1?}
    W -- yes --> CONSUME[tokens--; proceed]
    W -- no --> WAIT[sleep until next token]
    WAIT --> W
    REGEN[time elapsed × rate] -.refills.-> W
```

Each connector owns its own limiter so per-platform limits are
independent. The Reddit connector also explicitly tracks the soft-block
case (Reddit returns `200` + empty listing instead of `403` to
programmatic clients) and surfaces it as a connector-level
`LastError` rather than a silent zero in stats.

---

## 6. Storage layer

The system uses four stores. Each one has a clear bounded responsibility
and can be swapped without touching adjacent layers.

```mermaid
flowchart LR
    subgraph stores["What lives where"]
        direction TB
        OS[("OpenSearch")] -- "indexed events,\nfilterable + searchable" --- OS_NOTE[social_events index]
        PG[("Postgres")] --- PG_NOTE["monitors,\ncursors,\nseen_events (dedup fallback)"]
        RDS[("Redis")] --- RDS_NOTE["dedup:* keys\n24h TTL\nSETNX-atomic"]
        FS[("Filesystem /\nS3 / GCS / MinIO")] --- FS_NOTE["raw payloads\n/<platform>/YYYY/MM/DD/<id>.json"]
    end
```

```mermaid
erDiagram
    monitors {
        text id PK
        text name
        text query
        text_array platforms
        bool active
        timestamptz created_at
        timestamptz updated_at
    }
    alerts {
        text id PK
        text monitor_id FK
        text type
        text severity
        text title
        text message
        bool fired
        timestamptz fired_at
        timestamptz resolved_at
        timestamptz created_at
    }
    cursors {
        text connector_name PK
        text cursor_value
        timestamptz updated_at
    }
    seen_events {
        text event_id PK
        timestamptz seen_at
    }
    monitors ||--o{ alerts : "drives"
```

OpenSearch holds one document per `event_id`. The full document shape is
[§10](#10-canonical-socialevent-schema). `RawPayload` is `json:"-"` so
the raw bytes never reach OpenSearch — only the `RawRef` pointer to the
archive does.

---

## 7. Dedup

Three implementations, picked at startup in this order:

1. **Redis** (`internal/connector/dedup_redis.go`) — preferred. Atomic
   `SETNX` with 24h TTL.
2. **Postgres** (`PostgresStore.IsDuplicate`) — fallback. `INSERT … ON
   CONFLICT DO NOTHING` + `RowsAffected()`.
3. **Memory** (`MemoryDedupeChecker`) — last resort. Lost on restart.

```mermaid
flowchart TB
    START["main.go startup"] --> R{Redis<br/>reachable?}
    R -- yes --> RDS["use Redis<br/>(atomic SETNX, 24h TTL)"]
    R -- no --> P{Postgres<br/>reachable?}
    P -- yes --> PG["use Postgres<br/>(INSERT ON CONFLICT)"]
    P -- no --> M["use in-memory<br/>(lost on restart)"]
```

### Why SETNX

`IsDuplicate` and `MarkSeen` are the same Redis call. The boolean return
of `SETNX key value EX ttl` is "did this call create the key?" — exactly
the inverse of "is this a duplicate?". This collapses two operations
into one RTT and eliminates the race where two concurrent events with
the same ID both pass the IsDuplicate check before either reaches
MarkSeen:

```mermaid
sequenceDiagram
    participant E1 as Event A (worker 1)
    participant E2 as Event A (worker 2)
    participant R as Redis

    par
        E1->>R: SETNX dedup:A 1 EX 86400
    and
        E2->>R: SETNX dedup:A 1 EX 86400
    end
    R-->>E1: 1 (created)
    R-->>E2: 0 (key existed)
    Note right of E2: drop as duplicate
    E1->>E1: continue → archive → enrich → index
```

### Capacity

At 50 ev/s sustained, a 24h dedup window is ~4.3M keys × ~100B per
entry ≈ 430 MB Redis memory. Redis container has no explicit limit
configured today; if memory pressure becomes a concern the natural
next step is a Redis policy of `allkeys-lru` (the dedup window already
uses TTL so eviction is rare).

The Postgres fallback has no automatic eviction; call
`PostgresStore.PruneSeenEventsOlderThan(ctx, cutoff)` from a janitor
loop to keep the table bounded.

---

## 8. Raw payload archive

Implemented in [`backend/internal/archive`](../backend/internal/archive/).
The pipeline calls `archive.Save(ctx, &event, event.RawPayload)` after
dedup succeeds, records the returned location in `event.RawRef`, then
clears the byte slice before indexing.

### Wire-through

```mermaid
sequenceDiagram
    participant Plat as Platform
    participant Conn as Connector
    participant Pipe as Pipeline
    participant FS as FilesystemArchive
    participant OS as OpenSearch

    Plat-->>Conn: raw bytes (WS frame / HTTP body)
    Conn->>Conn: json.Unmarshal → typed struct
    Conn->>Conn: evt = normalize(typed)
    Conn->>Conn: evt.RawPayload = original bytes
    Conn->>Pipe: out <- evt
    Pipe->>FS: Save(&evt, evt.RawPayload)
    FS->>FS: write to disk
    FS-->>Pipe: Location{bucket, key}
    Pipe->>Pipe: evt.RawRef = Location\nevt.RawPayload = nil
    Pipe->>OS: IndexEvent(evt) — small, no raw bytes
```

The `RawPayload` field uses `json:"-"` so it's never serialized into
the OpenSearch document or any HTTP response. It exists strictly to
transit between the connector and the archive within one pipeline step.

### Layout

```
/var/lib/raw_archive/
├── bluesky/
│   └── 2026/04/29/
│       └── bluesky_did_plc_xxx_3mko3i2i7vk2z.json
├── mastodon/
│   └── 2026/04/29/
│       └── mastodon_fosstodon.org_116490208168179759.json
├── reddit/…
├── hackernews/…
└── discourse/…
```

Date partitioning matches typical data-lake conventions so a future S3
migration is a path-translation, not a re-keying. Filenames sanitize
the event ID to remove characters that could escape the partition
(`:`, `/`).

### Capturing per-event raw bytes from bulk APIs

Polling APIs return many events per HTTP response. To preserve each
event's _exact_ bytes (whitespace, key ordering) without re-marshaling,
the affected connectors decode through `json.RawMessage`:

- **Reddit**: custom `UnmarshalJSON` on `listing` keeps each child's
  raw `data` object alongside the parsed struct.
- **Mastodon**: decodes the response array as `[]json.RawMessage`
  first, then unmarshals each element into a typed `status`.
- **Discourse**: same `[]json.RawMessage` pattern, indexed by topic ID
  so the per-topic raw bytes are easy to look up.

RSS leaves `RawPayload` nil because the original bytes are XML, not
JSON, and re-marshaling would be lossy. The archive is a no-op on nil
payloads so this is supported by design.

### S3 swap

`Archive` is a one-method interface:

```go
type Archive interface {
    Save(ctx, *event, payload) (Location, error)
    Stats() Stats
}
```

A future `S3Archive` is a drop-in replacement; the partition-based key
already works as an S3 object key.

---

## 9. Enrichment plugins

The enrichment chain is the system's primary extension point for ML and
analysis workloads (sentiment, NER, topic classification, embeddings).
Plugins are external HTTP services in any language; the contract is in
[`backend/internal/enrichment/README.md`](../backend/internal/enrichment/README.md).

### Chain semantics

```mermaid
sequenceDiagram
    participant Pipe as Pipeline
    participant E1 as Enricher 1<br/>(entity-sentiment)
    participant E2 as Enricher 2<br/>(topic-classifier)
    participant E3 as Enricher 3<br/>(embedding)
    participant OS as OpenSearch

    Pipe->>E1: POST /enrich {event_id, text, ...}
    E1-->>Pipe: 200 {sentiment, entities, entity_sentiments}
    Pipe->>Pipe: merge sentiment+entities to typed fields\nstash full body in Extra["entity-sentiment"]
    Pipe->>E2: POST /enrich {event_id, text, ...}
    E2--xPipe: 500 / timeout
    Pipe->>Pipe: log + Stats.Errors++<br/>continue to next enricher
    Pipe->>E3: POST /enrich {event_id, text, ...}
    E3-->>Pipe: 200 {embedding_id}
    Pipe->>Pipe: stash in Extra["embedding"]
    Pipe->>OS: IndexEvent(event with merged Enrichment)
```

Three properties:

- **Plugins are isolated.** A 500, a 30-second hang (timeout cap is per
  plugin), or a malformed response only affects that plugin's stats —
  the event still indexes with whatever annotations succeeded.
- **Order matters.** Plugins run sequentially in the configured order,
  so a later plugin can read what an earlier one wrote on the event.
- **Two-way merge.** Each response is parsed twice: once for "well-known"
  top-level keys (`sentiment`, `intent`, `entities`, `topics`) which
  overlay the typed `Enrichment` fields, and once verbatim into
  `Enrichment.extra[<plugin_name>]`. Plugin-specific structures (e.g.
  ELSA's per-entity sentiment array) survive intact and are queryable
  in OpenSearch as a dynamic object.

### Enricher chain configuration

Numbered env-var slots, evaluated until a slot has neither `_NAME` nor `_URL`:

```yaml
backend:
  environment:
    ENRICHER_1_NAME: "entity-sentiment"
    ENRICHER_1_URL:  "http://elsa:8081/enrich"
    ENRICHER_1_TIMEOUT: "5s"
    ENRICHER_2_NAME: "topic-classifier"
    ENRICHER_2_URL:  "http://classifier:9000/enrich"
```

### Pluggable architecture diagram

```mermaid
flowchart LR
    EVT["SocialEvent"]
    subgraph chain["Chain (in pipeline order)"]
        direction TB
        E1["entity-sentiment"]
        E2["topic-classifier"]
        E3["embedding"]
    end

    EVT --> E1 --> E2 --> E3 --> IDX[/OpenSearch index/]

    subgraph external["Each plugin runs in its own container"]
        direction LR
        ELSA["Java/Spring/Javalin\nwraps EntitySentimentAnalyzer"]
        CLS["Python/FastAPI\nDistilBERT topic head"]
        EMB["Go/Rust binary\nsentence-transformers"]
    end

    E1 -.HTTP.-> ELSA
    E2 -.HTTP.-> CLS
    E3 -.HTTP.-> EMB
```

The Go pipeline never imports a model. New plugins are just new
containers + two env vars.

---

## 10. Canonical SocialEvent schema

Defined in [`backend/internal/model/event.go`](../backend/internal/model/event.go).
This is what every connector's `normalize()` produces and what
OpenSearch indexes.

```mermaid
classDiagram
    class SocialEvent {
        string EventID
        string Platform
        string SourceType
        string SourceURL
        Author Author
        Content Content
        Engagement Engagement
        Conversation Conversation
        Timestamps Timestamps
        Enrichment Enrichment
        RawRef RawRef
        []byte RawPayload "json:-"
    }
    class Author {
        string ID
        string Handle
        string DisplayName
        bool Verified
        int64 FollowerCount
    }
    class Content {
        string Text
        string Title
        string Language
        []Media Media
        []string URLs
    }
    class Engagement {
        int64 Likes
        int64 Shares
        int64 Comments
        int64 Views
        int64 Score
    }
    class Conversation {
        string ThreadID
        string ParentID
        string RootID
    }
    class Timestamps {
        time PublishedAt
        time IngestedAt
    }
    class Enrichment {
        []string Entities
        string Sentiment
        string Intent
        []string Topics
        string EmbeddingID
        map Extra "plugin output"
    }
    class RawRef {
        string Bucket
        string Key
    }
    SocialEvent --> Author
    SocialEvent --> Content
    SocialEvent --> Engagement
    SocialEvent --> Conversation
    SocialEvent --> Timestamps
    SocialEvent --> Enrichment
    SocialEvent --> RawRef
```

A few invariants enforced by the pipeline:

| Invariant | Where |
|---|---|
| `EventID` is globally unique (`<platform>:<id>` or `<platform>:<host>:<id>` for instance-scoped IDs) | Each connector's normalize |
| `RawPayload` is set by the connector and cleared by the pipeline before indexing | `pipeline.processEvents` |
| `Timestamps.IngestedAt` is set by the pipeline, not the connector | `pipeline.processEvents` |
| `RawRef` points at an extant archive object | Pipeline only sets it on successful Save |

---

## 11. Frontend

React 19 + TypeScript + Vite. Single-page app, served by nginx in
production, talks to the backend via the `/api/*` REST surface.

```mermaid
flowchart LR
    subgraph fe["frontend/"]
        APP[App.tsx]
        DASH[Dashboard]
        SRCH[SearchWorkbench]
        MON[Monitors]
        STAT[PipelineStatus]
        EVT[EventCard]
        API[api.ts<br/>fetch wrapper]
    end

    APP --> DASH
    APP --> SRCH
    APP --> MON
    APP --> STAT
    SRCH --> EVT
    DASH --> EVT

    DASH --> API
    SRCH --> API
    MON --> API
    STAT --> API

    API -->|"GET /api/search"| BE[(backend)]
    API -->|"GET /api/pipeline/status"| BE
    API -->|"GET/POST/DELETE /api/monitors"| BE
```

`VITE_API_URL` is **baked at build time** (Vite inlines `import.meta.env`)
so the frontend Dockerfile takes it as a build `ARG`. Changing the
backend URL means rebuilding the frontend image, not flipping a runtime
env var. See `frontend/Dockerfile` and the compose `args:` block.

---

## 12. Operational notes

### Port mapping

The backend container listens on `:8080` internally but is published on
host port **`18080`** because Rancher Desktop's `gvproxy` typically
owns `:8080` on local Macs and silently returns "HTTP 400" — easy to
mistake for a backend bug. See the project's memory file for context.

### Env var summary

| Var | Default | What |
|---|---|---|
| `PORT` | `8080` | Backend listen port (container-internal) |
| `OPENSEARCH_URL` | `http://opensearch:9200` | |
| `OPENSEARCH_INDEX` | `social_events` | |
| `POSTGRES_DSN` | `postgres://postgres:postgres@postgres:5432/socialmonitor` | |
| `REDIS_ADDR` | `redis:6379` | |
| `RAW_ARCHIVE_PATH` | `/var/lib/raw_archive` | Empty disables archival |
| `BLUESKY_ENABLED` | `true` | |
| `HN_ENABLED` | `true` | |
| `REDDIT_ENABLED` / `_SUBREDDITS` / `_BASE_URL` / `_USER_AGENT` | varies | |
| `MASTODON_ENABLED` / `_FEEDS` / `_ACCESS_TOKEN` | varies | |
| `DISCOURSE_ENABLED` / `_FORUMS` | varies | |
| `RSS_ENABLED` / `_FEEDS` | `false` / `[]` | |
| `ENRICHER_<N>_{NAME,URL,TIMEOUT,TOKEN,ENABLED}` | unset | Plugin slots, evaluated 1..N until empty |

### Failure modes — what fails open vs closed

| Failure | Behavior |
|---|---|
| One connector errors | Connector self-restarts (streaming) or skips the bad page (polling); pipeline keeps draining other connectors |
| Reddit soft-blocks (200 + empty listing) | Surfaced as `LastError` on `/api/pipeline/status`; events_total stays at 0 |
| Redis down at startup | Fall back to Postgres dedup, then memory |
| Postgres down at startup | Fall back to memory cursor + memory dedup; monitors API returns "database not available" |
| OpenSearch down | `pipeline.stats.Errors++`, event dropped — there is no DLQ today |
| Enricher plugin down | That enricher's call counts as an error in `Chain.Stats`; event still indexes with remaining enrichments + raw fields |
| Archive disk full | Logged as archive error; event still indexes (just with empty `RawRef`) |

The pipeline is biased toward "keep ingesting" — the only
unrecoverable failure path is OpenSearch indexing itself.

### Restart safety

- **Cursors** are persisted in Postgres so connectors resume from where
  they left off (Bluesky's cursor is saved every 500 events; polling
  cursors are saved per-page).
- **Dedup** is persisted in Redis with 24h TTL; restarting the backend
  cleanly leaves dedup state intact.
- **Monitors / alerts** live in Postgres.
- **Raw archive** is a Docker volume; survives container recreation.

### Scaling out

The single-`processEvents` goroutine plus a 1000-buffer event channel
is comfortable for the current ~50 ev/s firehose. To go significantly
higher:

1. Replace the channel with Redpanda (already provisioned in compose).
2. Run N `processEvents` workers consuming the same partition group,
   one per indexer. Dedup must remain a single SETNX in Redis — that
   already serializes correctly.
3. Switch the archive to S3 with multipart uploads.
4. Add a bulk-index batcher (OpenSearch supports `_bulk` natively).

---

## 13. What is intentionally not yet built

| Capability | Status | Why not yet |
|---|---|---|
| **Redpanda in the pipeline** | Provisioned in compose, not used | Single-consumer in-process channel is sufficient for current volume; deferred until horizontal scaling matters |
| **S3 / GCS archive** | Interface ready, only filesystem impl exists | One-file follow-up once cloud creds + bucket are decided |
| **Postgres dedup janitor** | `PruneSeenEventsOlderThan` exists, no scheduler | Trivial cron once Postgres dedup is preferred over Redis |
| **Embedding enricher / vector index** | Schema field exists (`Enrichment.EmbeddingID`); plugin contract supports it | Phase 2 of the plan — needs a model + index sized appropriately |
| **Saved-monitor evaluation engine** | Monitors stored, not evaluated against new events | Spike detection + alerts are Phase 1 milestone 6 |
| **DLQ for failed indexes** | None | Today the index error is logged + dropped; needs a Kafka topic or a Postgres table |
| **YouTube / owned-channel connectors** | Not started | OAuth flow + per-channel quota work; Phase 4 of the plan |
| **AuthN / multi-tenant** | None | Phase 5 |

Each of these has a documented next step that does not require
re-architecting the existing pipeline.
