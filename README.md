# Social Media Monitoring Workbench

An investigation-grade workbench for monitoring, searching, and acting on
conversations across social platforms, forums, and news sources. The
system normalizes events from multiple platforms into a canonical schema,
indexes them for full-text + filtered search, archives the original
payloads, and runs a configurable chain of enrichment plugins (sentiment,
NER, classifiers) before serving them through a search API and React UI.

- [`docs/architecture.md`](./docs/architecture.md) — design + diagrams (component view, event lifecycle, pipeline stages, dedup, archive, enrichment).
- [`social_media_monitoring_workbench_plan.md`](./social_media_monitoring_workbench_plan.md) — product vision and roadmap.

## Quick start

```sh
docker compose up -d --build
```

Then open:

| URL | What |
|---|---|
| http://localhost:3000 | Frontend (React + Vite) |
| http://localhost:18080/api/health | Backend health |
| http://localhost:18080/api/pipeline/status | Per-connector + enricher stats |
| http://localhost:18080/api/search?q=foo | Search ingested events |
| http://localhost:5601 | OpenSearch Dashboards |

> Backend is mapped to host port **18080** (not 8080) because Rancher
> Desktop's `gvproxy` typically owns 8080 on local Macs.

## Repository layout

```
backend/
  cmd/server/             — main entry point
  internal/
    api/                  — HTTP handlers (search, monitors, pipeline status)
    archive/              — raw-payload archive (filesystem + S3-ready interface)
    config/               — env-var driven config
    connector/            — platform connector framework + 6 connectors
      bluesky/            — AT Protocol Jetstream (streaming)
      hackernews/         — HN Firebase API (polling)
      rss/                — RSS / Atom feeds
      reddit/             — public subreddit JSON
      mastodon/           — public + tag timelines per instance
      discourse/          — /latest.json per forum
    enrichment/           — plugin chain for sentiment/NER/classifiers (HTTP-based)
    ingestion/            — pipeline orchestrator
    model/                — canonical SocialEvent schema
    search/               — OpenSearch client
    store/                — Postgres metadata + cursor + dedup persistence
frontend/                 — React + TypeScript + Vite app
docker-compose.yml        — local dev stack (postgres, opensearch, redis, redpanda, backend, frontend)
```

## Pipeline shape

```
connector → dedup → archive → enrich → index
```

- **Dedup**: Redis SETNX with 24h TTL (atomic), Postgres fallback, in-memory last resort.
- **Raw archive**: filesystem under `/var/lib/raw_archive/<platform>/YYYY/MM/DD/<id>.json`. The `Archive` interface is S3/GCS/MinIO-ready.
- **Enrich**: ordered chain of HTTP plugins. Each plugin receives a
  `{event_id, platform, language, title, text}` envelope and returns
  annotations that overlay typed `Enrichment` fields and/or land
  verbatim under `Enrichment.extra[<plugin_name>]`. Errors are non-fatal.
  See [`backend/internal/enrichment/README.md`](./backend/internal/enrichment/README.md).
- **Index**: OpenSearch.

## Adding a connector

Implement the `Connector` interface in `backend/internal/connector/connector.go`,
populate `event.RawPayload` if you have the original bytes, and wire it
into `cmd/server/main.go` + `internal/config/config.go`. Full guide and
patterns live in
[`backend/internal/connector/README.md`](./backend/internal/connector/README.md).

## Adding an enrichment plugin

Plugins are HTTP services in any language. Implement `POST /enrich`
per the contract in
[`backend/internal/enrichment/README.md`](./backend/internal/enrichment/README.md),
then declare the plugin via `ENRICHER_<N>_NAME` / `_URL` env vars on the
backend.

## Tests

```sh
cd backend && go test ./...
```

## License

TBD.
