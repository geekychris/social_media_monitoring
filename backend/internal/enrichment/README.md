# Enrichment plugins

Enrichment is a plug-in chain that runs after dedup + raw archive and
before indexing. Each plugin is an external HTTP service that receives a
SocialEvent envelope and returns annotations to merge onto the event.

For chain semantics (sequence diagrams, error isolation, ordering) see
[`docs/architecture.md` § Enrichment plugins](../../../docs/architecture.md#9-enrichment-plugins).

## Pipeline position

```
connector → dedup → archive → enrich → index
```

Errors from enrichers are logged and counted in `Chain.Stats()` but do
**not** stop the event from being indexed. A misbehaving plugin never
takes down ingestion.

## Wire format

### Request

`POST <plugin URL>` with `Content-Type: application/json`:

```json
{
  "event_id": "reddit:t3_abc",
  "platform": "reddit",
  "language": "en",
  "title": "Generics question",
  "text": "Why is this not allowed?"
}
```

`title` is omitted for content types that don't have one (Mastodon, Bluesky).
The plugin should treat `text` as authoritative; `title` is supplementary.

### Response

The response body is parsed twice:

1. **Well-known overlay.** Top-level keys `sentiment`, `intent`,
   `entities`, `topics` are merged onto `event.Enrichment` directly.
   Non-empty values from the plugin replace whatever was there before;
   `entities` and `topics` are merged with deduplication.

2. **Verbatim passthrough.** The full response body is preserved at
   `event.Enrichment.extra["<plugin_name>"]` as a JSON object, so any
   plugin-specific structure (per-entity sentiment, classifier
   confidences, embedding IDs, ...) survives intact and is queryable
   in OpenSearch.

```json
{
  "sentiment": "mixed",
  "entities": ["iPhone", "Android Phones"],
  "entity_sentiments": [
    {"entity": "iPhone",         "sentiment": "positive", "confidence": 0.88, "holder": "Chris"},
    {"entity": "Android Phones", "sentiment": "negative", "confidence": 0.92, "holder": "Chris"}
  ],
  "subjective_content": true
}
```

After this response runs:

- `event.Enrichment.Sentiment = "mixed"`
- `event.Enrichment.Entities = ["iPhone", "Android Phones"]`
- `event.Enrichment.Extra["entity-sentiment"] = <full body bytes>`

Returning a non-2xx status, an unparseable body, or timing out (default 5s)
all become per-call errors; they don't drop the event.

## Configuration

Plugins are registered via numbered env-var slots, one per enricher:

```yaml
backend:
  environment:
    ENRICHER_1_NAME: "entity-sentiment"
    ENRICHER_1_URL: "http://elsa:8081/enrich"
    ENRICHER_1_TIMEOUT: "5s"
    ENRICHER_1_TOKEN: ""        # optional bearer
    ENRICHER_1_ENABLED: "true"
```

Add `ENRICHER_2_*`, `ENRICHER_3_*`, etc. to chain more plugins. The
chain runs in numeric order so a later plugin can see earlier plugins'
output via `event.Enrichment`.

## ELSA integration sketch

[ELSA](../../../entity_level_sentiment/) is a Java library, not an
HTTP service, so it needs a thin wrapper to plug in. Sketch (Javalin):

```java
public class ElsaServer {
  public static void main(String[] args) {
    var analyzer = EntitySentimentAnalyzer.create(AnalyzerConfig.defaults());
    Javalin.create()
      .post("/enrich", ctx -> {
        var req = ctx.bodyAsClass(EnrichRequest.class);
        var result = analyzer.analyze(req.text);
        var resp = new HashMap<String,Object>();
        resp.put("entities", result.entities().stream()
            .map(e -> e.entity()).distinct().toList());
        resp.put("entity_sentiments", result.entities());
        resp.put("subjective_content", result.subjectiveContent());
        // crude rollup: majority sentiment becomes the document-level one
        resp.put("sentiment", rollup(result.entities()));
        ctx.json(resp);
      })
      .start(8081);
  }
}
```

Run as a sidecar in compose:

```yaml
elsa:
  build: ../../entity_level_sentiment
  ports:
    - "8081:8081"
```

Then set `ENRICHER_1_URL: "http://elsa:8081/enrich"` on the backend.
ELSA's ~850ms model load happens once at container startup; per-event
analysis is ~64ms, so the default 5s timeout has comfortable headroom.

## Adding a new enricher

In Go, you can also implement the `Enricher` interface in-process. The
HTTP route is preferred (language-agnostic, restartable independently),
but for built-ins (e.g. a regex-based entity extractor) just satisfy:

```go
type Enricher interface {
    Name() string
    Enrich(ctx context.Context, event *model.SocialEvent) error
}
```

…and append it to the slice passed to `enrichment.NewChain` in
`cmd/server/main.go`.
