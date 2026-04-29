// Package enrichment defines a plugin contract that lets external
// services (sentiment analyzers, NER models, classifiers, embedding
// providers) annotate SocialEvents as they flow through the ingestion
// pipeline.
//
// An Enricher receives a parsed event and may modify it in place, most
// commonly by populating fields on event.Enrichment. Errors are
// non-fatal: the pipeline logs and continues to the next enricher and
// then to indexing, so a single misbehaving plugin never stops ingestion.
//
// HTTPEnricher is the canonical implementation. It POSTs a small JSON
// envelope to a configured URL and merges the response back into the
// event. This makes the plugin language-agnostic — Java services
// (e.g. ELSA), Python ONNX servers, or any HTTP endpoint can plug in
// without changing the Go pipeline.
package enrichment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/socialmonitor/backend/internal/model"
)

// Enricher annotates a SocialEvent in place. Implementations must be
// safe to call from multiple goroutines.
type Enricher interface {
	Name() string
	Enrich(ctx context.Context, event *model.SocialEvent) error
}

// Stats are exposed via /api/pipeline/status so operators can watch
// each enricher's success and failure rates over time.
type Stats struct {
	Name     string `json:"name"`
	Calls    int64  `json:"calls"`
	Errors   int64  `json:"errors"`
	LastErr  string `json:"last_error,omitempty"`
	LastCall string `json:"last_call,omitempty"`
}

// Reporter is the optional interface enrichers can implement to expose
// their counters. The Chain wraps everyone with this regardless of
// whether they implement it themselves.
type Reporter interface {
	Stats() Stats
}

// Chain runs a fixed sequence of enrichers against each event. Failures
// are logged via the supplied logger but do not abort the chain.
type Chain struct {
	enrichers []Enricher
	stats     []*chainStats
	log       func(format string, args ...any)
}

type chainStats struct {
	calls    atomic.Int64
	errors   atomic.Int64
	lastErr  atomic.Value // string
	lastCall atomic.Value // time.Time
}

// NewChain constructs a Chain. Pass nil for log to use a no-op logger.
func NewChain(enrichers []Enricher, log func(format string, args ...any)) *Chain {
	if log == nil {
		log = func(string, ...any) {}
	}
	stats := make([]*chainStats, len(enrichers))
	for i := range enrichers {
		stats[i] = &chainStats{}
	}
	return &Chain{enrichers: enrichers, stats: stats, log: log}
}

// Run invokes each enricher in order. It always returns nil — errors
// are surfaced through Stats(), not returned, because losing one
// annotation is not a reason to lose an event from the index.
func (c *Chain) Run(ctx context.Context, event *model.SocialEvent) {
	for i, e := range c.enrichers {
		c.stats[i].calls.Add(1)
		c.stats[i].lastCall.Store(time.Now())
		if err := e.Enrich(ctx, event); err != nil {
			c.stats[i].errors.Add(1)
			c.stats[i].lastErr.Store(err.Error())
			c.log("[enrichment] %s: %v", e.Name(), err)
		}
	}
}

// Stats returns one entry per enricher in the order they run.
func (c *Chain) Stats() []Stats {
	out := make([]Stats, len(c.enrichers))
	for i, e := range c.enrichers {
		s := Stats{
			Name:   e.Name(),
			Calls:  c.stats[i].calls.Load(),
			Errors: c.stats[i].errors.Load(),
		}
		if v, ok := c.stats[i].lastErr.Load().(string); ok {
			s.LastErr = v
		}
		if v, ok := c.stats[i].lastCall.Load().(time.Time); ok && !v.IsZero() {
			s.LastCall = v.UTC().Format(time.RFC3339)
		}
		out[i] = s
	}
	return out
}

// Names returns the configured enricher names in order.
func (c *Chain) Names() []string {
	out := make([]string, len(c.enrichers))
	for i, e := range c.enrichers {
		out[i] = e.Name()
	}
	return out
}

// HTTPEnricherConfig configures an HTTP-based enricher.
type HTTPEnricherConfig struct {
	Name    string        // logical name, used as the key in event.Enrichment.Extra
	URL     string        // POST endpoint
	Timeout time.Duration // 0 → 5s
	// Optional bearer token sent as Authorization: Bearer <Token>.
	Token string
}

// HTTPEnricher posts a request envelope to URL and merges the response
// back into the event. It tolerates two response shapes:
//
//  1. The plugin's own structured output. The whole response body is
//     stored verbatim in event.Enrichment.Extra[name].
//  2. A "well-known" enrichment shape that overlays Enrichment fields
//     directly (sentiment, intent, entities, topics). Any of these
//     fields, when present, replace whatever the event had before.
//
// Both can be returned in the same response — the well-known fields
// are read at top level, and any additional payload also lands in Extra.
type HTTPEnricher struct {
	cfg    HTTPEnricherConfig
	client *http.Client
}

// EnrichRequest is the envelope sent to enricher services. Adding fields
// here is backward compatible: services ignore unknown fields.
type EnrichRequest struct {
	EventID  string `json:"event_id"`
	Platform string `json:"platform"`
	Language string `json:"language,omitempty"`
	Title    string `json:"title,omitempty"`
	Text     string `json:"text"`
}

// EnrichResponse is the canonical response shape. All fields optional.
// Services may also return additional JSON keys; they are preserved in
// event.Enrichment.Extra[name].
type EnrichResponse struct {
	Sentiment string   `json:"sentiment,omitempty"`
	Intent    string   `json:"intent,omitempty"`
	Entities  []string `json:"entities,omitempty"`
	Topics    []string `json:"topics,omitempty"`
}

// NewHTTPEnricher constructs an HTTPEnricher. The configured URL must
// be reachable from the backend at startup; the constructor does not
// probe it (a probe would couple ingestion startup to plugin readiness),
// but per-request errors will surface in Stats.
func NewHTTPEnricher(cfg HTTPEnricherConfig) *HTTPEnricher {
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	return &HTTPEnricher{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

func (h *HTTPEnricher) Name() string { return h.cfg.Name }

func (h *HTTPEnricher) Enrich(ctx context.Context, event *model.SocialEvent) error {
	if event == nil {
		return nil
	}
	if event.Content.Text == "" && event.Content.Title == "" {
		// Nothing to enrich; treat as a successful no-op rather than
		// counting it as an error.
		return nil
	}

	body, err := json.Marshal(EnrichRequest{
		EventID:  event.EventID,
		Platform: event.Platform,
		Language: event.Content.Language,
		Title:    event.Content.Title,
		Text:     event.Content.Text,
	})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if h.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+h.cfg.Token)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(preview))
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	// Apply well-known fields if present.
	var typed EnrichResponse
	if err := json.Unmarshal(raw, &typed); err == nil {
		mergeEnrichment(&event.Enrichment, typed)
	}

	// Stash full response in Extra under the enricher's name.
	if event.Enrichment.Extra == nil {
		event.Enrichment.Extra = make(map[string]json.RawMessage, 1)
	}
	event.Enrichment.Extra[h.cfg.Name] = json.RawMessage(append([]byte(nil), raw...))

	return nil
}

func mergeEnrichment(into *model.Enrichment, from EnrichResponse) {
	if from.Sentiment != "" {
		into.Sentiment = from.Sentiment
	}
	if from.Intent != "" {
		into.Intent = from.Intent
	}
	if len(from.Entities) > 0 {
		into.Entities = mergeUnique(into.Entities, from.Entities)
	}
	if len(from.Topics) > 0 {
		into.Topics = mergeUnique(into.Topics, from.Topics)
	}
}

func mergeUnique(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range a {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	for _, s := range b {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
