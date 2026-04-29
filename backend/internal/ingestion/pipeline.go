package ingestion

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/socialmonitor/backend/internal/archive"
	"github.com/socialmonitor/backend/internal/connector"
	"github.com/socialmonitor/backend/internal/enrichment"
	"github.com/socialmonitor/backend/internal/model"
	"github.com/socialmonitor/backend/internal/search"
)

// Pipeline orchestrates event ingestion from connectors to the search index.
//
// Stage order per event:
//
//	connector ─▶ dedup ─▶ archive ─▶ enrich ─▶ index
//
// Dedup runs first so we don't waste archive space or enricher calls on
// events we've already seen. Archive runs before enrich so the raw
// payload is preserved even if an enricher mutates the event in unexpected
// ways. Enrichment errors are non-fatal — the event still gets indexed.
type Pipeline struct {
	connectors   []connector.Connector
	dedup        connector.DedupeChecker
	archive      archive.Archive
	enrichers    *enrichment.Chain
	searchClient *search.OpenSearchClient
	eventChan    chan model.SocialEvent
	stats        PipelineStats

	mu       sync.RWMutex
	running  bool
	cancelFn context.CancelFunc
}

type PipelineStats struct {
	Ingested   atomic.Int64
	Duplicates atomic.Int64
	Errors     atomic.Int64
	Archived   atomic.Int64
	StartedAt  time.Time
}

// NewPipeline wires the ingestion pipeline. arch and enrichers may be
// nil; callers should pass archive.NewNoop() and enrichment.NewChain(nil, nil)
// to disable archival or enrichment cleanly rather than passing nil.
func NewPipeline(
	searchClient *search.OpenSearchClient,
	dedup connector.DedupeChecker,
	arch archive.Archive,
	enrichers *enrichment.Chain,
) *Pipeline {
	if arch == nil {
		arch = archive.NewNoop()
	}
	if enrichers == nil {
		enrichers = enrichment.NewChain(nil, nil)
	}
	return &Pipeline{
		searchClient: searchClient,
		dedup:        dedup,
		archive:      arch,
		enrichers:    enrichers,
		eventChan:    make(chan model.SocialEvent, 1000),
	}
}

func (p *Pipeline) AddConnector(c connector.Connector) {
	p.connectors = append(p.connectors, c)
}

func (p *Pipeline) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return nil
	}
	p.running = true
	p.stats.StartedAt = time.Now()
	ctx, p.cancelFn = context.WithCancel(ctx)
	p.mu.Unlock()

	// Start all connectors
	for _, c := range p.connectors {
		c := c
		go func() {
			log.Printf("[pipeline] starting connector: %s", c.Name())
			if err := c.Start(ctx, p.eventChan); err != nil {
				log.Printf("[pipeline] connector %s error: %v", c.Name(), err)
			}
		}()
	}

	// Start event processor
	go p.processEvents(ctx)

	return nil
}

func (p *Pipeline) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancelFn != nil {
		p.cancelFn()
	}
	p.running = false
	for _, c := range p.connectors {
		c.Stop()
	}
}

func (p *Pipeline) processEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-p.eventChan:
			// Stage 1: dedup. IsDuplicate is atomic with mark-seen for
			// the Redis/Postgres backends, so we don't call MarkSeen.
			dup, err := p.dedup.IsDuplicate(ctx, event.EventID)
			if err != nil {
				log.Printf("[pipeline] dedup check error: %v", err)
			}
			if dup {
				p.stats.Duplicates.Add(1)
				continue
			}

			// Stage 2: raw archive. Best-effort — failures don't block
			// indexing, but we still record the location on the event so
			// the indexed document points at the archive even if disk
			// writes are lagging.
			if len(event.RawPayload) > 0 {
				loc, err := p.archive.Save(ctx, &event, event.RawPayload)
				if err != nil {
					log.Printf("[pipeline] archive error for %s: %v", event.EventID, err)
				} else if loc.Key != "" {
					event.RawRef = model.RawRef{Bucket: loc.Bucket, Key: loc.Key}
					p.stats.Archived.Add(1)
				}
			}
			// Drop the bytes before they get any further so the indexed
			// document and any enricher request payloads stay small.
			event.RawPayload = nil

			// Stage 3: enrichment. Errors are surfaced via Chain.Stats;
			// the event continues even if every enricher fails.
			p.enrichers.Run(ctx, &event)

			// Stage 4: index. Set ingestion timestamp last so it reflects
			// when we're actually committing to the index.
			event.Timestamps.IngestedAt = time.Now().UTC()
			if err := p.searchClient.IndexEvent(ctx, event); err != nil {
				log.Printf("[pipeline] index error: %v", err)
				p.stats.Errors.Add(1)
				continue
			}

			p.stats.Ingested.Add(1)

			if p.stats.Ingested.Load()%100 == 0 {
				log.Printf("[pipeline] ingested %d events (dupes: %d, errors: %d)",
					p.stats.Ingested.Load(), p.stats.Duplicates.Load(), p.stats.Errors.Load())
			}
		}
	}
}

func (p *Pipeline) GetStats() map[string]any {
	connectorHealth := map[string]any{}
	for _, c := range p.connectors {
		connectorHealth[c.Name()] = c.Health()
	}
	return map[string]any{
		"running":    p.running,
		"started_at": p.stats.StartedAt,
		"ingested":   p.stats.Ingested.Load(),
		"duplicates": p.stats.Duplicates.Load(),
		"errors":     p.stats.Errors.Load(),
		"archived":   p.stats.Archived.Load(),
		"archive":    p.archive.Stats(),
		"enrichers":  p.enrichers.Stats(),
		"connectors": connectorHealth,
	}
}
