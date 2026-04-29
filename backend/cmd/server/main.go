package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/socialmonitor/backend/internal/api"
	"github.com/socialmonitor/backend/internal/archive"
	"github.com/socialmonitor/backend/internal/config"
	"github.com/socialmonitor/backend/internal/connector"
	"github.com/socialmonitor/backend/internal/connector/bluesky"
	"github.com/socialmonitor/backend/internal/connector/discourse"
	"github.com/socialmonitor/backend/internal/connector/hackernews"
	"github.com/socialmonitor/backend/internal/connector/mastodon"
	"github.com/socialmonitor/backend/internal/connector/reddit"
	rssconn "github.com/socialmonitor/backend/internal/connector/rss"
	"github.com/socialmonitor/backend/internal/enrichment"
	"github.com/socialmonitor/backend/internal/ingestion"
	"github.com/socialmonitor/backend/internal/search"
	"github.com/socialmonitor/backend/internal/store"
)

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// OpenSearch
	searchClient := search.NewOpenSearchClient(
		cfg.OpenSearch.URL, cfg.OpenSearch.Index,
		cfg.OpenSearch.Username, cfg.OpenSearch.Password,
	)
	if err := searchClient.EnsureIndex(ctx); err != nil {
		log.Printf("[main] warning: opensearch index setup: %v", err)
	}

	// Postgres (optional for development)
	var pgStore *store.PostgresStore
	pg, err := store.NewPostgresStore(cfg.Postgres.DSN)
	if err != nil {
		log.Printf("[main] warning: postgres not available: %v (monitors will be in-memory)", err)
	} else {
		pgStore = pg
		if err := pgStore.Migrate(ctx); err != nil {
			log.Printf("[main] warning: postgres migration: %v", err)
		}
		defer pgStore.Close()
	}

	// Cursor store
	var cursorStore connector.CursorStore
	if pgStore != nil {
		cursorStore = pgStore
	} else {
		cursorStore = connector.NewMemoryCursorStore()
	}

	// Dedup: Redis first (best for high-throughput, atomic SETNX with TTL),
	// fall back to Postgres (durable across redis restarts), and finally to
	// in-memory if neither is reachable.
	var dedup connector.DedupeChecker
	if rd, err := connector.NewRedisDedupeChecker(ctx, cfg.Redis.Addr, cfg.Redis.Password, 24*time.Hour); err == nil {
		log.Printf("[main] dedup: redis @ %s (24h TTL)", cfg.Redis.Addr)
		dedup = rd
	} else if pgStore != nil {
		log.Printf("[main] dedup: postgres (redis unavailable: %v)", err)
		dedup = pgStore
	} else {
		log.Printf("[main] dedup: in-memory (redis & postgres both unavailable)")
		dedup = connector.NewMemoryDedupeChecker()
	}

	// Raw payload archive: filesystem under cfg.Archive.Path, or noop
	// when the path is empty.
	var rawArchive archive.Archive = archive.NewNoop()
	if cfg.Archive.Path != "" {
		fa, err := archive.NewFilesystem(cfg.Archive.Path)
		if err != nil {
			log.Printf("[main] archive: filesystem at %s unavailable: %v (continuing without archive)", cfg.Archive.Path, err)
		} else {
			log.Printf("[main] archive: filesystem at %s", cfg.Archive.Path)
			rawArchive = fa
		}
	}

	// Enricher chain: each entry in cfg.Enrichers is one HTTP plugin.
	// Failures here are non-fatal and logged via Chain.Stats.
	var enrichers []enrichment.Enricher
	for _, e := range cfg.Enrichers {
		if !e.Enabled || e.Name == "" || e.URL == "" {
			continue
		}
		timeout, _ := time.ParseDuration(e.Timeout)
		enrichers = append(enrichers, enrichment.NewHTTPEnricher(enrichment.HTTPEnricherConfig{
			Name:    e.Name,
			URL:     e.URL,
			Timeout: timeout,
			Token:   e.Token,
		}))
		log.Printf("[main] enricher: %s → %s", e.Name, e.URL)
	}
	enrichChain := enrichment.NewChain(enrichers, log.Printf)

	// Pipeline
	pipeline := ingestion.NewPipeline(searchClient, dedup, rawArchive, enrichChain)

	// Connectors
	if cfg.Connectors.Bluesky.Enabled {
		bsky := bluesky.NewJetstreamConnector(
			cfg.Connectors.Bluesky.JetstreamURL,
			cfg.Connectors.Bluesky.WantedCollections,
			cursorStore,
		)
		pipeline.AddConnector(bsky)
	}

	if cfg.Connectors.HackerNews.Enabled {
		pollInterval, _ := time.ParseDuration(cfg.Connectors.HackerNews.PollInterval)
		if pollInterval == 0 {
			pollInterval = 30 * time.Second
		}
		hn := hackernews.NewHNConnector(pollInterval, cursorStore)
		pipeline.AddConnector(hn)
	}

	if cfg.Connectors.RSS.Enabled && len(cfg.Connectors.RSS.Feeds) > 0 {
		pollInterval, _ := time.ParseDuration(cfg.Connectors.RSS.PollInterval)
		if pollInterval == 0 {
			pollInterval = 5 * time.Minute
		}
		rss := rssconn.NewRSSConnector(cfg.Connectors.RSS.Feeds, pollInterval, cursorStore)
		pipeline.AddConnector(rss)
	}

	if cfg.Connectors.Reddit.Enabled && len(cfg.Connectors.Reddit.Subreddits) > 0 {
		poll, _ := time.ParseDuration(cfg.Connectors.Reddit.PollInterval)
		rc := reddit.New(reddit.Config{
			Subreddits:   cfg.Connectors.Reddit.Subreddits,
			PollInterval: poll,
			UserAgent:    cfg.Connectors.Reddit.UserAgent,
			BaseURL:      cfg.Connectors.Reddit.BaseURL,
		}, cursorStore)
		pipeline.AddConnector(rc)
	}

	if cfg.Connectors.Mastodon.Enabled {
		feeds, err := mastodon.ParseFeeds(cfg.Connectors.Mastodon.Feeds)
		if err != nil {
			log.Printf("[main] mastodon: invalid MASTODON_FEEDS: %v (skipping)", err)
		} else if len(feeds) > 0 {
			poll, _ := time.ParseDuration(cfg.Connectors.Mastodon.PollInterval)
			mc := mastodon.New(mastodon.Config{
				Feeds:        feeds,
				PollInterval: poll,
				AccessToken:  cfg.Connectors.Mastodon.AccessToken,
				UserAgent:    cfg.Connectors.Mastodon.UserAgent,
			}, cursorStore)
			pipeline.AddConnector(mc)
		}
	}

	if cfg.Connectors.Discourse.Enabled && len(cfg.Connectors.Discourse.Forums) > 0 {
		poll, _ := time.ParseDuration(cfg.Connectors.Discourse.PollInterval)
		dc := discourse.New(discourse.Config{
			Forums:       cfg.Connectors.Discourse.Forums,
			PollInterval: poll,
			UserAgent:    cfg.Connectors.Discourse.UserAgent,
		}, cursorStore)
		pipeline.AddConnector(dc)
	}

	// Start ingestion pipeline
	if err := pipeline.Start(ctx); err != nil {
		log.Fatalf("[main] pipeline start error: %v", err)
	}

	// API server
	handler := api.NewHandler(searchClient, pgStore, pipeline)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.Server.Port),
		Handler: api.CORSMiddleware(api.LoggingMiddleware(mux)),
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("[main] shutting down...")
		pipeline.Stop()
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		server.Shutdown(shutdownCtx)
	}()

	log.Printf("[main] server starting on :%s", cfg.Server.Port)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("[main] server error: %v", err)
	}
}
