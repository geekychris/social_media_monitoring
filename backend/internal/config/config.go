// Package config loads runtime configuration for the backend from
// environment variables. Defaults are tuned for local docker-compose; any
// secret or per-deploy value should be overridden via env.
package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Server     ServerConfig
	OpenSearch OpenSearchConfig
	Postgres   PostgresConfig
	Redis      RedisConfig
	Archive    ArchiveConfig
	Enrichers  []EnricherConfig
	Connectors ConnectorsConfig
}

// ArchiveConfig configures the raw payload archive. Empty Path disables
// archival.
type ArchiveConfig struct {
	Path string
}

// EnricherConfig declares one HTTP enricher plugin. Each plugin is an
// external HTTP service that receives an EnrichRequest envelope and
// returns annotations to merge onto the event. See
// internal/enrichment for the wire format.
type EnricherConfig struct {
	Enabled bool
	Name    string
	URL     string
	Timeout string
	Token   string
}

type ServerConfig struct {
	Port string
}

type OpenSearchConfig struct {
	URL      string
	Username string
	Password string
	Index    string
}

type PostgresConfig struct {
	DSN string
}

type RedisConfig struct {
	Addr     string
	Password string
}

type ConnectorsConfig struct {
	Bluesky    BlueskyConfig
	HackerNews HackerNewsConfig
	RSS        RSSConfig
	Reddit     RedditConfig
	Mastodon   MastodonConfig
	Discourse  DiscourseConfig
}

type BlueskyConfig struct {
	Enabled           bool
	JetstreamURL      string
	WantedCollections []string
}

type HackerNewsConfig struct {
	Enabled      bool
	PollInterval string
}

type RSSConfig struct {
	Enabled      bool
	Feeds        []string
	PollInterval string
}

type RedditConfig struct {
	Enabled      bool
	Subreddits   []string
	PollInterval string
	UserAgent    string
	BaseURL      string
}

// MastodonConfig configures the Mastodon connector. Feeds is the raw
// comma-separated string from MASTODON_FEEDS; the connector parses it
// with mastodon.ParseFeeds at startup.
type MastodonConfig struct {
	Enabled      bool
	Feeds        string
	PollInterval string
	AccessToken  string
	UserAgent    string
}

type DiscourseConfig struct {
	Enabled      bool
	Forums       []string
	PollInterval string
	UserAgent    string
}

func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Port: getEnv("PORT", "8080"),
		},
		Archive: ArchiveConfig{
			Path: getEnv("RAW_ARCHIVE_PATH", "/var/lib/raw_archive"),
		},
		Enrichers: loadEnrichers(),
		OpenSearch: OpenSearchConfig{
			URL:      getEnv("OPENSEARCH_URL", "http://localhost:9200"),
			Username: getEnv("OPENSEARCH_USERNAME", "admin"),
			Password: getEnv("OPENSEARCH_PASSWORD", "admin"),
			Index:    getEnv("OPENSEARCH_INDEX", "social_events"),
		},
		Postgres: PostgresConfig{
			DSN: getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/socialmonitor?sslmode=disable"),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
		},
		Connectors: ConnectorsConfig{
			Bluesky: BlueskyConfig{
				Enabled:           getEnv("BLUESKY_ENABLED", "true") == "true",
				JetstreamURL:      getEnv("BLUESKY_JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe"),
				WantedCollections: []string{"app.bsky.feed.post"},
			},
			HackerNews: HackerNewsConfig{
				Enabled:      getEnv("HN_ENABLED", "true") == "true",
				PollInterval: getEnv("HN_POLL_INTERVAL", "30s"),
			},
			RSS: RSSConfig{
				Enabled:      getEnv("RSS_ENABLED", "false") == "true",
				Feeds:        splitCSV(getEnv("RSS_FEEDS", "")),
				PollInterval: getEnv("RSS_POLL_INTERVAL", "5m"),
			},
			Reddit: RedditConfig{
				Enabled:      getEnv("REDDIT_ENABLED", "false") == "true",
				Subreddits:   splitCSV(getEnv("REDDIT_SUBREDDITS", "")),
				PollInterval: getEnv("REDDIT_POLL_INTERVAL", "1m"),
				UserAgent:    getEnv("REDDIT_USER_AGENT", ""),
				BaseURL:      getEnv("REDDIT_BASE_URL", "https://old.reddit.com"),
			},
			Mastodon: MastodonConfig{
				Enabled:      getEnv("MASTODON_ENABLED", "false") == "true",
				Feeds:        getEnv("MASTODON_FEEDS", ""),
				PollInterval: getEnv("MASTODON_POLL_INTERVAL", "30s"),
				AccessToken:  getEnv("MASTODON_ACCESS_TOKEN", ""),
				UserAgent:    getEnv("MASTODON_USER_AGENT", ""),
			},
			Discourse: DiscourseConfig{
				Enabled:      getEnv("DISCOURSE_ENABLED", "false") == "true",
				Forums:       splitCSV(getEnv("DISCOURSE_FORUMS", "")),
				PollInterval: getEnv("DISCOURSE_POLL_INTERVAL", "2m"),
				UserAgent:    getEnv("DISCOURSE_USER_AGENT", ""),
			},
		},
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// loadEnrichers reads ENRICHER_<N>_{NAME,URL,TIMEOUT,TOKEN,ENABLED} env
// vars for N in {1,2,...} until it finds a slot with neither NAME nor
// URL set. This keeps configuration in env vars (no extra config file)
// while still allowing multiple plugins to be declared.
func loadEnrichers() []EnricherConfig {
	var out []EnricherConfig
	for i := 1; i < 100; i++ {
		prefix := "ENRICHER_" + strconv.Itoa(i) + "_"
		name := os.Getenv(prefix + "NAME")
		url := os.Getenv(prefix + "URL")
		if name == "" && url == "" {
			break
		}
		out = append(out, EnricherConfig{
			Enabled: getEnv(prefix+"ENABLED", "true") == "true",
			Name:    name,
			URL:     url,
			Timeout: getEnv(prefix+"TIMEOUT", "5s"),
			Token:   os.Getenv(prefix + "TOKEN"),
		})
	}
	return out
}

// splitCSV returns a slice of trimmed, non-empty entries from s.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
