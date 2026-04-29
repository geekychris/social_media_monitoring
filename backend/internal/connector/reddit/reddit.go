// Package reddit implements a Connector that polls Reddit's public JSON
// endpoints (/r/{sub}/new.json) for new posts in one or more subreddits.
//
// Auth: this connector talks to the unauthenticated public JSON endpoints.
// Reddit's terms allow this for low-volume reads with a descriptive
// User-Agent, but rate limits are stricter than the OAuth API. Production
// deployments should add OAuth client_credentials (the "script" app type)
// and route requests through https://oauth.reddit.com. The shape of this
// connector — per-subreddit cursors and the normalize function — does not
// change with auth; only the request signing changes.
//
// Cursor: the fullname (e.g. "t3_abc") of the most recent post seen per
// subreddit, persisted via CursorStore under the key "reddit:r/<sub>".
// On the next poll the cursor is passed as `before=`, returning only
// posts newer than the last-seen one.
package reddit

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/socialmonitor/backend/internal/connector"
	"github.com/socialmonitor/backend/internal/model"
)

// Config configures a RedditConnector.
type Config struct {
	// Subreddits to poll, e.g. []string{"golang", "programming"}.
	Subreddits []string
	// PollInterval is how often each subreddit is polled. The connector
	// rate-limits between subreddit fetches independently of this.
	PollInterval time.Duration
	// UserAgent is sent on every request. Reddit's API rules require a
	// descriptive UA; using a generic one risks being throttled or blocked.
	UserAgent string
	// PageLimit is the number of posts to request per poll (max 100).
	PageLimit int
	// BaseURL is the Reddit host used for requests. Defaults to
	// https://www.reddit.com. Useful targets include https://old.reddit.com
	// (less aggressively WAF'd from cloud egress IPs) and
	// https://oauth.reddit.com when OAuth is wired up.
	BaseURL string
}

// RedditConnector polls subreddit JSON listings and emits SocialEvents.
type RedditConnector struct {
	cfg         Config
	cursorStore connector.CursorStore
	limiter     *connector.TokenBucketLimiter
	httpClient  *http.Client

	mu          sync.RWMutex
	health      connector.HealthStatus
	stopCh      chan struct{}
	eventsTotal atomic.Int64
	errorsTotal atomic.Int64
}

// listing is the top-level shape Reddit returns from /r/.../new.json.
type listing struct {
	Data struct {
		After    string  `json:"after"`
		Before   string  `json:"before"`
		Children []child `json:"children"`
	} `json:"data"`
}

// child holds the parsed post plus the original raw bytes of the
// child's `data` field, so we can persist the unmodified Reddit payload
// alongside the normalized event.
type child struct {
	Kind string          `json:"kind"`
	Data post            `json:"data"`
	Raw  json.RawMessage `json:"-"` // populated by listing.UnmarshalJSON
}

// UnmarshalJSON captures the per-child raw bytes during decode. Reddit
// returns a single envelope for an entire page; without this hook we'd
// have to re-marshal each child to archive it (lossy whitespace + key
// ordering). Doing it once at decode time keeps the original bytes intact.
func (l *listing) UnmarshalJSON(data []byte) error {
	type envelope struct {
		Data struct {
			After    string            `json:"after"`
			Before   string            `json:"before"`
			Children []json.RawMessage `json:"children"`
		} `json:"data"`
	}
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return err
	}
	l.Data.After = e.Data.After
	l.Data.Before = e.Data.Before
	l.Data.Children = make([]child, 0, len(e.Data.Children))
	for _, raw := range e.Data.Children {
		var c struct {
			Kind string          `json:"kind"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &c); err != nil {
			return err
		}
		var p post
		if err := json.Unmarshal(c.Data, &p); err != nil {
			return err
		}
		l.Data.Children = append(l.Data.Children, child{Kind: c.Kind, Data: p, Raw: c.Data})
	}
	return nil
}

// post is the subset of fields we extract from a Reddit post.
type post struct {
	ID                string  `json:"id"`
	Name              string  `json:"name"` // fullname, e.g. "t3_abc"
	Subreddit         string  `json:"subreddit"`
	SubredditPrefixed string  `json:"subreddit_name_prefixed"`
	Author            string  `json:"author"`
	AuthorFullname    string  `json:"author_fullname"`
	Title             string  `json:"title"`
	Selftext          string  `json:"selftext"`
	URL               string  `json:"url"`
	Permalink         string  `json:"permalink"`
	Score             int     `json:"score"`
	NumComments       int     `json:"num_comments"`
	CreatedUTC        float64 `json:"created_utc"`
	IsSelf            bool    `json:"is_self"`
	Over18            bool    `json:"over_18"`
	Stickied          bool    `json:"stickied"`
	RemovedByCategory string  `json:"removed_by_category"`
}

// New constructs a RedditConnector. Defaults are filled in for any
// zero-value Config fields.
func New(cfg Config, cursorStore connector.CursorStore) *RedditConnector {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Minute
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "social-monitoring/0.1 (workbench MVP)"
	}
	if cfg.PageLimit <= 0 || cfg.PageLimit > 100 {
		cfg.PageLimit = 25
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://www.reddit.com"
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &RedditConnector{
		cfg:         cfg,
		cursorStore: cursorStore,
		// Reddit unauth limit is ~60 req/min; 1 rps with a small burst is safe.
		limiter:    connector.NewTokenBucketLimiter(1),
		httpClient: &http.Client{Timeout: 15 * time.Second},
		stopCh:     make(chan struct{}),
	}
}

// Name returns the connector name used in pipeline logs and health output.
func (r *RedditConnector) Name() string { return "reddit" }

// Start blocks until ctx is cancelled or Stop is called, polling each
// configured subreddit every PollInterval and writing normalized events to out.
func (r *RedditConnector) Start(ctx context.Context, out chan<- model.SocialEvent) error {
	if len(r.cfg.Subreddits) == 0 {
		log.Printf("[reddit] no subreddits configured; connector will idle")
	} else {
		log.Printf("[reddit] starting; subreddits=%v poll=%s", r.cfg.Subreddits, r.cfg.PollInterval)
	}
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	r.pollAll(ctx, out)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return nil
		case <-ticker.C:
			r.pollAll(ctx, out)
		}
	}
}

// Stop signals the Start loop to exit and is safe to call once.
func (r *RedditConnector) Stop() error {
	close(r.stopCh)
	return nil
}

// Health returns the connector's current aggregated health.
func (r *RedditConnector) Health() connector.HealthStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h := r.health
	h.EventsTotal = r.eventsTotal.Load()
	h.ErrorsTotal = r.errorsTotal.Load()
	return h
}

func (r *RedditConnector) pollAll(ctx context.Context, out chan<- model.SocialEvent) {
	for _, sub := range r.cfg.Subreddits {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		default:
		}
		if err := r.pollOne(ctx, sub, out); err != nil {
			log.Printf("[reddit] poll r/%s: %v", sub, err)
			r.errorsTotal.Add(1)
			r.mu.Lock()
			r.health.LastError = err.Error()
			r.health.LastErrorTime = time.Now()
			r.mu.Unlock()
		}
	}
}

func (r *RedditConnector) pollOne(ctx context.Context, sub string, out chan<- model.SocialEvent) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}

	cursorKey := "reddit:r/" + sub
	before, _ := r.cursorStore.GetCursor(ctx, cursorKey)

	q := url.Values{}
	q.Set("limit", fmt.Sprintf("%d", r.cfg.PageLimit))
	q.Set("raw_json", "1") // disable HTML entity escaping in selftext
	if before != "" {
		q.Set("before", before)
	}
	endpoint := fmt.Sprintf("%s/r/%s/new.json?%s", r.cfg.BaseURL, url.PathEscape(sub), q.Encode())

	headers := map[string]string{
		"User-Agent": r.cfg.UserAgent,
		"Accept":     "application/json",
	}

	var resp listing
	if err := connector.GetJSON(ctx, r.httpClient, endpoint, headers, &resp); err != nil {
		return err
	}

	if len(resp.Data.Children) == 0 {
		// Reddit's anti-bot WAF often returns HTTP 200 with an empty
		// listing instead of 403 to programmatic clients (a silent
		// shadow-block). Surface that on the connector's health so the
		// pipeline status endpoint shows the soft failure rather than a
		// false "healthy: false, errors: 0" combination.
		r.mu.Lock()
		r.health.LastError = "empty listing for r/" + sub + " (Reddit may be soft-blocking; consider OAuth)"
		r.health.LastErrorTime = time.Now()
		r.mu.Unlock()
		return nil
	}

	// Reddit returns newest-first. Iterate in reverse so events arrive in
	// chronological order downstream and the newest fullname becomes the
	// final cursor write.
	var newestFullname string
	for i := len(resp.Data.Children) - 1; i >= 0; i-- {
		c := resp.Data.Children[i]
		if c.Kind != "t3" {
			continue
		}
		if c.Data.RemovedByCategory != "" {
			continue
		}
		evt := r.normalize(c.Data)
		evt.RawPayload = []byte(c.Raw)
		select {
		case out <- evt:
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return nil
		}
		r.eventsTotal.Add(1)
		newestFullname = c.Data.Name
	}

	if newestFullname != "" {
		_ = r.cursorStore.SetCursor(ctx, cursorKey, newestFullname)
		r.mu.Lock()
		r.health.Healthy = true
		r.health.LastEvent = time.Now()
		r.mu.Unlock()
	}
	return nil
}

// normalize converts a Reddit post into the canonical SocialEvent shape.
// It is exported via the package-level method for unit testing without
// touching the network.
func (r *RedditConnector) normalize(p post) model.SocialEvent {
	sourceURL := "https://www.reddit.com" + p.Permalink

	text := strings.TrimSpace(p.Selftext)
	if text == "" && !p.IsSelf {
		text = p.URL
	}

	urls := connector.FilterEmpty([]string{p.URL})

	return model.SocialEvent{
		EventID:    "reddit:" + p.Name,
		Platform:   "reddit",
		SourceType: "post",
		SourceURL:  sourceURL,
		Author: model.Author{
			ID:     p.AuthorFullname,
			Handle: p.Author,
		},
		Content: model.Content{
			Title:    p.Title,
			Text:     text,
			Language: "en",
			URLs:     urls,
		},
		Engagement: model.Engagement{
			Score:    int64(p.Score),
			Comments: int64(p.NumComments),
		},
		Conversation: model.Conversation{
			ThreadID: "reddit:" + p.Name,
			RootID:   "reddit:" + p.Name,
		},
		Timestamps: model.Timestamps{
			PublishedAt: time.Unix(int64(p.CreatedUTC), 0).UTC(),
		},
		Enrichment: model.Enrichment{
			Topics: []string{p.SubredditPrefixed},
		},
	}
}
