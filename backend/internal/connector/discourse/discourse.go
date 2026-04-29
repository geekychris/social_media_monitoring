// Package discourse implements a Connector that polls one or more Discourse
// forums for new topics via /latest.json.
//
// Discourse exposes /latest.json on every public forum without authentication.
// The connector emits one SocialEvent per topic (not per reply); replies are
// reachable from the topic URL. To stream replies, a future iteration would
// also poll /t/{id}.json once per active topic.
//
// Cursor: the highest topic ID seen per forum, persisted under
// "discourse:<host>". Topics with id <= cursor are skipped on subsequent polls.
package discourse

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

// Config configures a DiscourseConnector.
type Config struct {
	// Forums is a list of base URLs, e.g. "https://meta.discourse.org".
	Forums       []string
	PollInterval time.Duration
	UserAgent    string
}

type DiscourseConnector struct {
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

// latestResponse is the subset of /latest.json we consume.
type latestResponse struct {
	Users     []userRef `json:"users"`
	TopicList struct {
		Topics []topic `json:"topics"`
	} `json:"topic_list"`

	// rawTopics keeps the original JSON bytes of each topic, indexed by
	// topic ID. Populated by UnmarshalJSON so the archive can persist
	// the unmodified payload alongside the normalized event.
	rawTopics map[int64]json.RawMessage `json:"-"`
}

func (l *latestResponse) UnmarshalJSON(data []byte) error {
	type aux struct {
		Users     []userRef `json:"users"`
		TopicList struct {
			Topics []json.RawMessage `json:"topics"`
		} `json:"topic_list"`
	}
	var a aux
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	l.Users = a.Users
	l.TopicList.Topics = make([]topic, 0, len(a.TopicList.Topics))
	l.rawTopics = make(map[int64]json.RawMessage, len(a.TopicList.Topics))
	for _, raw := range a.TopicList.Topics {
		var t topic
		if err := json.Unmarshal(raw, &t); err != nil {
			return err
		}
		l.TopicList.Topics = append(l.TopicList.Topics, t)
		if t.ID != 0 {
			l.rawTopics[t.ID] = raw
		}
	}
	return nil
}

type userRef struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Name     string `json:"name"`
}

type topic struct {
	ID           int64    `json:"id"`
	Title        string   `json:"title"`
	Slug         string   `json:"slug"`
	PostsCount   int64    `json:"posts_count"`
	ReplyCount   int64    `json:"reply_count"`
	CreatedAt    time.Time `json:"created_at"`
	LastPostedAt time.Time `json:"last_posted_at"`
	Excerpt      string   `json:"excerpt"`
	Visible      bool     `json:"visible"`
	Closed       bool     `json:"closed"`
	Archived     bool     `json:"archived"`
	CategoryID   int64    `json:"category_id"`
	Tags         tagList  `json:"tags"`
	Views        int64    `json:"views"`
	LikeCount    int64    `json:"like_count"`
	Posters      []poster `json:"posters"`
}

type poster struct {
	UserID      int64  `json:"user_id"`
	Description string `json:"description"`
}

// tagList accepts both shapes Discourse forums emit for topic tags:
// - older/simpler: ["foo", "bar"]
// - newer (e.g. meta.discourse.org): [{"name": "foo", ...}, ...]
type tagList []string

func (t *tagList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*t = nil
		return nil
	}
	// []string fast path
	var asStrings []string
	if err := json.Unmarshal(data, &asStrings); err == nil {
		*t = asStrings
		return nil
	}
	// fallback: array of objects with .name
	var asObjects []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &asObjects); err != nil {
		return fmt.Errorf("tags: unsupported shape: %w", err)
	}
	out := make([]string, 0, len(asObjects))
	for _, o := range asObjects {
		if o.Name != "" {
			out = append(out, o.Name)
		}
	}
	*t = out
	return nil
}

// New returns a configured DiscourseConnector.
func New(cfg Config, cursorStore connector.CursorStore) *DiscourseConnector {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 2 * time.Minute
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "social-monitoring/0.1 (workbench MVP)"
	}
	return &DiscourseConnector{
		cfg:         cfg,
		cursorStore: cursorStore,
		limiter:     connector.NewTokenBucketLimiter(2),
		httpClient:  &http.Client{Timeout: 20 * time.Second},
		stopCh:      make(chan struct{}),
	}
}

func (d *DiscourseConnector) Name() string { return "discourse" }

func (d *DiscourseConnector) Start(ctx context.Context, out chan<- model.SocialEvent) error {
	if len(d.cfg.Forums) == 0 {
		log.Printf("[discourse] no forums configured; connector will idle")
	} else {
		log.Printf("[discourse] starting; forums=%v poll=%s", d.cfg.Forums, d.cfg.PollInterval)
	}
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	d.pollAll(ctx, out)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.stopCh:
			return nil
		case <-ticker.C:
			d.pollAll(ctx, out)
		}
	}
}

func (d *DiscourseConnector) Stop() error {
	close(d.stopCh)
	return nil
}

func (d *DiscourseConnector) Health() connector.HealthStatus {
	d.mu.RLock()
	defer d.mu.RUnlock()
	h := d.health
	h.EventsTotal = d.eventsTotal.Load()
	h.ErrorsTotal = d.errorsTotal.Load()
	return h
}

func (d *DiscourseConnector) pollAll(ctx context.Context, out chan<- model.SocialEvent) {
	for _, forum := range d.cfg.Forums {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		default:
		}
		if err := d.pollOne(ctx, forum, out); err != nil {
			log.Printf("[discourse] poll %s: %v", forum, err)
			d.errorsTotal.Add(1)
			d.mu.Lock()
			d.health.LastError = err.Error()
			d.health.LastErrorTime = time.Now()
			d.mu.Unlock()
		}
	}
}

func (d *DiscourseConnector) pollOne(ctx context.Context, forum string, out chan<- model.SocialEvent) error {
	if err := d.limiter.Wait(ctx); err != nil {
		return err
	}

	base, err := normalizeBase(forum)
	if err != nil {
		return err
	}
	host := base.Host
	endpoint := base.JoinPath("/latest.json").String()

	cursorKey := "discourse:" + host
	cursorStr, _ := d.cursorStore.GetCursor(ctx, cursorKey)
	var lastSeen int64
	if cursorStr != "" {
		_, _ = fmt.Sscanf(cursorStr, "%d", &lastSeen)
	}

	headers := map[string]string{
		"User-Agent": d.cfg.UserAgent,
		"Accept":     "application/json",
	}

	var resp latestResponse
	if err := connector.GetJSON(ctx, d.httpClient, endpoint, headers, &resp); err != nil {
		return err
	}
	if len(resp.TopicList.Topics) == 0 {
		return nil
	}

	users := make(map[int64]userRef, len(resp.Users))
	for _, u := range resp.Users {
		users[u.ID] = u
	}

	// Topics arrive newest-first by bumped time but we filter by id and
	// emit in ascending id order so cursor logic is monotonic.
	var newIDs []int64
	byID := make(map[int64]topic)
	for _, t := range resp.TopicList.Topics {
		if !t.Visible {
			continue
		}
		if t.ID <= lastSeen {
			continue
		}
		newIDs = append(newIDs, t.ID)
		byID[t.ID] = t
	}
	if len(newIDs) == 0 {
		return nil
	}
	// ascending sort
	for i := 1; i < len(newIDs); i++ {
		for j := i; j > 0 && newIDs[j] < newIDs[j-1]; j-- {
			newIDs[j], newIDs[j-1] = newIDs[j-1], newIDs[j]
		}
	}

	maxID := lastSeen
	for _, id := range newIDs {
		t := byID[id]
		evt := d.normalize(t, base, users)
		if raw, ok := resp.rawTopics[id]; ok {
			evt.RawPayload = []byte(raw)
		}
		select {
		case out <- evt:
		case <-ctx.Done():
			return ctx.Err()
		case <-d.stopCh:
			return nil
		}
		d.eventsTotal.Add(1)
		if id > maxID {
			maxID = id
		}
	}

	if maxID > lastSeen {
		_ = d.cursorStore.SetCursor(ctx, cursorKey, fmt.Sprintf("%d", maxID))
		d.mu.Lock()
		d.health.Healthy = true
		d.health.LastEvent = time.Now()
		d.mu.Unlock()
	}
	return nil
}

func normalizeBase(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty forum URL")
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse forum URL: %w", err)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawQuery = ""
	u.Fragment = ""
	return u, nil
}

func (d *DiscourseConnector) normalize(t topic, base *url.URL, users map[int64]userRef) model.SocialEvent {
	host := base.Host
	postURL := base.JoinPath(fmt.Sprintf("/t/%s/%d", t.Slug, t.ID)).String()

	// Find the original poster: in /latest.json, the poster with description
	// containing "Original Poster" is the OP. Fall back to the first poster.
	var op userRef
	if len(t.Posters) > 0 {
		op = users[t.Posters[0].UserID]
		for _, p := range t.Posters {
			if strings.Contains(strings.ToLower(p.Description), "original poster") {
				op = users[p.UserID]
				break
			}
		}
	}

	return model.SocialEvent{
		EventID:    fmt.Sprintf("discourse:%s:%d", host, t.ID),
		Platform:   "discourse",
		SourceType: "post",
		SourceURL:  postURL,
		Author: model.Author{
			ID:          fmt.Sprintf("%d", op.ID),
			Handle:      op.Username,
			DisplayName: op.Name,
		},
		Content: model.Content{
			Title:    t.Title,
			Text:     connector.StripHTML(t.Excerpt),
			Language: "en",
		},
		Engagement: model.Engagement{
			Likes:    t.LikeCount,
			Comments: t.ReplyCount,
			Views:    t.Views,
		},
		Conversation: model.Conversation{
			ThreadID: fmt.Sprintf("discourse:%s:%d", host, t.ID),
			RootID:   fmt.Sprintf("discourse:%s:%d", host, t.ID),
		},
		Timestamps: model.Timestamps{
			PublishedAt: t.CreatedAt.UTC(),
		},
		Enrichment: model.Enrichment{
			Topics: []string(t.Tags),
		},
	}
}
