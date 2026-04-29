// Package mastodon implements a Connector that polls one or more Mastodon
// instances for new public statuses.
//
// Each entry in Config.Feeds is either:
//
//	{Instance: "mastodon.social", Type: "public"}            // public timeline
//	{Instance: "mastodon.social", Type: "tag", Tag: "go"}    // hashtag timeline
//
// Mastodon's REST API exposes both as anonymous endpoints; no token is
// required for read access to public content. For higher throughput or
// access to private/local content, supply Config.AccessToken — it will be
// added as a Bearer header on every request.
//
// Cursor: the highest status ID seen per feed, persisted under
// "mastodon:<instance>:<type>[:<tag>]". On subsequent polls it is sent as
// since_id, which Mastodon defines as "return results newer than this ID."
package mastodon

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

// Feed identifies one Mastodon timeline to poll.
type Feed struct {
	Instance string // host like "mastodon.social"
	Type     string // "public" or "tag"
	Tag      string // hashtag without "#", required when Type == "tag"
}

// Config configures a MastodonConnector.
type Config struct {
	Feeds        []Feed
	PollInterval time.Duration
	AccessToken  string // optional; bearer token for authenticated reads
	UserAgent    string
	PageLimit    int // max 40 per Mastodon's API
}

type MastodonConnector struct {
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

// status is the subset of Mastodon's Status entity we use.
// See https://docs.joinmastodon.org/entities/Status/.
type status struct {
	ID                 string         `json:"id"`
	URI                string         `json:"uri"`
	URL                string         `json:"url"`
	CreatedAt          time.Time      `json:"created_at"`
	Content            string         `json:"content"` // HTML
	Language           string         `json:"language"`
	InReplyToID        string         `json:"in_reply_to_id"`
	Account            account        `json:"account"`
	MediaAttachments   []mediaAttach  `json:"media_attachments"`
	Tags               []tag          `json:"tags"`
	ReblogsCount       int64          `json:"reblogs_count"`
	FavouritesCount    int64          `json:"favourites_count"`
	RepliesCount       int64          `json:"replies_count"`
	Sensitive          bool           `json:"sensitive"`
	SpoilerText        string         `json:"spoiler_text"`
}

type account struct {
	ID             string `json:"id"`
	Username       string `json:"username"`
	Acct           string `json:"acct"`
	DisplayName    string `json:"display_name"`
	URL            string `json:"url"`
	FollowersCount int64  `json:"followers_count"`
	Bot            bool   `json:"bot"`
}

type mediaAttach struct {
	Type        string `json:"type"`
	URL         string `json:"url"`
	Description string `json:"description"`
}

type tag struct {
	Name string `json:"name"`
}

// New returns a configured MastodonConnector.
func New(cfg Config, cursorStore connector.CursorStore) *MastodonConnector {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "social-monitoring/0.1 (workbench MVP)"
	}
	if cfg.PageLimit <= 0 || cfg.PageLimit > 40 {
		cfg.PageLimit = 40
	}
	return &MastodonConnector{
		cfg:         cfg,
		cursorStore: cursorStore,
		limiter:     connector.NewTokenBucketLimiter(2),
		httpClient:  &http.Client{Timeout: 15 * time.Second},
		stopCh:      make(chan struct{}),
	}
}

func (m *MastodonConnector) Name() string { return "mastodon" }

func (m *MastodonConnector) Start(ctx context.Context, out chan<- model.SocialEvent) error {
	if len(m.cfg.Feeds) == 0 {
		log.Printf("[mastodon] no feeds configured; connector will idle")
	} else {
		log.Printf("[mastodon] starting; %d feeds poll=%s", len(m.cfg.Feeds), m.cfg.PollInterval)
	}
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()

	m.pollAll(ctx, out)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.stopCh:
			return nil
		case <-ticker.C:
			m.pollAll(ctx, out)
		}
	}
}

func (m *MastodonConnector) Stop() error {
	close(m.stopCh)
	return nil
}

func (m *MastodonConnector) Health() connector.HealthStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h := m.health
	h.EventsTotal = m.eventsTotal.Load()
	h.ErrorsTotal = m.errorsTotal.Load()
	return h
}

func (m *MastodonConnector) pollAll(ctx context.Context, out chan<- model.SocialEvent) {
	for _, f := range m.cfg.Feeds {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		default:
		}
		if err := m.pollOne(ctx, f, out); err != nil {
			log.Printf("[mastodon] poll %s/%s%s: %v", f.Instance, f.Type, tagSuffix(f), err)
			m.errorsTotal.Add(1)
			m.mu.Lock()
			m.health.LastError = err.Error()
			m.health.LastErrorTime = time.Now()
			m.mu.Unlock()
		}
	}
}

func tagSuffix(f Feed) string {
	if f.Tag == "" {
		return ""
	}
	return ":" + f.Tag
}

func (m *MastodonConnector) pollOne(ctx context.Context, f Feed, out chan<- model.SocialEvent) error {
	if err := m.limiter.Wait(ctx); err != nil {
		return err
	}

	endpoint, err := buildEndpoint(f)
	if err != nil {
		return err
	}

	cursorKey := cursorKey(f)
	sinceID, _ := m.cursorStore.GetCursor(ctx, cursorKey)

	q := endpoint.Query()
	q.Set("limit", fmt.Sprintf("%d", m.cfg.PageLimit))
	if sinceID != "" {
		q.Set("since_id", sinceID)
	}
	endpoint.RawQuery = q.Encode()

	headers := map[string]string{
		"User-Agent": m.cfg.UserAgent,
		"Accept":     "application/json",
	}
	if m.cfg.AccessToken != "" {
		headers["Authorization"] = "Bearer " + m.cfg.AccessToken
	}

	// Decode into RawMessage first so we can preserve each status's
	// original bytes for the raw archive without re-marshaling.
	var raws []json.RawMessage
	if err := connector.GetJSON(ctx, m.httpClient, endpoint.String(), headers, &raws); err != nil {
		return err
	}
	if len(raws) == 0 {
		return nil
	}
	statuses := make([]status, len(raws))
	for i, r := range raws {
		if err := json.Unmarshal(r, &statuses[i]); err != nil {
			return fmt.Errorf("decode status: %w", err)
		}
	}

	// Mastodon returns newest-first; reverse for chronological emit.
	var newestID string
	for i := len(statuses) - 1; i >= 0; i-- {
		s := statuses[i]
		evt := m.normalize(s, f.Instance)
		evt.RawPayload = []byte(raws[i])
		select {
		case out <- evt:
		case <-ctx.Done():
			return ctx.Err()
		case <-m.stopCh:
			return nil
		}
		m.eventsTotal.Add(1)
		newestID = s.ID
	}
	if newestID != "" {
		_ = m.cursorStore.SetCursor(ctx, cursorKey, newestID)
		m.mu.Lock()
		m.health.Healthy = true
		m.health.LastEvent = time.Now()
		m.mu.Unlock()
	}
	return nil
}

func buildEndpoint(f Feed) (*url.URL, error) {
	if f.Instance == "" {
		return nil, fmt.Errorf("feed missing instance")
	}
	host := f.Instance
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimSuffix(host, "/")

	var path string
	switch f.Type {
	case "public", "":
		path = "/api/v1/timelines/public"
	case "tag":
		if f.Tag == "" {
			return nil, fmt.Errorf("tag feed missing Tag")
		}
		path = "/api/v1/timelines/tag/" + url.PathEscape(f.Tag)
	default:
		return nil, fmt.Errorf("unknown feed type %q", f.Type)
	}
	return url.Parse("https://" + host + path)
}

func cursorKey(f Feed) string {
	if f.Type == "tag" {
		return "mastodon:" + f.Instance + ":tag:" + f.Tag
	}
	return "mastodon:" + f.Instance + ":public"
}

func (m *MastodonConnector) normalize(s status, instance string) model.SocialEvent {
	sourceType := "post"
	parentID := ""
	rootID := "mastodon:" + instance + ":" + s.ID
	threadID := rootID
	if s.InReplyToID != "" {
		sourceType = "reply"
		parentID = "mastodon:" + instance + ":" + s.InReplyToID
		// Without fetching the conversation we can't know the true root;
		// use the parent as a reasonable thread anchor.
		threadID = parentID
	}

	var media []model.Media
	for _, ma := range s.MediaAttachments {
		media = append(media, model.Media{
			Type: ma.Type,
			URL:  ma.URL,
			Alt:  ma.Description,
		})
	}

	var topics []string
	for _, t := range s.Tags {
		if t.Name != "" {
			topics = append(topics, t.Name)
		}
	}

	lang := s.Language
	if lang == "" {
		lang = "unknown"
	}

	return model.SocialEvent{
		EventID:    "mastodon:" + instance + ":" + s.ID,
		Platform:   "mastodon",
		SourceType: sourceType,
		SourceURL:  s.URL,
		Author: model.Author{
			ID:            s.Account.ID,
			Handle:        s.Account.Acct,
			DisplayName:   s.Account.DisplayName,
			FollowerCount: s.Account.FollowersCount,
		},
		Content: model.Content{
			Text:     connector.StripHTML(s.Content),
			Language: lang,
			Media:    media,
		},
		Engagement: model.Engagement{
			Likes:    s.FavouritesCount,
			Shares:   s.ReblogsCount,
			Comments: s.RepliesCount,
		},
		Conversation: model.Conversation{
			ThreadID: threadID,
			ParentID: parentID,
			RootID:   rootID,
		},
		Timestamps: model.Timestamps{
			PublishedAt: s.CreatedAt.UTC(),
		},
		Enrichment: model.Enrichment{
			Topics: topics,
		},
	}
}

// ParseFeeds parses comma-separated feed specs from an env var. Each spec
// is "<instance>:public" or "<instance>:tag:<tag>". Whitespace is trimmed.
// Empty input returns nil.
func ParseFeeds(spec string) ([]Feed, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	var feeds []Feed
	for _, raw := range strings.Split(spec, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		parts := strings.Split(raw, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid feed spec %q (want instance:type[:tag])", raw)
		}
		f := Feed{Instance: parts[0], Type: parts[1]}
		if f.Type == "tag" {
			if len(parts) < 3 || parts[2] == "" {
				return nil, fmt.Errorf("tag feed %q missing tag", raw)
			}
			f.Tag = parts[2]
		}
		feeds = append(feeds, f)
	}
	return feeds, nil
}
