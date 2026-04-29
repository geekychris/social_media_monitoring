package bluesky

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/socialmonitor/backend/internal/connector"
	"github.com/socialmonitor/backend/internal/model"
)

// JetstreamConnector consumes the Bluesky Jetstream firehose via WebSocket.
type JetstreamConnector struct {
	jetstreamURL      string
	wantedCollections []string
	cursorStore       connector.CursorStore

	mu        sync.RWMutex
	conn      *websocket.Conn
	health    connector.HealthStatus
	stopCh    chan struct{}
	eventsTotal atomic.Int64
	errorsTotal atomic.Int64
}

// Jetstream event types
type JetstreamEvent struct {
	Did        string          `json:"did"`
	TimeUS     int64           `json:"time_us"`
	Kind       string          `json:"kind"`       // commit, identity, account
	Commit     *CommitPayload  `json:"commit,omitempty"`
}

type CommitPayload struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"` // create, update, delete
	Collection string          `json:"collection"`
	RKey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"`
	CID        string          `json:"cid"`
}

type PostRecord struct {
	Type      string      `json:"$type"`
	Text      string      `json:"text"`
	CreatedAt string      `json:"createdAt"`
	Langs     []string    `json:"langs,omitempty"`
	Reply     *ReplyRef   `json:"reply,omitempty"`
	Embed     *EmbedRef   `json:"embed,omitempty"`
	Facets    []Facet     `json:"facets,omitempty"`
}

type ReplyRef struct {
	Root   StrongRef `json:"root"`
	Parent StrongRef `json:"parent"`
}

type StrongRef struct {
	URI string `json:"uri"`
	CID string `json:"cid"`
}

type EmbedRef struct {
	Type   string      `json:"$type"`
	Images []ImageRef  `json:"images,omitempty"`
}

type ImageRef struct {
	Alt   string `json:"alt"`
	Image any    `json:"image"`
}

type Facet struct {
	Features []FacetFeature `json:"features"`
}

type FacetFeature struct {
	Type string `json:"$type"`
	URI  string `json:"uri,omitempty"`
	Tag  string `json:"tag,omitempty"`
}

func NewJetstreamConnector(jetstreamURL string, wantedCollections []string, cursorStore connector.CursorStore) *JetstreamConnector {
	return &JetstreamConnector{
		jetstreamURL:      jetstreamURL,
		wantedCollections: wantedCollections,
		cursorStore:       cursorStore,
		stopCh:            make(chan struct{}),
	}
}

func (j *JetstreamConnector) Name() string { return "bluesky" }

func (j *JetstreamConnector) Start(ctx context.Context, out chan<- model.SocialEvent) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-j.stopCh:
			return nil
		default:
		}

		err := j.connectAndConsume(ctx, out)
		if err != nil {
			log.Printf("[bluesky] connection error: %v, reconnecting in 5s...", err)
			j.errorsTotal.Add(1)
			j.mu.Lock()
			j.health.LastError = err.Error()
			j.health.LastErrorTime = time.Now()
			j.mu.Unlock()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (j *JetstreamConnector) connectAndConsume(ctx context.Context, out chan<- model.SocialEvent) error {
	u, err := url.Parse(j.jetstreamURL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}

	q := u.Query()
	for _, c := range j.wantedCollections {
		q.Add("wantedCollections", c)
	}

	// Resume from cursor if available
	cursor, _ := j.cursorStore.GetCursor(ctx, "bluesky")
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	u.RawQuery = q.Encode()

	log.Printf("[bluesky] connecting to %s", u.String())
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	j.mu.Lock()
	j.conn = conn
	j.health.Healthy = true
	j.mu.Unlock()

	defer func() {
		conn.Close()
		j.mu.Lock()
		j.health.Healthy = false
		j.conn = nil
		j.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-j.stopCh:
			return nil
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var evt JetstreamEvent
		if err := json.Unmarshal(message, &evt); err != nil {
			continue
		}

		if evt.Kind != "commit" || evt.Commit == nil || evt.Commit.Operation != "create" {
			continue
		}
		if evt.Commit.Collection != "app.bsky.feed.post" {
			continue
		}

		socialEvent, err := j.normalize(evt)
		if err != nil {
			continue
		}
		// Preserve the original Jetstream frame for the raw archive.
		socialEvent.RawPayload = append([]byte(nil), message...)

		out <- socialEvent
		j.eventsTotal.Add(1)

		j.mu.Lock()
		j.health.LastEvent = time.Now()
		j.health.EventsTotal = j.eventsTotal.Load()
		j.mu.Unlock()

		// Save cursor periodically
		if j.eventsTotal.Load()%500 == 0 {
			cursorVal := fmt.Sprintf("%d", evt.TimeUS)
			j.cursorStore.SetCursor(ctx, "bluesky", cursorVal)
		}
	}
}

func (j *JetstreamConnector) normalize(evt JetstreamEvent) (model.SocialEvent, error) {
	var post PostRecord
	if err := json.Unmarshal(evt.Commit.Record, &post); err != nil {
		return model.SocialEvent{}, err
	}

	publishedAt, _ := time.Parse(time.RFC3339, post.CreatedAt)
	if publishedAt.IsZero() {
		publishedAt = time.UnixMicro(evt.TimeUS)
	}

	postURI := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", evt.Did, evt.Commit.RKey)
	webURL := fmt.Sprintf("https://bsky.app/profile/%s/post/%s", evt.Did, evt.Commit.RKey)

	lang := "unknown"
	if len(post.Langs) > 0 {
		lang = post.Langs[0]
	}

	sourceType := "post"
	var threadID, parentID, rootID string
	if post.Reply != nil {
		sourceType = "reply"
		parentID = post.Reply.Parent.URI
		rootID = post.Reply.Root.URI
		threadID = post.Reply.Root.URI
	} else {
		threadID = postURI
		rootID = postURI
	}

	// Extract URLs from facets
	var urls []string
	for _, f := range post.Facets {
		for _, feat := range f.Features {
			if feat.URI != "" {
				urls = append(urls, feat.URI)
			}
		}
	}

	// Extract media
	var media []model.Media
	if post.Embed != nil && strings.Contains(post.Embed.Type, "image") {
		for _, img := range post.Embed.Images {
			media = append(media, model.Media{Type: "image", Alt: img.Alt})
		}
	}

	return model.SocialEvent{
		EventID:    fmt.Sprintf("bluesky:%s:%s", evt.Did, evt.Commit.RKey),
		Platform:   "bluesky",
		SourceType: sourceType,
		SourceURL:  webURL,
		Author: model.Author{
			ID:     evt.Did,
			Handle: evt.Did, // resolve later via API if needed
		},
		Content: model.Content{
			Text:     post.Text,
			Language: lang,
			Media:    media,
			URLs:     urls,
		},
		Engagement: model.Engagement{},
		Conversation: model.Conversation{
			ThreadID: threadID,
			ParentID: parentID,
			RootID:   rootID,
		},
		Timestamps: model.Timestamps{
			PublishedAt: publishedAt,
		},
	}, nil
}

func (j *JetstreamConnector) Stop() error {
	close(j.stopCh)
	j.mu.Lock()
	if j.conn != nil {
		j.conn.Close()
	}
	j.mu.Unlock()
	return nil
}

func (j *JetstreamConnector) Health() connector.HealthStatus {
	j.mu.RLock()
	defer j.mu.RUnlock()
	h := j.health
	h.ErrorsTotal = j.errorsTotal.Load()
	return h
}
