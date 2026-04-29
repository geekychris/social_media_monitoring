package hackernews

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/socialmonitor/backend/internal/connector"
	"github.com/socialmonitor/backend/internal/model"
)

const (
	hnAPIBase     = "https://hacker-news.firebaseio.com/v0"
	hnAlgoliaBase = "https://hn.algolia.com/api/v1"
)

type HNConnector struct {
	pollInterval time.Duration
	cursorStore  connector.CursorStore
	limiter      *connector.TokenBucketLimiter
	httpClient   *http.Client

	mu          sync.RWMutex
	health      connector.HealthStatus
	stopCh      chan struct{}
	eventsTotal atomic.Int64
	errorsTotal atomic.Int64
}

type HNItem struct {
	ID          int64   `json:"id"`
	Type        string  `json:"type"` // story, comment, job, poll
	By          string  `json:"by"`
	Time        int64   `json:"time"`
	Text        string  `json:"text"`
	Title       string  `json:"title"`
	URL         string  `json:"url"`
	Score       int     `json:"score"`
	Descendants int     `json:"descendants"`
	Kids        []int64 `json:"kids"`
	Parent      int64   `json:"parent"`
	Dead        bool    `json:"dead"`
	Deleted     bool    `json:"deleted"`
}

func NewHNConnector(pollInterval time.Duration, cursorStore connector.CursorStore) *HNConnector {
	return &HNConnector{
		pollInterval: pollInterval,
		cursorStore:  cursorStore,
		limiter:      connector.NewTokenBucketLimiter(5),
		httpClient:   &http.Client{Timeout: 15 * time.Second},
		stopCh:       make(chan struct{}),
	}
}

func (h *HNConnector) Name() string { return "hackernews" }

func (h *HNConnector) Start(ctx context.Context, out chan<- model.SocialEvent) error {
	log.Printf("[hackernews] starting with poll interval %s", h.pollInterval)
	ticker := time.NewTicker(h.pollInterval)
	defer ticker.Stop()

	// Initial poll
	h.poll(ctx, out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.stopCh:
			return nil
		case <-ticker.C:
			h.poll(ctx, out)
		}
	}
}

func (h *HNConnector) poll(ctx context.Context, out chan<- model.SocialEvent) {
	// Get the max item ID
	maxID, err := h.getMaxItemID(ctx)
	if err != nil {
		log.Printf("[hackernews] failed to get max item: %v", err)
		h.errorsTotal.Add(1)
		return
	}

	// Get cursor (last seen ID)
	cursorStr, _ := h.cursorStore.GetCursor(ctx, "hackernews")
	var lastSeen int64
	if cursorStr != "" {
		fmt.Sscanf(cursorStr, "%d", &lastSeen)
	}
	if lastSeen == 0 {
		// On first run, start from recent items
		lastSeen = maxID - 50
	}

	// Fetch new items
	for id := lastSeen + 1; id <= maxID; id++ {
		select {
		case <-ctx.Done():
			return
		case <-h.stopCh:
			return
		default:
		}

		h.limiter.Wait(ctx)
		item, raw, err := h.getItem(ctx, id)
		if err != nil {
			h.errorsTotal.Add(1)
			continue
		}
		if item == nil || item.Deleted || item.Dead {
			continue
		}
		if item.Type != "story" && item.Type != "comment" {
			continue
		}

		event := h.normalize(*item)
		event.RawPayload = raw
		out <- event
		h.eventsTotal.Add(1)

		h.mu.Lock()
		h.health.LastEvent = time.Now()
		h.health.Healthy = true
		h.health.EventsTotal = h.eventsTotal.Load()
		h.mu.Unlock()
	}

	h.cursorStore.SetCursor(ctx, "hackernews", fmt.Sprintf("%d", maxID))
}

func (h *HNConnector) getMaxItemID(ctx context.Context) (int64, error) {
	h.limiter.Wait(ctx)
	resp, err := h.httpClient.Get(fmt.Sprintf("%s/maxitem.json", hnAPIBase))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var maxID int64
	json.NewDecoder(resp.Body).Decode(&maxID)
	return maxID, nil
}

func (h *HNConnector) getItem(ctx context.Context, id int64) (*HNItem, []byte, error) {
	url := fmt.Sprintf("%s/item/%d.json", hnAPIBase, id)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 || string(body) == "null" {
		return nil, nil, nil
	}

	var item HNItem
	if err := json.Unmarshal(body, &item); err != nil {
		return nil, body, err
	}
	return &item, body, nil
}

func (h *HNConnector) normalize(item HNItem) model.SocialEvent {
	sourceType := "post"
	if item.Type == "comment" {
		sourceType = "comment"
	}

	text := item.Text
	if item.Title != "" && item.Text == "" {
		text = item.Title
	}

	sourceURL := fmt.Sprintf("https://news.ycombinator.com/item?id=%d", item.ID)

	parentID := ""
	rootID := fmt.Sprintf("hn:%d", item.ID)
	if item.Parent > 0 {
		parentID = fmt.Sprintf("hn:%d", item.Parent)
	}

	return model.SocialEvent{
		EventID:    fmt.Sprintf("hn:%d", item.ID),
		Platform:   "hackernews",
		SourceType: sourceType,
		SourceURL:  sourceURL,
		Author: model.Author{
			ID:     item.By,
			Handle: item.By,
		},
		Content: model.Content{
			Text:     text,
			Title:    item.Title,
			Language: "en",
			URLs:     filterEmpty([]string{item.URL}),
		},
		Engagement: model.Engagement{
			Score:    int64(item.Score),
			Comments: int64(item.Descendants),
		},
		Conversation: model.Conversation{
			ThreadID: rootID,
			ParentID: parentID,
			RootID:   rootID,
		},
		Timestamps: model.Timestamps{
			PublishedAt: time.Unix(item.Time, 0).UTC(),
		},
	}
}

func (h *HNConnector) Stop() error {
	close(h.stopCh)
	return nil
}

func (h *HNConnector) Health() connector.HealthStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()
	hh := h.health
	hh.ErrorsTotal = h.errorsTotal.Load()
	return hh
}

func filterEmpty(s []string) []string {
	var result []string
	for _, v := range s {
		if v != "" {
			result = append(result, v)
		}
	}
	return result
}
