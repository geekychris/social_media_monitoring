package rss

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
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

type RSSConnector struct {
	feeds        []string
	pollInterval time.Duration
	cursorStore  connector.CursorStore
	httpClient   *http.Client

	mu          sync.RWMutex
	health      connector.HealthStatus
	stopCh      chan struct{}
	eventsTotal atomic.Int64
	errorsTotal atomic.Int64
}

// RSS/Atom feed structures
type RSSFeed struct {
	XMLName xml.Name   `xml:"rss"`
	Channel RSSChannel `xml:"channel"`
}

type RSSChannel struct {
	Title string    `xml:"title"`
	Link  string    `xml:"link"`
	Items []RSSItem `xml:"item"`
}

type RSSItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	Author      string `xml:"author"`
	Creator     string `xml:"creator"`
	PubDate     string `xml:"pubDate"`
	GUID        string `xml:"guid"`
	Categories  []string `xml:"category"`
}

type AtomFeed struct {
	XMLName xml.Name    `xml:"feed"`
	Title   string      `xml:"title"`
	Entries []AtomEntry `xml:"entry"`
}

type AtomEntry struct {
	Title     string     `xml:"title"`
	Link      AtomLink   `xml:"link"`
	ID        string     `xml:"id"`
	Content   string     `xml:"content"`
	Summary   string     `xml:"summary"`
	Published string     `xml:"published"`
	Updated   string     `xml:"updated"`
	Author    AtomAuthor `xml:"author"`
	Categories []AtomCategory `xml:"category"`
}

type AtomLink struct {
	Href string `xml:"href,attr"`
}

type AtomAuthor struct {
	Name string `xml:"name"`
}

type AtomCategory struct {
	Term string `xml:"term,attr"`
}

func NewRSSConnector(feeds []string, pollInterval time.Duration, cursorStore connector.CursorStore) *RSSConnector {
	return &RSSConnector{
		feeds:        feeds,
		pollInterval: pollInterval,
		cursorStore:  cursorStore,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		stopCh:       make(chan struct{}),
	}
}

func (r *RSSConnector) Name() string { return "rss" }

func (r *RSSConnector) Start(ctx context.Context, out chan<- model.SocialEvent) error {
	log.Printf("[rss] starting with %d feeds, poll interval %s", len(r.feeds), r.pollInterval)
	ticker := time.NewTicker(r.pollInterval)
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

func (r *RSSConnector) pollAll(ctx context.Context, out chan<- model.SocialEvent) {
	for _, feedURL := range r.feeds {
		select {
		case <-ctx.Done():
			return
		default:
		}
		r.pollFeed(ctx, feedURL, out)
	}
}

func (r *RSSConnector) pollFeed(ctx context.Context, feedURL string, out chan<- model.SocialEvent) {
	req, err := http.NewRequestWithContext(ctx, "GET", feedURL, nil)
	if err != nil {
		r.errorsTotal.Add(1)
		return
	}
	req.Header.Set("User-Agent", "SocialMonitor/1.0")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		log.Printf("[rss] fetch error for %s: %v", feedURL, err)
		r.errorsTotal.Add(1)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Try RSS first, then Atom
	var rssFeed RSSFeed
	if err := xml.Unmarshal(body, &rssFeed); err == nil && len(rssFeed.Channel.Items) > 0 {
		for _, item := range rssFeed.Channel.Items {
			event := r.normalizeRSSItem(item, rssFeed.Channel.Title, feedURL)
			out <- event
			r.eventsTotal.Add(1)
		}
	} else {
		var atomFeed AtomFeed
		if err := xml.Unmarshal(body, &atomFeed); err == nil {
			for _, entry := range atomFeed.Entries {
				event := r.normalizeAtomEntry(entry, atomFeed.Title, feedURL)
				out <- event
				r.eventsTotal.Add(1)
			}
		} else {
			log.Printf("[rss] parse error for %s", feedURL)
			r.errorsTotal.Add(1)
		}
	}

	r.mu.Lock()
	r.health.LastEvent = time.Now()
	r.health.Healthy = true
	r.health.EventsTotal = r.eventsTotal.Load()
	r.mu.Unlock()
}

func (r *RSSConnector) normalizeRSSItem(item RSSItem, feedTitle, feedURL string) model.SocialEvent {
	guid := item.GUID
	if guid == "" {
		guid = item.Link
	}
	eventID := fmt.Sprintf("rss:%x", sha256.Sum256([]byte(guid)))

	pubTime := parseDate(item.PubDate)
	author := item.Author
	if author == "" {
		author = item.Creator
	}
	if author == "" {
		author = feedTitle
	}

	return model.SocialEvent{
		EventID:    eventID,
		Platform:   "rss",
		SourceType: "article",
		SourceURL:  item.Link,
		Author: model.Author{
			Handle:      author,
			DisplayName: author,
		},
		Content: model.Content{
			Title:    item.Title,
			Text:     stripHTML(item.Description),
			Language: "en",
			URLs:     filterEmpty([]string{item.Link}),
		},
		Conversation: model.Conversation{
			ThreadID: eventID,
			RootID:   eventID,
		},
		Timestamps: model.Timestamps{
			PublishedAt: pubTime,
		},
		Enrichment: model.Enrichment{
			Topics: item.Categories,
		},
	}
}

func (r *RSSConnector) normalizeAtomEntry(entry AtomEntry, feedTitle, feedURL string) model.SocialEvent {
	id := entry.ID
	if id == "" {
		id = entry.Link.Href
	}
	eventID := fmt.Sprintf("atom:%x", sha256.Sum256([]byte(id)))

	pubTime := parseDate(entry.Published)
	if pubTime.IsZero() {
		pubTime = parseDate(entry.Updated)
	}

	text := entry.Content
	if text == "" {
		text = entry.Summary
	}

	author := entry.Author.Name
	if author == "" {
		author = feedTitle
	}

	var categories []string
	for _, c := range entry.Categories {
		if c.Term != "" {
			categories = append(categories, c.Term)
		}
	}

	return model.SocialEvent{
		EventID:    eventID,
		Platform:   "rss",
		SourceType: "article",
		SourceURL:  entry.Link.Href,
		Author: model.Author{
			Handle:      author,
			DisplayName: author,
		},
		Content: model.Content{
			Title:    entry.Title,
			Text:     stripHTML(text),
			Language: "en",
			URLs:     filterEmpty([]string{entry.Link.Href}),
		},
		Conversation: model.Conversation{
			ThreadID: eventID,
			RootID:   eventID,
		},
		Timestamps: model.Timestamps{
			PublishedAt: pubTime,
		},
		Enrichment: model.Enrichment{
			Topics: categories,
		},
	}
}

func (r *RSSConnector) Stop() error {
	close(r.stopCh)
	return nil
}

func (r *RSSConnector) Health() connector.HealthStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h := r.health
	h.ErrorsTotal = r.errorsTotal.Load()
	return h
}

func parseDate(s string) time.Time {
	formats := []string{
		time.RFC1123Z,
		time.RFC1123,
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05-07:00",
		"Mon, 02 Jan 2006 15:04:05 MST",
		"02 Jan 2006 15:04:05 -0700",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func stripHTML(s string) string {
	// Simple HTML tag removal
	result := make([]byte, 0, len(s))
	inTag := false
	for i := 0; i < len(s); i++ {
		if s[i] == '<' {
			inTag = true
			continue
		}
		if s[i] == '>' {
			inTag = false
			continue
		}
		if !inTag {
			result = append(result, s[i])
		}
	}
	return string(result)
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
