package model

import (
	"encoding/json"
	"time"
)

// SocialEvent is the canonical event schema for all social media content.
type SocialEvent struct {
	EventID    string    `json:"event_id"`
	Platform   string    `json:"platform"`
	SourceType string    `json:"source_type"` // post, comment, reply, article, review
	SourceURL  string    `json:"source_url"`
	Author     Author    `json:"author"`
	Content    Content   `json:"content"`
	Engagement Engagement `json:"engagement"`
	Conversation Conversation `json:"conversation"`
	Timestamps Timestamps `json:"timestamps"`
	Enrichment Enrichment `json:"enrichment"`
	RawRef     RawRef    `json:"raw_ref"`

	// RawPayload is the original platform payload bytes. It transits
	// through the ingestion pipeline so the archive can persist it, then
	// is cleared by the pipeline before indexing — never stored in
	// OpenSearch or returned by the API. Connectors that have access to
	// the raw bytes should populate this; ones that don't may leave it nil.
	RawPayload []byte `json:"-"`
}

type Author struct {
	ID            string `json:"id"`
	Handle        string `json:"handle"`
	DisplayName   string `json:"display_name"`
	Verified      bool   `json:"verified"`
	FollowerCount int64  `json:"follower_count"`
}

type Content struct {
	Text     string   `json:"text"`
	Title    string   `json:"title,omitempty"`
	Language string   `json:"language"`
	Media    []Media  `json:"media,omitempty"`
	URLs     []string `json:"urls,omitempty"`
}

type Media struct {
	Type string `json:"type"` // image, video, link
	URL  string `json:"url"`
	Alt  string `json:"alt,omitempty"`
}

type Engagement struct {
	Likes    int64 `json:"likes"`
	Shares   int64 `json:"shares"`
	Comments int64 `json:"comments"`
	Views    int64 `json:"views"`
	Score    int64 `json:"score"`
}

type Conversation struct {
	ThreadID string `json:"thread_id"`
	ParentID string `json:"parent_id,omitempty"`
	RootID   string `json:"root_id"`
}

type Timestamps struct {
	PublishedAt time.Time `json:"published_at"`
	IngestedAt  time.Time `json:"ingested_at"`
}

type Enrichment struct {
	Entities    []string `json:"entities,omitempty"`
	Sentiment   string   `json:"sentiment,omitempty"`   // positive, neutral, negative
	Intent      string   `json:"intent,omitempty"`      // complaint, question, praise, etc.
	Topics      []string `json:"topics,omitempty"`
	EmbeddingID string   `json:"embedding_id,omitempty"`

	// Extra holds plugin-specific enrichment output keyed by enricher name.
	// Each value is a raw JSON message so plugins can emit arbitrary
	// structured data (entity-level sentiment, embeddings, classifier
	// confidences, etc.) without forcing a schema change. The map is
	// indexed into OpenSearch as a dynamic object, so individual fields
	// remain queryable.
	Extra map[string]json.RawMessage `json:"extra,omitempty"`
}

type RawRef struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}
