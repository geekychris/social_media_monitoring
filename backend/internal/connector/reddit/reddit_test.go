package reddit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/socialmonitor/backend/internal/connector"
	"github.com/socialmonitor/backend/internal/model"
)

func TestNormalize(t *testing.T) {
	r := New(Config{}, connector.NewMemoryCursorStore())

	tests := []struct {
		name string
		in   post
		want model.SocialEvent
	}{
		{
			name: "self post",
			in: post{
				ID:                "abc",
				Name:              "t3_abc",
				Subreddit:         "golang",
				SubredditPrefixed: "r/golang",
				Author:            "alice",
				AuthorFullname:    "t2_xyz",
				Title:             "Generics question",
				Selftext:          "  Why is this not allowed?  ",
				Permalink:         "/r/golang/comments/abc/generics_question/",
				IsSelf:            true,
				Score:             42,
				NumComments:       7,
				CreatedUTC:        1714000000,
			},
			want: model.SocialEvent{
				EventID:    "reddit:t3_abc",
				Platform:   "reddit",
				SourceType: "post",
				SourceURL:  "https://www.reddit.com/r/golang/comments/abc/generics_question/",
				Author:     model.Author{ID: "t2_xyz", Handle: "alice"},
				Content: model.Content{
					Title:    "Generics question",
					Text:     "Why is this not allowed?",
					Language: "en",
				},
				Engagement: model.Engagement{Score: 42, Comments: 7},
				Conversation: model.Conversation{
					ThreadID: "reddit:t3_abc",
					RootID:   "reddit:t3_abc",
				},
				Timestamps: model.Timestamps{
					PublishedAt: time.Unix(1714000000, 0).UTC(),
				},
				Enrichment: model.Enrichment{Topics: []string{"r/golang"}},
			},
		},
		{
			name: "link post falls back to URL when selftext empty",
			in: post{
				ID:                "def",
				Name:              "t3_def",
				Subreddit:         "programming",
				SubredditPrefixed: "r/programming",
				Author:            "bob",
				AuthorFullname:    "t2_def",
				Title:             "Cool article",
				URL:               "https://example.com/article",
				Permalink:         "/r/programming/comments/def/cool_article/",
				IsSelf:            false,
				Score:             10,
				CreatedUTC:        1714000100,
			},
			want: model.SocialEvent{
				EventID:    "reddit:t3_def",
				Platform:   "reddit",
				SourceType: "post",
				SourceURL:  "https://www.reddit.com/r/programming/comments/def/cool_article/",
				Author:     model.Author{ID: "t2_def", Handle: "bob"},
				Content: model.Content{
					Title:    "Cool article",
					Text:     "https://example.com/article",
					Language: "en",
					URLs:     []string{"https://example.com/article"},
				},
				Engagement: model.Engagement{Score: 10},
				Conversation: model.Conversation{
					ThreadID: "reddit:t3_def",
					RootID:   "reddit:t3_def",
				},
				Timestamps: model.Timestamps{
					PublishedAt: time.Unix(1714000100, 0).UTC(),
				},
				Enrichment: model.Enrichment{Topics: []string{"r/programming"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.normalize(tt.in)
			assertEvent(t, got, tt.want)
		})
	}
}

func TestPollOne_AdvancesCursorAndEmitsChronologically(t *testing.T) {
	// Reddit returns newest-first; the connector reverses that so events
	// arrive in chronological order and the cursor lands on the newest item.
	posts := []post{
		{Name: "t3_new", Title: "newer", CreatedUTC: 200, IsSelf: true, Selftext: "n", Permalink: "/r/x/comments/new/"},
		{Name: "t3_old", Title: "older", CreatedUTC: 100, IsSelf: true, Selftext: "o", Permalink: "/r/x/comments/old/"},
	}
	body := listing{}
	for _, p := range posts {
		body.Data.Children = append(body.Data.Children, child{Kind: "t3", Data: p})
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	cs := connector.NewMemoryCursorStore()
	r := New(Config{
		Subreddits:   []string{"x"},
		PollInterval: time.Hour,
		PageLimit:    25,
		BaseURL:      srv.URL,
	}, cs)

	out := make(chan model.SocialEvent, 4)
	if err := r.pollOne(context.Background(), "x", out); err != nil {
		t.Fatalf("pollOne: %v", err)
	}
	close(out)

	var got []string
	for e := range out {
		got = append(got, e.EventID)
	}
	want := []string{"reddit:t3_old", "reddit:t3_new"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("emit order = %v, want %v", got, want)
	}

	cur, _ := cs.GetCursor(context.Background(), "reddit:r/x")
	if cur != "t3_new" {
		t.Errorf("cursor after poll = %q, want %q", cur, "t3_new")
	}
}

func TestPollOne_SkipsRemovedAndNonPosts(t *testing.T) {
	body := listing{}
	body.Data.Children = []child{
		{Kind: "t1", Data: post{Name: "t1_comment"}}, // not a post
		{Kind: "t3", Data: post{Name: "t3_removed", RemovedByCategory: "moderator"}},
		{Kind: "t3", Data: post{Name: "t3_ok", Title: "ok", IsSelf: true, Selftext: "x", CreatedUTC: 1, Permalink: "/r/x/comments/ok/"}},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	cs := connector.NewMemoryCursorStore()
	r := New(Config{Subreddits: []string{"x"}, PollInterval: time.Hour, BaseURL: srv.URL}, cs)

	out := make(chan model.SocialEvent, 4)
	if err := r.pollOne(context.Background(), "x", out); err != nil {
		t.Fatalf("pollOne: %v", err)
	}
	close(out)

	count := 0
	for e := range out {
		count++
		if e.EventID != "reddit:t3_ok" {
			t.Errorf("unexpected event %q emitted", e.EventID)
		}
	}
	if count != 1 {
		t.Errorf("emitted %d events, want 1 (only the live post)", count)
	}
}

func assertEvent(t *testing.T, got, want model.SocialEvent) {
	t.Helper()
	if got.EventID != want.EventID {
		t.Errorf("EventID = %q, want %q", got.EventID, want.EventID)
	}
	if got.Platform != want.Platform {
		t.Errorf("Platform = %q, want %q", got.Platform, want.Platform)
	}
	if got.SourceType != want.SourceType {
		t.Errorf("SourceType = %q, want %q", got.SourceType, want.SourceType)
	}
	if got.SourceURL != want.SourceURL {
		t.Errorf("SourceURL = %q, want %q", got.SourceURL, want.SourceURL)
	}
	if got.Author != want.Author {
		t.Errorf("Author = %+v, want %+v", got.Author, want.Author)
	}
	if got.Content.Title != want.Content.Title {
		t.Errorf("Content.Title = %q, want %q", got.Content.Title, want.Content.Title)
	}
	if got.Content.Text != want.Content.Text {
		t.Errorf("Content.Text = %q, want %q", got.Content.Text, want.Content.Text)
	}
	if got.Content.Language != want.Content.Language {
		t.Errorf("Content.Language = %q, want %q", got.Content.Language, want.Content.Language)
	}
	if !equalStrings(got.Content.URLs, want.Content.URLs) {
		t.Errorf("Content.URLs = %v, want %v", got.Content.URLs, want.Content.URLs)
	}
	if got.Engagement != want.Engagement {
		t.Errorf("Engagement = %+v, want %+v", got.Engagement, want.Engagement)
	}
	if got.Conversation != want.Conversation {
		t.Errorf("Conversation = %+v, want %+v", got.Conversation, want.Conversation)
	}
	if !got.Timestamps.PublishedAt.Equal(want.Timestamps.PublishedAt) {
		t.Errorf("PublishedAt = %v, want %v", got.Timestamps.PublishedAt, want.Timestamps.PublishedAt)
	}
	if !equalStrings(got.Enrichment.Topics, want.Enrichment.Topics) {
		t.Errorf("Enrichment.Topics = %v, want %v", got.Enrichment.Topics, want.Enrichment.Topics)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
