package discourse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/socialmonitor/backend/internal/connector"
	"github.com/socialmonitor/backend/internal/model"
)

func TestTagListUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"null", `null`, nil},
		{"strings", `["a","b"]`, []string{"a", "b"}},
		{"objects", `[{"name":"a"},{"name":"b"}]`, []string{"a", "b"}},
		{"objects with extras", `[{"name":"a","id":1}]`, []string{"a"}},
		{"mixed objects skips empty", `[{"name":""},{"name":"b"}]`, []string{"b"}},
		{"empty array", `[]`, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got tagList
			if err := json.Unmarshal([]byte(tt.in), &got); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual([]string(got), tt.want) {
				t.Errorf("= %v, want %v", []string(got), tt.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	d := New(Config{}, connector.NewMemoryCursorStore())
	base, _ := url.Parse("https://meta.discourse.org")

	users := map[int64]userRef{
		1: {ID: 1, Username: "alice", Name: "Alice"},
		2: {ID: 2, Username: "bob", Name: "Bob"},
	}
	tp := topic{
		ID:         42,
		Title:      "How do I X?",
		Slug:       "how-do-i-x",
		ReplyCount: 5,
		Views:      100,
		LikeCount:  3,
		Visible:    true,
		CreatedAt:  time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC),
		Excerpt:    "<p>Help, I am stuck.</p>",
		Tags:       []string{"howto", "support"},
		Posters: []poster{
			{UserID: 2, Description: "Most Recent Poster"},
			{UserID: 1, Description: "Original Poster"},
		},
	}

	got := d.normalize(tp, base, users)

	want := model.SocialEvent{
		EventID:    "discourse:meta.discourse.org:42",
		Platform:   "discourse",
		SourceType: "post",
		SourceURL:  "https://meta.discourse.org/t/how-do-i-x/42",
		Author:     model.Author{ID: "1", Handle: "alice", DisplayName: "Alice"},
		Content: model.Content{
			Title:    "How do I X?",
			Text:     "Help, I am stuck.",
			Language: "en",
		},
		Engagement: model.Engagement{Likes: 3, Comments: 5, Views: 100},
		Conversation: model.Conversation{
			ThreadID: "discourse:meta.discourse.org:42",
			RootID:   "discourse:meta.discourse.org:42",
		},
		Timestamps: model.Timestamps{PublishedAt: time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)},
		Enrichment: model.Enrichment{Topics: []string{"howto", "support"}},
	}

	if got.EventID != want.EventID {
		t.Errorf("EventID = %q, want %q", got.EventID, want.EventID)
	}
	if got.SourceURL != want.SourceURL {
		t.Errorf("SourceURL = %q, want %q", got.SourceURL, want.SourceURL)
	}
	if got.Author != want.Author {
		t.Errorf("Author = %+v, want %+v", got.Author, want.Author)
	}
	if got.Content.Text != want.Content.Text {
		t.Errorf("Text = %q, want %q", got.Content.Text, want.Content.Text)
	}
	if got.Engagement != want.Engagement {
		t.Errorf("Engagement = %+v, want %+v", got.Engagement, want.Engagement)
	}
	if !reflect.DeepEqual(got.Enrichment.Topics, want.Enrichment.Topics) {
		t.Errorf("Topics = %v, want %v", got.Enrichment.Topics, want.Enrichment.Topics)
	}
	if !got.Timestamps.PublishedAt.Equal(want.Timestamps.PublishedAt) {
		t.Errorf("PublishedAt = %v, want %v", got.Timestamps.PublishedAt, want.Timestamps.PublishedAt)
	}
}

func TestPollOne_FiltersByCursorAndAdvances(t *testing.T) {
	resp := latestResponse{
		Users: []userRef{{ID: 1, Username: "alice"}},
	}
	resp.TopicList.Topics = []topic{
		{ID: 100, Title: "newest", Slug: "newest", Visible: true, CreatedAt: time.Now().UTC(), Posters: []poster{{UserID: 1}}},
		{ID: 50, Title: "old, already-seen", Slug: "old", Visible: true, CreatedAt: time.Now().UTC(), Posters: []poster{{UserID: 1}}},
		{ID: 75, Title: "middle", Slug: "middle", Visible: true, CreatedAt: time.Now().UTC(), Posters: []poster{{UserID: 1}}},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cs := connector.NewMemoryCursorStore()
	// Pretend we already saw topic 50.
	_ = cs.SetCursor(context.Background(), "discourse:"+stripScheme(srv.URL), "50")

	d := New(Config{Forums: []string{srv.URL}, PollInterval: time.Hour}, cs)

	out := make(chan model.SocialEvent, 4)
	if err := d.pollOne(context.Background(), srv.URL, out); err != nil {
		t.Fatalf("pollOne: %v", err)
	}
	close(out)

	var ids []string
	for e := range out {
		ids = append(ids, e.EventID)
	}
	host := stripScheme(srv.URL)
	want := []string{"discourse:" + host + ":75", "discourse:" + host + ":100"}
	if !reflect.DeepEqual(ids, want) {
		t.Errorf("emitted = %v, want %v", ids, want)
	}

	cur, _ := cs.GetCursor(context.Background(), "discourse:"+host)
	if cur != "100" {
		t.Errorf("cursor = %q, want 100", cur)
	}
}

func TestPollOne_SkipsInvisible(t *testing.T) {
	resp := latestResponse{}
	resp.TopicList.Topics = []topic{
		{ID: 1, Visible: false, Title: "hidden"},
		{ID: 2, Visible: true, Title: "shown", Slug: "shown", Posters: []poster{{UserID: 1}}, CreatedAt: time.Now().UTC()},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cs := connector.NewMemoryCursorStore()
	d := New(Config{Forums: []string{srv.URL}, PollInterval: time.Hour}, cs)

	out := make(chan model.SocialEvent, 4)
	if err := d.pollOne(context.Background(), srv.URL, out); err != nil {
		t.Fatalf("pollOne: %v", err)
	}
	close(out)

	count := 0
	for range out {
		count++
	}
	if count != 1 {
		t.Errorf("emitted %d, want 1 (invisible should be filtered)", count)
	}
}

func stripScheme(s string) string {
	for _, p := range []string{"http://", "https://"} {
		if len(s) > len(p) && s[:len(p)] == p {
			return s[len(p):]
		}
	}
	return s
}
