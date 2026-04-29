package mastodon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/socialmonitor/backend/internal/connector"
	"github.com/socialmonitor/backend/internal/model"
)

func TestParseFeeds(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []Feed
		wantErr bool
	}{
		{"empty", "", nil, false},
		{"one public", "mastodon.social:public", []Feed{{Instance: "mastodon.social", Type: "public"}}, false},
		{"public + tag", "mastodon.social:public, fosstodon.org:tag:golang",
			[]Feed{{Instance: "mastodon.social", Type: "public"}, {Instance: "fosstodon.org", Type: "tag", Tag: "golang"}}, false},
		{"tag missing tag", "mastodon.social:tag", nil, true},
		{"missing type", "mastodon.social", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFeeds(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr=%v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseFeeds(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	m := New(Config{}, connector.NewMemoryCursorStore())

	s := status{
		ID:              "12345",
		URL:             "https://mastodon.social/@alice/12345",
		CreatedAt:       time.Date(2026, 4, 29, 10, 0, 0, 0, time.UTC),
		Content:         "<p>Hello <a>world</a></p>",
		Language:        "en",
		FavouritesCount: 7,
		ReblogsCount:    2,
		RepliesCount:    1,
		Account: account{
			ID:             "1",
			Username:       "alice",
			Acct:           "alice@mastodon.social",
			DisplayName:    "Alice",
			FollowersCount: 500,
		},
		Tags: []tag{{Name: "go"}, {Name: ""}},
		MediaAttachments: []mediaAttach{
			{Type: "image", URL: "https://cdn.example/1.png", Description: "alt"},
		},
	}

	got := m.normalize(s, "mastodon.social")

	if got.EventID != "mastodon:mastodon.social:12345" {
		t.Errorf("EventID = %q", got.EventID)
	}
	if got.SourceType != "post" {
		t.Errorf("SourceType = %q, want post", got.SourceType)
	}
	if got.Content.Text != "Hello world" {
		t.Errorf("Content.Text = %q", got.Content.Text)
	}
	if got.Author.Handle != "alice@mastodon.social" || got.Author.DisplayName != "Alice" || got.Author.FollowerCount != 500 {
		t.Errorf("Author = %+v", got.Author)
	}
	if got.Engagement.Likes != 7 || got.Engagement.Shares != 2 || got.Engagement.Comments != 1 {
		t.Errorf("Engagement = %+v", got.Engagement)
	}
	if len(got.Content.Media) != 1 || got.Content.Media[0].URL != "https://cdn.example/1.png" {
		t.Errorf("Media = %+v", got.Content.Media)
	}
	if !reflect.DeepEqual(got.Enrichment.Topics, []string{"go"}) {
		t.Errorf("Topics = %v", got.Enrichment.Topics)
	}
	if got.Conversation.ThreadID != "mastodon:mastodon.social:12345" {
		t.Errorf("ThreadID = %q", got.Conversation.ThreadID)
	}
}

func TestNormalize_Reply(t *testing.T) {
	m := New(Config{}, connector.NewMemoryCursorStore())
	s := status{
		ID:          "5",
		InReplyToID: "4",
		CreatedAt:   time.Now().UTC(),
		Content:     "<p>reply</p>",
		Account:     account{ID: "1", Acct: "a"},
	}
	got := m.normalize(s, "mastodon.social")
	if got.SourceType != "reply" {
		t.Errorf("SourceType = %q, want reply", got.SourceType)
	}
	if got.Conversation.ParentID != "mastodon:mastodon.social:4" {
		t.Errorf("ParentID = %q", got.Conversation.ParentID)
	}
	if got.Conversation.ThreadID != "mastodon:mastodon.social:4" {
		t.Errorf("ThreadID = %q", got.Conversation.ThreadID)
	}
}

func TestPollOne_AdvancesCursor(t *testing.T) {
	statuses := []status{
		{ID: "20", CreatedAt: time.Now().UTC(), Content: "<p>newer</p>", Account: account{ID: "1", Acct: "a"}},
		{ID: "10", CreatedAt: time.Now().UTC(), Content: "<p>older</p>", Account: account{ID: "1", Acct: "a"}},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(statuses)
	}))
	defer srv.Close()

	cs := connector.NewMemoryCursorStore()
	m := New(Config{Feeds: []Feed{{Instance: "x", Type: "public"}}, PollInterval: time.Hour}, cs)
	m.httpClient.Transport = redirectTransport{target: srv.URL}

	out := make(chan model.SocialEvent, 4)
	if err := m.pollOne(context.Background(), Feed{Instance: "x", Type: "public"}, out); err != nil {
		t.Fatalf("pollOne: %v", err)
	}
	close(out)

	var got []string
	for e := range out {
		got = append(got, e.EventID)
	}
	want := []string{"mastodon:x:10", "mastodon:x:20"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("emit order = %v, want %v", got, want)
	}

	cur, _ := cs.GetCursor(context.Background(), "mastodon:x:public")
	if cur != "20" {
		t.Errorf("cursor = %q, want 20", cur)
	}
}

type redirectTransport struct{ target string }

func (rt redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	req2.URL.Scheme = "http"
	req2.URL.Host = stripScheme(rt.target)
	req2.Host = req2.URL.Host
	return http.DefaultTransport.RoundTrip(req2)
}

func stripScheme(s string) string {
	for _, p := range []string{"http://", "https://"} {
		if len(s) > len(p) && s[:len(p)] == p {
			return s[len(p):]
		}
	}
	return s
}
