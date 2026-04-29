package enrichment

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/socialmonitor/backend/internal/model"
)

func TestHTTPEnricher_MergesWellKnownAndExtra(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", req.Method)
		}
		if ct := req.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}
		var got EnrichRequest
		if err := json.NewDecoder(req.Body).Decode(&got); err != nil {
			t.Fatalf("decode req: %v", err)
		}
		if got.EventID != "evt-1" || got.Text != "Chris loves the iPhone." {
			t.Errorf("unexpected request: %+v", got)
		}
		_, _ = w.Write([]byte(`{
			"sentiment": "positive",
			"entities": ["iPhone"],
			"entity_sentiments": [
				{"entity": "iPhone", "sentiment": "positive", "confidence": 0.9, "holder": "Chris"}
			]
		}`))
	}))
	defer srv.Close()

	e := NewHTTPEnricher(HTTPEnricherConfig{Name: "elsa", URL: srv.URL})
	evt := &model.SocialEvent{
		EventID:  "evt-1",
		Platform: "test",
		Content:  model.Content{Text: "Chris loves the iPhone.", Language: "en"},
		Enrichment: model.Enrichment{
			Topics: []string{"existing-topic"},
		},
	}

	if err := e.Enrich(context.Background(), evt); err != nil {
		t.Fatalf("Enrich: %v", err)
	}

	if evt.Enrichment.Sentiment != "positive" {
		t.Errorf("sentiment = %q", evt.Enrichment.Sentiment)
	}
	if !reflect.DeepEqual(evt.Enrichment.Entities, []string{"iPhone"}) {
		t.Errorf("entities = %v", evt.Enrichment.Entities)
	}
	// Pre-existing topics preserved when service didn't return any.
	if !reflect.DeepEqual(evt.Enrichment.Topics, []string{"existing-topic"}) {
		t.Errorf("topics = %v", evt.Enrichment.Topics)
	}

	raw, ok := evt.Enrichment.Extra["elsa"]
	if !ok {
		t.Fatal("Extra[elsa] missing")
	}
	var passthrough struct {
		EntitySentiments []struct {
			Entity     string  `json:"entity"`
			Sentiment  string  `json:"sentiment"`
			Confidence float64 `json:"confidence"`
			Holder     string  `json:"holder"`
		} `json:"entity_sentiments"`
	}
	if err := json.Unmarshal(raw, &passthrough); err != nil {
		t.Fatalf("Extra[elsa] not valid JSON: %v", err)
	}
	if len(passthrough.EntitySentiments) != 1 || passthrough.EntitySentiments[0].Holder != "Chris" {
		t.Errorf("passthrough = %+v", passthrough)
	}
}

func TestHTTPEnricher_NoTextSkipsCall(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	}))
	defer srv.Close()

	e := NewHTTPEnricher(HTTPEnricherConfig{Name: "x", URL: srv.URL})
	evt := &model.SocialEvent{EventID: "evt-empty"}

	if err := e.Enrich(context.Background(), evt); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if called {
		t.Error("enricher should not POST when there is no text or title")
	}
}

func TestHTTPEnricher_Non2xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"err":"down"}`)
	}))
	defer srv.Close()

	e := NewHTTPEnricher(HTTPEnricherConfig{Name: "x", URL: srv.URL})
	evt := &model.SocialEvent{Content: model.Content{Text: "hi"}}
	err := e.Enrich(context.Background(), evt)
	if err == nil {
		t.Fatal("expected error on 5xx")
	}
}

func TestChain_SwallowsEnricherErrors(t *testing.T) {
	good := &fakeEnricher{name: "good", fn: func(_ context.Context, e *model.SocialEvent) error {
		e.Enrichment.Sentiment = "neutral"
		return nil
	}}
	bad := &fakeEnricher{name: "bad", fn: func(context.Context, *model.SocialEvent) error {
		return errors.New("boom")
	}}
	gotLog := ""
	chain := NewChain([]Enricher{good, bad, good}, func(format string, args ...any) {
		gotLog = format
	})

	evt := &model.SocialEvent{}
	chain.Run(context.Background(), evt)

	if evt.Enrichment.Sentiment != "neutral" {
		t.Errorf("good enricher should have run; sentiment = %q", evt.Enrichment.Sentiment)
	}
	if gotLog == "" {
		t.Error("error should have been logged")
	}

	stats := chain.Stats()
	if len(stats) != 3 {
		t.Fatalf("len(stats) = %d, want 3", len(stats))
	}
	if stats[0].Errors != 0 || stats[1].Errors != 1 || stats[2].Errors != 0 {
		t.Errorf("error counts = %v %v %v", stats[0].Errors, stats[1].Errors, stats[2].Errors)
	}
	if stats[0].Calls != 1 || stats[1].Calls != 1 || stats[2].Calls != 1 {
		t.Errorf("call counts = %v %v %v", stats[0].Calls, stats[1].Calls, stats[2].Calls)
	}
	if stats[1].LastErr == "" {
		t.Errorf("LastErr should be populated for bad enricher")
	}
}

func TestMergeUnique(t *testing.T) {
	got := mergeUnique([]string{"a", "b"}, []string{"b", "c"})
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("= %v, want %v", got, want)
	}
}

type fakeEnricher struct {
	name string
	fn   func(context.Context, *model.SocialEvent) error
}

func (f *fakeEnricher) Name() string { return f.name }
func (f *fakeEnricher) Enrich(ctx context.Context, e *model.SocialEvent) error {
	if f.fn != nil {
		return f.fn(ctx, e)
	}
	return nil
}

