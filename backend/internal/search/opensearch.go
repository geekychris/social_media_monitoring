package search

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/socialmonitor/backend/internal/model"
)

type OpenSearchClient struct {
	baseURL    string
	index      string
	httpClient *http.Client
	username   string
	password   string
}

func NewOpenSearchClient(url, index, username, password string) *OpenSearchClient {
	return &OpenSearchClient{
		baseURL: strings.TrimRight(url, "/"),
		index:   index,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
		username: username,
		password: password,
	}
}

func (c *OpenSearchClient) EnsureIndex(ctx context.Context) error {
	mapping := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
		"mappings": map[string]any{
			"properties": map[string]any{
				"event_id":    map[string]any{"type": "keyword"},
				"platform":    map[string]any{"type": "keyword"},
				"source_type": map[string]any{"type": "keyword"},
				"source_url":  map[string]any{"type": "keyword"},
				"author": map[string]any{
					"properties": map[string]any{
						"id":             map[string]any{"type": "keyword"},
						"handle":         map[string]any{"type": "keyword"},
						"display_name":   map[string]any{"type": "text"},
						"verified":       map[string]any{"type": "boolean"},
						"follower_count": map[string]any{"type": "long"},
					},
				},
				"content": map[string]any{
					"properties": map[string]any{
						"text":     map[string]any{"type": "text", "analyzer": "standard"},
						"title":    map[string]any{"type": "text", "analyzer": "standard"},
						"language": map[string]any{"type": "keyword"},
					},
				},
				"engagement": map[string]any{
					"properties": map[string]any{
						"likes":    map[string]any{"type": "long"},
						"shares":   map[string]any{"type": "long"},
						"comments": map[string]any{"type": "long"},
						"views":    map[string]any{"type": "long"},
						"score":    map[string]any{"type": "long"},
					},
				},
				"timestamps": map[string]any{
					"properties": map[string]any{
						"published_at": map[string]any{"type": "date"},
						"ingested_at":  map[string]any{"type": "date"},
					},
				},
				"enrichment": map[string]any{
					"properties": map[string]any{
						"entities":  map[string]any{"type": "keyword"},
						"sentiment": map[string]any{"type": "keyword"},
						"intent":    map[string]any{"type": "keyword"},
						"topics":    map[string]any{"type": "keyword"},
					},
				},
			},
		},
	}

	body, _ := json.Marshal(mapping)
	req, err := http.NewRequestWithContext(ctx, "PUT", fmt.Sprintf("%s/%s", c.baseURL, c.index), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opensearch create index: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 400 {
		// Index already exists
		return nil
	}
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("opensearch create index: status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *OpenSearchClient) IndexEvent(ctx context.Context, event model.SocialEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s/_doc/%s", c.baseURL, c.index, event.EventID)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opensearch index: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("opensearch index: status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

type SearchParams struct {
	Query     string   `json:"query"`
	Platforms []string `json:"platforms,omitempty"`
	Sentiment string   `json:"sentiment,omitempty"`
	StartDate string   `json:"start_date,omitempty"`
	EndDate   string   `json:"end_date,omitempty"`
	SortBy    string   `json:"sort_by,omitempty"` // relevance, date, engagement
	From      int      `json:"from"`
	Size      int      `json:"size"`
}

type SearchResult struct {
	Total  int64               `json:"total"`
	Events []model.SocialEvent `json:"events"`
}

func (c *OpenSearchClient) Search(ctx context.Context, params SearchParams) (*SearchResult, error) {
	if params.Size == 0 {
		params.Size = 20
	}

	must := []any{}
	if params.Query != "" {
		must = append(must, map[string]any{
			"multi_match": map[string]any{
				"query":  params.Query,
				"fields": []string{"content.text", "content.title", "author.handle", "author.display_name"},
			},
		})
	}

	filter := []any{}
	if len(params.Platforms) > 0 {
		filter = append(filter, map[string]any{
			"terms": map[string]any{"platform": params.Platforms},
		})
	}
	if params.Sentiment != "" {
		filter = append(filter, map[string]any{
			"term": map[string]any{"enrichment.sentiment": params.Sentiment},
		})
	}
	if params.StartDate != "" || params.EndDate != "" {
		dateRange := map[string]any{}
		if params.StartDate != "" {
			dateRange["gte"] = params.StartDate
		}
		if params.EndDate != "" {
			dateRange["lte"] = params.EndDate
		}
		filter = append(filter, map[string]any{
			"range": map[string]any{"timestamps.published_at": dateRange},
		})
	}

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must":   must,
				"filter": filter,
			},
		},
		"from": params.From,
		"size": params.Size,
		"sort": c.buildSort(params.SortBy),
	}

	if len(must) == 0 && len(filter) == 0 {
		query["query"] = map[string]any{"match_all": map[string]any{}}
	}

	body, _ := json.Marshal(query)
	url := fmt.Sprintf("%s/%s/_search", c.baseURL, c.index)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opensearch search: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("opensearch search: status %d: %s", resp.StatusCode, string(respBody))
	}

	var osResp struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source model.SocialEvent `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(respBody, &osResp); err != nil {
		return nil, fmt.Errorf("opensearch unmarshal: %w", err)
	}

	result := &SearchResult{Total: osResp.Hits.Total.Value}
	for _, hit := range osResp.Hits.Hits {
		result.Events = append(result.Events, hit.Source)
	}
	return result, nil
}

func (c *OpenSearchClient) buildSort(sortBy string) []any {
	switch sortBy {
	case "engagement":
		return []any{map[string]any{"engagement.likes": "desc"}, "_score"}
	case "date":
		return []any{map[string]any{"timestamps.published_at": "desc"}}
	default:
		return []any{"_score", map[string]any{"timestamps.published_at": "desc"}}
	}
}

func (c *OpenSearchClient) GetStats(ctx context.Context) (map[string]any, error) {
	url := fmt.Sprintf("%s/%s/_count", c.baseURL, c.index)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var countResp struct {
		Count int64 `json:"count"`
	}
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &countResp)

	// Get platform breakdown
	aggQuery := map[string]any{
		"size": 0,
		"aggs": map[string]any{
			"platforms": map[string]any{
				"terms": map[string]any{"field": "platform"},
			},
			"sentiment": map[string]any{
				"terms": map[string]any{"field": "enrichment.sentiment"},
			},
		},
	}
	aggBody, _ := json.Marshal(aggQuery)
	aggReq, _ := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/%s/_search", c.baseURL, c.index), bytes.NewReader(aggBody))
	aggReq.Header.Set("Content-Type", "application/json")
	aggReq.SetBasicAuth(c.username, c.password)

	aggResp, err := c.httpClient.Do(aggReq)
	if err != nil {
		return map[string]any{"total_events": countResp.Count}, nil
	}
	defer aggResp.Body.Close()

	var aggResult struct {
		Aggregations struct {
			Platforms struct {
				Buckets []struct {
					Key      string `json:"key"`
					DocCount int64  `json:"doc_count"`
				} `json:"buckets"`
			} `json:"platforms"`
			Sentiment struct {
				Buckets []struct {
					Key      string `json:"key"`
					DocCount int64  `json:"doc_count"`
				} `json:"buckets"`
			} `json:"sentiment"`
		} `json:"aggregations"`
	}
	aggBytes, _ := io.ReadAll(aggResp.Body)
	json.Unmarshal(aggBytes, &aggResult)

	platforms := map[string]int64{}
	for _, b := range aggResult.Aggregations.Platforms.Buckets {
		platforms[b.Key] = b.DocCount
	}
	sentiment := map[string]int64{}
	for _, b := range aggResult.Aggregations.Sentiment.Buckets {
		sentiment[b.Key] = b.DocCount
	}

	return map[string]any{
		"total_events": countResp.Count,
		"platforms":    platforms,
		"sentiment":    sentiment,
	}, nil
}
