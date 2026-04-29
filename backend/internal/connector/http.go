package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// GetJSON performs an HTTP GET against url, applies the given headers, and
// decodes the JSON response body into out. Non-2xx responses become errors
// that include up to 1 KiB of the response body for debugging.
//
// It is the canonical entry point for poll-based JSON connectors (Reddit,
// Mastodon, Discourse). Callers are responsible for rate limiting before
// invoking GetJSON; this function does not add any global throttle of its
// own.
func GetJSON(ctx context.Context, client *http.Client, url string, headers map[string]string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http get %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("http get %s: status %d: %s", url, resp.StatusCode, string(body))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
