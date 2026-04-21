// Package zeus provides an HTTP client for proxying requests to the
// zeus-go Archer API. Manteion uses this to forward workload, attack,
// and policy operations to the load generation platform.
package zeus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is an HTTP client for the zeus-go Archer API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a zeus client targeting the given Archer base URL.
// Example: "http://archer:8080"
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Do forwards an HTTP request to Archer. The path should be relative to
// the Archer API root (e.g., "/workloads", "/attacks/abc123").
// The full URL becomes: baseURL + "/api/v1" + path
//
// The caller is responsible for closing the response body.
func (c *Client) Do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	url := c.baseURL + "/api/v1" + path

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("zeus: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("zeus: request failed: %w", err)
	}

	return resp, nil
}

// Healthy checks if Archer is reachable by hitting GET /api/v1/status.
// Returns true if the response is 200 OK.
func (c *Client) Healthy(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := c.Do(ctx, http.MethodGet, "/status", nil)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	return resp.StatusCode == http.StatusOK
}

// BaseURL returns the configured Archer base URL.
func (c *Client) BaseURL() string {
	return c.baseURL
}
