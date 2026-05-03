package promql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultQueryTimeout = 5 * time.Second

type Client struct {
	baseURL    string
	httpClient *http.Client
}

// ClientOption configures a Client at construction time.
type ClientOption func(*Client)

// WithTimeout overrides the default 5s HTTP timeout. Callers under heavier
// Prometheus load (or higher network latency) should raise this; the policy
// engine and FSM treat timeouts as "indeterminate" so missed evaluations are
// safe but noisy in logs.
func WithTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.httpClient.Timeout = d }
}

func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: defaultQueryTimeout},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// QueryInstant executes a PromQL instant query and returns the scalar result.
// Returns an error if the query returns no data.
func (c *Client) QueryInstant(ctx context.Context, query string) (float64, error) {
	u := c.baseURL + "/api/v1/query?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("prometheus query failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]json.RawMessage `json:"value"` // [timestamp, value_string]
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode prometheus response: %w", err)
	}
	if result.Status != "success" {
		return 0, fmt.Errorf("prometheus returned status %q", result.Status)
	}
	if len(result.Data.Result) == 0 {
		return 0, fmt.Errorf("prometheus query %q returned no data", query)
	}

	var valStr string
	if err := json.Unmarshal(result.Data.Result[0].Value[1], &valStr); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(valStr, 64)
}

// Healthy returns true if Prometheus is reachable.
func (c *Client) Healthy(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/-/healthy", nil)
	if err != nil {
		return false
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
