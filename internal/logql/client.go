// Package logql is a minimal Loki client for LogQL metric queries. It returns
// the same sample shape as the Prometheus client (promql.Sample).
package logql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"manteion-go/internal/promql"
)

const defaultQueryTimeout = 5 * time.Second

// maxErrorLine bounds the body excerpt carried by a non-2xx error.
const maxErrorLine = 200

// Client queries Loki's instant-query endpoint.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// ClientOption configures a Client at construction time.
type ClientOption func(*Client)

// WithTimeout overrides the default 5s HTTP timeout.
func WithTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.httpClient.Timeout = d }
}

// NewClient returns a Loki client for baseURL (a trailing "/" is trimmed)
// with a 5s default timeout, mirroring promql.NewClient.
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

// QueryVector executes a LogQL instant query (GET {base}/loki/api/v1/query)
// and returns its samples. A non-2xx answer is the error
// "loki returned status <code>: <first line of the body>"; otherwise the body
// is decoded with promql.DecodeVector, so a log (streams) query fails with its
// "metric queries only" error.
func (c *Client) QueryVector(ctx context.Context, query string) ([]promql.Sample, error) {
	u := c.baseURL + "/loki/api/v1/query?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("loki query failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read loki response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("loki returned status %d: %s", resp.StatusCode, firstLine(body))
	}
	return promql.DecodeVector(bytes.NewReader(body))
}

// firstLine returns the body's first line, at most maxErrorLine bytes.
func firstLine(body []byte) string {
	if i := bytes.IndexByte(body, '\n'); i >= 0 {
		body = body[:i]
	}
	if len(body) > maxErrorLine {
		body = body[:maxErrorLine]
	}
	return strings.TrimSpace(string(body))
}
