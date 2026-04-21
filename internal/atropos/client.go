package atropos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	http *http.Client
}

type Option func(*Client)

func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.http = h }
}

func NewClient(opts ...Option) *Client {
	c := &Client{http: http.DefaultClient}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) do(ctx context.Context, method, url string, body any) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, &TransportError{Method: method, URL: url, Err: err}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &TransportError{Method: method, URL: url, Err: err}
	}
	return resp, nil
}

const maxErrorBody = 1024

func (c *Client) doExpectStatus(ctx context.Context, method, url string, body any, wantStatus int) ([]byte, error) {
	resp, err := c.do(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
	if resp.StatusCode != wantStatus {
		bodyStr := string(data)
		if len(bodyStr) > maxErrorBody {
			bodyStr = bodyStr[:maxErrorBody]
		}
		return nil, &HTTPError{
			Method: method,
			URL:    url,
			Status: resp.StatusCode,
			Body:   bodyStr,
		}
	}
	return data, nil
}
