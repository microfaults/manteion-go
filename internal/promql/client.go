package promql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// Sample is one series of an instant-query vector: its label set and value.
// It is the vector shape both the Prometheus and the Loki clients return.
type Sample struct {
	Labels map[string]string `json:"metric"`
	Value  float64           `json:"value"`
}

// DecodeVector decodes the Prometheus instant-query envelope, which Loki
// metric queries share. A status other than "success" is an error carrying
// the envelope's errorType and error. A "vector" result yields one Sample per
// series (an empty vector yields an empty, non-nil slice); a "scalar" result
// yields one Sample with an empty label map. Any other resultType ("matrix",
// "streams", "string") is rejected: only metric instant queries are supported.
func DecodeVector(r io.Reader) ([]Sample, error) {
	var env struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
		Error     string `json:"error"`
		Data      struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(r).Decode(&env); err != nil {
		return nil, fmt.Errorf("decode query response: %w", err)
	}
	if env.Status != "success" {
		return nil, fmt.Errorf("query returned status %q: %s: %s", env.Status, env.ErrorType, env.Error)
	}
	switch t := env.Data.ResultType; t {
	case "vector":
		var series []struct {
			Metric map[string]string  `json:"metric"`
			Value  [2]json.RawMessage `json:"value"` // [timestamp, value_string]
		}
		if err := json.Unmarshal(env.Data.Result, &series); err != nil {
			return nil, fmt.Errorf("decode vector result: %w", err)
		}
		out := make([]Sample, 0, len(series))
		for _, s := range series {
			v, err := parseSampleValue(s.Value)
			if err != nil {
				return nil, err
			}
			labels := s.Metric
			if labels == nil {
				labels = map[string]string{}
			}
			out = append(out, Sample{Labels: labels, Value: v})
		}
		return out, nil
	case "scalar":
		var pair [2]json.RawMessage
		if err := json.Unmarshal(env.Data.Result, &pair); err != nil {
			return nil, fmt.Errorf("decode scalar result: %w", err)
		}
		v, err := parseSampleValue(pair)
		if err != nil {
			return nil, err
		}
		return []Sample{{Labels: map[string]string{}, Value: v}}, nil
	default:
		return nil, fmt.Errorf("unsupported resultType %q (metric queries only)", t)
	}
}

// parseSampleValue parses the quoted float of a [timestamp, "value"] pair.
func parseSampleValue(pair [2]json.RawMessage) (float64, error) {
	var valStr string
	if err := json.Unmarshal(pair[1], &valStr); err != nil {
		return 0, fmt.Errorf("decode sample value: %w", err)
	}
	return strconv.ParseFloat(valStr, 64)
}

// QueryVector executes a PromQL instant query (GET {base}/api/v1/query) and
// returns every sample of the result, decoded by DecodeVector.
func (c *Client) QueryVector(ctx context.Context, query string) ([]Sample, error) {
	u := c.baseURL + "/api/v1/query?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prometheus query failed: %w", err)
	}
	defer resp.Body.Close()
	return DecodeVector(resp.Body)
}

// QueryInstant executes a PromQL instant query and returns the first sample's
// value. An empty result is the error "prometheus query %q returned no data",
// a text callers rely on.
func (c *Client) QueryInstant(ctx context.Context, query string) (float64, error) {
	samples, err := c.QueryVector(ctx, query)
	if err != nil {
		return 0, err
	}
	if len(samples) == 0 {
		return 0, fmt.Errorf("prometheus query %q returned no data", query)
	}
	return samples[0].Value, nil
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
