// Package zeus provides an HTTP client for proxying requests to the
// zeus-go Archer API. Manteion uses this to forward workload, attack,
// and policy operations to the load generation platform.
package zeus

import (
	"bytes"
	"context"
	"encoding/json"
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

// AttackRequest is the body sent to POST /api/v1/attacks to launch a load attack.
//
// ID is an optional client-generated correlation ID. When set, Zeus must use
// this ID for the attack (de-duping on conflict so retries are idempotent).
// Manteion always sets this so a crash between Zeus.StartAttack and DB persist
// can be reconciled on restart by calling GetAttack(ID).
type AttackRequest struct {
	ID           string `json:"id,omitempty"`
	WorkloadID   string `json:"workload_id"`
	Service      string `json:"service"`
	Role         string `json:"role"`
	TargetURL    string `json:"target_url"`
	TargetMethod string `json:"target_method"`
	Rate         int    `json:"rate"`
	DurationMs   int64  `json:"duration_ms"`
	MetaTraceID  string `json:"meta_trace_id,omitempty"`
}

// AttackResponse is the body returned by POST /api/v1/attacks.
type AttackResponse struct {
	ID string `json:"id"`
}

// StartAttack POSTs to /api/v1/attacks and returns the new attack ID.
func (c *Client) StartAttack(ctx context.Context, req AttackRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("zeus: marshal attack request: %w", err)
	}
	resp, err := c.Do(ctx, http.MethodPost, "/attacks", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("zeus: start attack: status %d: %s", resp.StatusCode, raw)
	}
	var ar AttackResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return "", fmt.Errorf("zeus: decode attack response: %w", err)
	}
	return ar.ID, nil
}

// StopAttack sends DELETE /api/v1/attacks/{id} to halt a running attack.
func (c *Client) StopAttack(ctx context.Context, attackID string) error {
	resp, err := c.Do(ctx, http.MethodDelete, "/attacks/"+attackID, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("zeus: stop attack %q: status %d", attackID, resp.StatusCode)
	}
	return nil
}

// AttackInfo is returned by GET /api/v1/attacks/{id}.
type AttackInfo struct {
	ID          string     `json:"id"`
	WorkloadID  string     `json:"workload_id"`
	Service     string     `json:"service"`
	Status      string     `json:"status"` // pending, running, completed, stopped
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// GetAttack fetches the current status of an attack by ID.
func (c *Client) GetAttack(ctx context.Context, attackID string) (*AttackInfo, error) {
	resp, err := c.Do(ctx, http.MethodGet, "/attacks/"+attackID, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("zeus: attack %q not found", attackID)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("zeus: get attack %q: status %d: %s", attackID, resp.StatusCode, raw)
	}
	var info AttackInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("zeus: decode attack info: %w", err)
	}
	return &info, nil
}

// AttackResultInfo is returned by GET /api/v1/attacks/{id}/result.
// Returns nil, ErrAttackResultNotReady if the attack has not yet completed.
type AttackResultInfo struct {
	AttackID      string  `json:"attack_id"`
	Service       string  `json:"service"`
	TotalRequests int64   `json:"total_requests"`
	DurationMs    int64   `json:"duration_ms"`
	RateActual    float64 `json:"rate_actual"`
	SuccessRate   float64 `json:"success_rate"`
	LatencyP50Us  int64   `json:"latency_p50_us"`
	LatencyP90Us  int64   `json:"latency_p90_us"`
	LatencyP95Us  int64   `json:"latency_p95_us"`
	LatencyP99Us  int64   `json:"latency_p99_us"`
	ThroughputRPS float64 `json:"throughput_rps"`
}

// ErrAttackResultNotReady is returned when the attack result is not yet available.
var ErrAttackResultNotReady = fmt.Errorf("zeus: attack result not ready")

// GetAttackResult fetches the final metrics for a completed attack.
// Returns ErrAttackResultNotReady if the attack has not completed yet (404).
func (c *Client) GetAttackResult(ctx context.Context, attackID string) (*AttackResultInfo, error) {
	resp, err := c.Do(ctx, http.MethodGet, "/attacks/"+attackID+"/result", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrAttackResultNotReady
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("zeus: get attack result %q: status %d: %s", attackID, resp.StatusCode, raw)
	}
	var result AttackResultInfo
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("zeus: decode attack result: %w", err)
	}
	return &result, nil
}
