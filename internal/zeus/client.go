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

// workflowEnvelope is the request body for zeus's workflow register and
// stateless validate endpoints: {"workflow": <DSL v2 doc>, "overwrite"?: bool}.
type workflowEnvelope struct {
	Workflow  json.RawMessage `json:"workflow"`
	Overwrite bool            `json:"overwrite,omitempty"`
}

// ValidateWorkflowDoc runs a DSL v2 document through zeus's stateless
// validator (POST /api/v1/workflows/validate) without registering it.
// Returns nil when valid; a descriptive error carrying zeus's message when
// invalid; and a transport error when zeus is unreachable (callers fail
// closed — zeus owns DSL semantics, manteion does not guess).
func (c *Client) ValidateWorkflowDoc(ctx context.Context, doc json.RawMessage) error {
	body, err := json.Marshal(workflowEnvelope{Workflow: doc})
	if err != nil {
		return fmt.Errorf("zeus: marshal workflow doc: %w", err)
	}
	resp, err := c.Do(ctx, http.MethodPost, "/workflows/validate", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	var ve struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &ve)
	if ve.Error != "" {
		return fmt.Errorf("workflow validation failed: %s", ve.Error)
	}
	return fmt.Errorf("zeus: validate workflow: status %d: %s", resp.StatusCode, raw)
}

// RegisterWorkflow materializes a manteion-owned definition into zeus's
// in-memory store (POST /api/v1/workflows with overwrite). Called before
// triggering runs so zeus always has the current definition; the document
// must carry the manteion id/name so both systems share workflow identity.
func (c *Client) RegisterWorkflow(ctx context.Context, doc json.RawMessage) error {
	body, err := json.Marshal(workflowEnvelope{Workflow: doc, Overwrite: true})
	if err != nil {
		return fmt.Errorf("zeus: marshal workflow doc: %w", err)
	}
	resp, err := c.Do(ctx, http.MethodPost, "/workflows", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("zeus: register workflow: status %d: %s", resp.StatusCode, raw)
	}
	io.Copy(io.Discard, resp.Body)
	return nil
}

// DeleteWorkflow removes a workflow from zeus's in-memory store. A 404 is
// tolerated (zeus restarts empty); other failures surface so callers can
// decide whether materialization may proceed.
func (c *Client) DeleteWorkflow(ctx context.Context, id string) error {
	resp, err := c.Do(ctx, http.MethodDelete, "/workflows/"+id, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("zeus: delete workflow %q: status %d", id, resp.StatusCode)
	}
}

// AttackTargetSpec matches zeus's attacker.TargetSpec JSON schema.
type AttackTargetSpec struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
}

// AttackDedupBypass selects a per-request uniquification strategy. Mirrors
// zeus's attacker.DedupBypassSpec. Strategy is "header" or "query"; Source is
// the header name or query param name to randomize. Omit Source to use zeus's
// strategy-specific default (X-Idempotency-Key / nonce).
type AttackDedupBypass struct {
	Strategy string `json:"strategy"`
	Source   string `json:"source,omitempty"`
}

// AttackRequest matches zeus's attacker.AttackConfig JSON schema.
// ID is an optional client-generated correlation ID for crash recovery.
//
// Wire-format breaking change vs. prior versions of this client:
//   - Duration (Go duration string) → DurationS (integer seconds)
//   - DedupBypass (string)          → DedupBypass (*AttackDedupBypass)
//   - RunRef removed — it duplicated MetaTraceID (both carried the phase id)
//     and nothing consumed it: the atropos SDK tags cache entries by the
//     workflow_label baggage, manteion attributes results by attack id, and
//     zeus does not execute runs in the single-URL model. meta_trace_id +
//     experiment_id + workflow_label carry all correlation.
//
// The optional Timeout/MaxConnections/MaxBody/Redirects fields are vegeta
// tuning knobs on zeus; omit (zero) to use vegeta defaults.
type AttackRequest struct {
	ID        string           `json:"id,omitempty"`
	Target    AttackTargetSpec `json:"target"`
	Rate      int              `json:"rate"`
	DurationS int              `json:"duration_s"`

	TimeoutS       int   `json:"timeout_s,omitempty"`
	MaxConnections int   `json:"max_connections,omitempty"`
	MaxBodyBytes   int64 `json:"max_body_bytes,omitempty"`
	Redirects      int   `json:"redirects,omitempty"`

	DedupBypass   *AttackDedupBypass `json:"dedup_bypass,omitempty"`
	MetaTraceID   string             `json:"meta_trace_id,omitempty"`
	ExperimentID  string             `json:"experiment_id,omitempty"`
	WorkflowLabel string             `json:"workflow_label,omitempty"`
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

// --- Workflow runs (k6 DAG execution) ---
//
// A run executes a workflow's DSL v2 document via zeus's k6 launcher. It is
// the primary, workflow-shaped load driver for a phase; flat attacks (above)
// run additively alongside it. The run is scoped to (experiment_id, phase_id):
// experiment_id tags it for cross-service correlation and meta_trace_id
// carries the phase id, so recorded traffic and traces slice by phase.

// RunRequest is the POST body for /api/v1/workflows/{id}/runs.
type RunRequest struct {
	RunID         string            `json:"run_id,omitempty"`
	ExperimentID  string            `json:"experiment_id,omitempty"`
	DatasetID     string            `json:"dataset_id,omitempty"`
	VUs           int               `json:"vus,omitempty"`
	DurationS     int               `json:"duration_s,omitempty"`
	Persona       string            `json:"persona,omitempty"`
	MetaTraceID   string            `json:"meta_trace_id,omitempty"`
	WorkflowLabel string            `json:"workflow_label,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

// RunResponse is the 202 envelope from POST /api/v1/workflows/{id}/runs.
type RunResponse struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

// StartRun starts a workflow run in zeus and returns the run id. workflowID
// is the zeus-side workflow id (== manteion's workflow id — manteion
// materializes with its own id). A 422 (dataset schema rejection) surfaces as
// an error; the caller decides whether to fail the phase.
func (c *Client) StartRun(ctx context.Context, workflowID string, req RunRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("zeus: marshal run request: %w", err)
	}
	resp, err := c.Do(ctx, http.MethodPost, "/workflows/"+workflowID+"/runs", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return "", fmt.Errorf("zeus: start run for workflow %q: status %d: %s", workflowID, resp.StatusCode, raw)
	}
	var rr RunResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return "", fmt.Errorf("zeus: decode run response: %w", err)
	}
	return rr.RunID, nil
}

// StopRun sends DELETE /api/v1/runs/{id}. A 409 (already terminal) is
// tolerated — stopping a finished run is a no-op, which is what teardown
// wants.
func (c *Client) StopRun(ctx context.Context, runID string) error {
	resp, err := c.Do(ctx, http.MethodDelete, "/runs/"+runID, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusConflict, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("zeus: stop run %q: status %d", runID, resp.StatusCode)
	}
}

// RunInfo is the subset of GET /api/v1/runs/{id} the orchestrator polls.
type RunInfo struct {
	ID     string `json:"id"`
	Status string `json:"status"` // starting, validating, running, completing, completed, stopped, failed, rejected
}

// GetRun fetches a run's current status.
func (c *Client) GetRun(ctx context.Context, runID string) (*RunInfo, error) {
	resp, err := c.Do(ctx, http.MethodGet, "/runs/"+runID, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("zeus: run %q not found", runID)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, fmt.Errorf("zeus: get run %q: status %d: %s", runID, resp.StatusCode, raw)
	}
	var info RunInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("zeus: decode run info: %w", err)
	}
	return &info, nil
}
