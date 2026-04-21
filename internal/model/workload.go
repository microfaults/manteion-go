package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Flow is a k6 flow definition. Steps/thresholds stored as opaque JSON
// since k6 consumes these directly.
type Flow struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	Description       string          `json:"description,omitempty"`
	Targets           []string        `json:"targets"`
	EstimatedRPSPerVU float64         `json:"estimated_rps_per_vu"`
	// Steps is a DSL v2 workflow tree (sequence/parallel/delay/optional/request nodes).
	// Stored opaquely -- k6 parses this at runtime. Tree nodes may reference persona
	// keys (e.g., "persona_key": "explore_prob" on optional nodes, or think-time
	// lookups on delay nodes). Resolution happens at k6 execution time by looking up
	// the key in the Persona associated with the Workload.
	Steps      json.RawMessage `json:"steps"`
	Thresholds json.RawMessage `json:"thresholds,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

func (f *Flow) Validate() error {
	if f.ID == "" {
		return errors.New("flow: id required")
	}
	if f.Name == "" {
		return errors.New("flow: name required")
	}
	if len(f.Targets) == 0 {
		return errors.New("flow: at least one target required")
	}
	if len(f.Steps) == 0 || string(f.Steps) == "null" {
		return errors.New("flow: steps required")
	}
	return nil
}

// Persona defines a k6 behavioral profile.
type Persona struct {
	ID           string  `json:"id"`
	Name         string  `json:"name"`
	Description  string  `json:"description,omitempty"`
	ExploreProb  float64 `json:"explore_prob"`
	EngageProb   float64 `json:"engage_prob"`
	CommitProb   float64 `json:"commit_prob"`
	RepeatProb   float64 `json:"repeat_prob,omitempty"`
	ThinkTimeMin int     `json:"think_time_min_ms"`
	ThinkTimeMax int     `json:"think_time_max_ms"`
}

func (p *Persona) Validate() error {
	if p.ID == "" {
		return errors.New("persona: id required")
	}
	if p.Name == "" {
		return errors.New("persona: name required")
	}
	if p.ThinkTimeMin < 0 || p.ThinkTimeMax < 0 {
		return errors.New("persona: think times must be non-negative")
	}
	if p.ThinkTimeMax > 0 && p.ThinkTimeMin > p.ThinkTimeMax {
		return errors.New("persona: think_time_min must be <= think_time_max")
	}
	return nil
}

// Workload represents a k6 load generation session.
type Workload struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	FlowID      string     `json:"flow_id"`
	PersonaID   string     `json:"persona_id"`
	VUs         int        `json:"vus"`
	Rate        float64    `json:"rate"`
	MetaTraceID string     `json:"meta_trace_id"`
	Status      string     `json:"status"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

var validWorkloadStatuses = map[string]bool{
	"pending": true, "running": true, "completed": true, "stopped": true, "failed": true,
}

func (w *Workload) Validate() error {
	if w.ID == "" {
		return errors.New("workload: id required")
	}
	if w.Name == "" {
		return errors.New("workload: name required")
	}
	if w.FlowID == "" {
		return errors.New("workload: flow_id required")
	}
	if w.PersonaID == "" {
		return errors.New("workload: persona_id required")
	}
	if w.VUs <= 0 {
		return errors.New("workload: vus must be > 0")
	}
	if !validWorkloadStatuses[w.Status] {
		return fmt.Errorf("workload: invalid status %q", w.Status)
	}
	return nil
}

// Attack represents a vegeta precision load targeting a specific endpoint.
type Attack struct {
	ID              string            `json:"id"`
	WorkloadID      string            `json:"workload_id,omitempty"`
	ExperimentRunID string            `json:"experiment_run_id,omitempty"`
	PolicyRuleID    string            `json:"policy_rule_id,omitempty"`
	Service         string            `json:"service"`
	Role            string            `json:"role"` // "primary" or "background"
	TargetURL       string            `json:"target_url"`
	TargetMethod    string            `json:"target_method"`
	TargetHeaders   map[string]string `json:"target_headers,omitempty"`
	Rate            int               `json:"rate"`
	DurationMs      int64             `json:"duration_ms"`
	DedupBypass     string            `json:"dedup_bypass,omitempty"`
	MetaTraceID     string            `json:"meta_trace_id,omitempty"`
	Status          string            `json:"status"`
	StartedAt       *time.Time        `json:"started_at,omitempty"`
	CompletedAt     *time.Time        `json:"completed_at,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
}

var validAttackStatuses = map[string]bool{
	"pending": true, "running": true, "completed": true, "stopped": true,
}

func (a *Attack) Validate() error {
	if a.ID == "" {
		return errors.New("attack: id required")
	}
	if a.Service == "" {
		return errors.New("attack: service required")
	}
	if a.Role != "primary" && a.Role != "background" {
		return fmt.Errorf("attack: invalid role %q", a.Role)
	}
	if a.TargetURL == "" {
		return errors.New("attack: target_url required")
	}
	if a.TargetMethod == "" {
		return errors.New("attack: target_method required")
	}
	if a.Rate <= 0 {
		return errors.New("attack: rate must be > 0")
	}
	if a.DurationMs <= 0 {
		return errors.New("attack: duration_ms must be > 0")
	}
	if !validAttackStatuses[a.Status] {
		return fmt.Errorf("attack: invalid status %q", a.Status)
	}
	return nil
}

// AttackResult stores the outcome metrics of a completed attack.
type AttackResult struct {
	AttackID      string         `json:"attack_id"`
	Service       string         `json:"service"`
	TotalRequests uint64         `json:"total_requests"`
	DurationMs    int64          `json:"duration_ms"`
	RateActual    float64        `json:"rate_actual"`
	SuccessRate   float64        `json:"success_rate"`
	StatusCodes   map[string]int `json:"status_codes"`
	LatencyP50Us  int64          `json:"latency_p50_us"`
	LatencyP90Us  int64          `json:"latency_p90_us"`
	LatencyP95Us  int64          `json:"latency_p95_us"`
	LatencyP99Us  int64          `json:"latency_p99_us"`
	LatencyMinUs  int64          `json:"latency_min_us"`
	LatencyMaxUs  int64          `json:"latency_max_us"`
	BytesInTotal  int64          `json:"bytes_in_total"`
	BytesOutTotal int64          `json:"bytes_out_total"`
	Errors        []string       `json:"errors,omitempty"`
	CompletedAt   time.Time      `json:"completed_at"`
}

func (r *AttackResult) Validate() error {
	if r.AttackID == "" {
		return errors.New("attack result: attack_id required")
	}
	if r.Service == "" {
		return errors.New("attack result: service required")
	}
	return nil
}
