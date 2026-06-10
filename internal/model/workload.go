package model

import (
	"errors"
	"fmt"
	"time"
)

// Attack represents a vegeta precision load targeting a specific endpoint.
// Manteion is the source of truth for trigger config + results.
// Zeus handles execution state (running → completed/failed).
//
// Migration #19 dropped Attack.ExperimentRunID — phase-driven attacks are
// addressed via phase_workflows.zeus_attack_id instead.
type Attack struct {
	ID            string            `json:"id"`
	PolicyRuleID  string            `json:"policy_rule_id,omitempty"`
	Service       string            `json:"service"`
	TargetURL     string            `json:"target_url"`
	TargetMethod  string            `json:"target_method"`
	TargetHeaders map[string]string `json:"target_headers,omitempty"`
	Rate          int               `json:"rate"`
	DurationMs    int64             `json:"duration_ms"`
	DedupBypass   string            `json:"dedup_bypass,omitempty"`
	MetaTraceID   string            `json:"meta_trace_id,omitempty"`
	ZeusAttackID  string            `json:"zeus_attack_id,omitempty"`

	// Result fields — populated on completion callback from zeus.
	LatencyP50Us  int64          `json:"latency_p50_us,omitempty"`
	LatencyP90Us  int64          `json:"latency_p90_us,omitempty"`
	LatencyP95Us  int64          `json:"latency_p95_us,omitempty"`
	LatencyP99Us  int64          `json:"latency_p99_us,omitempty"`
	TotalRequests uint64         `json:"total_requests,omitempty"`
	SuccessRate   float64        `json:"success_rate,omitempty"`
	StatusCodes   map[string]int `json:"status_codes,omitempty"`
	Errors        []string       `json:"errors,omitempty"`

	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

func (a *Attack) Validate() error {
	if a.ID == "" {
		return errors.New("attack: id required")
	}
	if a.Service == "" {
		return errors.New("attack: service required")
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
	return nil
}

// AttackResult stores the outcome metrics of a completed attack.
// Deprecated: result fields are now inlined on Attack. Kept temporarily
// for the workload_repo persistence path until that is migrated.
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

// WorkflowRef identifies a zeus workflow for experiment orchestration.
// Replaces the transitional Flow/Persona/Workload models.
type WorkflowRef struct {
	WorkflowID string `json:"workflow_id"`
	Rate       int    `json:"rate,omitempty"`
	VUs        int    `json:"vus,omitempty"`
}

func (w *WorkflowRef) Validate() error {
	if w.WorkflowID == "" {
		return fmt.Errorf("workflow_ref: workflow_id required")
	}
	return nil
}
