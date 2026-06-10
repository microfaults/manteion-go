package model

import (
	"errors"
	"time"
)

// Attack is a reusable DEFINITION of a vegeta precision load targeting a
// specific endpoint. It carries no execution state and no experiment
// association — triggering one (manually or during a phase) creates an
// AttackResult execution record. Zeus executes; manteion owns the
// definition and the results.
type Attack struct {
	ID            string            `json:"id"` // 'atk-<uuidv7>'
	Name          string            `json:"name,omitempty"`
	Description   string            `json:"description,omitempty"`
	Service       string            `json:"service"`
	TargetURL     string            `json:"target_url"`
	TargetMethod  string            `json:"target_method"`
	TargetHeaders map[string]string `json:"target_headers,omitempty"`
	Rate          int               `json:"rate"`
	DurationMs    int64             `json:"duration_ms"`
	DedupBypass   string            `json:"dedup_bypass,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
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

// AttackResult is one EXECUTION of an attack definition (N per attack).
// PhaseID is the optional "ran during this phase" tag — results survive
// phase deletion with the tag nulled (FK ON DELETE SET NULL).
type AttackResult struct {
	ID           string  `json:"id"`                 // 'atkres-<uuidv7>'
	AttackID     string  `json:"attack_id"`          // definition this run executed
	PhaseID      *string `json:"phase_id,omitempty"` // optional phase tag
	ZeusAttackID string  `json:"zeus_attack_id,omitempty"`
	MetaTraceID  string  `json:"meta_trace_id,omitempty"`
	Service      string  `json:"service"`

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

	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

func (r *AttackResult) Validate() error {
	if r.ID == "" {
		return errors.New("attack result: id required")
	}
	if r.AttackID == "" {
		return errors.New("attack result: attack_id required")
	}
	if r.Service == "" {
		return errors.New("attack result: service required")
	}
	return nil
}
