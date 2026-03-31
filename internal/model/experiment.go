package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Experiment is an experiment plan. Maps to one of 5 experiment types
// from the research protocol:
//   - interference:     cross-workflow interference quantification (all live, varying load)
//   - isolation:        freeze shared services, show interference eliminated
//   - attribution:      freeze all except one, measure single-service contribution
//   - scenario:         inject synthetic latency into frozen service ("what-if")
//   - cache_fidelity:   measure response divergence between live and cached
//
// PrimaryWorkloadID identifies the workflow being measured (e.g. checkout at 50 RPS).
// Background workloads (interference sources) are captured per-run via Attack
// entities with Role="background", allowing load to vary across runs.
type Experiment struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Description       string     `json:"description,omitempty"`
	ExperimentType    string     `json:"experiment_type"`
	PrimaryWorkloadID string     `json:"primary_workload_id"`
	Status            string     `json:"status"`
	CreatedAt         time.Time  `json:"created_at"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
}

var validExperimentTypes = map[string]bool{
	"interference": true, "isolation": true, "attribution": true,
	"scenario": true, "cache_fidelity": true,
}

var validExperimentStatuses = map[string]bool{
	"planned": true, "running": true, "completed": true, "failed": true, "cancelled": true,
}

func (e *Experiment) Validate() error {
	if e.ID == "" {
		return errors.New("experiment: id required")
	}
	if e.Name == "" {
		return errors.New("experiment: name required")
	}
	if !validExperimentTypes[e.ExperimentType] {
		return fmt.Errorf("experiment: invalid experiment_type %q", e.ExperimentType)
	}
	if e.PrimaryWorkloadID == "" {
		return errors.New("experiment: primary_workload_id required")
	}
	if !validExperimentStatuses[e.Status] {
		return fmt.Errorf("experiment: invalid status %q", e.Status)
	}
	return nil
}

// ExperimentRun is one execution phase within an experiment.
// An attribution experiment has 1 baseline + N isolation + C(N,2) combination runs.
// Isolation runs CAN overlap (they freeze different services). If any run fails,
// the experiment aborts. Partial results from completed runs are preserved.
type ExperimentRun struct {
	ID             string            `json:"id"`
	ExperimentID   string            `json:"experiment_id"`
	RunType        string            `json:"run_type"` // "baseline", "isolation", "combination"
	RunIndex       int               `json:"run_index"`
	FrozenServices []CacheBoxConfig  `json:"frozen_services,omitempty"`
	MetaTraceID    string            `json:"meta_trace_id"`
	Status         string            `json:"status"` // pending, running, completed, failed
	NodePlacement  map[string]string `json:"node_placement,omitempty"`
	StartedAt      *time.Time        `json:"started_at,omitempty"`
	CompletedAt    *time.Time        `json:"completed_at,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
}

var validRunTypes = map[string]bool{
	"baseline": true, "isolation": true, "combination": true,
}

var validRunStatuses = map[string]bool{
	"pending": true, "running": true, "completed": true, "failed": true,
}

func (r *ExperimentRun) Validate() error {
	if r.ID == "" {
		return errors.New("experiment run: id required")
	}
	if r.ExperimentID == "" {
		return errors.New("experiment run: experiment_id required")
	}
	if !validRunTypes[r.RunType] {
		return fmt.Errorf("experiment run: invalid run_type %q", r.RunType)
	}
	if !validRunStatuses[r.Status] {
		return fmt.Errorf("experiment run: invalid status %q", r.Status)
	}
	if r.RunType == "baseline" && len(r.FrozenServices) > 0 {
		return errors.New("experiment run: baseline run must not have frozen services")
	}
	if (r.RunType == "isolation" || r.RunType == "combination") && len(r.FrozenServices) == 0 {
		return errors.New("experiment run: isolation/combination run requires frozen services")
	}
	for i, cfg := range r.FrozenServices {
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("experiment run: frozen_services[%d]: %w", i, err)
		}
	}
	return nil
}

// WorkflowRunResult stores end-to-end workflow latency for one run.
// One row per (run, workflow) pair. This is the measurement the delta formula
// operates on — what the load generator observes at the workflow entry point.
//
// Source: aggregated from AttackResults of primary-role attacks, or from
// k6 summary output for broad-traffic workloads.
type WorkflowRunResult struct {
	ID              string          `json:"id"`
	ExperimentRunID string          `json:"experiment_run_id"`
	Workflow        string          `json:"workflow"`
	LatencyP50Us    int64           `json:"latency_p50_us"`
	LatencyP95Us    int64           `json:"latency_p95_us"`
	LatencyP99Us    int64           `json:"latency_p99_us"`
	LatencyP999Us   int64           `json:"latency_p999_us"`
	RequestCount    int64           `json:"request_count"`
	ErrorRate       float64         `json:"error_rate"`
	ThroughputRPS   float64         `json:"throughput_rps"`
	RawMetrics      json.RawMessage `json:"raw_metrics,omitempty"`
}

func (r *WorkflowRunResult) Validate() error {
	if r.ID == "" {
		return errors.New("workflow run result: id required")
	}
	if r.ExperimentRunID == "" {
		return errors.New("workflow run result: experiment_run_id required")
	}
	if r.Workflow == "" {
		return errors.New("workflow run result: workflow required")
	}
	return nil
}

// ServiceRunResult stores per-service metrics for one service in one run.
// One row per (run, service, workflow) triple. Source: trace backends
// (Jaeger/Tempo for per-service latency), Prometheus (resource utilization),
// and cache-box internals (fidelity metrics for frozen services).
type ServiceRunResult struct {
	ID              string          `json:"id"`
	ExperimentRunID string          `json:"experiment_run_id"`
	Service         string          `json:"service"`
	Workflow        string          `json:"workflow,omitempty"`
	LatencyP50Us    *int64          `json:"latency_p50_us,omitempty"`
	LatencyP95Us    *int64          `json:"latency_p95_us,omitempty"`
	LatencyP99Us    *int64          `json:"latency_p99_us,omitempty"`
	CPUMillicores   *int64          `json:"cpu_millicores,omitempty"`
	MemoryMB        *int64          `json:"memory_mb,omitempty"`
	CacheHitRate    *float64        `json:"cache_hit_rate,omitempty"`
	CacheExactMatch *float64        `json:"cache_exact_match,omitempty"`
	CacheStaleness  *float64        `json:"cache_staleness_ms,omitempty"`
	RawMetrics      json.RawMessage `json:"raw_metrics,omitempty"`
}

func (r *ServiceRunResult) Validate() error {
	if r.ID == "" {
		return errors.New("service run result: id required")
	}
	if r.ExperimentRunID == "" {
		return errors.New("service run result: experiment_run_id required")
	}
	if r.Service == "" {
		return errors.New("service run result: service required")
	}
	return nil
}

// ContributionResult is derived by comparing workflow-level latency between
// a baseline run and an isolation run.
// Computed: delta_service = baseline_workflow_latency - isolated_workflow_latency.
//
// CacheBoxMode distinguishes two isolation types:
//   - "replay":            removes ALL contribution (contention + intrinsic)
//   - "replay_with_delay": preserves intrinsic timing, removes contention only
//
// Intrinsic cost = total_delta (replay) - contention_delta (replay_with_delay).
type ContributionResult struct {
	ID             string `json:"id"`
	ExperimentID   string `json:"experiment_id"`
	Service        string `json:"service"`
	Workflow       string `json:"workflow"`
	CacheBoxMode   string `json:"cachebox_mode"` // "replay" or "replay_with_delay"
	BaselineRunID  string `json:"baseline_run_id"`
	IsolationRunID string `json:"isolation_run_id"`

	DeltaP50Us int64 `json:"delta_p50_us"`
	DeltaP95Us int64 `json:"delta_p95_us"`
	DeltaP99Us int64 `json:"delta_p99_us"`

	InteractionEffect *float64 `json:"interaction_effect,omitempty"`
	CombinationRunID  string   `json:"combination_run_id,omitempty"`
}

var validCacheBoxModeContribution = map[string]bool{
	"replay": true, "replay_with_delay": true,
}

func (c *ContributionResult) Validate() error {
	if c.ID == "" {
		return errors.New("contribution result: id required")
	}
	if c.ExperimentID == "" {
		return errors.New("contribution result: experiment_id required")
	}
	if c.Service == "" {
		return errors.New("contribution result: service required")
	}
	if c.Workflow == "" {
		return errors.New("contribution result: workflow required")
	}
	if !validCacheBoxModeContribution[c.CacheBoxMode] {
		return fmt.Errorf("contribution result: invalid cachebox_mode %q", c.CacheBoxMode)
	}
	if c.BaselineRunID == "" {
		return errors.New("contribution result: baseline_run_id required")
	}
	if c.IsolationRunID == "" {
		return errors.New("contribution result: isolation_run_id required")
	}
	return nil
}
