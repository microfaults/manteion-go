package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// PhaseTransition defines when a run should advance to its next phase.
type PhaseTransition struct {
	Metric    string        `json:"metric"`
	Operator  string        `json:"operator"` // gt, gte, lt, lte, eq
	Threshold float64       `json:"threshold"`
	Window    time.Duration `json:"window_ns"`
}

// PhaseRuleSet is the fault rules active during one phase of a run.
type PhaseRuleSet struct {
	Phase       int      `json:"phase"`
	Description string   `json:"description"`
	RuleIDs     []string `json:"rule_ids"`
}

// Experiment is an experiment plan for quantifying per-service contribution
// to workflow latency. The experiment's behavior is fully determined by its
// runs' FrozenServices and each cache-box's Mode — there is no separate
// "type" discriminator.
//
// PrimaryWorkloadID identifies the workflow being measured (e.g. checkout at 50 RPS).
// Background workloads (interference sources) are captured per-run via Attack
// entities with Role="background", allowing load to vary across runs.
type Experiment struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Description       string     `json:"description,omitempty"`
	PrimaryWorkloadID string     `json:"primary_workload_id"`
	Status            string     `json:"status"`
	CreatedAt         time.Time  `json:"created_at"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
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
//
// Run FSM: pending → running → completed
//
//	running → paused → running (resume)
//	running → failed
//
// Run-to-run sequencing is declarative via DependsOn: a run only starts when
// every run ID it lists has reached status='completed'. Runs with empty
// DependsOn are entry points (started by StartExperiment). The orchestrator —
// not the policy engine — is the single authority that advances the DAG.
type ExperimentRun struct {
	ID             string            `json:"id"`
	ExperimentID   string            `json:"experiment_id"`
	RunType        string            `json:"run_type"` // "baseline", "isolation", "combination"
	RunIndex       int               `json:"run_index"`
	FrozenServices []CacheBoxConfig  `json:"frozen_services,omitempty"`
	MetaTraceID    string            `json:"meta_trace_id"`
	Status         string            `json:"status"` // pending, running, paused, completed, failed
	NodePlacement  map[string]string `json:"node_placement,omitempty"`
	StartedAt      *time.Time        `json:"started_at,omitempty"`
	CompletedAt    *time.Time        `json:"completed_at,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	PhaseRules     []PhaseRuleSet    `json:"phase_rules,omitempty"`
	TransitionCond *PhaseTransition  `json:"transition_condition,omitempty"`
	CurrentPhase   int               `json:"current_phase"`
	// DependsOn lists run IDs that must reach 'completed' before this run starts.
	// Empty for entry-point runs (typically baseline). Cycles and cross-experiment
	// references are rejected by ValidateRunGraph.
	DependsOn []string `json:"depends_on,omitempty"`
	// PersistCache opts the run into accepting cache-box entry ingestion via
	// POST /api/v1/cache/ingest. Default false. Typically true on baseline
	// runs whose cache will be replayed by isolation runs that depend on them.
	PersistCache bool `json:"persist_cache,omitempty"`
	// ZeusAttackID is the primary Zeus attack ID (first/only for single-workflow runs).
	ZeusAttackID string `json:"zeus_attack_id,omitempty"`
	// WorkloadIDs lists workloads to drive for this run; falls back to Experiment.PrimaryWorkloadID if empty.
	WorkloadIDs []string `json:"workload_ids,omitempty"`
	// ZeusAttackIDs holds all attack IDs for multi-workflow runs.
	ZeusAttackIDs []string `json:"zeus_attack_ids,omitempty"`
}

var validRunTypes = map[string]bool{
	"baseline": true, "isolation": true, "combination": true,
}

var validRunStatuses = map[string]bool{
	"pending": true, "running": true, "paused": true, "completed": true, "failed": true,
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
	if r.TransitionCond != nil {
		if r.TransitionCond.Metric == "" {
			return errors.New("experiment run: transition_condition.metric required")
		}
		switch r.TransitionCond.Operator {
		case "gt", "gte", "lt", "lte", "eq":
		default:
			return fmt.Errorf("experiment run: transition_condition.operator %q invalid", r.TransitionCond.Operator)
		}
		if r.TransitionCond.Window <= 0 {
			return errors.New("experiment run: transition_condition.window_ns must be > 0")
		}
	}
	for i, ps := range r.PhaseRules {
		if ps.Phase != i {
			return fmt.Errorf("experiment run: phase_rules[%d].phase must be %d (sequential from 0)", i, i)
		}
	}
	for _, dep := range r.DependsOn {
		if dep == r.ID {
			return errors.New("experiment run: depends_on must not include own id")
		}
		if dep == "" {
			return errors.New("experiment run: depends_on must not contain empty id")
		}
	}
	return nil
}

// ValidateRunGraph checks the cross-run dependency graph for an experiment.
// It rejects:
//   - cycles in DependsOn,
//   - dependencies on run IDs not present in the experiment,
//   - dependencies that span experiments.
//
// Call this once at experiment-start time before any StartRun is issued.
func ValidateRunGraph(runs []*ExperimentRun) error {
	byID := make(map[string]*ExperimentRun, len(runs))
	for _, r := range runs {
		byID[r.ID] = r
	}

	visited := make(map[string]bool, len(runs))
	onPath := make(map[string]bool, len(runs))

	var dfs func(id string) error
	dfs = func(id string) error {
		if onPath[id] {
			return fmt.Errorf("experiment runs: dependency cycle through run %q", id)
		}
		if visited[id] {
			return nil
		}
		onPath[id] = true
		run := byID[id]
		for _, dep := range run.DependsOn {
			depRun, ok := byID[dep]
			if !ok {
				return fmt.Errorf("experiment run %q: depends_on references unknown run %q", id, dep)
			}
			if depRun.ExperimentID != run.ExperimentID {
				return fmt.Errorf("experiment run %q: depends_on %q is in a different experiment", id, dep)
			}
			if err := dfs(dep); err != nil {
				return err
			}
		}
		onPath[id] = false
		visited[id] = true
		return nil
	}

	for _, r := range runs {
		if err := dfs(r.ID); err != nil {
			return err
		}
	}

	// Non-baseline runs with frozen services (cache-box replay) must
	// transitively depend on at least one baseline run. Without this,
	// preloadCacheEntries silently skips and the isolation run produces
	// meaningless results.
	for _, r := range runs {
		if r.RunType == "baseline" || len(r.FrozenServices) == 0 {
			continue
		}
		if !hasBaselineAncestor(r.ID, byID) {
			return fmt.Errorf("experiment run %q (%s): has frozen_services but no baseline run in its dependency chain", r.ID, r.RunType)
		}
	}
	return nil
}

func hasBaselineAncestor(id string, byID map[string]*ExperimentRun) bool {
	seen := make(map[string]bool)
	var walk func(string) bool
	walk = func(cur string) bool {
		if seen[cur] {
			return false
		}
		seen[cur] = true
		r := byID[cur]
		if r.RunType == "baseline" {
			return true
		}
		for _, dep := range r.DependsOn {
			if walk(dep) {
				return true
			}
		}
		return false
	}
	for _, dep := range byID[id].DependsOn {
		if walk(dep) {
			return true
		}
	}
	return false
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
