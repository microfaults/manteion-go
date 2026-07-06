package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// =====================================================================
// Experiment domain — phase-first model (migration #19).
//
// Hierarchy:
//
//   Experiment
//     └── ExperimentPhase[]      (sequential; first-class run unit)
//           ├── PhaseWorkflow[]   (per-(phase, workflow) attack config)
//           ├── PhaseRule[]       (rules active during the phase)
//           └── results: PhaseWorkflowResult, PhaseServiceLatency,
//                        PhaseServiceCache
//
//   ExperimentResults                (rollup row, recomputed on transitions)
//
// Phases are ordered by position (0-based). A future iteration will
// support a phase DAG via depends_on; until then the orchestrator runs
// phases in position order. See docs/figma-changes.md for the UI note.
// =====================================================================

// ---------- Experiment ----------

// Experiment is a control-plane plan: metadata + an ordered list of
// associated workflows (zeus-owned) and phases. The plan carries NO
// attack config and NO measurement target — those live on the phases.
type Experiment struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	Hypothesis  string     `json:"hypothesis,omitempty"`
	Status      string     `json:"status"`
	CreatedBy   string     `json:"created_by,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

var validExperimentStatuses = setOf(ExperimentStatusValues...)

// ValidExperimentStatuses returns the set of allowed status values
// (exposed so the API layer can validate query params).
func ValidExperimentStatuses() map[string]bool {
	out := make(map[string]bool, len(validExperimentStatuses))
	for k, v := range validExperimentStatuses {
		out[k] = v
	}
	return out
}

func (e *Experiment) Validate() error {
	if e.ID == "" {
		return errors.New("experiment: id required")
	}
	if e.Name == "" {
		return errors.New("experiment: name required")
	}
	if !validExperimentStatuses[e.Status] {
		return fmt.Errorf("experiment: invalid status %q", e.Status)
	}
	return nil
}

// ---------- Phase ----------

// ExperimentPhase is one ordered step of an experiment. The phase encodes
// the experimental method via FrozenServices:
//
//   - empty FrozenServices         → baseline (no isolation)
//   - one entry                    → single-service isolation
//   - two or more entries          → multi-service / combined isolation
//
// There is no separate "run_type" discriminator. The old baseline /
// isolation / combination typology is derived from FrozenServices; the
// orchestrator and the UI agree to read the JSON shape, not a string tag.
type ExperimentPhase struct {
	ID             string           `json:"id"`
	ExperimentID   string           `json:"experiment_id"`
	Name           string           `json:"name"` // free-form (e.g. "baseline", "isolation-productcatalog")
	Position       int              `json:"position"`
	Status         string           `json:"status"`
	FrozenServices []CacheBoxConfig `json:"frozen_services"`
	PersistCache   bool             `json:"persist_cache"`
	StartedAt      *time.Time       `json:"started_at,omitempty"`
	CompletedAt    *time.Time       `json:"completed_at,omitempty"`
}

var validPhaseStatuses = setOf(PhaseStatusValues...)

func (p *ExperimentPhase) Validate() error {
	if p.ID == "" {
		return errors.New("experiment phase: id required")
	}
	if p.ExperimentID == "" {
		return errors.New("experiment phase: experiment_id required")
	}
	if p.Name == "" {
		return errors.New("experiment phase: name required")
	}
	if p.Position < 0 {
		return errors.New("experiment phase: position must be >= 0")
	}
	if !validPhaseStatuses[p.Status] {
		return fmt.Errorf("experiment phase: invalid status %q", p.Status)
	}
	for i, cfg := range p.FrozenServices {
		c := cfg
		if err := c.Validate(); err != nil {
			return fmt.Errorf("experiment phase: frozen_services[%d]: %w", i, err)
		}
	}
	return nil
}

// PhaseWorkflow is the per-(phase, workflow) attack configuration.
// VUs and DurationSec are required; RateRPS, TargetURL, TargetMethod
// are overrides for vegeta-style precision attacks (optional — defaults
// come from the zeus workflow definition). ZeusAttackID is populated by
// the orchestrator once the attack starts.
type PhaseWorkflow struct {
	PhaseID      string  `json:"phase_id"`
	WorkflowID   string  `json:"workflow_id"`
	VUs          int     `json:"vus"`
	RateRPS      float64 `json:"rate_rps,omitempty"`
	DurationSec  int     `json:"duration_sec"`
	TargetURL    string  `json:"target_url,omitempty"`
	TargetMethod string  `json:"target_method,omitempty"`
	// ZeusAttackID is the flat vegeta attack handle (additive load against
	// TargetURL); ZeusRunID is the k6 workflow-run handle (the DSL DAG). A
	// phase workflow drives both independently -- the poller waits for both.
	ZeusAttackID string `json:"zeus_attack_id,omitempty"`
	ZeusRunID    string `json:"zeus_run_id,omitempty"`
}

func (pw *PhaseWorkflow) Validate() error {
	if pw.PhaseID == "" {
		return errors.New("phase workflow: phase_id required")
	}
	if pw.WorkflowID == "" {
		return errors.New("phase workflow: workflow_id required")
	}
	if pw.VUs <= 0 {
		return errors.New("phase workflow: vus must be > 0")
	}
	if pw.DurationSec <= 0 {
		return errors.New("phase workflow: duration_sec must be > 0")
	}
	if pw.RateRPS < 0 {
		return errors.New("phase workflow: rate_rps must be >= 0")
	}
	return nil
}

// PhaseRule links a phase to a rule that applies during execution.
// Position determines apply order within the phase.
type PhaseRule struct {
	PhaseID  string `json:"phase_id"`
	RuleID   string `json:"rule_id"`
	Position int    `json:"position"`
}

func (pr *PhaseRule) Validate() error {
	if pr.PhaseID == "" {
		return errors.New("phase rule: phase_id required")
	}
	if pr.RuleID == "" {
		return errors.New("phase rule: rule_id required")
	}
	if pr.Position < 0 {
		return errors.New("phase rule: position must be >= 0")
	}
	return nil
}

var validFaultEventSources = setOf(FaultEventSourceValues...)

// PhaseFaultEvent is one entry in a phase's fault apply→clear audit trail.
// EndedAt is nil while the fault is active; the orchestrator stamps it when
// the phase clears rules / thaws cache-box at finish.
type PhaseFaultEvent struct {
	ID        string          `json:"id"`
	PhaseID   string          `json:"phase_id"`
	Source    string          `json:"source"` // rule | cachebox | fault_config
	Service   string          `json:"service"`
	Kind      string          `json:"kind"` // "cachebox:replay", "inline:latency", …
	Detail    json.RawMessage `json:"detail,omitempty"`
	StartedAt time.Time       `json:"started_at"`
	EndedAt   *time.Time      `json:"ended_at,omitempty"`
}

func (e *PhaseFaultEvent) Validate() error {
	if e.ID == "" {
		return errors.New("phase fault event: id required")
	}
	if e.PhaseID == "" {
		return errors.New("phase fault event: phase_id required")
	}
	if !validFaultEventSources[e.Source] {
		return fmt.Errorf("phase fault event: invalid source %q", e.Source)
	}
	if e.Service == "" {
		return errors.New("phase fault event: service required")
	}
	if e.Kind == "" {
		return errors.New("phase fault event: kind required")
	}
	return nil
}

// PhaseListItem is the cross-experiment list projection for GET /api/v1/phases
// (a "run" row). Composed by ExperimentRepo.ListPhasesPaged.
type PhaseListItem struct {
	ID                 string     `json:"id"`
	ExperimentID       string     `json:"experiment_id"`
	ExperimentName     string     `json:"experiment_name"`
	Name               string     `json:"name"`
	Position           int        `json:"position"`
	WorkflowIDs        []string   `json:"workflow_ids"`
	FrozenServiceCount int        `json:"frozen_service_count"`
	Status             string     `json:"status"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
}

// ---------- Results ----------

// PhaseWorkflowResult is the end-to-end latency and throughput for one
// (phase, workflow) pair. Every field is required; null/optional fields
// of the old shape were the source of "is this row populated?" ambiguity.
type PhaseWorkflowResult struct {
	PhaseID       string          `json:"phase_id"`
	WorkflowID    string          `json:"workflow_id"`
	RequestCount  int64           `json:"request_count"`
	ErrorCount    int64           `json:"error_count"`
	ErrorRate     float64         `json:"error_rate"`
	ThroughputRPS float64         `json:"throughput_rps"`
	LatencyP50Us  int64           `json:"latency_p50_us"`
	LatencyP95Us  int64           `json:"latency_p95_us"`
	LatencyP99Us  int64           `json:"latency_p99_us"`
	LatencyP999Us int64           `json:"latency_p999_us"`
	ComputedAt    time.Time       `json:"computed_at"`
	RawMetrics    json.RawMessage `json:"raw_metrics,omitempty"`
}

func (r *PhaseWorkflowResult) Validate() error {
	if r.PhaseID == "" {
		return errors.New("phase workflow result: phase_id required")
	}
	if r.WorkflowID == "" {
		return errors.New("phase workflow result: workflow_id required")
	}
	if r.RequestCount < 0 {
		return errors.New("phase workflow result: request_count must be >= 0")
	}
	if r.ErrorCount < 0 {
		return errors.New("phase workflow result: error_count must be >= 0")
	}
	return nil
}

// PhaseServiceLatency is per-(phase, service[, workflow]) request-side
// latency. WorkflowID == "" means service-wide (all workflows combined).
// Row presence is the existence signal — every metric column is required.
type PhaseServiceLatency struct {
	PhaseID      string    `json:"phase_id"`
	Service      string    `json:"service"`
	WorkflowID   string    `json:"workflow_id"`
	LatencyP50Us int64     `json:"latency_p50_us"`
	LatencyP95Us int64     `json:"latency_p95_us"`
	LatencyP99Us int64     `json:"latency_p99_us"`
	RequestCount int64     `json:"request_count"`
	ComputedAt   time.Time `json:"computed_at"`
}

func (r *PhaseServiceLatency) Validate() error {
	if r.PhaseID == "" {
		return errors.New("phase service latency: phase_id required")
	}
	if r.Service == "" {
		return errors.New("phase service latency: service required")
	}
	return nil
}

// PhaseServiceCache is per-(phase, service) cache-box fidelity stats. A row
// is written for a service frozen in replay mode (hit_rate/request_count) and
// for a service that recorded into a baseline phase (recorded_entry_count).
type PhaseServiceCache struct {
	PhaseID         string  `json:"phase_id"`
	Service         string  `json:"service"`
	CacheHitRate    float64 `json:"cache_hit_rate"`
	CacheExactMatch float64 `json:"cache_exact_match"`
	CacheStaleness  float64 `json:"cache_staleness"`
	// RequestCount is hits+misses observed against the frozen (replay) service
	// — disambiguates "0 requests served" from "all misses" (both hit_rate 0).
	RequestCount int64 `json:"request_count"`
	// RecordedEntryCount is the recording coverage: distinct entries captured
	// for this service on a baseline phase, or available to replay on an
	// isolation phase. Together with hit_rate it verifies recording fidelity.
	RecordedEntryCount int64     `json:"recorded_entry_count"`
	ComputedAt         time.Time `json:"computed_at"`
}

func (r *PhaseServiceCache) Validate() error {
	if r.PhaseID == "" {
		return errors.New("phase service cache: phase_id required")
	}
	if r.Service == "" {
		return errors.New("phase service cache: service required")
	}
	return nil
}

// ExperimentResults is the per-experiment rollup. Recomputed when any
// phase transitions to a terminal status, or when the experiment itself
// reaches a terminal status.
//
// WorstP99 / BestP99 are the max / min of per-phase p99s, NOT a true
// experiment-level p99 (which would require the underlying histograms,
// not just precomputed percentiles). The "worst phase" framing is more
// honest than a wrong-but-precise aggregate; see the data-model doc's
// "percentile-aggregation caveat" section.
type ExperimentResults struct {
	ExperimentID        string    `json:"experiment_id"`
	PhaseCount          int       `json:"phase_count"`
	CompletedPhaseCount int       `json:"completed_phase_count"`
	TotalRequestCount   int64     `json:"total_request_count"`
	TotalErrorCount     int64     `json:"total_error_count"`
	OverallErrorRate    float64   `json:"overall_error_rate"`
	WorstP99Us          int64     `json:"worst_p99_us"`
	WorstP99PhaseID     string    `json:"worst_p99_phase_id"`
	BestP99Us           int64     `json:"best_p99_us"`
	BestP99PhaseID      string    `json:"best_p99_phase_id"`
	ComputedAt          time.Time `json:"computed_at"`
}

func (r *ExperimentResults) Validate() error {
	if r.ExperimentID == "" {
		return errors.New("experiment results: experiment_id required")
	}
	if r.PhaseCount < 0 || r.CompletedPhaseCount < 0 {
		return errors.New("experiment results: counts must be >= 0")
	}
	if r.CompletedPhaseCount > r.PhaseCount {
		return errors.New("experiment results: completed_phase_count > phase_count")
	}
	return nil
}
