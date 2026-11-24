// Package demo runs the public demo: anyone may queue a run of one scenario of
// a fixed catalog (three 40 s phases: baseline with no fault, a memory fault,
// then a CPU fault, each in the scenario's target service) and watch it run.
// Its central promise is that no fault stays served after a run ends. Each run
// gets its own two fault rules, created disabled and each attached to its own
// fault phase; they are enabled only just before the run starts and disabled
// on every ending, and a disabled rule is never served.
//
// This file is the public contract: the exact JSON every demo route and event
// returns. No body ever carries an experiment, phase, rule or zeus id, a
// hostname, an IP address or any error text. Reasons and error codes are
// closed types: they cannot be built from a string outside this package, and
// they refuse to marshal any value outside the contract's list.
package demo

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"time"
)

// Names fixed by the contract.
const (
	// HeaderClient carries the client key the per-client limits use.
	HeaderClient = "X-Demo-Client"
	// HeaderEdgeSecret must carry MANTEION_DEMO_EDGE_SECRET on POST /runs.
	HeaderEdgeSecret = "X-Demo-Edge-Secret"
	// DefaultClient is the client key of a request without HeaderClient.
	DefaultClient = "local"
	// MaxClientBytes is the longest client key accepted; a longer one is 400.
	MaxClientBytes = 200

	// PhaseDurationS is each phase's load duration, in seconds.
	PhaseDurationS = 40
)

// PhaseNames are a run's phases in order: no fault, the memory fault, the CPU
// fault.
var PhaseNames = [3]string{"baseline", "memory", "cpu"}

// The three Prometheus queries the live samples use, verbatim.
const (
	QueryRPS       = `sum(rate(http_server_requests_total{service="frontend"}[30s]))`
	QueryP95Ms     = `1000 * histogram_quantile(0.95, sum by (le) (rate(http_server_request_duration_seconds_bucket{service="frontend"}[30s])))`
	QueryErrorRate = `(sum(rate(http_server_requests_total{service="frontend",status_code=~"5.."}[30s])) or vector(0)) / sum(rate(http_server_requests_total{service="frontend"}[30s]))`
)

// Event types of a run's event stream.
const (
	EventState  = "state"  // Run, always the first frame
	EventQueue  = "queue"  // QueueEvent
	EventPhase  = "phase"  // PhaseEvent
	EventSample = "sample" // Sample
	EventEnd    = "end"    // Run, the last frame
)

// Event is one frame of a run's event stream: Type is one of the Event*
// constants and Data the matching contract type.
type Event struct {
	Type string
	Data any
}

// ---- Statuses ----

// RunStatus is a run's status. Marshalling a value outside the contract's
// list is an error.
type RunStatus string

const (
	RunQueued    RunStatus = "queued"
	RunRunning   RunStatus = "running"
	RunCompleted RunStatus = "completed"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

func (s RunStatus) valid() bool {
	switch s {
	case RunQueued, RunRunning, RunCompleted, RunFailed, RunCancelled:
		return true
	}
	return false
}

func (s RunStatus) MarshalJSON() ([]byte, error) {
	if !s.valid() {
		return nil, errors.New("demo: run status outside the public list")
	}
	return json.Marshal(string(s))
}

func (s *RunStatus) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil || !RunStatus(v).valid() {
		return errors.New("demo: run status outside the public list")
	}
	*s = RunStatus(v)
	return nil
}

// PhaseStatus is a phase's status. The core's paused and draining are shown as
// running; marshalling a value outside the contract's list is an error, so a
// core status that was not mapped cannot reach a client.
type PhaseStatus string

const (
	PhasePending   PhaseStatus = "pending"
	PhaseRunning   PhaseStatus = "running"
	PhaseCompleted PhaseStatus = "completed"
	PhaseFailed    PhaseStatus = "failed"
	PhaseSkipped   PhaseStatus = "skipped"
)

func (s PhaseStatus) valid() bool {
	switch s {
	case PhasePending, PhaseRunning, PhaseCompleted, PhaseFailed, PhaseSkipped:
		return true
	}
	return false
}

func (s PhaseStatus) MarshalJSON() ([]byte, error) {
	if !s.valid() {
		return nil, errors.New("demo: phase status outside the public list")
	}
	return json.Marshal(string(s))
}

func (s *PhaseStatus) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil || !PhaseStatus(v).valid() {
		return errors.New("demo: phase status outside the public list")
	}
	*s = PhaseStatus(v)
	return nil
}

// ---- Reasons ----

// Reason says why a run ended without completing. The zero Reason is JSON
// null (a run that completed or has not ended). Reason is a closed type: it
// has no conversion from string, only the Reason* values exist, and an
// internal error can never become one.
type Reason struct{ v string }

var (
	ReasonLabBusy                  = Reason{"lab_busy"}
	ReasonLoadGeneratorUnavailable = Reason{"load_generator_unavailable"}
	ReasonTargetUnavailable        = Reason{"target_unavailable"}
	ReasonPhaseFailed              = Reason{"phase_failed"}
	ReasonTimedOut                 = Reason{"timed_out"}
	ReasonInterrupted              = Reason{"interrupted"}
	ReasonInternalError            = Reason{"internal_error"}
)

var reasonList = []Reason{
	ReasonLabBusy, ReasonLoadGeneratorUnavailable, ReasonTargetUnavailable,
	ReasonPhaseFailed, ReasonTimedOut, ReasonInterrupted, ReasonInternalError,
}

var errReason = errors.New("demo: reason outside the public list")

// IsZero reports whether r is the null reason.
func (r Reason) IsZero() bool { return r.v == "" }

// String returns the reason's wire value ("" for the null reason).
func (r Reason) String() string { return r.v }

func (r Reason) known() bool {
	for _, k := range reasonList {
		if r == k {
			return true
		}
	}
	return false
}

func (r Reason) MarshalJSON() ([]byte, error) {
	if r.IsZero() {
		return []byte("null"), nil
	}
	if !r.known() {
		return nil, errReason
	}
	return json.Marshal(r.v)
}

func (r *Reason) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*r = Reason{}
		return nil
	}
	var v string
	if err := json.Unmarshal(b, &v); err != nil || !(Reason{v}).known() {
		return errReason
	}
	*r = Reason{v}
	return nil
}

// ---- Error codes ----

// Code is the error code of an error body. Like Reason it is closed: only the
// Code* values exist, and the zero Code refuses to marshal.
type Code struct{ v string }

var (
	CodeBadRequest     = Code{"bad_request"}
	CodeForbidden      = Code{"forbidden"}
	CodeNotFound       = Code{"not_found"}
	CodeClientHasRun   = Code{"client_has_run"}
	CodeQueueFull      = Code{"queue_full"}
	CodeRateLimited    = Code{"rate_limited"}
	CodeTooManyStreams = Code{"too_many_streams"}
	CodeUnavailable    = Code{"unavailable"}
	CodeInternal       = Code{"internal"}
)

var codeStatus = map[Code]int{
	CodeBadRequest:     http.StatusBadRequest,
	CodeForbidden:      http.StatusForbidden,
	CodeNotFound:       http.StatusNotFound,
	CodeClientHasRun:   http.StatusTooManyRequests,
	CodeQueueFull:      http.StatusTooManyRequests,
	CodeRateLimited:    http.StatusTooManyRequests,
	CodeTooManyStreams: http.StatusTooManyRequests,
	CodeUnavailable:    http.StatusServiceUnavailable,
	CodeInternal:       http.StatusInternalServerError,
}

var errCode = errors.New("demo: error code outside the public list")

// String returns the code's wire value.
func (c Code) String() string { return c.v }

// HTTPStatus is the status the contract pairs with the code (500 for a code
// outside the list).
func (c Code) HTTPStatus() int {
	if s, ok := codeStatus[c]; ok {
		return s
	}
	return http.StatusInternalServerError
}

func (c Code) MarshalJSON() ([]byte, error) {
	if _, ok := codeStatus[c]; !ok {
		return nil, errCode
	}
	return json.Marshal(c.v)
}

func (c *Code) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return errCode
	}
	if _, ok := codeStatus[Code{v}]; !ok {
		return errCode
	}
	*c = Code{v}
	return nil
}

// ErrorBody is every error response: {"error": Code}.
type ErrorBody struct {
	Error Code `json:"error"`
}

// ---- Runs ----

// Run is one queued, running or finished run (GET /runs/{id}, the state and
// end frames). Every key is always present; Reason, Position, StartedAt and
// EndedAt are null when unset. Position is 0 for the running run, 1 or 2
// while waiting, and null once the run has ended. Scenario is the id of the
// catalog scenario the run runs.
type Run struct {
	RunID     string       `json:"run_id"`
	Scenario  string       `json:"scenario"`
	Status    RunStatus    `json:"status"`
	Reason    Reason       `json:"reason"`
	Position  *int         `json:"position"`
	QueuedAt  time.Time    `json:"queued_at"`
	StartedAt *time.Time   `json:"started_at"`
	EndedAt   *time.Time   `json:"ended_at"`
	Phases    [3]PhaseView `json:"phases"`
	Testbed   Testbed      `json:"testbed"`
}

func (r Run) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		RunID     string       `json:"run_id"`
		Scenario  string       `json:"scenario"`
		Status    RunStatus    `json:"status"`
		Reason    Reason       `json:"reason"`
		Position  *int         `json:"position"`
		QueuedAt  string       `json:"queued_at"`
		StartedAt *string      `json:"started_at"`
		EndedAt   *string      `json:"ended_at"`
		Phases    [3]PhaseView `json:"phases"`
		Testbed   Testbed      `json:"testbed"`
	}{r.RunID, r.Scenario, r.Status, r.Reason, r.Position, stamp(r.QueuedAt), stampPtr(r.StartedAt), stampPtr(r.EndedAt), r.Phases, r.Testbed})
}

// PhaseView is one phase of a Run. Fault, Target and Intensity are null on a
// phase without a fault (baseline); on a fault phase Fault is the fault's kind
// (memory or cpu), Target the service it runs in and Intensity its public
// label. Result is null until the phase's results are harvested. Measure is
// null until the fault phase has completed (its proof is measured then).
// FaultObserved marshals as null unless Measure holds both figures as finite
// numbers, whatever the view holds: this is the one place that rule is
// enforced.
type PhaseView struct {
	Name          string      `json:"name"`
	Fault         *string     `json:"fault"`
	Target        *string     `json:"target"`
	Intensity     *string     `json:"intensity"`
	Status        PhaseStatus `json:"status"`
	StartedAt     *time.Time  `json:"started_at"`
	EndedAt       *time.Time  `json:"ended_at"`
	DurationS     int         `json:"duration_s"`
	Result        *Result     `json:"result"`
	FaultObserved *bool       `json:"fault_observed"`
	Measure       *Measure    `json:"measure"`
}

func (p PhaseView) MarshalJSON() ([]byte, error) {
	observed := p.FaultObserved
	if p.Measure == nil || finite(p.Measure.Value) == nil || finite(p.Measure.Baseline) == nil {
		observed = nil
	}
	return json.Marshal(struct {
		Name          string      `json:"name"`
		Fault         *string     `json:"fault"`
		Target        *string     `json:"target"`
		Intensity     *string     `json:"intensity"`
		Status        PhaseStatus `json:"status"`
		StartedAt     *string     `json:"started_at"`
		EndedAt       *string     `json:"ended_at"`
		DurationS     int         `json:"duration_s"`
		Result        *Result     `json:"result"`
		FaultObserved *bool       `json:"fault_observed"`
		Measure       *Measure    `json:"measure"`
	}{p.Name, p.Fault, p.Target, p.Intensity, p.Status, stampPtr(p.StartedAt), stampPtr(p.EndedAt), p.DurationS, p.Result,
		observed, p.Measure})
}

// Names of a Measure's metric.
const (
	// MetricTargetMemoryBytes: the target's resident memory, in bytes.
	MetricTargetMemoryBytes = "target_memory_bytes"
	// MetricTargetCPUCores: the target's CPU use, in cores.
	MetricTargetCPUCores = "target_cpu_cores"
)

var errMetric = errors.New("demo: measure metric outside the public list")

// Measure is a fault phase's measured proof: the target's own memory (memory
// phase: its peak during the phase as Value, its lowest during the baseline
// phase as Baseline) or CPU (cpu phase: over the phase as Value, over the
// baseline phase as Baseline), each read over its phase's own window. A
// figure whose query failed is nil; nil, NaN and ±Inf marshal as null. Metric
// must be MetricTargetMemoryBytes or MetricTargetCPUCores: any other refuses
// to marshal.
type Measure struct {
	Metric   string   `json:"metric"`
	Value    *float64 `json:"value"`
	Baseline *float64 `json:"baseline"`
}

func (m Measure) MarshalJSON() ([]byte, error) {
	if m.Metric != MetricTargetMemoryBytes && m.Metric != MetricTargetCPUCores {
		return nil, errMetric
	}
	return json.Marshal(struct {
		Metric   string   `json:"metric"`
		Value    *float64 `json:"value"`
		Baseline *float64 `json:"baseline"`
	}{m.Metric, finite(m.Value), finite(m.Baseline)})
}

// Result is a finished phase's load result, from its phase_workflow_results
// row: request_count, error_count (failed HTTP requests) and latency_p*_us/1000.
// The contract types the latencies as numbers, not nullable, so a NaN or
// infinite latency marshals as 0 rather than failing the whole response.
type Result struct {
	Requests   int64   `json:"requests"`
	HTTPErrors int64   `json:"http_errors"`
	P50Ms      float64 `json:"p50_ms"`
	P95Ms      float64 `json:"p95_ms"`
	P99Ms      float64 `json:"p99_ms"`
}

func (r Result) MarshalJSON() ([]byte, error) {
	type plain Result
	p := plain(r)
	p.P50Ms, p.P95Ms, p.P99Ms = finiteOr0(p.P50Ms), finiteOr0(p.P95Ms), finiteOr0(p.P99Ms)
	return json.Marshal(p)
}

// Testbed describes the machine the demo runs on. It is fixed: use
// FixedTestbed.
type Testbed struct {
	Machine        string    `json:"machine"`
	CPULimitsM     CPULimits `json:"cpu_limits_m"`
	PhaseDurationS int       `json:"phase_duration_s"`
	Note           string    `json:"note"`
}

// CPULimits are the CPU limits in millicores of the parts a visitor sees:
// the frontend, every service a scenario targets, the load generator and the
// control plane.
type CPULimits struct {
	Frontend              int `json:"frontend"`
	ProductCatalogService int `json:"productcatalogservice"`
	CartService           int `json:"cartservice"`
	LoadGenerator         int `json:"load_generator"`
	ControlPlane          int `json:"control_plane"`
}

// FixedTestbed returns the contract's testbed description.
func FixedTestbed() Testbed {
	return Testbed{
		Machine:        "One shared 6-core machine that also runs unrelated services.",
		CPULimitsM:     CPULimits{Frontend: 200, ProductCatalogService: 200, CartService: 200, LoadGenerator: 950, ControlPlane: 200},
		PhaseDurationS: PhaseDurationS,
		Note:           "Figures vary from run to run.",
	}
}

// Enqueued is the 201 body of POST /runs.
type Enqueued struct {
	RunID    string    `json:"run_id"`
	Status   RunStatus `json:"status"`
	Position int       `json:"position"`
}

// RecentRuns is the body of GET /runs/recent; an empty list is [].
type RecentRuns struct {
	Runs []Run `json:"runs"`
}

func (r RecentRuns) MarshalJSON() ([]byte, error) {
	runs := r.Runs
	if runs == nil {
		runs = []Run{}
	}
	return json.Marshal(struct {
		Runs []Run `json:"runs"`
	}{runs})
}

// ---- Scenarios ----

// ScenarioList is the body of GET /scenarios: the catalog in order; an empty
// list is [].
type ScenarioList struct {
	Scenarios []ScenarioView `json:"scenarios"`
}

func (l ScenarioList) MarshalJSON() ([]byte, error) {
	s := l.Scenarios
	if s == nil {
		s = []ScenarioView{}
	}
	return json.Marshal(struct {
		Scenarios []ScenarioView `json:"scenarios"`
	}{s})
}

// ScenarioView is a scenario as a visitor sees it. It carries no fault
// parameter beyond the intensity label.
type ScenarioView struct {
	ID          string           `json:"id"`
	Title       string           `json:"title"`
	Description string           `json:"description"`
	Default     bool             `json:"default"`
	Workload    WorkloadView     `json:"workload"`
	Phases      [3]ScenarioPhase `json:"phases"`
}

// WorkloadView is the load a scenario's every phase sends: the workflow, the
// dataset it reads and a one-line description.
type WorkloadView struct {
	Workflow    string `json:"workflow"`
	Dataset     string `json:"dataset"`
	Description string `json:"description"`
}

// ScenarioPhase is one phase of a ScenarioView. Fault, Target and Intensity
// are null on the phase without a fault.
type ScenarioPhase struct {
	Name      string  `json:"name"`
	DurationS int     `json:"duration_s"`
	Fault     *string `json:"fault"`
	Target    *string `json:"target"`
	Intensity *string `json:"intensity"`
}

// ViewOf is sc as the contract shows it: its id, title, description, default
// flag and workload, and its three phases (no fault, the memory fault, the CPU
// fault) with each fault's kind, target and intensity label. No target_load.
func ViewOf(sc Scenario) ScenarioView {
	v := ScenarioView{
		ID:          sc.ID,
		Title:       sc.Title,
		Description: sc.Description,
		Default:     sc.Default,
		Workload:    WorkloadView{Workflow: sc.Workload.WorkflowID, Dataset: sc.Workload.DatasetName, Description: sc.Workload.Description},
	}
	steps := faultSteps(&sc)
	for i, name := range PhaseNames {
		p := ScenarioPhase{Name: name, DurationS: PhaseDurationS}
		if st := steps[i]; st != nil {
			kind, target, intensity := string(st.Kind), st.Target, st.Intensity
			p.Fault, p.Target, p.Intensity = &kind, &target, &intensity
		}
		v.Phases[i] = p
	}
	return v
}

// ---- State and health ----

// State is the body of GET /state. Queue lists every run that has not ended,
// the running one first; it is [] exactly when nothing is queued or running,
// and then Running is null.
type State struct {
	Capacity  int           `json:"capacity"`
	Accepting bool          `json:"accepting"`
	Queue     []QueueEntry  `json:"queue"`
	Running   *RunningPhase `json:"running"`
}

func (s State) MarshalJSON() ([]byte, error) {
	type plain State
	p := plain(s)
	if p.Queue == nil {
		p.Queue = []QueueEntry{}
	}
	return json.Marshal(p)
}

// QueueEntry is one run in State.Queue.
type QueueEntry struct {
	RunID    string    `json:"run_id"`
	Position int       `json:"position"`
	Status   RunStatus `json:"status"`
}

// RunningPhase is State.Running: where the position-0 run is. Progress
// marshals clamped to 0..1, NaN and ±Inf as 0.
type RunningPhase struct {
	RunID          string     `json:"run_id"`
	Phase          string     `json:"phase"`
	PhaseIndex     int        `json:"phase_index"`
	PhaseStartedAt *time.Time `json:"phase_started_at"`
	PhaseDurationS int        `json:"phase_duration_s"`
	Progress       float64    `json:"progress"`
}

func (r RunningPhase) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		RunID          string  `json:"run_id"`
		Phase          string  `json:"phase"`
		PhaseIndex     int     `json:"phase_index"`
		PhaseStartedAt *string `json:"phase_started_at"`
		PhaseDurationS int     `json:"phase_duration_s"`
		Progress       float64 `json:"progress"`
	}{r.RunID, r.Phase, r.PhaseIndex, stampPtr(r.PhaseStartedAt), r.PhaseDurationS, clampProgress(r.Progress)})
}

// clampProgress keeps progress in 0..1; NaN and ±Inf are 0.
func clampProgress(p float64) float64 {
	switch {
	case math.IsNaN(p) || math.IsInf(p, 0) || p < 0:
		return 0
	case p > 1:
		return 1
	}
	return p
}

// Health is the body of GET /health: 200 iff Checks.All(), else 503.
type Health struct {
	OK        bool         `json:"ok"`
	Accepting bool         `json:"accepting"`
	Checks    HealthChecks `json:"checks"`
}

// HealthChecks are the individual health checks.
type HealthChecks struct {
	Database      bool `json:"database"`
	LoadGenerator bool `json:"load_generator"`
	Metrics       bool `json:"metrics"`
	Target        bool `json:"target"`
	Idle          bool `json:"idle"`
}

// All reports whether every check passed.
func (c HealthChecks) All() bool {
	return c.Database && c.LoadGenerator && c.Metrics && c.Target && c.Idle
}

// ---- Event payloads ----

// QueueEvent is the data of a queue frame: the run's new position.
type QueueEvent struct {
	RunID    string `json:"run_id"`
	Position int    `json:"position"`
}

// PhaseEvent is the data of a phase frame.
type PhaseEvent struct {
	RunID      string      `json:"run_id"`
	Phase      string      `json:"phase"`
	PhaseIndex int         `json:"phase_index"`
	Status     PhaseStatus `json:"status"`
	At         time.Time   `json:"at"`
}

func (e PhaseEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		RunID      string      `json:"run_id"`
		Phase      string      `json:"phase"`
		PhaseIndex int         `json:"phase_index"`
		Status     PhaseStatus `json:"status"`
		At         string      `json:"at"`
	}{e.RunID, e.Phase, e.PhaseIndex, e.Status, stamp(e.At)})
}

// Sample is the data of a sample frame: the frontend's request rate, p95
// latency and error rate, and the running phase's target's own CPU (cores)
// and memory (bytes). A value whose query failed is nil; nil, NaN and ±Inf all
// marshal as null. A sample with every value null is not sent (see Empty).
type Sample struct {
	RunID             string    `json:"run_id"`
	Phase             string    `json:"phase"`
	At                time.Time `json:"at"`
	RPS               *float64  `json:"rps"`
	P95Ms             *float64  `json:"p95_ms"`
	ErrorRate         *float64  `json:"error_rate"`
	TargetCPUCores    *float64  `json:"target_cpu_cores"`
	TargetMemoryBytes *float64  `json:"target_memory_bytes"`
}

// Empty reports whether no value of the sample is a finite number.
func (s Sample) Empty() bool {
	return finite(s.RPS) == nil && finite(s.P95Ms) == nil && finite(s.ErrorRate) == nil &&
		finite(s.TargetCPUCores) == nil && finite(s.TargetMemoryBytes) == nil
}

func (s Sample) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		RunID             string   `json:"run_id"`
		Phase             string   `json:"phase"`
		At                string   `json:"at"`
		RPS               *float64 `json:"rps"`
		P95Ms             *float64 `json:"p95_ms"`
		ErrorRate         *float64 `json:"error_rate"`
		TargetCPUCores    *float64 `json:"target_cpu_cores"`
		TargetMemoryBytes *float64 `json:"target_memory_bytes"`
	}{s.RunID, s.Phase, stamp(s.At), finite(s.RPS), finite(s.P95Ms), finite(s.ErrorRate),
		finite(s.TargetCPUCores), finite(s.TargetMemoryBytes)})
}

// finiteOr0 returns v, or 0 when v is NaN or infinite.
func finiteOr0(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

// finite returns v, or nil when v is nil, NaN or infinite.
func finite(v *float64) *float64 {
	if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return v
}

// ---- Times ----

// wireTime is RFC 3339 in UTC with milliseconds, the form JavaScript's Date
// parses everywhere.
const wireTime = "2006-01-02T15:04:05.000Z07:00"

func stamp(t time.Time) string { return t.UTC().Format(wireTime) }

func stampPtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := stamp(*t)
	return &s
}
