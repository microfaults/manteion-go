// Package orchestrator drives experiment execution on the phase-first
// (epoch-2) model: experiments → experiment_phases → phase_workflows.
//
// The phase is the FSM unit (pending → running ⇄ paused → completed |
// failed | skipped). Race safety rests on three primitives ported from the
// legacy run FSM (commits e81fdb0, 5153d5f):
//
//  1. compare-and-swap status transitions in SQL (store.TransitionPhase /
//     TransitionExperiment) — exactly one of N racing callers wins;
//  2. a per-experiment lock serializing the phase scheduler
//     (advanceExperiment) so concurrent phase completions can't double-start;
//  3. a single terminal path (finishPhase, runner.go) whose CAS winner runs
//     all side effects — teardown, harvest, rollup — exactly once.
//
// Experiment-level pause is DERIVED state: the experiment_status enum has no
// 'paused' label (phase_status does, deliberately). PauseExperiment pauses
// the running phase(s) and leaves the experiment 'running'; a paused phase
// blocks the scheduler, which is what makes the pause stick.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/promql"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// Orchestrator drives experiment execution: starting/stopping phases,
// pushing rules, freezing cache-box services, supervising zeus attacks,
// harvesting results, and updating phase status as the experiment progresses.
type Orchestrator struct {
	experiments *store.ExperimentRepo
	rules       *store.RuleRepo
	faults      *store.FaultRepo
	workloads   *store.WorkloadRepo
	workflows   *store.WorkflowRepo
	controller  *atrocontrol.Controller
	prom        *promql.Client // reserved for metric-driven transitions (see policy-engine freeze decision)
	zeusClient  *zeus.Client
	cacheStore  *cachestore.Store
	faultEvents *store.PhaseFaultEventRepo
	logger      *slog.Logger

	maxPollDuration time.Duration
	pollInterval    time.Duration
	autoComplete    bool // auto-complete driver-less phases (off in FSM unit tests)

	// Drain barrier (MANT-2). drainTimeout bounds the wait for every expected
	// SDK to flush + drain-report; drainPollInterval is how often the gate
	// re-checks. allowDegradedBaseline lets an isolation phase start from a
	// degraded recording (MANTEION_ALLOW_DEGRADED_BASELINE).
	drainTimeout          time.Duration
	drainPollInterval     time.Duration
	allowDegradedBaseline bool

	mu       sync.Mutex
	running  map[string]context.CancelFunc // phase ID → poller cancel
	expLocks map[string]*sync.Mutex        // experiment ID → scheduler serializer
}

// New constructs an Orchestrator.
func New(
	experiments *store.ExperimentRepo,
	rules *store.RuleRepo,
	faults *store.FaultRepo,
	workloads *store.WorkloadRepo,
	workflows *store.WorkflowRepo,
	controller *atrocontrol.Controller,
	prom *promql.Client,
	zeusClient *zeus.Client,
	cs *cachestore.Store,
	faultEvents *store.PhaseFaultEventRepo,
	logger *slog.Logger,
) *Orchestrator {
	return &Orchestrator{
		experiments:       experiments,
		rules:             rules,
		faults:            faults,
		workloads:         workloads,
		workflows:         workflows,
		controller:        controller,
		prom:              prom,
		zeusClient:        zeusClient,
		cacheStore:        cs,
		faultEvents:       faultEvents,
		logger:            logger,
		maxPollDuration:   defaultMaxPollDuration,
		pollInterval:      defaultZeusPollInterval,
		autoComplete:      true,
		drainTimeout:      30 * time.Second,
		drainPollInterval: 200 * time.Millisecond,
		running:           make(map[string]context.CancelFunc),
		expLocks:          make(map[string]*sync.Mutex),
	}
}

// WithDrainTimeout overrides the recording-phase drain barrier's bound
// (MANTEION_DRAIN_TIMEOUT; must be ≥ 3× the SDK poll interval + flush time).
func (o *Orchestrator) WithDrainTimeout(d time.Duration) { o.drainTimeout = d }

// WithDrainPollInterval overrides how often the drain gate re-checks (test seam).
func (o *Orchestrator) WithDrainPollInterval(d time.Duration) { o.drainPollInterval = d }

// WithAllowDegradedBaseline lets isolation phases start from a degraded
// recording (MANTEION_ALLOW_DEGRADED_BASELINE).
func (o *Orchestrator) WithAllowDegradedBaseline(v bool) { o.allowDegradedBaseline = v }

// WithMaxPollDuration overrides the default Zeus poll timeout.
func (o *Orchestrator) WithMaxPollDuration(d time.Duration) {
	o.maxPollDuration = d
}

// WithPollInterval overrides the Zeus poll tick (test seam).
func (o *Orchestrator) WithPollInterval(d time.Duration) {
	o.pollInterval = d
}

// experimentLock returns the per-experiment mutex that serializes
// advanceExperiment so concurrent phase completions can't race the
// scheduler on a stale snapshot.
func (o *Orchestrator) experimentLock(experimentID string) *sync.Mutex {
	o.mu.Lock()
	defer o.mu.Unlock()
	lk, ok := o.expLocks[experimentID]
	if !ok {
		lk = &sync.Mutex{}
		o.expLocks[experimentID] = lk
	}
	return lk
}

// StartExperiment transitions an experiment from "planned" to "running" and
// starts its first pending phase via the scheduler. Returns an error if the
// experiment has no phases or is not planned.
func (o *Orchestrator) StartExperiment(ctx context.Context, experimentID string) error {
	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		return fmt.Errorf("orchestrator: list phases: %w", err)
	}
	if len(phases) == 0 {
		return errors.New("orchestrator: experiment has no phases")
	}

	// Admission control (MANT-5): refuse a service footprint that overlaps a
	// running experiment, so two experiments never fight over a service
	// (INV-5). Checked before the claim so a rejected start leaves the
	// experiment planned. Deliberately NOT overridable: the SDK holds one
	// replay set, one preload staging slot, and one freeze delay source per
	// instance, so shared-service concurrency doesn't degrade -- it silently
	// serves one experiment the other's data. Disjoint-footprint experiments
	// already pass this check without any flag.
	if err := o.checkServiceOverlap(ctx, experimentID); err != nil {
		return err
	}

	// Atomically claim planned → running so two concurrent starts can't both
	// walk the scheduler believing they own the kickoff.
	claimed, err := o.experiments.TransitionExperiment(ctx, experimentID, "running", "planned")
	if err != nil {
		return fmt.Errorf("orchestrator: claim experiment: %w", err)
	}
	if !claimed {
		return fmt.Errorf("orchestrator: experiment %q is not planned", experimentID)
	}

	o.advanceExperiment(ctx, experimentID)
	o.logger.Info("orchestrator: experiment started",
		"experiment_id", experimentID, "phases", len(phases))
	return nil
}

// PauseExperiment pauses a running experiment by pausing its running
// phase(s). The experiment row stays 'running' (the status enum has no
// 'paused'); the paused phase gates the scheduler, which is what halts
// progression. Returns an error when nothing was pausable.
func (o *Orchestrator) PauseExperiment(ctx context.Context, experimentID string) error {
	lk := o.experimentLock(experimentID)
	lk.Lock()
	defer lk.Unlock()

	exp, err := o.experiments.Get(ctx, experimentID)
	if err != nil {
		return err
	}
	if exp.Status != "running" {
		return fmt.Errorf("orchestrator: experiment %q is not running (status=%s)", experimentID, exp.Status)
	}

	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		return fmt.Errorf("orchestrator: list phases: %w", err)
	}
	paused := 0
	for _, p := range phases {
		if p.Status != "running" {
			continue
		}
		if err := o.PausePhase(ctx, p.ID); err != nil {
			o.logger.Warn("orchestrator: pause experiment: pause phase failed",
				"phase_id", p.ID, "error", err)
			continue
		}
		paused++
	}
	if paused == 0 {
		return fmt.Errorf("orchestrator: experiment %q has no running phase to pause", experimentID)
	}
	o.logger.Info("orchestrator: experiment paused", "experiment_id", experimentID)
	return nil
}

// ResumeExperiment resumes a paused experiment: every paused phase is
// restarted (rules re-pushed, attacks re-launched, poller respawned), then
// the scheduler re-walks in case the experiment stalled between phases.
func (o *Orchestrator) ResumeExperiment(ctx context.Context, experimentID string) error {
	lk := o.experimentLock(experimentID)
	lk.Lock()

	exp, err := o.experiments.Get(ctx, experimentID)
	if err != nil {
		lk.Unlock()
		return err
	}
	if exp.Status != "running" {
		lk.Unlock()
		return fmt.Errorf("orchestrator: experiment %q is not running (status=%s)", experimentID, exp.Status)
	}

	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		lk.Unlock()
		return fmt.Errorf("orchestrator: list phases: %w", err)
	}
	resumed := 0
	for _, p := range phases {
		if p.Status != "paused" {
			continue
		}
		if err := o.StartPhase(ctx, p.ID); err != nil {
			o.logger.Warn("orchestrator: resume experiment: resume phase failed",
				"phase_id", p.ID, "error", err)
			continue
		}
		resumed++
	}
	if resumed == 0 {
		lk.Unlock()
		return fmt.Errorf("orchestrator: experiment %q has no paused phase to resume", experimentID)
	}
	o.logger.Info("orchestrator: experiment resumed", "experiment_id", experimentID)
	lk.Unlock()

	// Re-walk the scheduler (acquires the lock itself) to heal any stall.
	o.advanceExperiment(ctx, experimentID)
	return nil
}

// CancelExperiment cancels a planned/running experiment: every non-terminal
// phase is finalized as "skipped" (tearing down rules, freezes, and zeus
// attacks) and the experiment becomes 'cancelled'.
func (o *Orchestrator) CancelExperiment(ctx context.Context, experimentID string) error {
	return o.terminateExperiment(ctx, experimentID, "cancelled")
}

// StopExperiment is the operator terminal entry point kept for the /stop
// endpoint: finalize the experiment as completed/failed/cancelled (default
// cancelled), skipping all non-terminal phases.
func (o *Orchestrator) StopExperiment(ctx context.Context, experimentID, finalStatus string) error {
	switch finalStatus {
	case "":
		finalStatus = "cancelled"
	case "completed", "failed", "cancelled":
	default:
		return fmt.Errorf("orchestrator: invalid final status %q", finalStatus)
	}
	return o.terminateExperiment(ctx, experimentID, finalStatus)
}

// terminateExperiment is the shared cancel/stop path: CAS the experiment to
// the terminal status, then finalize every non-terminal phase as "skipped".
func (o *Orchestrator) terminateExperiment(ctx context.Context, experimentID, finalStatus string) error {
	lk := o.experimentLock(experimentID)
	lk.Lock()
	defer lk.Unlock()

	terminated, err := o.experiments.TransitionExperiment(ctx, experimentID, finalStatus,
		"planned", "running")
	if err != nil {
		return err
	}
	if !terminated {
		return fmt.Errorf("orchestrator: experiment %q is already terminal", experimentID)
	}

	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		return fmt.Errorf("orchestrator: list phases: %w", err)
	}
	for _, p := range phases {
		switch p.Status {
		case "running", "paused", "draining":
			// finishPhase tears down the poller, rules, freezes, and attacks. A
			// draining phase is skipped straight from draining (no re-drain).
			o.finishPhase(ctx, p.ID, "skipped", "running", "paused", "draining")
		case "pending":
			if _, err := o.experiments.TransitionPhase(ctx, p.ID, "skipped", "pending"); err != nil {
				o.logger.Warn("orchestrator: terminate: skip pending phase failed",
					"phase_id", p.ID, "error", err)
			}
		}
	}
	o.logger.Info("orchestrator: experiment terminated",
		"experiment_id", experimentID, "status", finalStatus)
	return nil
}

// advanceExperiment is the sequential phase scheduler. Phases run one at a
// time in position order; a failed phase skips everything after it (the
// sequential analogue of the old DAG failure cascade); when no phase remains
// startable the experiment is finalized. Idempotent — safe to call
// repeatedly; called from StartExperiment, ResumeExperiment, finishPhase,
// and Recover.
func (o *Orchestrator) advanceExperiment(ctx context.Context, experimentID string) {
	lk := o.experimentLock(experimentID)
	lk.Lock()
	defer lk.Unlock()

	// Only advance a running experiment — cancelled/terminal experiments must
	// not start phases. (Pause is phase-level: a paused phase returns below.)
	exp, err := o.experiments.Get(ctx, experimentID)
	if err != nil {
		o.logger.Warn("orchestrator: advance: get experiment failed",
			"experiment_id", experimentID, "error", err)
		return
	}
	if exp.Status != "running" {
		return
	}

	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		o.logger.Warn("orchestrator: advance: list phases failed",
			"experiment_id", experimentID, "error", err)
		return
	}

	anyFailed := false
	for _, p := range phases {
		switch p.Status {
		case "running", "paused", "draining":
			// A phase in flight (running), paused by the operator, or draining
			// (completing through the drain barrier) blocks the scheduler —
			// sequential execution, the pause gate, and the drain gate.
			return
		case "failed":
			anyFailed = true
		}
	}

	// Failure cascade: later phases assume their predecessors ran, so a
	// failed phase invalidates everything still pending.
	if anyFailed {
		for _, p := range phases {
			if p.Status != "pending" {
				continue
			}
			if _, err := o.experiments.TransitionPhase(ctx, p.ID, "skipped", "pending"); err != nil {
				o.logger.Warn("orchestrator: advance: cascade-skip phase failed",
					"phase_id", p.ID, "error", err)
			} else {
				o.logger.Info("orchestrator: phase skipped (earlier phase failed)",
					"phase_id", p.ID)
			}
		}
		o.finalizeExperiment(ctx, experimentID, "failed")
		return
	}

	// Start the first pending phase.
	for _, p := range phases {
		if p.Status != "pending" {
			continue
		}
		if err := o.StartPhase(ctx, p.ID); err != nil {
			// StartPhase finalizes the phase as failed on enter-sequence
			// errors; the resulting finishPhase re-advances asynchronously
			// and the cascade above collapses the rest.
			o.logger.Error("orchestrator: advance: start phase failed",
				"experiment_id", experimentID, "phase_id", p.ID, "error", err)
		}
		return
	}

	// Nothing pending, running, or paused — every phase is terminal.
	o.finalizeExperiment(ctx, experimentID, "completed")
}

// finalizeExperiment CASes running → terminal so a concurrent cancel/stop
// wins instead of being clobbered, then recomputes the rollup.
func (o *Orchestrator) finalizeExperiment(ctx context.Context, experimentID, status string) {
	ok, err := o.experiments.TransitionExperiment(ctx, experimentID, status, "running")
	if err != nil {
		o.logger.Warn("orchestrator: finalize experiment failed",
			"experiment_id", experimentID, "status", status, "error", err)
		return
	}
	if !ok {
		return
	}
	if _, err := o.experiments.RecomputeExperimentResults(ctx, experimentID); err != nil {
		o.logger.Warn("orchestrator: finalize: recompute results failed",
			"experiment_id", experimentID, "error", err)
	}
	o.logger.Info("orchestrator: experiment finalized",
		"experiment_id", experimentID, "status", status)
}

// Recover restores in-memory state from the DB after a process restart.
// For every 'running' experiment:
//
//   - 'running' phases get their zeus attacks reconciled (GetAttack with
//     retries). All attacks lost → the phase is finalized 'failed'; survivors
//     → the poller goroutine is respawned; no attacks configured → the phase
//     auto-completes (it has no driver).
//   - 'paused' phases are left alone; they wait for an explicit resume.
//
// Finally the scheduler re-walks each experiment to cover a crash that
// landed between phases. Recover must be called before the API server
// starts accepting requests.
func (o *Orchestrator) Recover(ctx context.Context) error {
	exps, _, err := o.experiments.List(ctx,
		store.ExperimentFilter{Status: "running"}, store.Page{Limit: 200})
	if err != nil {
		return fmt.Errorf("recover: list running experiments: %w", err)
	}
	if len(exps) == 0 {
		o.logger.Info("orchestrator: recover: no in-flight experiments")
		return nil
	}

	for _, exp := range exps {
		phases, err := o.experiments.ListPhasesForExperiment(ctx, exp.ID)
		if err != nil {
			o.logger.Error("orchestrator: recover: list phases failed",
				"experiment_id", exp.ID, "error", err)
			continue
		}
		for _, p := range phases {
			switch p.Status {
			case "paused":
				o.logger.Info("orchestrator: recover: paused phase left in place",
					"phase_id", p.ID)
			case "running":
				o.recoverRunningPhase(ctx, p.ID)
			}
		}
		// Heal a crash between phases (nothing in flight, next never started).
		o.advanceExperiment(ctx, exp.ID)
	}
	return nil
}

// recoverRunningPhase reattaches to a phase that was running when the
// process died: reconcile its persisted attack IDs against zeus and either
// fail it (all lost), respawn its poller (survivors), or auto-complete it
// (no attacks were ever configured — a driver-less phase).
func (o *Orchestrator) recoverRunningPhase(ctx context.Context, phaseID string) {
	pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID)
	if err != nil {
		o.logger.Error("orchestrator: recover: list phase workflows failed",
			"phase_id", phaseID, "error", err)
		return
	}

	// Both load-driver kinds must be reconciled: a phase whose only driver is
	// a workflow run (no additive attack) must not be seen as driver-less and
	// wrongly auto-completed while its run is still generating traffic.
	var maxDur time.Duration
	drivers := 0
	for _, pw := range pws {
		if pw.ZeusAttackID != "" {
			drivers++
		}
		if pw.ZeusRunID != "" {
			drivers++
		}
		if d := time.Duration(pw.DurationSec) * time.Second; d > maxDur {
			maxDur = d
		}
	}

	if drivers == 0 {
		if o.autoComplete {
			o.logger.Info("orchestrator: recover: phase has no load drivers; auto-completing",
				"phase_id", phaseID)
			go o.finishPhase(context.Background(), phaseID, "completed", "running")
		}
		return
	}

	if o.zeusClient != nil {
		lost := 0
		for _, pw := range pws {
			if pw.ZeusAttackID != "" && !o.reconcileOneAttack(ctx, phaseID, pw.ZeusAttackID) {
				lost++
			}
			if pw.ZeusRunID != "" && !o.reconcileOneRun(ctx, phaseID, pw.ZeusRunID) {
				lost++
			}
		}
		if lost == drivers {
			o.logger.Warn("orchestrator: recover: all zeus load drivers lost; marking phase failed",
				"phase_id", phaseID)
			o.finishPhase(ctx, phaseID, "failed", "running")
			return
		}
		if lost > 0 {
			o.logger.Warn("orchestrator: recover: some zeus load drivers lost; continuing with survivors",
				"phase_id", phaseID, "lost", lost, "total", drivers)
		}
	}

	o.spawnPoller(phaseID, maxDur)
	o.logger.Info("orchestrator: recover: running phase restored",
		"phase_id", phaseID, "drivers", drivers)
}

const reconcileRetries = 3

var reconcileBackoff = [reconcileRetries]time.Duration{
	100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second,
}

// reconcileOneAttack calls Zeus.GetAttack with retries to tolerate transient
// errors. Reports whether zeus still knows the attack.
func (o *Orchestrator) reconcileOneAttack(ctx context.Context, phaseID, attackID string) bool {
	var lastErr error
	for attempt := 0; attempt < reconcileRetries; attempt++ {
		_, err := o.zeusClient.GetAttack(ctx, attackID)
		if err == nil {
			return true
		}
		lastErr = err
		if attempt < reconcileRetries-1 {
			time.Sleep(reconcileBackoff[attempt])
		}
	}
	o.logger.Warn("orchestrator: recover: zeus attack not found after retries",
		"phase_id", phaseID, "attack_id", attackID, "error", lastErr)
	return false
}

// reconcileOneRun calls Zeus.GetRun with retries. Reports whether zeus still
// knows the run (the k6 subprocess survives a manteion restart because it
// runs inside zeus; it is lost only if zeus itself restarted).
func (o *Orchestrator) reconcileOneRun(ctx context.Context, phaseID, runID string) bool {
	var lastErr error
	for attempt := 0; attempt < reconcileRetries; attempt++ {
		if _, err := o.zeusClient.GetRun(ctx, runID); err == nil {
			return true
		} else {
			lastErr = err
		}
		if attempt < reconcileRetries-1 {
			time.Sleep(reconcileBackoff[attempt])
		}
	}
	o.logger.Warn("orchestrator: recover: zeus run not found after retries",
		"phase_id", phaseID, "run_id", runID, "error", lastErr)
	return false
}
