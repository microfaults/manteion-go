package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

type Orchestrator struct {
	experiments *store.ExperimentRepo
	rules       *store.RuleRepo
	faults      *store.FaultRepo
	workloads   *store.WorkloadRepo
	controller  *atrocontrol.Controller
	zeusClient  *zeus.Client
	cacheStore  *cachestore.Store
	logger      *slog.Logger

	maxPollDuration time.Duration
	autoComplete    bool // auto-complete driver-less runs (off in FSM unit tests)

	mu       sync.Mutex
	running  map[string]*runHandles // run ID → cancel funcs
	expLocks map[string]*sync.Mutex // experiment ID → advanceExperiment serializer
}

// runHandles tracks the per-run goroutine cancel funcs. Only the Zeus poller
// remains (metric-driven phase watching was removed); its lifetime spans
// StartRun→terminal/PauseRun.
type runHandles struct {
	poller context.CancelFunc
}

func (h *runHandles) cancelAll() {
	if h == nil {
		return
	}
	if h.poller != nil {
		h.poller()
	}
}

func New(
	experiments *store.ExperimentRepo,
	rules *store.RuleRepo,
	faults *store.FaultRepo,
	workloads *store.WorkloadRepo,
	controller *atrocontrol.Controller,
	zeusClient *zeus.Client,
	cs *cachestore.Store,
	logger *slog.Logger,
) *Orchestrator {
	return &Orchestrator{
		experiments:     experiments,
		rules:           rules,
		faults:          faults,
		workloads:       workloads,
		controller:      controller,
		zeusClient:      zeusClient,
		cacheStore:      cs,
		logger:          logger,
		maxPollDuration: defaultMaxPollDuration,
		autoComplete:    true,
		running:         make(map[string]*runHandles),
		expLocks:        make(map[string]*sync.Mutex),
	}
}

// experimentLock returns the per-experiment mutex that serializes
// advanceExperiment so concurrent run completions can't race the DAG scheduler.
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

// WithMaxPollDuration overrides the default Zeus poll timeout.
func (o *Orchestrator) WithMaxPollDuration(d time.Duration) {
	o.maxPollDuration = d
}

// Recover restores in-memory state from the DB after a process restart.
// For every run in 'running': reconciles persisted Zeus attack IDs by calling
// GetAttack on each (marking the run failed if Zeus has lost all of them),
// and respawns the watcher and poller goroutines.
//
// For every run in 'paused': leaves them alone; the run waits for an explicit
// ResumeRun before its goroutines are revived.
//
// Recover should be called from main after orchestrator.New, before the API
// server starts accepting requests.
func (o *Orchestrator) Recover(ctx context.Context) error {
	runs, err := o.experiments.ListRunsByStatus(ctx, []string{"running", "paused"})
	if err != nil {
		return fmt.Errorf("recover: list in-flight runs: %w", err)
	}
	if len(runs) == 0 {
		o.logger.Info("orchestrator: recover: no in-flight runs")
		return nil
	}

	for _, run := range runs {
		if run.Status == "paused" {
			o.logger.Info("orchestrator: recover: paused run left in place",
				"run_id", run.ID)
			continue
		}

		// Status is "running" — reconcile Zeus state, respawn goroutines.
		if o.zeusClient != nil {
			lostAll, anyLost := o.reconcileAttacks(ctx, run)
			if lostAll && (len(run.ZeusAttackIDs) > 0 || run.ZeusAttackID != "") {
				o.logger.Warn("orchestrator: recover: all Zeus attacks lost; marking run failed",
					"run_id", run.ID)
				if err := o.experiments.UpdateRunStatus(ctx, run.ID, "failed"); err != nil {
					o.logger.Error("orchestrator: recover: update run failed status",
						"run_id", run.ID, "error", err)
				}
				continue
			}
			if anyLost {
				o.logger.Warn("orchestrator: recover: some Zeus attacks lost; continuing with survivors",
					"run_id", run.ID)
			}
		}

		handles := &runHandles{}
		if len(run.ZeusAttackIDs) > 0 || run.ZeusAttackID != "" {
			pollCtx, pollCancel := context.WithCancel(context.Background())
			handles.poller = pollCancel
			go o.pollZeusStatus(pollCtx, run.ID)
		}

		o.mu.Lock()
		o.running[run.ID] = handles
		o.mu.Unlock()

		if handles.poller == nil {
			// A 'running' row in the DB with no driver — orphaned by a crash
			// before any work began. Auto-complete to release it (only from
			// 'running', so a concurrent pause/stop wins the race).
			if o.autoComplete {
				o.logger.Info("orchestrator: recover: run has no poller; auto-completing",
					"run_id", run.ID)
				go o.finishRun(context.Background(), run.ID, "completed", "running")
			}
			continue
		}

		o.logger.Info("orchestrator: recover: running run restored",
			"run_id", run.ID, "phase", run.CurrentPhase)
	}
	return nil
}

const reconcileRetries = 3

// reconcileAttacks calls Zeus.GetAttack on each persisted attack ID with
// retries to tolerate transient errors. Returns (lostAll, anyLost): whether
// all attacks were lost (persistent failure from Zeus after retries), and
// whether at least one was. Used during Recover to detect runs whose Zeus
// state is gone.
func (o *Orchestrator) reconcileAttacks(ctx context.Context, run *model.ExperimentRun) (lostAll, anyLost bool) {
	ids := run.ZeusAttackIDs
	if len(ids) == 0 && run.ZeusAttackID != "" {
		ids = []string{run.ZeusAttackID}
	}
	if len(ids) == 0 {
		return false, false
	}

	lost := 0
	for _, id := range ids {
		if !o.reconcileOneAttack(ctx, run.ID, id) {
			anyLost = true
			lost++
		}
	}
	return lost == len(ids), anyLost
}

var reconcileBackoff = [reconcileRetries]time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}

func (o *Orchestrator) reconcileOneAttack(ctx context.Context, runID, attackID string) (ok bool) {
	var lastErr error
	for attempt := 0; attempt < reconcileRetries; attempt++ {
		_, err := o.zeusClient.GetAttack(ctx, attackID)
		if err == nil {
			return true
		}
		lastErr = err
		o.logger.Warn("orchestrator: recover: zeus attack check failed; retrying",
			"run_id", runID, "attack_id", attackID,
			"attempt", attempt+1, "error", err)
		if attempt < reconcileRetries-1 {
			time.Sleep(reconcileBackoff[attempt])
		}
	}
	o.logger.Warn("orchestrator: recover: zeus attack not found after retries",
		"run_id", runID, "attack_id", attackID, "error", lastErr)
	return false
}

// StartRun transitions a run from "pending" to "running" and begins
// phase execution. Returns error if the run is not in "pending" state.
func (o *Orchestrator) StartRun(ctx context.Context, runID string) error {
	run, err := o.experiments.GetRun(ctx, runID)
	if err != nil {
		return err
	}

	// Atomically claim the run: pending → running. If it doesn't transition the
	// run isn't pending — already started, or a concurrent StartRun (e.g. two
	// advanceExperiment goroutines) won the race — so abort rather than
	// double-start goroutines and Zeus attacks.
	claimed, err := o.experiments.TransitionRun(ctx, runID, "running", "pending")
	if err != nil {
		return err
	}
	if !claimed {
		return fmt.Errorf("run %q is not pending", runID)
	}

	if run.RunType != "baseline" {
		if err := o.preloadCacheEntries(ctx, run); err != nil {
			o.logger.Warn("orchestrator: cache preload failed",
				"run_id", runID, "error", err)
		}
	}

	if err := o.enterPhase(ctx, run, 0); err != nil {
		// Run was already claimed as running; finalize it failed so it isn't stuck.
		o.finishRun(ctx, runID, "failed", "running")
		return fmt.Errorf("enter phase 0: %w", err)
	}

	// Start Zeus load generation for all workloads.
	if o.zeusClient != nil {
		if attackIDs, err := o.startZeusAttacks(ctx, run); err != nil {
			o.logger.Warn("orchestrator: zeus attack start failed",
				"run_id", runID, "error", err)
		} else if len(attackIDs) > 0 {
			run.ZeusAttackIDs = attackIDs
			run.ZeusAttackID = attackIDs[0]
			if err := o.experiments.UpdateRunZeusAttacks(ctx, runID, attackIDs); err != nil {
				o.logger.Warn("orchestrator: update zeus_attack_ids failed",
					"run_id", runID, "error", err)
			}
		}
	}

	handles := &runHandles{}
	// Only poll Zeus when the run actually has attacks; otherwise the poller
	// would loop forever returning (false, false) and the run would hang.
	if len(run.ZeusAttackIDs) > 0 || run.ZeusAttackID != "" {
		pollCtx, pollCancel := context.WithCancel(context.Background())
		handles.poller = pollCancel
		go o.pollZeusStatus(pollCtx, runID)
	}

	o.mu.Lock()
	o.running[runID] = handles
	o.mu.Unlock()

	// No poller driving this run → nothing takes it to a terminal state.
	// Auto-complete (configured-no-op runs are valid — e.g. a placeholder run
	// fanning out to dependents). Only from 'running', so a concurrent
	// PauseRun/StopRun wins instead of being clobbered.
	if o.autoComplete && handles.poller == nil {
		o.logger.Info("orchestrator: run has no poller; auto-completing", "run_id", runID)
		go o.finishRun(context.Background(), runID, "completed", "running")
		return nil
	}

	o.logger.Info("orchestrator: run started", "run_id", runID, "phase", 0)
	return nil
}

// PauseRun suspends a running run: stops the Zeus attack and phase watcher,
// but preserves current phase so it can be resumed.
func (o *Orchestrator) PauseRun(ctx context.Context, runID string) error {
	run, err := o.experiments.GetRun(ctx, runID)
	if err != nil {
		return err
	}

	// Atomically claim running → paused; if it loses (a concurrent
	// completion/stop won), abort and leave the terminal status intact.
	paused, err := o.experiments.TransitionRun(ctx, runID, "paused", "running")
	if err != nil {
		return err
	}
	if !paused {
		return fmt.Errorf("run %q is not running", runID)
	}

	o.mu.Lock()
	if handles, ok := o.running[runID]; ok {
		handles.cancelAll()
		delete(o.running, runID)
	}
	o.mu.Unlock()

	for _, id := range run.ZeusAttackIDs {
		if err := o.zeusClient.StopAttack(ctx, id); err != nil {
			o.logger.Warn("orchestrator: stop zeus attack on pause failed",
				"run_id", runID, "attack_id", id, "error", err)
		}
	}
	if len(run.ZeusAttackIDs) == 0 && run.ZeusAttackID != "" {
		if err := o.zeusClient.StopAttack(ctx, run.ZeusAttackID); err != nil {
			o.logger.Warn("orchestrator: stop zeus attack on pause failed",
				"run_id", runID, "error", err)
		}
	}

	o.logger.Info("orchestrator: run paused", "run_id", runID)
	return nil
}

// ResumeRun restarts a paused run from its current phase.
func (o *Orchestrator) ResumeRun(ctx context.Context, runID string) error {
	run, err := o.experiments.GetRun(ctx, runID)
	if err != nil {
		return err
	}

	// Atomically claim paused → running.
	resumed, err := o.experiments.TransitionRun(ctx, runID, "running", "paused")
	if err != nil {
		return err
	}
	if !resumed {
		return fmt.Errorf("run %q is not paused", runID)
	}

	if err := o.enterPhase(ctx, run, run.CurrentPhase); err != nil {
		o.finishRun(ctx, runID, "failed", "running")
		return fmt.Errorf("re-enter phase %d: %w", run.CurrentPhase, err)
	}

	// Restart Zeus attacks.
	if o.zeusClient != nil {
		if attackIDs, err := o.startZeusAttacks(ctx, run); err != nil {
			o.logger.Warn("orchestrator: zeus attack restart failed",
				"run_id", runID, "error", err)
		} else if len(attackIDs) > 0 {
			run.ZeusAttackIDs = attackIDs
			run.ZeusAttackID = attackIDs[0]
			if err := o.experiments.UpdateRunZeusAttacks(ctx, runID, attackIDs); err != nil {
				o.logger.Warn("orchestrator: update zeus_attack_ids on resume failed",
					"run_id", runID, "error", err)
			}
		}
	}

	handles := &runHandles{}
	if len(run.ZeusAttackIDs) > 0 || run.ZeusAttackID != "" {
		pollCtx, pollCancel := context.WithCancel(context.Background())
		handles.poller = pollCancel
		go o.pollZeusStatus(pollCtx, runID)
	}

	o.mu.Lock()
	o.running[runID] = handles
	o.mu.Unlock()

	if o.autoComplete && handles.poller == nil {
		o.logger.Info("orchestrator: resumed run has no poller; auto-completing", "run_id", runID)
		go o.finishRun(context.Background(), runID, "completed", "running")
		return nil
	}

	o.logger.Info("orchestrator: run resumed", "run_id", runID, "phase", run.CurrentPhase)
	return nil
}

// StartExperiment transitions an experiment from "planned" to "running" and
// starts only its entry-point runs (those with empty DependsOn). Subsequent
// runs are started by advanceExperiment as their dependencies complete.
//
// Rejects the experiment if the run-dependency graph has cycles or unresolved
// references (model.ValidateRunGraph). The policy engine never calls this —
// run-to-run advancement is the orchestrator's exclusive responsibility.
func (o *Orchestrator) StartExperiment(ctx context.Context, experimentID string) error {
	exp, err := o.experiments.Get(ctx, experimentID)
	if err != nil {
		return err
	}
	if exp.Status != "planned" {
		return fmt.Errorf("experiment %q is not planned (status=%s)", experimentID, exp.Status)
	}

	allRuns, err := o.experiments.ListRunsByExperiment(ctx, experimentID)
	if err != nil {
		return fmt.Errorf("list runs: %w", err)
	}
	if err := model.ValidateRunGraph(allRuns); err != nil {
		return fmt.Errorf("invalid run graph: %w", err)
	}

	if err := o.experiments.UpdateStatus(ctx, experimentID, "running"); err != nil {
		return err
	}

	o.advanceExperiment(ctx, experimentID)

	o.logger.Info("orchestrator: experiment started",
		"experiment_id", experimentID, "total_runs", len(allRuns))
	return nil
}

// advanceExperiment is the run-to-run scheduler. It cascades failures down the
// dependency graph, then starts any newly-ready pending run, and finalizes the
// experiment status when no runs remain in flight or runnable.
//
// Called from StartExperiment (initial walk) and StopRun (after each terminal
// transition). Idempotent — safe to call repeatedly.
func (o *Orchestrator) advanceExperiment(ctx context.Context, experimentID string) {
	// Serialize per experiment so concurrent run completions can't race the
	// scheduler on a stale snapshot (which previously allowed double-starts).
	lk := o.experimentLock(experimentID)
	lk.Lock()
	defer lk.Unlock()

	// Only advance a running experiment — paused/cancelled/terminal experiments
	// must not start or cascade runs. This is what makes Pause/CancelExperiment
	// actually halt the DAG.
	exp, err := o.experiments.Get(ctx, experimentID)
	if err != nil {
		o.logger.Warn("orchestrator: advance: get experiment failed",
			"experiment_id", experimentID, "error", err)
		return
	}
	if exp.Status != "running" {
		return
	}

	runs, err := o.experiments.ListRunsByExperiment(ctx, experimentID)
	if err != nil {
		o.logger.Warn("orchestrator: list runs for advance failed",
			"experiment_id", experimentID, "error", err)
		return
	}

	statusByID := make(map[string]string, len(runs))
	for _, r := range runs {
		statusByID[r.ID] = r.Status
	}

	// 1) Cascade failures: any pending run whose any dependency is failed
	//    cannot proceed; mark it failed too. Iterate to fixpoint so chains
	//    of dependents collapse in one advance call.
	for changed := true; changed; {
		changed = false
		for _, r := range runs {
			if r.Status != "pending" {
				continue
			}
			for _, dep := range r.DependsOn {
				if statusByID[dep] == "failed" {
					if err := o.experiments.UpdateRunStatus(ctx, r.ID, "failed"); err != nil {
						o.logger.Warn("orchestrator: cascade-fail run update failed",
							"run_id", r.ID, "error", err)
						continue
					}
					o.logger.Info("orchestrator: run cascade-failed (dependency failed)",
						"run_id", r.ID, "failed_dep", dep)
					statusByID[r.ID] = "failed"
					r.Status = "failed"
					changed = true
					break
				}
			}
		}
	}

	// 2) Start any pending run whose dependencies are all completed.
	for _, r := range runs {
		if r.Status != "pending" {
			continue
		}
		ready := true
		for _, dep := range r.DependsOn {
			if statusByID[dep] != "completed" {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		if err := o.StartRun(ctx, r.ID); err != nil {
			o.logger.Error("orchestrator: start run failed in advance",
				"experiment_id", experimentID, "run_id", r.ID, "error", err)
		}
	}

	// 3) Finalize the experiment when nothing is in flight or runnable.
	allTerminal := true
	anyFailed := false
	for _, r := range runs {
		// Re-read status from our map since cascades above mutated it.
		s := statusByID[r.ID]
		switch s {
		case "pending", "running", "paused":
			allTerminal = false
		case "failed":
			anyFailed = true
		}
	}
	if !allTerminal {
		return
	}
	newStatus := "completed"
	if anyFailed {
		newStatus = "failed"
	}
	// Guard against finalizing a non-running experiment (e.g. cancelled
	// concurrently): only running → terminal wins.
	if ok, err := o.experiments.TransitionExperiment(ctx, experimentID, newStatus, "running"); err != nil {
		o.logger.Warn("orchestrator: finalize experiment status failed",
			"experiment_id", experimentID, "status", newStatus, "error", err)
		return
	} else if !ok {
		return
	}
	o.logger.Info("orchestrator: experiment finalized",
		"experiment_id", experimentID, "status", newStatus)
}

// finishRun is the single race-safe terminal path for a run. It atomically
// transitions runID to `status` only if the run's current status is one of
// `from`; ONLY the caller that wins that transition runs the side effects
// (tear down goroutines, clear injected rules, stop Zeus attacks, harvest once
// for "completed", advance the experiment DAG). Losers are a no-op, which is
// what makes auto-complete, the poller, StopRun, and cancellation safe to race.
// Returns whether this call performed the transition.
func (o *Orchestrator) finishRun(ctx context.Context, runID, status string, from ...string) bool {
	// Snapshot run details (services, attack IDs, experiment ID) before the flip.
	run, err := o.experiments.GetRun(ctx, runID)
	if err != nil {
		o.logger.Error("orchestrator: finish run: get run failed", "run_id", runID, "error", err)
		return false
	}

	transitioned, err := o.experiments.TransitionRun(ctx, runID, status, from...)
	if err != nil {
		o.logger.Error("orchestrator: finish run: transition failed",
			"run_id", runID, "status", status, "error", err)
		return false
	}
	if !transitioned {
		return false // lost the race / not in an allowed source state
	}

	o.mu.Lock()
	if handles, ok := o.running[runID]; ok {
		handles.cancelAll()
		delete(o.running, runID)
	}
	o.mu.Unlock()

	for _, svc := range o.collectServices(run) {
		if _, err := o.controller.PushRules(ctx, svc, nil); err != nil {
			o.logger.Warn("orchestrator: clear rules failed on finish",
				"service", svc, "error", err)
		}
	}
	if o.zeusClient != nil {
		attackIDs := run.ZeusAttackIDs
		if len(attackIDs) == 0 && run.ZeusAttackID != "" {
			attackIDs = []string{run.ZeusAttackID}
		}
		for _, id := range attackIDs {
			if err := o.zeusClient.StopAttack(ctx, id); err != nil {
				o.logger.Warn("orchestrator: zeus attack stop failed",
					"run_id", runID, "attack_id", id, "error", err)
			}
		}
	}
	if status == "completed" {
		o.HarvestResults(ctx, run) // exactly once — only the transition winner reaches here
	}

	o.logger.Info("orchestrator: run finished", "run_id", runID, "status", status)
	go o.advanceExperiment(context.Background(), run.ExperimentID)
	return true
}

// StopRun is the operator/external terminal entry point: finalize a running or
// paused run as completed/failed. Idempotent — a no-op if the run already
// reached a terminal state.
func (o *Orchestrator) StopRun(ctx context.Context, runID string, status string) error {
	if status != "completed" && status != "failed" {
		return fmt.Errorf("StopRun: status must be \"completed\" or \"failed\", got %q", status)
	}
	o.finishRun(ctx, runID, status, "running", "paused")
	return nil
}

// startZeusAttacks launches a Zeus attack using the experiment's attack config.
// Returns all started attack IDs (may be empty if no attack config is set).
func (o *Orchestrator) startZeusAttacks(ctx context.Context, run *model.ExperimentRun) ([]string, error) {
	exp, err := o.experiments.Get(ctx, run.ExperimentID)
	if err != nil {
		return nil, fmt.Errorf("get experiment: %w", err)
	}
	if exp.TargetURL == "" {
		return nil, nil
	}

	id, err := o.startOneAttack(ctx, run, exp)
	if err != nil {
		o.logger.Warn("orchestrator: start attack failed",
			"run_id", run.ID, "error", err)
		return nil, err
	}
	return []string{id}, nil
}

func (o *Orchestrator) startOneAttack(ctx context.Context, run *model.ExperimentRun, exp *model.Experiment) (string, error) {
	attackID := uuid.NewString()
	if err := o.experiments.AppendRunAttackID(ctx, run.ID, attackID); err != nil {
		return "", fmt.Errorf("persist attack id: %w", err)
	}

	method := exp.TargetMethod
	if method == "" {
		method = "GET"
	}
	durationS := exp.DurationSec
	if durationS <= 0 {
		durationS = 30
	}

	zeusID, err := o.zeusClient.StartAttack(ctx, zeus.AttackRequest{
		ID:            attackID,
		Target:        zeus.AttackTargetSpec{URL: exp.TargetURL, Method: method},
		Rate:          exp.Rate,
		DurationS:     durationS,
		MetaTraceID:   run.MetaTraceID,
		ExperimentID:  exp.ID,
		WorkflowLabel: exp.PrimaryWorkflowID,
	})
	if err != nil {
		return "", err
	}
	if zeusID != "" && zeusID != attackID {
		// Zeus did not honor the client-supplied ID (zeus-go pre-update).
		// Log so the operator notices, and trust Zeus's ID for our records —
		// recovery's GetAttack call needs the ID Zeus knows.
		o.logger.Warn("orchestrator: zeus returned different attack id; using zeus id",
			"run_id", run.ID, "client_id", attackID, "zeus_id", zeusID)
		return zeusID, nil
	}
	return attackID, nil
}
