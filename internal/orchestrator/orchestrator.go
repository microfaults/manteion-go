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
// pushing rules, supervising zeus attacks, harvesting results, and
// updating phase status as the experiment progresses.
//
// Migration #19 collapsed the legacy ExperimentRun shape into phases as
// first-class rows. The orchestrator surface in this file is a thin
// stub that compiles against the new model and persists status changes;
// the rich run/watcher/poller machinery that used to live here will be
// rebuilt phase-aware in a follow-up. The handler layer can rely on the
// stub for happy-path status transitions today.
type Orchestrator struct {
	experiments *store.ExperimentRepo
	rules       *store.RuleRepo
	faults      *store.FaultRepo
	workloads   *store.WorkloadRepo
	controller  *atrocontrol.Controller
	prom        *promql.Client
	zeusClient  *zeus.Client
	cacheStore  *cachestore.Store
	logger      *slog.Logger

	maxPollDuration time.Duration

	mu      sync.Mutex
	running map[string]context.CancelFunc // experiment_id → cancel
}

const defaultMaxPollDuration = 30 * time.Minute

// New constructs an Orchestrator. The signature is preserved from the
// pre-migration shape so cmd/manteion/main.go does not need to change.
func New(
	experiments *store.ExperimentRepo,
	rules *store.RuleRepo,
	faults *store.FaultRepo,
	workloads *store.WorkloadRepo,
	controller *atrocontrol.Controller,
	prom *promql.Client,
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
		prom:            prom,
		zeusClient:      zeusClient,
		cacheStore:      cs,
		logger:          logger,
		maxPollDuration: defaultMaxPollDuration,
		running:         make(map[string]context.CancelFunc),
	}
}

// WithMaxPollDuration overrides the default Zeus poll timeout.
func (o *Orchestrator) WithMaxPollDuration(d time.Duration) {
	o.maxPollDuration = d
}

// Recover is a no-op stub for the migration-#19 model. Phase-aware
// recovery (resuming in-flight phases by reattaching to zeus attacks
// and rebuilding rule state on services) is a follow-up.
func (o *Orchestrator) Recover(ctx context.Context) error {
	o.logger.Info("orchestrator: recover skipped — phase-aware orchestration is a follow-up")
	return nil
}

// StartExperiment marks the experiment as running and starts the first
// pending phase. Returns an error if the experiment has no phases.
func (o *Orchestrator) StartExperiment(ctx context.Context, experimentID string) error {
	exp, err := o.experiments.Get(ctx, experimentID)
	if err != nil {
		return fmt.Errorf("orchestrator: load experiment: %w", err)
	}
	if exp.Status == "running" {
		return errors.New("orchestrator: experiment already running")
	}

	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		return fmt.Errorf("orchestrator: list phases: %w", err)
	}
	if len(phases) == 0 {
		return errors.New("orchestrator: experiment has no phases")
	}

	if err := o.experiments.UpdateStatus(ctx, experimentID, "running"); err != nil {
		return fmt.Errorf("orchestrator: update experiment status: %w", err)
	}
	// Kick the first pending phase.
	for _, p := range phases {
		if p.Status == "pending" {
			if err := o.StartPhase(ctx, p.ID); err != nil {
				o.logger.Error("orchestrator: start first phase failed",
					"experiment_id", experimentID, "phase_id", p.ID, "error", err)
				_ = o.experiments.UpdateStatus(ctx, experimentID, "failed")
				return err
			}
			break
		}
	}
	return nil
}

// StopExperiment marks the experiment and any running phase as cancelled.
func (o *Orchestrator) StopExperiment(ctx context.Context, experimentID, finalStatus string) error {
	if finalStatus == "" {
		finalStatus = "cancelled"
	}

	o.mu.Lock()
	if cancel, ok := o.running[experimentID]; ok {
		cancel()
		delete(o.running, experimentID)
	}
	o.mu.Unlock()

	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		return fmt.Errorf("orchestrator: list phases: %w", err)
	}
	for _, p := range phases {
		if p.Status == "running" || p.Status == "paused" {
			if err := o.experiments.UpdatePhaseStatus(ctx, p.ID, "skipped"); err != nil {
				o.logger.Warn("orchestrator: skip phase failed",
					"phase_id", p.ID, "error", err)
			}
		}
	}
	return o.experiments.UpdateStatus(ctx, experimentID, finalStatus)
}

// StartPhase marks the phase as running. The full implementation (push
// rules, launch zeus attacks, watch transition conditions) is a follow-up.
func (o *Orchestrator) StartPhase(ctx context.Context, phaseID string) error {
	p, err := o.experiments.GetPhase(ctx, phaseID)
	if err != nil {
		return fmt.Errorf("orchestrator: load phase: %w", err)
	}
	if p.Status != "pending" && p.Status != "paused" {
		return fmt.Errorf("orchestrator: phase %q not startable from status %q", phaseID, p.Status)
	}
	if err := o.experiments.UpdatePhaseStatus(ctx, phaseID, "running"); err != nil {
		return fmt.Errorf("orchestrator: update phase status: %w", err)
	}
	o.logger.Info("orchestrator: phase started (status-only stub)", "phase_id", phaseID)
	return nil
}

// StopPhase marks the phase with a terminal status. finalStatus must be
// one of "completed", "failed", "skipped".
func (o *Orchestrator) StopPhase(ctx context.Context, phaseID, finalStatus string) error {
	if finalStatus == "" {
		finalStatus = "completed"
	}
	if err := o.experiments.UpdatePhaseStatus(ctx, phaseID, finalStatus); err != nil {
		return fmt.Errorf("orchestrator: update phase status: %w", err)
	}
	// Recompute rollup; ignore "no measurements yet" cases.
	p, err := o.experiments.GetPhase(ctx, phaseID)
	if err == nil {
		if _, err := o.experiments.RecomputeExperimentResults(ctx, p.ExperimentID); err != nil {
			o.logger.Warn("orchestrator: recompute results failed",
				"experiment_id", p.ExperimentID, "error", err)
		}
	}
	return nil
}

// PausePhase puts a running phase into the paused state. Resume via
// StartPhase (which accepts both pending and paused).
func (o *Orchestrator) PausePhase(ctx context.Context, phaseID string) error {
	return o.experiments.UpdatePhaseStatus(ctx, phaseID, "paused")
}
