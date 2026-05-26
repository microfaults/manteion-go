package orchestrator

import (
	"context"
	"fmt"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// AdvancePhase moves a running run to its next phase and applies that phase's
// rules; if the run is already on its last phase it finishes as completed.
//
// This is the procedural seam for a future generic phase-advance callback hook.
// Metric-driven (promQL) advancement was removed, so nothing invokes this
// automatically today: a run applies phase 0 at StartRun and is driven to
// completion by the Zeus poller (or auto-completes when it has no attacks).
func (o *Orchestrator) AdvancePhase(ctx context.Context, runID string) error {
	run, err := o.experiments.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status != "running" {
		return fmt.Errorf("run %q is not running (status=%s)", runID, run.Status)
	}

	next := run.CurrentPhase + 1
	if next >= len(run.PhaseRules) {
		o.finishRun(ctx, runID, "completed", "running")
		return nil
	}
	if err := o.enterPhase(ctx, run, next); err != nil {
		o.logger.Error("orchestrator: enter phase failed",
			"run_id", runID, "phase", next, "error", err)
		o.finishRun(ctx, runID, "failed", "running")
		return fmt.Errorf("enter phase %d: %w", next, err)
	}
	return o.experiments.UpdateRunPhase(ctx, runID, next, "running")
}

// enterPhase pushes the rules for the given phase to all target services.
// An empty RuleIDs list pushes nil, clearing any previous rules on those services.
func (o *Orchestrator) enterPhase(ctx context.Context, run *model.ExperimentRun, phase int) error {
	var ruleIDs []string
	if phase < len(run.PhaseRules) {
		ruleIDs = run.PhaseRules[phase].RuleIDs
	}

	compiled, err := o.loadCompiledRules(ctx, ruleIDs)
	if err != nil {
		return err
	}

	for _, svc := range o.collectServices(run) {
		if _, err := o.controller.PushRules(ctx, svc, compiled); err != nil {
			o.logger.Warn("orchestrator: push rules failed",
				"service", svc, "phase", phase, "error", err)
		}
	}
	return nil
}

func (o *Orchestrator) collectServices(run *model.ExperimentRun) []string {
	seen := make(map[string]struct{})
	var svcs []string
	for _, fs := range run.FrozenServices {
		if _, ok := seen[fs.Service]; !ok {
			seen[fs.Service] = struct{}{}
			svcs = append(svcs, fs.Service)
		}
	}
	return svcs
}

// preloadCacheEntries reads baseline cache files and fans them out to all
// frozen services' SDK instances before the isolation run begins.
func (o *Orchestrator) preloadCacheEntries(ctx context.Context, run *model.ExperimentRun) error {
	baseline, err := o.experiments.GetBaselineRun(ctx, run.ExperimentID)
	if err == store.ErrNotFound {
		o.logger.Info("orchestrator: no completed baseline run; skipping cache preload",
			"run_id", run.ID)
		return nil
	}
	if err != nil {
		return err
	}

	for _, svc := range o.collectServices(run) {
		entries, err := o.cacheStore.Read(baseline.ID, svc)
		if err != nil {
			o.logger.Warn("orchestrator: read cache entries failed",
				"run_id", run.ID, "service", svc, "error", err)
			continue
		}
		if len(entries) == 0 {
			o.logger.Info("orchestrator: no cache entries for service",
				"baseline_run_id", baseline.ID, "service", svc)
			continue
		}
		if _, err := o.controller.PreloadEntries(ctx, svc, entries); err != nil {
			o.logger.Warn("orchestrator: preload entries failed",
				"run_id", run.ID, "service", svc, "error", err)
		}
	}
	return nil
}
