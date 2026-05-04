package orchestrator

import (
	"context"
	"time"

	"manteion-go/internal/conditions"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

const watchInterval = 10 * time.Second

func (o *Orchestrator) watchPhase(ctx context.Context, run *model.ExperimentRun) {
	cond := run.TransitionCond
	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()

	var condHeldSince time.Time
	condHeld := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			value, err := o.prom.QueryInstant(ctx, cond.Metric)
			if err != nil {
				o.logger.Warn("orchestrator: condition query failed",
					"run_id", run.ID, "error", err)
				condHeld = false
				condHeldSince = time.Time{}
				continue
			}

			if conditions.Met(value, cond.Operator, cond.Threshold) {
				if !condHeld {
					condHeld = true
					condHeldSince = time.Now()
				} else if time.Since(condHeldSince) >= cond.Window {
					o.advancePhase(ctx, run)
					return
				}
			} else {
				condHeld = false
				condHeldSince = time.Time{}
			}
		}
	}
}

func (o *Orchestrator) advancePhase(ctx context.Context, run *model.ExperimentRun) {
	nextPhase := run.CurrentPhase + 1
	o.logger.Info("orchestrator: advancing phase",
		"run_id", run.ID, "from", run.CurrentPhase, "to", nextPhase)

	if nextPhase >= len(run.PhaseRules) {
		o.StopRun(ctx, run.ID, "completed")
		return
	}

	if err := o.enterPhase(ctx, run, nextPhase); err != nil {
		o.logger.Error("orchestrator: enter phase failed",
			"run_id", run.ID, "phase", nextPhase, "error", err)
		o.StopRun(ctx, run.ID, "failed")
		return
	}

	run.CurrentPhase = nextPhase
	o.experiments.UpdateRunPhase(ctx, run.ID, nextPhase, "running")

	if nextPhase+1 < len(run.PhaseRules) && run.TransitionCond != nil {
		watchCtx, watchCancel := context.WithCancel(context.Background())
		o.mu.Lock()
		if handles, ok := o.running[run.ID]; ok {
			// Cancel the previous watcher (this goroutine) and rebind. The
			// poller stays alive across phase advances — its lifetime is
			// StartRun→StopRun/PauseRun, not per-phase.
			if handles.watcher != nil {
				handles.watcher()
			}
			handles.watcher = watchCancel
		} else {
			// Run was stopped concurrently; cancel the new watcher we just
			// created so it exits immediately.
			watchCancel()
		}
		o.mu.Unlock()
		go o.watchPhase(watchCtx, run)
	}
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

