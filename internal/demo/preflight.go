package demo

import (
	"context"
	"log/slog"
	"time"
)

// Preflight checks that the lab can run the preset now, in this order, and
// returns the public reason of the first check that fails:
//
//   - the load generator answers and holds a usable demo-products dataset
//     (else load_generator_unavailable);
//   - productcatalogservice has at least one instance whose status is "alive"
//     (else target_unavailable): a fault on a service nobody runs would be
//     recorded as a fault phase that did nothing;
//   - no experiment other than ownExperimentID is running (else lab_busy), or
//     internal_error when that cannot be read.
//
// It reads only; it creates nothing and never calls the orchestrator. Every
// probe runs on ctx, so the caller bounds the whole check with ctx's
// deadline; a probe cut short by it counts as failed. Details of a failure go
// to the logs, never into the result.
//
// Without a run cap to hand, the dataset only has to outlive the next minute;
// the worker checks against its own cap (preflight), and BuildRun checks again
// before it writes anything.
func Preflight(ctx context.Context, d Deps, ownExperimentID string) (Reason, bool) {
	return preflight(ctx, d, ownExperimentID, 0)
}

// preflight is Preflight with the run cap the dataset must outlive.
func preflight(ctx context.Context, d Deps, ownExperimentID string, runCap time.Duration) (Reason, bool) {
	log := d.logger()
	if !loadGenReady(ctx, d, runCap, log) {
		return ReasonLoadGeneratorUnavailable, false
	}
	if !targetReady(ctx, d, log) {
		return ReasonTargetUnavailable, false
	}
	idle, err := labIdle(ctx, d, ownExperimentID)
	if err != nil {
		log.Warn("demo: preflight could not list the running experiments", "error", err)
		return ReasonInternalError, false
	}
	if !idle {
		return ReasonLabBusy, false
	}
	return Reason{}, true
}

// loadGenReady: zeus answers and holds a demo-products dataset with rows that
// outlives now + runCap + 1 min.
func loadGenReady(ctx context.Context, d Deps, runCap time.Duration, log *slog.Logger) bool {
	if d.Zeus == nil || !d.Zeus.Healthy(ctx) {
		log.Warn("demo: preflight: the load generator is not healthy")
		return false
	}
	var now time.Time
	if d.Clock != nil {
		now = d.Clock.Now()
	}
	dataset := DefaultScenario().Workload.DatasetName // Task 4: the run's scenario's dataset
	if _, err := pickDataset(ctx, d.Zeus, dataset, now, runCap); err != nil {
		log.Warn("demo: preflight: no usable dataset", "dataset", dataset, "error", err)
		return false
	}
	return true
}

// targetReady: at least one instance of the default scenario's target
// (productcatalogservice, both of its steps') is alive.
func targetReady(ctx context.Context, d Deps, log *slog.Logger) bool {
	if d.Instances == nil {
		return false
	}
	// Task 4: every service the run's scenario targets; for the health check,
	// every CatalogTargets() service.
	service := DefaultScenario().Memory.Target
	insts, err := d.Instances.InstancesForService(ctx, service)
	if err != nil {
		log.Warn("demo: preflight could not list the target's instances", "service", service, "error", err)
		return false
	}
	for _, in := range insts {
		if in != nil && in.Status == "alive" {
			return true
		}
	}
	log.Warn("demo: preflight: the target has no alive instance", "service", service, "instances", len(insts))
	return false
}

// labIdle: no running experiment but ownExperimentID ("" for none).
func labIdle(ctx context.Context, d Deps, ownExperimentID string) (bool, error) {
	if d.Experiments == nil {
		return false, errNoExperiments
	}
	ids, err := d.Experiments.RunningExperimentIDs(ctx)
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		if id != ownExperimentID {
			return false, nil
		}
	}
	return true, nil
}
