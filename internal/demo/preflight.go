package demo

import (
	"context"
	"log/slog"
	"slices"
	"time"
)

// Preflight checks that the lab can run scenario sc now, in this order, and
// returns the public reason of the first check that fails:
//
//   - the load generator answers and holds a usable dataset of sc's workload
//     (else load_generator_unavailable);
//   - every service sc targets (its memory step's, then its CPU step's) has
//     at least one instance whose status is "alive" (else
//     target_unavailable): a fault on a service nobody runs would be
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
func Preflight(ctx context.Context, d Deps, sc Scenario, ownExperimentID string) (Reason, bool) {
	return preflight(ctx, d, sc, ownExperimentID, 0)
}

// preflight is Preflight with the run cap the dataset must outlive.
func preflight(ctx context.Context, d Deps, sc Scenario, ownExperimentID string, runCap time.Duration) (Reason, bool) {
	log := d.logger()
	if !loadGenReady(ctx, d, []string{sc.Workload.DatasetName}, runCap, log) {
		return ReasonLoadGeneratorUnavailable, false
	}
	if !targetReady(ctx, d, scenarioTargets(sc), log) {
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

// scenarioTargets are the distinct services sc's faults run in, in step
// order: the memory step's, then the CPU step's.
func scenarioTargets(sc Scenario) []string {
	return slices.Compact([]string{sc.Memory.Target, sc.CPU.Target})
}

// catalogDatasets are the distinct datasets the catalog's workloads read,
// sorted.
func catalogDatasets() []string {
	var out []string
	for _, sc := range Catalog() {
		out = append(out, sc.Workload.DatasetName)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// loadGenReady: zeus answers and holds, for each of the named datasets, one
// with rows that outlives now + runCap + 1 min (usableDataset). Zeus is asked
// once.
func loadGenReady(ctx context.Context, d Deps, datasets []string, runCap time.Duration, log *slog.Logger) bool {
	if d.Zeus == nil || !d.Zeus.Healthy(ctx) {
		log.Warn("demo: preflight: the load generator is not healthy")
		return false
	}
	var now time.Time
	if d.Clock != nil {
		now = d.Clock.Now()
	}
	all, err := d.Zeus.ListDatasets(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = ctxErr
	}
	if err != nil {
		log.Warn("demo: preflight: the datasets could not be listed", "datasets", datasets, "error", err)
		return false
	}
	for _, name := range datasets {
		if _, ok := usableDataset(all, name, now, runCap); !ok {
			log.Warn("demo: preflight: no usable dataset", "dataset", name, "error", errNoUsableDataset)
			return false
		}
	}
	return true
}

// targetReady: each of the services has at least one alive instance. They are
// read in order, and the first without one ends the check.
func targetReady(ctx context.Context, d Deps, services []string, log *slog.Logger) bool {
	if d.Instances == nil || len(services) == 0 {
		return false
	}
	for _, service := range services {
		insts, err := d.Instances.InstancesForService(ctx, service)
		if err != nil {
			log.Warn("demo: preflight could not list the target's instances", "service", service, "error", err)
			return false
		}
		alive := false
		for _, in := range insts {
			if in != nil && in.Status == "alive" {
				alive = true
				break
			}
		}
		if !alive {
			log.Warn("demo: preflight: the target has no alive instance", "service", service, "instances", len(insts))
			return false
		}
	}
	return true
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
