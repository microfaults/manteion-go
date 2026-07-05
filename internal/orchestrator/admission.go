package orchestrator

import (
	"context"
	"fmt"
	"sort"
)

// experimentServiceSet is the set of services an experiment touches: the union,
// across all its phases, of frozen services and rule-target services
// (phaseServices). This is the footprint the admission controller reasons over.
func (o *Orchestrator) experimentServiceSet(ctx context.Context, experimentID string) map[string]bool {
	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		o.logger.Warn("orchestrator: admission: list phases failed",
			"experiment_id", experimentID, "error", err)
		return nil
	}
	set := map[string]bool{}
	for _, p := range phases {
		for _, svc := range o.phaseServices(ctx, p) {
			set[svc] = true
		}
	}
	return set
}

// checkServiceOverlap refuses to start an experiment whose service footprint
// intersects a running experiment's (INV-5 admission control, MANT-5): two
// experiments sharing a service are rule-conflict irreconcilable — one freezes
// it, the other wants it live — no matter how keys are scoped. The error names
// both experiments and the shared services; MANTEION_ALLOW_CONCURRENT_OVERLAP
// overrides.
func (o *Orchestrator) checkServiceOverlap(ctx context.Context, experimentID string) error {
	candidate := o.experimentServiceSet(ctx, experimentID)
	if len(candidate) == 0 {
		return nil
	}
	runningIDs, err := o.experiments.RunningExperimentIDs(ctx)
	if err != nil {
		return fmt.Errorf("admission: list running experiments: %w", err)
	}
	for _, rid := range runningIDs {
		if rid == experimentID {
			continue
		}
		var shared []string
		for svc := range o.experimentServiceSet(ctx, rid) {
			if candidate[svc] {
				shared = append(shared, svc)
			}
		}
		if len(shared) > 0 {
			sort.Strings(shared)
			return fmt.Errorf("experiment %q cannot start: services %v overlap running experiment %q; "+
				"set MANTEION_ALLOW_CONCURRENT_OVERLAP=true to override", experimentID, shared, rid)
		}
	}
	return nil
}
