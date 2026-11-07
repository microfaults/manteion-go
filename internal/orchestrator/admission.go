package orchestrator

import (
	"context"
	"fmt"
	"sort"

	"manteion-go/internal/model"
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
		svcs, err := o.phaseServices(ctx, p)
		if err != nil {
			o.logger.Warn("orchestrator: admission: phase services incomplete",
				"phase_id", p.ID, "error", err)
		}
		for _, svc := range svcs {
			set[svc] = true
		}
	}
	return set
}

// checkServiceOverlap refuses to start an experiment whose service footprint
// intersects a running experiment's (INV-5 admission control, MANT-5): two
// experiments sharing a service are rule-conflict irreconcilable — one freezes
// it, the other wants it live — no matter how keys are scoped, and the SDK is
// single-tenant per instance (one replay set, one preload staging slot), so
// overlap corrupts silently rather than degrading. Not overridable. The error
// names both experiments and the shared services.
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
			return &ErrServiceOverlap{ExperimentID: experimentID, RunningExperimentID: rid, Services: shared}
		}
	}
	return nil
}

// checkCacheBoxRuleAgreement refuses, ErrValidation, to start an experiment
// or one of its phases while a user-authored cache-box rule attached to any of
// its phases does not key like the frozen-service config of its service
// (model.ValidateCacheBoxStrategyAgreement): the attach checked it, but a rule
// can be edited after it was attached. The check is experiment-wide because a
// baseline records every service the experiment freezes.
func (o *Orchestrator) checkCacheBoxRuleAgreement(ctx context.Context, experimentID string) error {
	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		return fmt.Errorf("orchestrator: cache-box rule agreement: list phases: %w", err)
	}
	keying := make([]model.ExperimentPhase, 0, len(phases))
	var rules []*model.Rule
	for _, p := range phases {
		keying = append(keying, model.ExperimentPhase{FrozenServices: p.FrozenServices})
		prs, err := o.experiments.ListPhaseRules(ctx, p.ID)
		if err != nil {
			return fmt.Errorf("orchestrator: cache-box rule agreement: list rules of phase %q: %w", p.ID, err)
		}
		for _, pr := range prs {
			r, err := o.rules.Get(ctx, pr.RuleID)
			if err != nil {
				return fmt.Errorf("orchestrator: cache-box rule agreement: load rule %q: %w", pr.RuleID, err)
			}
			rules = append(rules, r)
		}
	}
	if err := model.ValidateCacheBoxStrategyAgreement(keying, rules...); err != nil {
		return validationError(fmt.Errorf("orchestrator: %w", err))
	}
	return nil
}
