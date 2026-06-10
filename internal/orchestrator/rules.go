package orchestrator

import (
	"context"
	"fmt"

	"manteion-go/internal/ruleconv"
)

// loadCompiledRules fetches rules by ID, resolves fault specs via ruleconv,
// and returns CompiledRules in wire format ready for atrocontrol.PushRules.
// An empty or nil ruleIDs slice returns nil (clears rules on the target service).
//
// Retained from the pre-migration #19 codebase — the phase-aware orchestrator
// will reuse this when pushing rules at phase start.
func (o *Orchestrator) loadCompiledRules(ctx context.Context, ruleIDs []string) ([]ruleconv.CompiledRule, error) {
	if len(ruleIDs) == 0 {
		return nil, nil
	}

	out := make([]ruleconv.CompiledRule, 0, len(ruleIDs))
	for _, id := range ruleIDs {
		r, err := o.rules.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load rule %q: %w", id, err)
		}
		specResolver := &ruleconv.FuncResolver{Fn: o.faults.SpecResolver(ctx)}
		compResolver := &ruleconv.FuncCompositionResolver{Fn: o.faults.CompositionResolver(ctx)}
		compiled, err := ruleconv.CompileRule(r, specResolver, compResolver)
		if err != nil {
			return nil, fmt.Errorf("compile rule %q: %w", id, err)
		}
		out = append(out, compiled)
	}

	return out, nil
}
