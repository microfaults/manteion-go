package orchestrator

import (
	"context"
	"fmt"

	"manteion-go/internal/ruleconv"
)

// loadCompiledRules fetches rules by ID, resolves fault specs via ruleconv,
// and returns CompiledRules in wire format ready for atrocontrol.PushRules.
// An empty or nil ruleIDs slice returns nil (clears rules on the target service).
func (o *Orchestrator) loadCompiledRules(ctx context.Context, ruleIDs []string) ([]ruleconv.CompiledRule, error) {
	if len(ruleIDs) == 0 {
		return nil, nil
	}

	modelRules := make([]*ruleconv.CompiledRule, 0, len(ruleIDs))
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
		modelRules = append(modelRules, &compiled)
	}

	out := make([]ruleconv.CompiledRule, len(modelRules))
	for i, cr := range modelRules {
		out[i] = *cr
	}
	return out, nil
}
