package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"

	atroposdk "atropos-go"
	"manteion-go/internal/ruleconv"
)

// loadCompiledRules fetches rules by ID, resolves fault specs via ruleconv,
// and returns StaticRules ready for atrocontrol.PushRules.
// An empty or nil ruleIDs slice returns nil (clears rules on the target service).
func (o *Orchestrator) loadCompiledRules(ctx context.Context, ruleIDs []string) ([]atroposdk.StaticRule, error) {
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

	// JSON roundtrip: ruleconv.CompiledRule → atroposdk.CompiledRule (same wire schema).
	data, err := json.Marshal(modelRules)
	if err != nil {
		return nil, fmt.Errorf("marshal compiled rules: %w", err)
	}
	var sdkCompiled []atroposdk.CompiledRule
	if err := json.Unmarshal(data, &sdkCompiled); err != nil {
		return nil, fmt.Errorf("unmarshal compiled rules: %w", err)
	}

	staticRules, err := atroposdk.DecodeCompiledRules(sdkCompiled)
	if err != nil {
		return nil, fmt.Errorf("decode compiled rules: %w", err)
	}
	return staticRules, nil
}
