package atrocontrol

import (
	"context"

	"manteion-go/internal/ruleconv"
)

func (c *Controller) PushRules(ctx context.Context, service string, rules []ruleconv.CompiledRule, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)

	c.intent.SetRules(service, rules, co.runID) // keeps the freeze config and active fault

	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return FanoutResult{}, err
	}

	result := fanout(ctx, targets, func(ctx context.Context, t target) error {
		return c.tx.PostRules(ctx, t.address, rules)
	}, co)

	c.logFanout("push_rules", service, co.runID, result)
	return result, nil
}

// PushRulesToInstance pushes rules to one instance. It records no intent (the
// tracker is per service), so the service's freeze config is left alone.
func (c *Controller) PushRulesToInstance(ctx context.Context, instanceID string, rules []ruleconv.CompiledRule, opts ...CallOption) error {
	inst, err := c.resolveInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	co := c.resolveCallOpts(opts)
	opCtx, cancel := context.WithTimeout(ctx, co.timeout)
	defer cancel()
	return c.tx.PostRules(opCtx, inst.Address, rules)
}
