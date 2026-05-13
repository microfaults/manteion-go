package atrocontrol

import (
	"context"

	"manteion-go/internal/ruleconv"
)

func (c *Controller) PushRules(ctx context.Context, service string, rules []ruleconv.CompiledRule, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)

	c.intent.Set(service, ServiceIntent{
		Rules: rules,
		RunID: co.runID,
	})

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
