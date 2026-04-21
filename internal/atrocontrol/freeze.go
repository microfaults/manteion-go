package atrocontrol

import (
	"context"

	atroposdk "atropos-go"
)

func (c *Controller) FreezeService(ctx context.Context, service string, cfg atroposdk.DelayRequest, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)

	// Write intent before fan-out so late-joining registrations see target state.
	c.intent.Set(service, ServiceIntent{
		FreezeCfg: &cfg,
		RunID:     co.runID,
	})

	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return FanoutResult{}, err
	}

	result := fanout(ctx, targets, func(ctx context.Context, t target) error {
		return c.tx.PostCacheBoxDelay(ctx, t.address, cfg)
	}, co)

	c.logFanout("freeze_service", service, co.runID, result)
	return result, nil
}

func (c *Controller) ClearService(ctx context.Context, service string, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)
	c.intent.Clear(service)

	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return FanoutResult{}, err
	}

	result := fanout(ctx, targets, func(ctx context.Context, t target) error {
		return c.tx.ClearCacheBox(ctx, t.address)
	}, co)

	c.logFanout("clear_service", service, co.runID, result)
	return result, nil
}

func (c *Controller) FreezeInstance(ctx context.Context, instanceID string, cfg atroposdk.DelayRequest, opts ...CallOption) error {
	inst, err := c.resolveInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	co := c.resolveCallOpts(opts)
	opCtx, cancel := context.WithTimeout(ctx, co.timeout)
	defer cancel()
	return c.tx.PostCacheBoxDelay(opCtx, inst.Address, cfg)
}

func (c *Controller) ClearInstance(ctx context.Context, instanceID string, opts ...CallOption) error {
	inst, err := c.resolveInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	co := c.resolveCallOpts(opts)
	opCtx, cancel := context.WithTimeout(ctx, co.timeout)
	defer cancel()
	return c.tx.ClearCacheBox(opCtx, inst.Address)
}
