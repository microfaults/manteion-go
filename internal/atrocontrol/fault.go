package atrocontrol

import (
	"context"

	atroposdk "atropos-go"
)

func (c *Controller) InjectFault(ctx context.Context, service string, req atroposdk.FaultRequest, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)

	c.intent.Set(service, ServiceIntent{
		ActiveFault: &req,
		RunID:       co.runID,
	})

	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return FanoutResult{}, err
	}

	result := fanout(ctx, targets, func(ctx context.Context, t target) error {
		_, err := c.tx.PostFault(ctx, t.address, req)
		return err
	}, co)

	c.logFanout("inject_fault", service, co.runID, result)
	return result, nil
}

func (c *Controller) ClearFault(ctx context.Context, service string, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)
	c.intent.Clear(service)

	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return FanoutResult{}, err
	}

	result := fanout(ctx, targets, func(ctx context.Context, t target) error {
		return c.tx.DeleteFault(ctx, t.address)
	}, co)

	c.logFanout("clear_fault", service, co.runID, result)
	return result, nil
}

func (c *Controller) InjectFaultOnInstance(ctx context.Context, instanceID string, req atroposdk.FaultRequest, opts ...CallOption) error {
	inst, err := c.resolveInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	co := c.resolveCallOpts(opts)
	opCtx, cancel := context.WithTimeout(ctx, co.timeout)
	defer cancel()
	_, err = c.tx.PostFault(opCtx, inst.Address, req)
	return err
}

func (c *Controller) ClearFaultOnInstance(ctx context.Context, instanceID string, opts ...CallOption) error {
	inst, err := c.resolveInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	co := c.resolveCallOpts(opts)
	opCtx, cancel := context.WithTimeout(ctx, co.timeout)
	defer cancel()
	return c.tx.DeleteFault(opCtx, inst.Address)
}
