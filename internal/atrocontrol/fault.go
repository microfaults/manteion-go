package atrocontrol

import (
	"context"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

func (c *Controller) InjectFault(ctx context.Context, service string, req atroposdk.FaultRequest, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)

	c.intent.SetFaultSlot(service, req.Category, &req)

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

func (c *Controller) ClearFault(ctx context.Context, service, category string, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)
	c.intent.ClearFaultSlot(service, category)

	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return FanoutResult{}, err
	}

	result := fanout(ctx, targets, func(ctx context.Context, t target) error {
		return c.tx.DeleteFault(ctx, t.address, category)
	}, co)

	c.logFanout("clear_fault", service, co.runID, result)
	return result, nil
}

// ClearAllFaults fans out DELETE /admin/fault and clears all slots from intent.
func (c *Controller) ClearAllFaults(ctx context.Context, service string, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)
	// We clear the intent for ALL faults. Since there's no atomic intent-clear for active faults
	// we just wipe the ServiceIntent from state? Wait, ServiceIntent also holds Rules and FreezeCfg.
	// So we need a t.intent.ClearAllFaultSlots(service). Let's implement that in intent.go shortly.
	c.intent.ClearAllFaultSlots(service)

	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return FanoutResult{}, err
	}

	result := fanout(ctx, targets, func(ctx context.Context, t target) error {
		return c.tx.DeleteAllFaults(ctx, t.address)
	}, co)

	c.logFanout("clear_all_faults", service, co.runID, result)
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

func (c *Controller) ClearFaultOnInstance(ctx context.Context, instanceID, category string, opts ...CallOption) error {
	inst, err := c.resolveInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	co := c.resolveCallOpts(opts)
	opCtx, cancel := context.WithTimeout(ctx, co.timeout)
	defer cancel()
	return c.tx.DeleteFault(opCtx, inst.Address, category)
}

func (c *Controller) ClearAllFaultsOnInstance(ctx context.Context, instanceID string, opts ...CallOption) error {
	inst, err := c.resolveInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	co := c.resolveCallOpts(opts)
	opCtx, cancel := context.WithTimeout(ctx, co.timeout)
	defer cancel()
	return c.tx.DeleteAllFaults(opCtx, inst.Address)
}
