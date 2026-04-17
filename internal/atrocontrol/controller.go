package atrocontrol

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"manteion-go/internal/atropos"
	"manteion-go/internal/model"
)

type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type Controller struct {
	tx       *atropos.Client
	resolver InstanceResolver
	intent   *IntentTracker
	logger   logger
	defaults controllerOpts
}

func New(tx *atropos.Client, resolver InstanceResolver, opts ...ControllerOption) *Controller {
	c := &Controller{
		tx:       tx,
		resolver: resolver,
		intent:   newIntentTracker(),
		logger:   slog.Default(),
		defaults: controllerOpts{
			timeout:     2 * time.Second,
			concurrency: 16,
			filter:      FilterAliveOrSuspect,
		},
	}
	for _, o := range opts {
		o(&c.defaults)
	}
	if c.defaults.logger != nil {
		c.logger = c.defaults.logger
	}
	return c
}

func (c *Controller) IntentReader() IntentReader { return c.intent }

func (c *Controller) resolveCallOpts(opts []CallOption) callOpts {
	cfg := callOpts{
		timeout:     c.defaults.timeout,
		concurrency: c.defaults.concurrency,
		filter:      c.defaults.filter,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func (c *Controller) resolveTargets(ctx context.Context, service string, filter InstanceFilter) ([]target, error) {
	instances, err := c.resolver.ForService(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("resolve service %q: %w", service, err)
	}
	var targets []target
	for _, inst := range instances {
		if filter != nil && !filter(inst) {
			continue
		}
		targets = append(targets, target{instanceID: inst.ID, address: inst.Address})
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no instances found for service %q", service)
	}
	return targets, nil
}

func (c *Controller) resolveInstance(ctx context.Context, instanceID string) (*model.SDKInstance, error) {
	inst, err := c.resolver.ForInstance(ctx, instanceID)
	if err != nil {
		return nil, fmt.Errorf("resolve instance %q: %w", instanceID, err)
	}
	return inst, nil
}

func (c *Controller) logFanout(action, service, runID string, result FanoutResult) {
	c.logger.Info("atrocontrol.fanout",
		"action", action,
		"service", service,
		"run_id", runID,
		"targeted", len(result.Targeted),
		"ok", len(result.OK),
		"failed", len(result.Failed),
		"duration_ms", result.Duration.Milliseconds(),
	)
}
