package atrocontrol

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/atropos"
	"manteion-go/internal/model"
)

type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type Controller struct {
	tx        *atropos.Client
	preloadTx *atropos.Client // dedicated long-timeout transport for staged preload (MANT-1)
	resolver  InstanceResolver
	intent    *IntentTracker
	logger    logger
	defaults  controllerOpts
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
	c.preloadTx = c.defaults.preloadTx
	if c.preloadTx == nil {
		c.preloadTx = tx // fall back to the command transport if no dedicated one is set
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

// InstancesForService returns the live (alive/suspect) registered instances of
// a service — the registry half of the drain gate's expected set (MANT-2).
// Unlike resolveTargets it does not error on an empty fleet.
func (c *Controller) InstancesForService(ctx context.Context, service string) ([]*model.SDKInstance, error) {
	instances, err := c.resolver.ForService(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("resolve service %q: %w", service, err)
	}
	var live []*model.SDKInstance
	for _, inst := range instances {
		if c.defaults.filter == nil || c.defaults.filter(inst) {
			live = append(live, inst)
		}
	}
	return live, nil
}

// FetchFidelity pulls one instance's W6 fidelity snapshot for (exp, phase) — the
// drain-timeout fallback (MANT-2) and verdict collection (MANT-6). Uses the
// command transport with a per-call timeout.
func (c *Controller) FetchFidelity(ctx context.Context, addr, experimentID, phaseID string, timeout time.Duration) (atroposdk.FidelitySnapshot, error) {
	opCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.tx.GetCacheBoxFidelity(opCtx, addr, experimentID, phaseID)
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
