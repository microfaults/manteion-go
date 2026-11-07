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

// Controller reads the SDK fleet: instance status over the admin endpoints
// (StatusByService), W6 fidelity snapshots (FetchFidelity) and the live
// registered instances of a service (InstancesForService). It pushes nothing:
// rules, faults, the freeze and replay sets reach the instances through the
// desired state they poll (internal/desired).
type Controller struct {
	tx       *atropos.Client
	resolver InstanceResolver
	logger   logger
	defaults controllerOpts
}

// target is one instance a status read addresses.
type target struct {
	instanceID string
	address    string
}

func New(tx *atropos.Client, resolver InstanceResolver, opts ...ControllerOption) *Controller {
	c := &Controller{
		tx:       tx,
		resolver: resolver,
		logger:   slog.Default(),
		defaults: controllerOpts{
			timeout:     2 * time.Second,
			concurrency: 16,
			filter:      FilterLive,
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
