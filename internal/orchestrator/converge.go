package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
)

// Convergence barrier defaults (contract §4). MANTEION_CONVERGE_TIMEOUT
// overrides the timeout; the poll interval is a test seam.
const (
	defaultConvergeTimeout      = 30 * time.Second
	defaultConvergePollInterval = 500 * time.Millisecond
)

// waitConverged is the convergence barrier (desired-state contract §4). It
// blocks until every alive instance of each service in targets reports
// observed_generation >= the service's target and, for a service in artifacts
// (a frozen one), holds that artifact committed. observed_generation is
// compared with >=, never ==: it can lag one poll, and a later bump can carry
// an instance past the target. The fleet is re-read on every tick, so an
// instance that registers, recovers or falls behind meanwhile counts from the
// next look.
//
// Only alive instances are waited on. A suspect one, which the liveness
// classification calls stale (it missed polls but is not dead yet: a pod that
// crashed without deregistering, or a slow poller), is named in the log, and
// in the timeout error next to the lagging ones, and is waited on again once it
// polls. A graceful SDK shutdown deregisters, so suspect instances are the
// exception, not a rolling deploy's old pods.
//
// Some outcomes are known at once and end the wait before the timeout: a
// frozen service with no target (its bump failed, which is never "nothing to
// wait for") or with no alive instance to hold its replay set (INV-4), an
// alive instance of a frozen service whose host has no cache-box, and an
// instance reporting the assigned set failed. That last one is an optimization
// only: the SDK can fold a failed report into a later fetching one and retries
// a failed pull on its own timer, so the timeout stays the guarantee. On
// timeout the error lists every lagging instance.
//
// holds, when non-nil, is asked before each look whether the wait still has a
// reason: an entry's wait passes entryHolds, so a pause, a stop or a newer
// entry that takes the phase ends the wait at the next look with
// errLeftRunning instead of after the timeout. Exit and drain waits pass nil.
func (o *Orchestrator) waitConverged(ctx context.Context, targets map[string]uint64, artifacts map[string]string, timeout time.Duration, holds func(context.Context) error) error {
	deadline := time.Now().Add(timeout)
	interval := o.convergePollInterval
	if interval <= 0 {
		interval = defaultConvergePollInterval // an orchestrator not built by New: never spin
	}
	logged := map[string]bool{} // suspect instances already named in the log
	for {
		if holds != nil {
			if err := holds(ctx); err != nil {
				return err
			}
		}
		lagging, suspect, err := o.laggingInstances(ctx, targets, artifacts)
		if err != nil {
			return err
		}
		for _, inst := range suspect {
			if !logged[inst.ID] {
				logged[inst.ID] = true
				o.logger.Warn("orchestrator: converge: suspect instance not waited on (missed polls)",
					"instance_id", inst.ID, "service", inst.Service, "last_poll_at", inst.LastPollAt,
					"observed_generation", inst.ObservedGeneration, "target", targets[inst.Service])
			}
		}
		if len(lagging) == 0 {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &ErrNotConverged{Timeout: timeout, Lagging: lagging, Suspect: suspectLines(suspect, targets)}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w while %d instance(s) lagged: %s", ctx.Err(), len(lagging), strings.Join(lagging, "; "))
		case <-time.After(min(interval, remaining)):
		}
	}
}

// laggingInstances is one look at the fleet for waitConverged: the alive
// instances that have not converged yet, one line each naming what is behind;
// the suspect instances it does not wait on; or the error that ends the wait at
// once (see waitConverged). Services are visited in name order, so the lines
// and the first fatal error are deterministic.
func (o *Orchestrator) laggingInstances(ctx context.Context, targets map[string]uint64, artifacts map[string]string) (lagging []string, suspect []*model.SDKInstance, err error) {
	for _, svc := range slices.Sorted(maps.Keys(artifacts)) {
		if _, ok := targets[svc]; !ok {
			return nil, nil, fmt.Errorf("%s: frozen, but there is no generation target to wait for (its bump failed)", svc)
		}
	}
	for _, svc := range slices.Sorted(maps.Keys(targets)) {
		target := targets[svc]
		instances, err := o.controller.InstancesForService(ctx, svc) // alive and stale, never dead
		if err != nil {
			return nil, nil, fmt.Errorf("list instances of %q: %w", svc, err)
		}
		artifact, frozen := artifacts[svc]
		var svcSuspect []*model.SDKInstance
		alive := 0
		for _, inst := range instances {
			if inst.Status == "stale" {
				svcSuspect = append(svcSuspect, inst)
				continue
			}
			alive++
			switch {
			case frozen && !desired.CapabilitiesOf(inst).Allows("cachebox"):
				// The materializer never serves it a replay set: waiting would
				// only burn the timeout.
				return nil, nil, fmt.Errorf("%s(%s): cannot hold the replay set: its host advertises no cachebox capability (capabilities: %s)",
					inst.ID, svc, strings.Join(inst.Capabilities, ","))
			case inst.ObservedGeneration < target:
				lagging = append(lagging, withErrors(fmt.Sprintf("%s(%s): observed=%d target=%d", inst.ID, svc, inst.ObservedGeneration, target), inst))
			case frozen && inst.ReplaySetID == artifact && inst.ReplaySetState == "failed":
				return nil, nil, fmt.Errorf("%s(%s): replay set %s failed: %s", inst.ID, svc, artifact, observedErrors(inst))
			case frozen && (inst.ReplaySetID != artifact || inst.ReplaySetState != "committed"):
				lagging = append(lagging, withErrors(fmt.Sprintf("%s(%s): replay_set=%s target=%s/committed", inst.ID, svc, replaySetOf(inst), artifact), inst))
			}
		}
		if frozen && alive == 0 {
			msg := fmt.Sprintf("%s: no live instances to hold the replay set", svc)
			if len(svcSuspect) > 0 {
				msg += " (suspect, missed polls: " + strings.Join(suspectLines(svcSuspect, targets), "; ") + ")"
			}
			return nil, nil, errors.New(msg)
		}
		suspect = append(suspect, svcSuspect...)
	}
	return lagging, suspect, nil
}

// awaitConvergedNonFatal is the bounded wait after an exit or a drain bump
// (barrier names which: "exit" or "drain"). The phase has already left
// 'running', so a straggler only delays this call, and its next poll
// converges it: observed_generation can lag one poll, and a recording instance
// reports only after its drain POST. A timeout is logged, never a failure.
func (o *Orchestrator) awaitConvergedNonFatal(ctx context.Context, p *model.ExperimentPhase, targets map[string]uint64, barrier string) {
	if err := o.waitConverged(ctx, targets, nil, o.convergeTimeout, nil); err != nil {
		o.logger.Warn("orchestrator: fleet did not converge on the "+barrier+" bump; the next poll converges stragglers",
			"phase_id", p.ID, "barrier", barrier, "error", err)
	}
}

// suspectLines words the suspect instances for an error, one line each:
// id(service), the last poll, and how far behind the target each one is.
func suspectLines(suspect []*model.SDKInstance, targets map[string]uint64) []string {
	lines := make([]string, 0, len(suspect))
	for _, inst := range suspect {
		lines = append(lines, fmt.Sprintf("%s(%s): last poll %s, observed=%d target=%d",
			inst.ID, inst.Service, inst.LastPollAt.UTC().Format(time.RFC3339), inst.ObservedGeneration, targets[inst.Service]))
	}
	return lines
}

// withErrors appends to a lagging line the errors the instance last reported,
// often the reason it lags.
func withErrors(line string, inst *model.SDKInstance) string {
	if len(inst.ObservedErrors) == 0 {
		return line
	}
	return line + " (errors: " + strings.Join(inst.ObservedErrors, " | ") + ")"
}

// replaySetOf words the replay set an instance last reported: "none", or
// "<id>/<state>".
func replaySetOf(inst *model.SDKInstance) string {
	if inst.ReplaySetID == "" {
		return "none"
	}
	return inst.ReplaySetID + "/" + inst.ReplaySetState
}

// observedErrors words the errors an instance last reported.
func observedErrors(inst *model.SDKInstance) string {
	if len(inst.ObservedErrors) == 0 {
		return "no error reported"
	}
	return strings.Join(inst.ObservedErrors, "; ")
}
