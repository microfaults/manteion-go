package orchestrator

import (
	"context"
	"time"

	"manteion-go/internal/model"
)

// drainTarget is one instance the drain gate expects a report from. address is
// the instance's control endpoint (empty when the instance pushed but is no
// longer registered — a deregistered mid-phase pusher).
type drainTarget struct {
	service string
	address string
}

// runDrainBarrier is the winner-only body of a recording phase's drain (the
// caller already CAS'd running → draining). It stops attacks and the poller so
// no new records are generated, bumps the rule version so the recording rule
// drops from the poll set (the SDK's drain tracker then flushes + reports),
// waits (bounded) for every expected instance, and persists the outcome.
func (o *Orchestrator) runDrainBarrier(ctx context.Context, p *model.ExperimentPhase, pws []model.PhaseWorkflow) {
	o.cancelPoller(p.ID)
	o.stopZeusAttacks(ctx, pws)
	if err := o.rules.BumpVersion(ctx); err != nil {
		o.logger.Warn("orchestrator: drain: bump version failed", "phase_id", p.ID, "error", err)
	}

	result := o.awaitDrain(ctx, p)
	if err := o.experiments.UpsertPhaseDrain(ctx, p.ID, result); err != nil {
		o.logger.Warn("orchestrator: drain: persist result failed", "phase_id", p.ID, "error", err)
	}
	o.logger.Info("orchestrator: drain complete",
		"phase_id", p.ID, "status", result.Status,
		"missing_instances", len(result.MissingInstances), "shortfall", result.ShortfallEntries)
}

// awaitDrain blocks until every expected instance has flushed and reported with
// per-instance received == entries_recorded (clean), or the drain timeout
// elapses (degraded, with the culprits named — after a fidelity-pull fallback to
// distinguish a lost report from a dead instance). INV-3.
func (o *Orchestrator) awaitDrain(ctx context.Context, p *model.ExperimentPhase) *model.PhaseDrainResult {
	expected := o.expectedDrainSet(ctx, p)
	if len(expected) == 0 {
		return &model.PhaseDrainResult{Status: "clean"} // nothing recorded ⇒ trivially complete
	}

	deadline := time.Now().Add(o.drainTimeout)
	for {
		if missing, _ := o.drainPending(p, expected); len(missing) == 0 {
			return &model.PhaseDrainResult{Status: "clean"}
		}
		if !time.Now().Before(deadline) {
			return o.classifyDrainTimeout(ctx, p, expected)
		}
		select {
		case <-ctx.Done():
			missing, shortfall := o.drainPending(p, expected)
			return &model.PhaseDrainResult{Status: "degraded", MissingInstances: missing, ShortfallEntries: shortfall}
		case <-time.After(o.drainPollInterval):
		}
	}
}

// drainPending returns the expected instances that have not yet satisfied the
// gate (no drain report, or a report whose entries_recorded exceeds manteion's
// received count) plus the total entry shortfall.
func (o *Orchestrator) drainPending(p *model.ExperimentPhase, expected map[string]drainTarget) (missing []string, shortfall int64) {
	reports := o.cacheStore.DrainReports(p.ExperimentID, p.ID)
	for instID, tgt := range expected {
		rep, ok := reports[instID]
		if !ok {
			missing = append(missing, instID)
			continue
		}
		received := o.cacheStore.ReceivedCount(p.ExperimentID, p.ID, tgt.service, instID)
		if received < rep.EntriesRecorded {
			missing = append(missing, instID)
			shortfall += rep.EntriesRecorded - received
		}
	}
	return missing, shortfall
}

// classifyDrainTimeout runs at the drain deadline: for each still-pending
// instance it pulls the fidelity snapshot to close the gap (a lost report whose
// data manteion fully received) or confirm degradation (a dead instance, a real
// shortfall, or a deregistered pusher it cannot reach).
func (o *Orchestrator) classifyDrainTimeout(ctx context.Context, p *model.ExperimentPhase, expected map[string]drainTarget) *model.PhaseDrainResult {
	reports := o.cacheStore.DrainReports(p.ExperimentID, p.ID)
	var missing []string
	var shortfall int64

	pending, _ := o.drainPending(p, expected)
	for _, instID := range pending {
		tgt := expected[instID]
		received := o.cacheStore.ReceivedCount(p.ExperimentID, p.ID, tgt.service, instID)

		if rep, ok := reports[instID]; ok {
			// Reported, but with a shortfall — the drop is real.
			missing = append(missing, instID)
			shortfall += rep.EntriesRecorded - received
			continue
		}
		// No report: try the fidelity fallback to distinguish dead from lost.
		if tgt.address == "" {
			missing = append(missing, instID) // deregistered pusher, unreachable
			continue
		}
		snap, err := o.controller.FetchFidelity(ctx, tgt.address, p.ExperimentID, p.ID, 5*time.Second)
		if err != nil {
			missing = append(missing, instID) // dead / unreachable
			continue
		}
		if snap.RecordDropped == 0 && received >= snap.RecordPushed {
			continue // gap closed: manteion received everything; the report was merely lost
		}
		missing = append(missing, instID)
		if snap.RecordEnqueued > received {
			shortfall += snap.RecordEnqueued - received
		}
	}

	if len(missing) == 0 {
		return &model.PhaseDrainResult{Status: "clean"}
	}
	return &model.PhaseDrainResult{Status: "degraded", MissingInstances: missing, ShortfallEntries: shortfall}
}

// expectedDrainSet is the set of instances owing a drain report (design doc Q2):
// the registered instances of the services this experiment records (its frozen
// set) ∪ every instance that pushed ≥ 1 batch (covers instances that appeared
// mid-phase or have since deregistered).
func (o *Orchestrator) expectedDrainSet(ctx context.Context, p *model.ExperimentPhase) map[string]drainTarget {
	expected := map[string]drainTarget{}
	for _, svc := range o.experimentFrozenServices(ctx, p.ExperimentID) {
		instances, err := o.controller.InstancesForService(ctx, svc)
		if err != nil {
			o.logger.Warn("orchestrator: drain: list instances failed", "service", svc, "error", err)
			continue
		}
		for _, inst := range instances {
			expected[inst.ID] = drainTarget{service: svc, address: inst.Address}
		}
	}
	for instID, svc := range o.cacheStore.PushedInstances(p.ExperimentID, p.ID) {
		if _, ok := expected[instID]; !ok {
			expected[instID] = drainTarget{service: svc} // address unknown (not currently registered)
		}
	}
	return expected
}

// experimentFrozenServices is the set of services frozen anywhere in the
// experiment — the services its baseline records (MANT-4) and thus the services
// whose instances owe drain reports.
func (o *Orchestrator) experimentFrozenServices(ctx context.Context, experimentID string) []string {
	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		o.logger.Warn("orchestrator: drain: list phases failed", "experiment_id", experimentID, "error", err)
		return nil
	}
	seen := map[string]bool{}
	var svcs []string
	for _, ph := range phases {
		for _, fs := range ph.FrozenServices {
			if !seen[fs.Service] {
				seen[fs.Service] = true
				svcs = append(svcs, fs.Service)
			}
		}
	}
	return svcs
}
