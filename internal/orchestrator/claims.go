package orchestrator

import (
	"context"
	"slices"
	"sync"

	"manteion-go/internal/model"
)

// Phase claims. A phase entry (a fresh start or a resume) holds a claim on its
// phase from the running CAS until it has started the phase's load or given
// up. Every transition that takes the phase out of running — a pause, a stop,
// a terminal transition, a resume's return to paused — ends the claim, and the
// next start or resume makes a new one. After its convergence wait an entry
// acts on the phase only while its claim is still the phase's current one: a
// pause and a resume during the wait start a second entry, and the first one
// must then leave the phase to it, starting no load, failing nothing and
// re-pausing nothing. An entry whose claim ends while it starts the load, after
// its last check, stops that load itself: it records the phase's poller only in
// one step with a final check of the claim (spawnPollerClaimed). The
// phase-level pause and resume take no experiment lock, so the claim is what
// orders them against an entry in flight.
//
// Claims live in memory, which suffices for a single replica: a restart ends
// every entry in flight along with its claim.
//
// A phase's status CAS and its claim move together under the phase's
// transition lock (phaseLock), so the claim a CAS records can never overwrite
// one recorded by a later CAS. An entry's driver-handle writes on the phase's
// workflow rows take the same lock (writeClaimed), so none can land after the
// claim that made it ended: the entry that holds the phase next never finds
// its handles overwritten by an entry whose claim ended. The lock is held for
// one CAS (with a teardown's row read, below), one handle write or one entry's
// audit-trail inserts, never across a zeus call.
//
// A transition that tears the phase's load down (a pause, a drain, a terminal
// transition) cancels the poller and reads the phase's workflow rows inside
// the same critical section as its CAS (endClaimSnapshot), and stops the
// drivers that one snapshot names after the lock is released: no zeus call
// runs under the lock, and no newer claim's handles can be in the snapshot,
// since a newer claim starts only with a CAS that takes the lock after it. A
// terminal transition also takes a snapshot before its CAS, for the teardown
// that has to precede its verdict (snapshotForEnd), and its winner sweeps
// again after the CAS from the second snapshot, so a driver or a poller that
// an entry still holding its claim recorded between the two is stopped too.

// claimToken identifies one claim on a phase; zero is no claim.
type claimToken uint64

// phaseLock returns the mutex that serializes a phase's status CASes with its
// claim bookkeeping.
func (o *Orchestrator) phaseLock(phaseID string) *sync.Mutex {
	o.mu.Lock()
	defer o.mu.Unlock()
	lk, ok := o.phaseLocks[phaseID]
	if !ok {
		lk = &sync.Mutex{}
		o.phaseLocks[phaseID] = lk
	}
	return lk
}

// newClaimLocked records a new claim on phaseID, ending any earlier one, and
// returns its token. The caller holds the phase's transition lock.
func (o *Orchestrator) newClaimLocked(phaseID string) claimToken {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claimSeq++
	o.claims[phaseID] = o.claimSeq
	return o.claimSeq
}

// claimPhase gives phaseID, which is already running, a new claim, ending any
// earlier one: the claim of an entry sequence started without a CAS of its
// own (enterPhase).
func (o *Orchestrator) claimPhase(phaseID string) claimToken {
	lk := o.phaseLock(phaseID)
	lk.Lock()
	defer lk.Unlock()
	return o.newClaimLocked(phaseID)
}

// claimRunning moves phaseID from `from` to running and, when this call made
// the transition, gives the phase a new claim: the start or resume's entry.
func (o *Orchestrator) claimRunning(phaseID string, cas func() (bool, error)) (claimToken, bool, error) {
	lk := o.phaseLock(phaseID)
	lk.Lock()
	defer lk.Unlock()
	claimed, err := cas()
	if err != nil || !claimed {
		return 0, claimed, err
	}
	return o.newClaimLocked(phaseID), true, nil
}

// holdsClaim reports whether claim is phaseID's current claim.
func (o *Orchestrator) holdsClaim(phaseID string, claim claimToken) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.holdsClaimLocked(phaseID, claim)
}

// holdsClaimLocked is holdsClaim for a caller that holds o.mu.
func (o *Orchestrator) holdsClaimLocked(phaseID string, claim claimToken) bool {
	cur, ok := o.claims[phaseID]
	return ok && cur == claim
}

// writeClaimed runs write, a short DB write an entry makes on its phase (a
// driver handle on a workflow row, the phase's audit-trail events), under the phase's transition lock and
// only while claim is still the phase's current one. It reports whether write
// ran, and write's error. A claim ends, and a newer one starts, only under
// that lock, so a write made here lands before the claim ends (a pause, which
// reads the rows after its CAS, finds the handle and stops its driver) and
// never over the handles of the entry that holds the phase next.
func (o *Orchestrator) writeClaimed(phaseID string, claim claimToken, write func() error) (bool, error) {
	lk := o.phaseLock(phaseID)
	lk.Lock()
	defer lk.Unlock()
	if !o.holdsClaim(phaseID, claim) {
		return false, nil
	}
	return true, write()
}

// endClaim runs cas, a CAS that takes phaseID out of running (or ends it),
// under the phase's transition lock. A non-zero claim must still be the
// phase's current claim, or cas does not run and nothing changes; a zero claim
// (an operator, the poller, recovery, the scheduler) asks for none. When cas
// makes its transition, the phase's claim ends.
func (o *Orchestrator) endClaim(phaseID string, claim claimToken, cas func() (bool, error)) (bool, error) {
	lk := o.phaseLock(phaseID)
	lk.Lock()
	defer lk.Unlock()
	if claim != 0 && !o.holdsClaim(phaseID, claim) {
		return false, nil
	}
	done, err := cas()
	if err == nil && done {
		o.mu.Lock()
		delete(o.claims, phaseID)
		o.mu.Unlock()
	}
	return done, err
}

// endClaimSnapshot is endClaim for a transition that tears the phase's load
// down: when cas makes its transition, the phase's poller is cancelled and
// its workflow rows are read in the same critical section, before any newer
// claim can record a handle or a poller. It returns whether cas made its
// transition and the rows read (nil when the read failed, which is logged):
// the drivers the caller stops, outside the lock.
func (o *Orchestrator) endClaimSnapshot(ctx context.Context, phaseID string, claim claimToken, cas func() (bool, error)) (bool, []model.PhaseWorkflow, error) {
	lk := o.phaseLock(phaseID)
	lk.Lock()
	defer lk.Unlock()
	if claim != 0 && !o.holdsClaim(phaseID, claim) {
		return false, nil, nil
	}
	done, err := cas()
	if err != nil || !done {
		return done, nil, err
	}
	o.mu.Lock()
	delete(o.claims, phaseID)
	o.mu.Unlock()
	o.cancelPoller(phaseID)
	pws, lerr := o.experiments.ListPhaseWorkflows(ctx, phaseID)
	if lerr != nil {
		o.logger.Warn("orchestrator: teardown: list workflows failed; the load is left to the next sweep",
			"phase_id", phaseID, "error", lerr)
		return true, nil, nil
	}
	return true, pws, nil
}

// snapshotForEnd is the first half of a terminal transition's teardown, taken
// under the phase's transition lock: it reads the phase and, when its status
// is one of from (and a non-zero claim is still the phase's current one),
// cancels the poller and reads the workflow rows whose drivers the caller
// stops before its CAS. ok is false, with nothing touched, when the phase is
// not the caller's to end; a read failure is logged and also reports false.
func (o *Orchestrator) snapshotForEnd(ctx context.Context, phaseID string, claim claimToken, from []string) (*model.ExperimentPhase, []model.PhaseWorkflow, bool) {
	lk := o.phaseLock(phaseID)
	lk.Lock()
	defer lk.Unlock()
	if claim != 0 && !o.holdsClaim(phaseID, claim) {
		return nil, nil, false // the entry's claim ended: the phase is someone else's
	}
	p, err := o.experiments.GetPhase(ctx, phaseID)
	if err != nil {
		o.logger.Error("orchestrator: finish phase: get phase failed",
			"phase_id", phaseID, "error", err)
		return nil, nil, false
	}
	if !slices.Contains(from, p.Status) {
		return nil, nil, false // not in an allowed source state: nothing to end, nothing touched
	}
	pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID)
	if err != nil {
		o.logger.Error("orchestrator: finish phase: list workflows failed",
			"phase_id", phaseID, "error", err)
		return nil, nil, false
	}
	o.cancelPoller(phaseID)
	return p, pws, true
}
