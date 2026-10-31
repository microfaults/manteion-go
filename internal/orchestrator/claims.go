package orchestrator

import (
	"sync"
)

// Phase claims. A phase entry (a fresh start or a resume) holds a claim on its
// phase from the running CAS until it has started the phase's load or given
// up. Every transition that takes the phase out of running — a pause, a stop,
// a terminal transition, a resume's return to paused — ends the claim, and the
// next start or resume makes a new one. After its convergence wait an entry
// acts on the phase only while its claim is still the phase's current one: a
// pause and a resume during the wait start a second entry, and the first one
// must then leave the phase to it, starting no load, failing nothing and
// re-pausing nothing. The phase-level pause and resume take no experiment
// lock, so the claim is what orders them against an entry in flight.
//
// Claims live in memory, which suffices for a single replica: a restart ends
// every entry in flight along with its claim.
//
// A phase's status CAS and its claim move together under the phase's
// transition lock (phaseLock), so the claim a CAS records can never overwrite
// one recorded by a later CAS. The lock is held for the CAS alone.

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
	cur, ok := o.claims[phaseID]
	return ok && cur == claim
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
