package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/zeus"
)

// StartPhase claims a phase (pending → running for a fresh start, paused →
// running for a resume) and runs the enter sequence: replay-set artifacts for
// the frozen services (built on a fresh start, re-read on a resume) → one
// generation bump per service → wait for the fleet to converge on it →
// materialize workflows into zeus → start runs and attacks → spawn the
// poller.
//
// Deliberately permissive about ordering: the scheduler always starts phases
// in position order, but an operator may start any pending phase directly
// via the API.
func (o *Orchestrator) StartPhase(ctx context.Context, phaseID string) error {
	p, err := o.experiments.GetPhase(ctx, phaseID)
	if err != nil {
		return fmt.Errorf("orchestrator: load phase: %w", err)
	}

	var fresh bool
	switch p.Status {
	case "pending":
		fresh = true
	case "paused":
		fresh = false
	default:
		return invalidState("phase", phaseID, p.Status, "pending|paused",
			fmt.Sprintf("orchestrator: phase %q not startable from status %q", phaseID, p.Status))
	}

	claim, claimed, err := o.claimRunning(phaseID, func() (bool, error) {
		return o.experiments.TransitionPhase(ctx, phaseID, "running", p.Status)
	})
	if err != nil {
		return fmt.Errorf("orchestrator: claim phase: %w", err)
	}
	if !claimed {
		// A concurrent start/pause/finish won the race; report what it left.
		return invalidState("phase", phaseID, o.phaseStatus(ctx, phaseID), p.Status,
			fmt.Sprintf("orchestrator: phase %q is no longer %s", phaseID, p.Status))
	}

	return o.enterClaimed(ctx, p, fresh, claim)
}

// phaseStatus re-reads a phase's status after a lost CAS race, "" when the
// row cannot be read.
func (o *Orchestrator) phaseStatus(ctx context.Context, phaseID string) string {
	p, err := o.experiments.GetPhase(ctx, phaseID)
	if err != nil {
		return ""
	}
	return p.Status
}

// PausePhase suspends a running phase: the poller, zeus attacks and k6 runs
// stop. A paused phase is not running, so its rules, its cache-box role (an
// isolation phase's replay rule, a baseline's record rule) and a frozen
// service's freeze_cfg and replay_set all leave the desired state of every
// service it touches and stop applying on those instances' next poll; pause
// bumps each service's generation so held long-polls take that poll now. Pause
// thaws. Resume via StartPhase, whose enter serves the phase again, bumps the
// same services and waits for the fleet like a fresh start (a frozen service's
// instances pull the phase's replay set again).
func (o *Orchestrator) PausePhase(ctx context.Context, phaseID string) error {
	// The pause ends the claim of an entry still waiting for its fleet: from
	// here on that entry leaves the phase alone (claims.go).
	paused, err := o.endClaim(phaseID, 0, func() (bool, error) {
		return o.experiments.TransitionPhase(ctx, phaseID, "paused", "running")
	})
	if err != nil {
		return fmt.Errorf("orchestrator: pause phase: %w", err)
	}
	if !paused {
		// Nothing matched: the phase is missing or not running.
		p, err := o.experiments.GetPhase(ctx, phaseID)
		if err != nil {
			return fmt.Errorf("orchestrator: load phase: %w", err)
		}
		return invalidState("phase", phaseID, p.Status, "running",
			fmt.Sprintf("orchestrator: phase %q is not running", phaseID))
	}

	o.cancelPoller(phaseID)
	o.stopPhaseAttacks(ctx, phaseID)
	// k6 runs aren't resumable; pause kills them and resume re-launches
	// (StartPhase fresh=false re-runs startPhaseRuns), matching attacks.
	if pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID); err == nil {
		o.stopPhaseRuns(ctx, pws)
	}
	// The phase left 'running' at the CAS above, so its rules and cache-box
	// role are already out of the materialized documents; the bump wakes the
	// held long-polls to fetch them. After the load stops, as in endPhase (M1).
	if p, err := o.experiments.GetPhase(ctx, phaseID); err != nil {
		o.logger.Warn("orchestrator: pause: load phase for the generation bump failed",
			"phase_id", phaseID, "error", err)
	} else {
		_, _ = o.bumpDesired(ctx, p) // a failed bump is logged per service; the pause stands
	}
	o.logger.Info("orchestrator: phase paused", "phase_id", phaseID)
	return nil
}

// StopPhase is the operator terminal entry point: finalize a phase as
// completed/failed/skipped (default completed). Idempotent — a no-op if the
// phase already reached a terminal state. An unknown phase is
// store.ErrNotFound; an unknown final status is ErrValidation.
func (o *Orchestrator) StopPhase(ctx context.Context, phaseID, finalStatus string) error {
	from := []string{"running", "paused", "draining"}
	switch finalStatus {
	case "":
		finalStatus = "completed"
	case "completed", "failed":
	case "skipped":
		from = append(from, "pending") // skipping a never-started phase is legal
	default:
		return validationError(fmt.Errorf("orchestrator: invalid phase final status %q", finalStatus))
	}
	if _, err := o.experiments.GetPhase(ctx, phaseID); err != nil {
		return fmt.Errorf("orchestrator: load phase: %w", err)
	}
	if finalStatus == "failed" {
		o.failPhase(ctx, phaseID, operatorFailReason, from...)
	} else {
		o.finishPhase(ctx, phaseID, finalStatus, from...)
	}
	return nil
}

// enterPhase runs the phase enter sequence for a phase that is already
// running, under a claim of its own (claimPhase) instead of StartPhase's CAS.
func (o *Orchestrator) enterPhase(ctx context.Context, p *model.ExperimentPhase, fresh bool) error {
	return o.enterClaimed(ctx, p, fresh, o.claimPhase(p.ID))
}

// enterClaimed runs the phase enter sequence under claim, the claim the
// running CAS gave this entry. Data errors (broken rules, unloadable
// workflows, zero startable attacks) and a fleet that does not converge fail
// the phase via failEntry, recording the error as the phase's failure reason —
// except a resume whose fleet does not converge, which leaves the phase paused
// (leavePaused). Once its claim has ended (a pause, a stop, or a newer entry
// took the phase), the entry leaves the phase alone.
func (o *Orchestrator) enterClaimed(ctx context.Context, p *model.ExperimentPhase, fresh bool, claim claimToken) error {
	fail := func(err error) error { return o.failEntry(ctx, p, claim, err) }

	exp, err := o.experiments.Get(ctx, p.ExperimentID)
	if err != nil {
		return fail(fmt.Errorf("orchestrator: load experiment: %w", err))
	}

	// Replay sets (INV-4): a fresh frozen phase builds one immutable artifact
	// per frozen service from the baseline recording and assigns it; a resume
	// re-reads the assignment. Both refusals (no baseline, zero entries,
	// degraded recording) keep the preload-gate wording.
	var artifacts map[string]string
	if len(p.FrozenServices) > 0 {
		if fresh {
			artifacts, err = o.buildReplayArtifacts(ctx, p)
		} else {
			artifacts, err = o.assignedArtifacts(ctx, p)
		}
		if err != nil {
			return fail(fmt.Errorf("orchestrator: preload gate: %w", err))
		}
	}

	// Inputs are written (phase running, rules attached, artifacts assigned,
	// freeze config derived from the phase). Bump every service once and wait
	// for the fleet to observe it (contract §4) — this replaces freeze, rule
	// push and preload; a timeout fails the phase naming the laggards. A bump
	// that fails is a convergence failure, never "nothing to wait for". Every
	// enter bumps, a resume included (M5): a paused phase had dropped out of
	// the document, so its frozen services were serving live.
	targets, err := o.bumpDesired(ctx, p)
	if err == nil {
		err = o.waitConverged(ctx, targets, artifacts, o.convergeTimeout,
			func(ctx context.Context) error { return o.entryHolds(ctx, p.ID, claim) })
	}
	if errors.Is(err, errLeftRunning) {
		return o.leftRunning(ctx, p) // a pause, a stop or a newer entry took the phase
	}
	if err != nil {
		err = fmt.Errorf("orchestrator: converge gate: %w", err)
		if !fresh {
			return o.leavePaused(ctx, p, claim, err)
		}
		return fail(err)
	}

	// The wait can be long: a pause, a stop or a newer entry that took the
	// phase meanwhile owns it now, and no load may start under an ended claim
	// or for a phase that left 'running'.
	if err := o.entryHolds(ctx, p.ID, claim); err != nil {
		if errors.Is(err, errLeftRunning) {
			return o.leftRunning(ctx, p)
		}
		return fail(fmt.Errorf("orchestrator: reload phase after the converge gate: %w", err))
	}
	o.logger.Info("orchestrator: fleet converged on phase entry",
		"phase_id", p.ID, "services", len(targets), "frozen", len(artifacts), "fresh", fresh)

	if fresh {
		o.recordFreezeEvents(ctx, p)
		o.recordRuleEvents(ctx, p)
	}

	if err := o.materializePhaseWorkflows(ctx, p.ID); err != nil {
		return fail(fmt.Errorf("orchestrator: materialize workflows: %w", err))
	}

	// A phase workflow drives load two ways, both scoped to (experiment, phase):
	// the k6 workflow RUN executes the DSL v2 DAG (the primary, workflow-shaped
	// driver), and a flat vegeta ATTACK against target_url runs additively for
	// targeted precision load. Start runs first so the DAG is generating traffic
	// before the additive attacks pile on.
	runs, err := o.startPhaseRuns(ctx, exp, p)
	if err != nil {
		return fail(err)
	}

	atks, err := o.startPhaseAttacks(ctx, exp, p)
	if err != nil {
		return fail(err)
	}

	// Fail only if drivers were configured but NONE of either kind started --
	// a workflow may be run-only, attack-only, or both, and losing one kind
	// while the other runs is a warning, not a phase failure. The failure
	// reason lists why each configured driver did not start.
	configured := runs.configured + atks.configured
	started := runs.started + atks.started
	if configured > 0 && started == 0 {
		failures := make([]string, 0, len(runs.failures)+len(atks.failures))
		failures = append(failures, runs.failures...)
		failures = append(failures, atks.failures...)
		return fail(fmt.Errorf("orchestrator: no load driver could be started (%d configured): %s",
			configured, strings.Join(failures, "; ")))
	}

	maxDur := runs.maxDur
	if atks.maxDur > maxDur {
		maxDur = atks.maxDur
	}

	if started > 0 {
		o.spawnPoller(p.ID, maxDur)
		o.logger.Info("orchestrator: phase started",
			"phase_id", p.ID, "runs", runs.started, "attacks", atks.started, "fresh", fresh)
		return nil
	}

	// No run and no attack → no driver takes this phase to a terminal state.
	// Auto-complete (a driver-less phase is valid — e.g. rules-only against
	// externally generated load). Only from 'running', so a concurrent
	// pause/stop wins the race.
	if o.autoComplete {
		o.logger.Info("orchestrator: phase has no load driver; auto-completing",
			"phase_id", p.ID)
		go o.finishPhase(context.Background(), p.ID, "completed", "running")
	} else {
		o.logger.Info("orchestrator: phase started without load driver",
			"phase_id", p.ID)
	}
	return nil
}

// errLeftRunning is an entry finding that its phase is no longer its own: its
// claim ended (a pause, a stop, a newer entry) or the phase left running.
var errLeftRunning = errors.New("the phase left running while its fleet converged")

// entryHolds reports whether the entry holding claim still owns phaseID: nil
// while the claim is the phase's current one and the phase is running,
// errLeftRunning once either stopped being true, and a read error as is.
func (o *Orchestrator) entryHolds(ctx context.Context, phaseID string, claim claimToken) error {
	if !o.holdsClaim(phaseID, claim) {
		return errLeftRunning
	}
	cur, err := o.experiments.GetPhase(ctx, phaseID)
	if err != nil {
		return err
	}
	if cur.Status != "running" {
		return errLeftRunning
	}
	return nil
}

// leftRunning is an entry's refusal once its phase is no longer its own: an
// invalid-state error naming the status the phase is in now. Whoever took the
// phase owns it; the entry starts no load and changes nothing.
func (o *Orchestrator) leftRunning(ctx context.Context, p *model.ExperimentPhase) error {
	status := o.phaseStatus(ctx, p.ID)
	o.logger.Warn("orchestrator: phase left running during the converge gate; no load started",
		"phase_id", p.ID, "status", status)
	return invalidState("phase", p.ID, status, "running",
		fmt.Sprintf("orchestrator: phase %q left running while its fleet converged (now %s); no load started", p.ID, status))
}

// failEntry fails the phase an entry holds, recording err as the reason, and
// returns err. The converge gate can hold the caller for
// MANTEION_CONVERGE_TIMEOUT, and a caller that goes away meanwhile (an
// operator's client hanging up) must leave the phase failed with its reason,
// not wedged 'running' with no load driver and no poller, so the failure is
// finalized on a context the caller cannot cancel. An entry whose claim has
// ended changes nothing: the phase belongs to whoever took it.
func (o *Orchestrator) failEntry(ctx context.Context, p *model.ExperimentPhase, claim claimToken, err error) error {
	if !o.holdsClaim(p.ID, claim) {
		o.logger.Warn("orchestrator: phase enter failed after its claim ended; phase left to its owner",
			"phase_id", p.ID, "error", err)
		return err
	}
	o.logger.Error("orchestrator: phase enter failed",
		"phase_id", p.ID, "error", err)
	o.endPhaseClaimed(context.WithoutCancel(ctx), p.ID, "failed", failureReason(err), claim, "running")
	return err
}

// leavePaused ends a resume whose fleet did not converge: the phase goes back
// to paused, and the resume returns cause. Nothing is lost by waiting — load
// has not started (the converge gate runs before any driver) — so a timed-out
// resume does not fail the phase; the operator can resume again. The CAS back
// to paused takes the phase's rules and freeze out of the documents its resume
// claim had put them back in, so its services are bumped again, after it. It
// runs on a context the caller cannot cancel, as failEntry does, and only
// while claim is still the phase's current one.
func (o *Orchestrator) leavePaused(ctx context.Context, p *model.ExperimentPhase, claim claimToken, cause error) error {
	ctx = context.WithoutCancel(ctx)
	repaused, err := o.endClaim(p.ID, claim, func() (bool, error) {
		return o.experiments.TransitionPhase(ctx, p.ID, "paused", "running")
	})
	switch {
	case err != nil:
		o.logger.Error("orchestrator: return an unconverged resume to paused failed",
			"phase_id", p.ID, "error", err)
	case repaused:
		o.logger.Warn("orchestrator: resume did not converge; phase left paused",
			"phase_id", p.ID, "error", cause)
		_, _ = o.bumpDesired(ctx, p) // a failed bump is logged per service
	default:
		// The claim ended, or the phase moved on: whoever took it owns it now.
		o.logger.Warn("orchestrator: resume did not converge after its claim ended; phase left to its owner",
			"phase_id", p.ID, "error", cause)
	}
	return cause
}

// failureReason renders an enter-sequence error as the phase's failure
// reason: the error text without the "orchestrator: " log prefix.
func failureReason(err error) string {
	return strings.TrimPrefix(err.Error(), "orchestrator: ")
}

// finishPhase ends a phase as "completed" or "skipped" — see endPhase for
// the winner-only contract. A failure goes through failPhase so it always
// carries its reason; a "failed" here is a programming error and is recorded
// with a placeholder reason rather than none.
func (o *Orchestrator) finishPhase(ctx context.Context, phaseID, status string, from ...string) bool {
	if status == "failed" {
		o.logger.Error("orchestrator: finishPhase called with status failed and no reason; use failPhase",
			"phase_id", phaseID)
		return o.failPhase(ctx, phaseID, "phase failed (no reason recorded)", from...)
	}
	return o.endPhase(ctx, phaseID, status, "", from...)
}

// failPhase ends a phase as "failed", recording reason — what step refused,
// which workflow / zeus handle, the underlying error text — in the same CAS
// statement as the transition (store.FailPhase), so a failed phase can never
// lack its why. Same winner-only contract as finishPhase.
func (o *Orchestrator) failPhase(ctx context.Context, phaseID, reason string, from ...string) bool {
	return o.endPhase(ctx, phaseID, "failed", reason, from...)
}

// endPhase is endPhaseClaimed for a caller that holds no claim: an operator,
// the poller, recovery, the scheduler. Its transition ends any entry's claim.
func (o *Orchestrator) endPhase(ctx context.Context, phaseID, status, reason string, from ...string) bool {
	return o.endPhaseClaimed(ctx, phaseID, status, reason, 0, from...)
}

// endPhaseClaimed is the single race-safe terminal path for a phase. It
// atomically transitions phaseID to `status` only if the phase's current
// status is one of `from` (for "failed", together with reason); the transition
// ends any entry's claim on the phase (claims.go). Just before
// that CAS it stops the poller and the zeus load and pulls the fidelity
// snapshots of a completed frozen phase — idempotent or read-only steps that
// must happen while the phase still holds its status. ONLY the caller that
// wins the transition runs the side effects that must happen once: record the
// verdict, bump the phase's services off its desired state (their rules and
// freeze leave every document) and wait non-fatally for the fleet, harvest
// once for "completed", recompute the experiment rollup, and re-walk the
// scheduler. A phase outside `from` is left untouched, and a loser changes
// nothing, which is what makes auto-complete, the poller, StopPhase, and
// cancellation safe to race. Returns whether this call performed the
// transition.
//
// Callers on a context that the poller teardown is about to cancel (i.e. the
// poller itself) must pass context.Background().
//
// A non-zero claim is an entry ending its own phase (failEntry): nothing
// happens unless the claim is still the phase's current one, checked before
// any side effect and again, under the phase's transition lock, at the CAS.
func (o *Orchestrator) endPhaseClaimed(ctx context.Context, phaseID, status, reason string, claim claimToken, from ...string) bool {
	if claim != 0 && !o.holdsClaim(phaseID, claim) {
		return false // the entry's claim ended: the phase is someone else's
	}
	// Snapshot phase + workflows (frozen services, attack IDs) before the flip.
	p, err := o.experiments.GetPhase(ctx, phaseID)
	if err != nil {
		o.logger.Error("orchestrator: finish phase: get phase failed",
			"phase_id", phaseID, "error", err)
		return false
	}
	pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID)
	if err != nil {
		o.logger.Error("orchestrator: finish phase: list workflows failed",
			"phase_id", phaseID, "error", err)
		return false
	}
	if !slices.Contains(from, p.Status) {
		return false // not in an allowed source state: nothing to end, nothing touched
	}

	// A recording (persist_cache) phase completes through the drain barrier
	// (INV-3): running → draining (drain gate) → completed. The CAS winner of
	// running→draining owns the drain AND the completion; losers no-op — the
	// winner-only discipline holds across BOTH hops. A failed/cancelled/skipped
	// recording phase skips the barrier (it never drains).
	if status == "completed" && p.PersistCache {
		won, err := o.endClaim(phaseID, claim, func() (bool, error) {
			return o.experiments.TransitionPhase(ctx, phaseID, "draining", from...)
		})
		if err != nil {
			o.logger.Error("orchestrator: finish phase: transition to draining failed",
				"phase_id", phaseID, "error", err)
			return false
		}
		if !won {
			return false // lost the race / not in an allowed source state
		}
		o.runDrainBarrier(ctx, p, pws)
		from = []string{"draining"} // now complete from draining
		claim = 0                   // the draining CAS ended the claim; the drain winner completes
	}

	// Before the terminal transition, while the phase still holds its status —
	// an isolation phase still 'running', so its frozen services' documents
	// still carry the replay set — stop the poller and the load, then pull the
	// fidelity snapshots the verdict reads. M1: on an operator StopPhase of a
	// still-firing phase, tail traffic must not reach the service once its
	// instances drop the phase's rules, replay rule and replay set, or that
	// traffic is harvested as frozen data (silent, plausible numbers); the
	// poller goes first, or it would read the stopped load as a natural end and
	// race this finish. INV-6: from the CAS on the phase is out of the
	// documents, and an instance whose held poll re-materializes in that window
	// (its wait elapsing) drops the replay set and reports
	// Preload.Committed=false. All of this is idempotent or read-only — the
	// recording-phase drain barrier already stopped the load — so a caller that
	// then loses the CAS has changed nothing; only the winner records the
	// verdict.
	o.cancelPoller(phaseID)
	o.stopZeusAttacks(ctx, pws)
	o.stopPhaseRuns(ctx, pws)
	var pull *fidelityPull
	if status == "completed" && len(p.FrozenServices) > 0 {
		pull = o.pullFidelity(ctx, p)
	}

	transitioned, err := o.endClaim(phaseID, claim, func() (bool, error) {
		if status == "failed" {
			return o.experiments.FailPhase(ctx, phaseID, reason, from...)
		}
		return o.experiments.TransitionPhase(ctx, phaseID, status, from...)
	})
	if err != nil {
		o.logger.Error("orchestrator: finish phase: transition failed",
			"phase_id", phaseID, "status", status, "error", err)
		return false
	}
	if !transitioned {
		return false // lost the race
	}

	// The verdict (INV-6), from the snapshots pulled above; harvest carries the
	// per-service replay aggregates (hits/misses/age) it trusted, not the SDK's
	// store counters.
	var fidelity map[string]replayFidelity
	if pull != nil {
		fidelity = o.recordVerdict(ctx, p, pull)
	}

	// The phase is terminal: its rules are no longer served, its cache-box rule
	// no longer synthesized, and a frozen service's freeze_cfg and replay_set
	// have left its document. Bump each service once — this bump is the rule
	// clear and the thaw — and wait, bounded and non-fatally, for the fleet to
	// observe it. A completed recording (persist_cache) phase already lost its
	// record rule at running → draining, where the drain barrier bumped — that
	// removal is what the SDK's drain tracker keys off (MANT-2); for a
	// recording phase that failed or was skipped without draining, this bump is
	// the removal signal.
	targets, _ := o.bumpDesired(ctx, p) // a failed bump is logged per service
	o.awaitConvergedNonFatal(ctx, p, targets)

	if o.faultEvents != nil {
		if err := o.faultEvents.EndOpenForPhase(ctx, phaseID, time.Now()); err != nil {
			o.logger.Warn("orchestrator: close fault events failed", "phase_id", phaseID, "error", err)
		}
	}

	if status == "completed" {
		o.harvestPhase(ctx, p, pws, fidelity) // exactly once — only the transition winner reaches here
	}

	if _, err := o.experiments.RecomputeExperimentResults(ctx, p.ExperimentID); err != nil {
		o.logger.Warn("orchestrator: recompute results failed",
			"experiment_id", p.ExperimentID, "error", err)
	}

	o.logger.Info("orchestrator: phase finished", "phase_id", phaseID, "status", status, "reason", reason)
	go o.advanceExperiment(context.Background(), p.ExperimentID)
	return true
}

// driverStart is what starting one kind of load driver for a phase yielded:
// how many workflow rows configured it, how many actually started, the
// longest configured duration, and one operator-readable line per driver
// that did not start — the phase's failure reason when none did.
type driverStart struct {
	started, configured int
	maxDur              time.Duration
	failures            []string
}

// driverStartFailure words one load driver (kind "run" or "attack") that did
// not start: a zeus refusal carries zeus's status and body, a transport
// failure says zeus was unreachable, anything else its own text.
func driverStartFailure(kind, driverID, workflowID string, err error) string {
	var re *zeus.ResponseError
	var ue *url.Error
	switch {
	case errors.As(err, &re):
		return fmt.Sprintf("zeus rejected %s %s for workflow %s: status %d: %s",
			kind, driverID, workflowID, re.Status, strings.TrimSpace(re.Body))
	case errors.As(err, &ue):
		return fmt.Sprintf("start %s %s for workflow %s: zeus unreachable: %v", kind, driverID, workflowID, ue)
	default:
		return fmt.Sprintf("start %s %s for workflow %s: %v", kind, driverID, workflowID, err)
	}
}

// =========================================================================
// Enter-sequence pieces
// =========================================================================

// materializePhaseWorkflows pushes every workflow definition attached to the
// phase into zeus: best-effort delete of any stale copy under the same id
// (covers renames, where overwrite-by-name would miss), then register with
// overwrite. Zeus's in-memory store is a cache of manteion's workflows table.
func (o *Orchestrator) materializePhaseWorkflows(ctx context.Context, phaseID string) error {
	if o.zeusClient == nil {
		return nil
	}
	pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID)
	if err != nil {
		return fmt.Errorf("list phase workflows: %w", err)
	}
	for _, pw := range pws {
		wf, err := o.workflows.Get(ctx, pw.WorkflowID)
		if err != nil {
			return fmt.Errorf("load workflow %q: %w", pw.WorkflowID, err)
		}
		if err := o.zeusClient.DeleteWorkflow(ctx, wf.ID); err != nil {
			o.logger.Warn("orchestrator: stale zeus workflow delete failed; proceeding",
				"workflow_id", wf.ID, "error", err)
		}
		// Stamp manteion's workflow id into the DSL doc so zeus stores it under
		// the SAME id manteion deletes and runs by. Zeus mints its own id when
		// the doc omits one, which would make DeleteWorkflow(wf.ID) and
		// StartRun(wf.ID) miss. The DSL is otherwise opaque to manteion.
		doc, err := withDocID(wf.DSL, wf.ID)
		if err != nil {
			return fmt.Errorf("stamp workflow id %q: %w", wf.ID, err)
		}
		if err := o.zeusClient.RegisterWorkflow(ctx, doc); err != nil {
			return zeusError(fmt.Sprintf("register workflow %q in zeus", wf.ID), err)
		}
	}
	return nil
}

// withDocID sets the top-level "id" field of a JSON DSL document to id,
// preserving every other field. This is the one place manteion reaches into
// the otherwise-opaque DSL: zeus keys its workflow store on the doc id, and
// manteion must control that key to address the workflow it materialized.
func withDocID(doc json.RawMessage, id string) (json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(doc, &m); err != nil {
		return nil, err
	}
	idJSON, err := json.Marshal(id)
	if err != nil {
		return nil, err
	}
	m["id"] = idJSON
	return json.Marshal(m)
}

// startPhaseAttacks launches one zeus attack per phase_workflows row that
// has a target URL (manteion treats workflow DSL as opaque and cannot derive
// one). The atk- id is persisted on the row BEFORE StartAttack so crash
// recovery can always find the handle; when zeus answers with a different
// id, the row is overwritten with zeus's (recovery must query the id zeus
// knows). Each attack that does not start is recorded on the returned
// driverStart's failures.
func (o *Orchestrator) startPhaseAttacks(ctx context.Context, exp *model.Experiment, p *model.ExperimentPhase) (driverStart, error) {
	var ds driverStart
	if o.zeusClient == nil {
		return ds, nil
	}
	pws, err := o.experiments.ListPhaseWorkflows(ctx, p.ID)
	if err != nil {
		return ds, fmt.Errorf("orchestrator: list phase workflows: %w", err)
	}

	for _, pw := range pws {
		if pw.TargetURL == "" {
			o.logger.Info("orchestrator: phase workflow has no target_url; not attack-driven",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID)
			continue
		}
		ds.configured++

		attackID := id.New("atk")
		if err := o.experiments.UpdatePhaseWorkflowZeusAttack(ctx, p.ID, pw.WorkflowID, attackID); err != nil {
			o.logger.Error("orchestrator: persist attack id failed",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID, "error", err)
			ds.failures = append(ds.failures, driverStartFailure("attack", attackID, pw.WorkflowID,
				fmt.Errorf("persist attack id: %w", err)))
			continue
		}

		method := pw.TargetMethod
		if method == "" {
			method = "GET"
		}
		rate := int(pw.RateRPS)
		if rate <= 0 {
			// No precision rate configured — approximate 1 rps per VU so the
			// open-loop attacker has a usable rate.
			rate = pw.VUs
			o.logger.Info("orchestrator: no rate_rps; approximating from vus",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID, "rate", rate)
		}

		zeusID, err := o.zeusClient.StartAttack(ctx, zeus.AttackRequest{
			ID:            attackID,
			Target:        zeus.AttackTargetSpec{URL: pw.TargetURL, Method: method},
			Rate:          rate,
			DurationS:     pw.DurationSec,
			MetaTraceID:   p.ID,
			ExperimentID:  exp.ID,
			WorkflowLabel: pw.WorkflowID,
		})
		if err != nil {
			o.logger.Error("orchestrator: start attack failed",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID, "error", err)
			ds.failures = append(ds.failures, driverStartFailure("attack", attackID, pw.WorkflowID, err))
			continue
		}
		if zeusID != "" && zeusID != attackID {
			o.logger.Warn("orchestrator: zeus returned different attack id; using zeus id",
				"phase_id", p.ID, "client_id", attackID, "zeus_id", zeusID)
			if err := o.experiments.UpdatePhaseWorkflowZeusAttack(ctx, p.ID, pw.WorkflowID, zeusID); err != nil {
				o.logger.Error("orchestrator: persist zeus attack id failed",
					"phase_id", p.ID, "workflow_id", pw.WorkflowID, "error", err)
			}
		}

		ds.started++
		if d := time.Duration(pw.DurationSec) * time.Second; d > ds.maxDur {
			ds.maxDur = d
		}
	}
	return ds, nil
}

// startPhaseRuns launches one k6 workflow run per phase_workflows row,
// executing the workflow's DSL v2 DAG. The run is scoped to the phase:
// experiment_id + meta_trace_id=phase_id tag the traffic so records and
// traces slice by phase. The run id is persisted BEFORE StartRun so crash
// recovery can find the handle even if the response is lost; zeus echoes the
// id back (manteion mints it). Each run that does not start is recorded on
// the returned driverStart's failures.
//
// The workflow is already validated-and-registered in zeus by
// materializePhaseWorkflows, so a StartRun failure here is a live-fleet
// problem (zeus down, run rejected), logged per-row rather than fatal — the
// phase can still be driven by additive attacks, and a fully driver-less
// phase is handled by the caller (whose failure reason lists these).
func (o *Orchestrator) startPhaseRuns(ctx context.Context, exp *model.Experiment, p *model.ExperimentPhase) (driverStart, error) {
	var ds driverStart
	if o.zeusClient == nil {
		return ds, nil
	}
	pws, err := o.experiments.ListPhaseWorkflows(ctx, p.ID)
	if err != nil {
		return ds, fmt.Errorf("orchestrator: list phase workflows: %w", err)
	}

	for _, pw := range pws {
		ds.configured++
		runID := id.New("run")

		// Unlike attacks, the run id is stamped only AFTER a successful start:
		// a failed StartRun (zeus down, DSL rejected) must leave no handle, or
		// the poller would wait forever on a run that never existed. The
		// crash-recovery window this trades away (a manteion death between
		// StartRun returning and the stamp) is negligible -- an orphaned run
		// self-terminates after its DurationS.
		zeusRunID, err := o.zeusClient.StartRun(ctx, pw.WorkflowID, zeus.RunRequest{
			RunID:         runID,
			ExperimentID:  exp.ID,
			DatasetID:     runDatasetID(pw.DatasetID, o.zeusDatasetID),
			VUs:           pw.VUs,
			RateRPS:       pw.RateRPS,
			DurationS:     pw.DurationSec,
			MetaTraceID:   p.ID,
			WorkflowLabel: pw.WorkflowID,
		})
		if err != nil {
			o.logger.Error("orchestrator: start run failed",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID, "error", err)
			ds.failures = append(ds.failures, driverStartFailure("run", runID, pw.WorkflowID, err))
			continue
		}
		if zeusRunID == "" {
			zeusRunID = runID
		}
		if err := o.experiments.UpdatePhaseWorkflowZeusRun(ctx, p.ID, pw.WorkflowID, zeusRunID); err != nil {
			o.logger.Error("orchestrator: persist run id failed",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID, "run_id", zeusRunID, "error", err)
			// The run is live in zeus but unrecorded here; still counts as a
			// started driver so the phase isn't mistaken for driver-less.
		}

		ds.started++
		if d := time.Duration(pw.DurationSec) * time.Second; d > ds.maxDur {
			ds.maxDur = d
		}
	}
	return ds, nil
}

// runDatasetID picks the dataset a phase_workflows row's run binds to: the
// row's own dataset_id (dataset per workflow, product decision 3), else the
// process-wide MANTEION_ZEUS_DATASET_ID so plans that predate the column keep
// working. Attack legs (startPhaseAttacks) carry no dataset either way.
func runDatasetID(row, fallback string) string {
	if row != "" {
		return row
	}
	return fallback
}

// stopPhaseRuns best-effort stops every workflow run recorded on the phase.
func (o *Orchestrator) stopPhaseRuns(ctx context.Context, pws []model.PhaseWorkflow) {
	if o.zeusClient == nil {
		return
	}
	for _, pw := range pws {
		if pw.ZeusRunID == "" {
			continue
		}
		if err := o.zeusClient.StopRun(ctx, pw.ZeusRunID); err != nil {
			o.logger.Warn("orchestrator: stop zeus run failed",
				"attack_id", pw.ZeusRunID, "error", err)
		}
	}
}

// stopPhaseAttacks best-effort stops every attack recorded on the phase.
func (o *Orchestrator) stopPhaseAttacks(ctx context.Context, phaseID string) {
	if o.zeusClient == nil {
		return
	}
	pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID)
	if err != nil {
		o.logger.Warn("orchestrator: stop attacks: list workflows failed",
			"phase_id", phaseID, "error", err)
		return
	}
	for _, pw := range pws {
		if pw.ZeusAttackID == "" {
			continue
		}
		if err := o.zeusClient.StopAttack(ctx, pw.ZeusAttackID); err != nil {
			o.logger.Warn("orchestrator: stop zeus attack failed",
				"phase_id", phaseID, "attack_id", pw.ZeusAttackID, "error", err)
		}
	}
}

// stopZeusAttacks stops each of the phase's running zeus attacks. Best-effort:
// a stop error (e.g. an already-finished attack) is logged, not fatal. Safe to
// call more than once (the drain barrier stops attacks before the gate; the
// terminal teardown calls it again).
func (o *Orchestrator) stopZeusAttacks(ctx context.Context, pws []model.PhaseWorkflow) {
	if o.zeusClient == nil {
		return
	}
	for _, pw := range pws {
		if pw.ZeusAttackID == "" {
			continue
		}
		if err := o.zeusClient.StopAttack(ctx, pw.ZeusAttackID); err != nil {
			o.logger.Warn("orchestrator: zeus attack stop failed",
				"attack_id", pw.ZeusAttackID, "error", err)
		}
	}
}

// =========================================================================
// Replay sets (the cache-box freeze's inputs)
// =========================================================================

// buildReplayArtifacts is the replay-set half of a fresh frozen phase's entry
// (INV-4): for each frozen service it builds an immutable, content-addressed
// artifact from the experiment's baseline recording (cachestore.BuildArtifact)
// and assigns it to the (phase, service) pair, which is what the materializer
// serves as that service's replay_set (desired.PhaseSources). It returns
// service → artifact id: the sets the convergence barrier waits for. The
// baseline is the experiment's most recently completed phase that persisted
// its cache and froze nothing.
//
// The refusals are the former preload gate's, with its wording: no completed
// baseline, a degraded one (unless MANTEION_ALLOW_DEGRADED_BASELINE), a frozen
// service that recorded zero entries, or entries recorded under another key
// strategy than the freeze replays with. Load never starts against a replay set
// that cannot be built.
func (o *Orchestrator) buildReplayArtifacts(ctx context.Context, p *model.ExperimentPhase) (map[string]string, error) {
	baseline, err := o.baselinePhase(ctx, p.ExperimentID)
	if err != nil {
		return nil, fmt.Errorf("resolve baseline: %w", err)
	}
	if baseline == nil {
		return nil, fmt.Errorf("no completed baseline recording for experiment %q; cannot preload frozen services", p.ExperimentID)
	}

	// Refuse to build a replay set from a degraded recording (INV-3): its
	// coverage is unverified, so any isolation run over it is suspect. The
	// operator can override with MANTEION_ALLOW_DEGRADED_BASELINE.
	drain, err := o.experiments.GetPhaseDrain(ctx, baseline.ID)
	if err != nil {
		return nil, fmt.Errorf("read baseline drain result: %w", err)
	}
	if drain.Degraded() && !o.allowDegradedBaseline {
		return nil, fmt.Errorf("baseline recording %q is degraded (%d missing instances, %d entry shortfall); "+
			"refusing to preload — set MANTEION_ALLOW_DEGRADED_BASELINE=true to override",
			baseline.ID, len(drain.MissingInstances), drain.ShortfallEntries)
	}

	out := make(map[string]string, len(p.FrozenServices))
	for _, fs := range p.FrozenServices {
		// The entries are read once here for the two preflights, and again by
		// BuildArtifact, which encodes them.
		entries, err := o.cacheStore.Read(baseline.ExperimentID, baseline.ID, fs.Service)
		if err != nil {
			return nil, fmt.Errorf("read baseline entries for %q: %w", fs.Service, err)
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("baseline recorded zero entries for frozen service %q; nothing to replay", fs.Service)
		}
		if err := verifyRecordedStrategy(fs.Service, entries, model.ResolveKeyStrategy(fs.KeyStrategy)); err != nil {
			return nil, err
		}
		info, err := o.cacheStore.BuildArtifact(baseline.ExperimentID, baseline.ID, fs.Service)
		if err != nil {
			return nil, fmt.Errorf("build replay set for %q: %w", fs.Service, err)
		}
		if err := o.artifacts.Upsert(ctx, &model.ReplayArtifact{
			ID: info.ID, ExperimentID: p.ExperimentID, PhaseID: p.ID, Service: fs.Service,
			EntryCount: info.Count, W5SHA256: info.W5SHA256, SizeBytes: info.SizeBytes, Path: info.Path,
		}); err != nil {
			return nil, fmt.Errorf("assign replay set for %q: %w", fs.Service, err)
		}
		out[fs.Service] = info.ID
		o.logger.Info("orchestrator: replay set assigned",
			"phase_id", p.ID, "service", fs.Service, "artifact", info.ID,
			"entries", info.Count, "baseline_phase_id", baseline.ID)
	}
	return out, nil
}

// assignedArtifacts is the replay-set half of a resume: the artifacts the
// phase's fresh start assigned, service → artifact id, re-read rather than
// rebuilt, so a resumed phase replays exactly the sets it started with. A
// frozen service with none assigned is an error.
func (o *Orchestrator) assignedArtifacts(ctx context.Context, p *model.ExperimentPhase) (map[string]string, error) {
	out := make(map[string]string, len(p.FrozenServices))
	for _, fs := range p.FrozenServices {
		a, err := o.artifacts.ForPhaseService(ctx, p.ID, fs.Service)
		if err != nil {
			return nil, fmt.Errorf("read the replay set assigned to %q: %w", fs.Service, err)
		}
		if a == nil {
			return nil, fmt.Errorf("no replay set assigned for frozen service %q (resume before a fresh start?)", fs.Service)
		}
		out[fs.Service] = a.ID
	}
	return out, nil
}

// verifyRecordedStrategy is the replay-set strategy preflight (MANT-5/INV-2): the
// key strategy the entries were recorded under must equal the strategy the
// freeze context will replay with, or every key would derive differently and
// the isolation run would be 100% miss. An entry with an empty key_strategy
// (recorded by a legacy SDK) is not checked. This converts a would-be silent
// all-miss run into an explicit preflight failure.
func verifyRecordedStrategy(service string, entries []atroposdk.CacheBoxWireEntry, expected string) error {
	for i := range entries {
		if s := entries[i].KeyStrategy; s != "" && s != expected {
			return fmt.Errorf("preload %q: key_strategy_mismatch: recorded under %q but the freeze context uses %q "+
				"(record and replay must key identically)", service, s, expected)
		}
	}
	return nil
}

// baselinePhase returns the experiment's most recently completed phase with
// persist_cache=true and no frozen services, or nil when none exists.
func (o *Orchestrator) baselinePhase(ctx context.Context, experimentID string) (*model.ExperimentPhase, error) {
	phases, err := o.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		return nil, fmt.Errorf("list phases: %w", err)
	}
	var best *model.ExperimentPhase
	for _, p := range phases {
		if p.Status != "completed" || !p.PersistCache || len(p.FrozenServices) > 0 {
			continue
		}
		if best == nil {
			best = p
			continue
		}
		// Prefer the most recent completion; fall back to position order.
		switch {
		case p.CompletedAt != nil && best.CompletedAt != nil:
			if p.CompletedAt.After(*best.CompletedAt) {
				best = p
			}
		case p.Position > best.Position:
			best = p
		}
	}
	return best, nil
}

// =========================================================================
// Desired-state services and bumps
// =========================================================================

// phaseServices returns the de-duplicated union of the phase's frozen
// services and its rules' target services — every service the phase may
// have touched. An error means some rule could not be read: the services
// returned are then incomplete, every one that could be resolved.
func (o *Orchestrator) phaseServices(ctx context.Context, p *model.ExperimentPhase) ([]string, error) {
	seen := make(map[string]struct{})
	var svcs []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			svcs = append(svcs, s)
		}
	}
	for _, fs := range p.FrozenServices {
		add(fs.Service)
	}
	prs, err := o.experiments.ListPhaseRules(ctx, p.ID)
	if err != nil {
		return svcs, fmt.Errorf("list the rules of phase %q: %w", p.ID, err)
	}
	var errs []error
	for _, pr := range prs {
		r, err := o.rules.Get(ctx, pr.RuleID)
		if err != nil {
			errs = append(errs, fmt.Errorf("load rule %q: %w", pr.RuleID, err))
			continue
		}
		add(r.Service)
	}
	return svcs, errors.Join(errs...)
}

// desiredServices is every service whose materialized desired state this
// phase changes: the frozen services and rule targets (phaseServices), plus,
// for a baseline, the services frozen anywhere in the experiment — the
// baseline is what synthesizes their record rule (ActiveCacheBoxPhaseForService).
// An error means the list is incomplete (see phaseServices).
func (o *Orchestrator) desiredServices(ctx context.Context, p *model.ExperimentPhase) ([]string, error) {
	svcs, err := o.phaseServices(ctx, p)
	if !p.PersistCache {
		return svcs, err
	}
	frozen, ferr := o.experimentFrozenServices(ctx, p.ExperimentID)
	seen := make(map[string]struct{}, len(svcs))
	for _, s := range svcs {
		seen[s] = struct{}{}
	}
	for _, s := range frozen {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			svcs = append(svcs, s)
		}
	}
	return svcs, errors.Join(err, ferr)
}

// bumpDesired bumps the generation of every service this phase's desired
// state touches (one bump each, in the same order every time) and returns the
// targets — the generations instances must observe to have converged. Every
// service it can resolve is bumped; a service it cannot resolve or bump is
// missing from targets and named in the error (each one logged too). A caller
// that waits must treat that error as a convergence failure: a missing target
// is never "nothing to wait for".
func (o *Orchestrator) bumpDesired(ctx context.Context, p *model.ExperimentPhase) (map[string]uint64, error) {
	svcs, err := o.desiredServices(ctx, p)
	if err != nil {
		o.logger.Warn("orchestrator: resolve the services to bump failed", "phase_id", p.ID, "error", err)
		err = fmt.Errorf("resolve the services to bump: %w", err)
	}
	errs := []error{err}
	targets := map[string]uint64{}
	for _, svc := range svcs {
		gen, err := o.generations.Bump(ctx, svc)
		if err != nil {
			o.logger.Warn("orchestrator: bump generation failed", "phase_id", p.ID, "service", svc, "error", err)
			errs = append(errs, fmt.Errorf("bump the generation of %q: %w", svc, err))
			continue
		}
		targets[svc] = gen
	}
	return targets, errors.Join(errs...)
}

// =========================================================================
// Fault-event audit trail
// =========================================================================

// recordFreezeEvents opens a cachebox fault event per frozen service.
func (o *Orchestrator) recordFreezeEvents(ctx context.Context, p *model.ExperimentPhase) {
	if o.faultEvents == nil {
		return
	}
	for _, fs := range p.FrozenServices {
		detail, _ := json.Marshal(map[string]string{"mode": fs.Mode, "key_strategy": fs.KeyStrategy, "mutation_policy": fs.MutationPolicy})
		ev := &model.PhaseFaultEvent{
			ID: id.New("fevt"), PhaseID: p.ID, Source: "cachebox", Service: fs.Service,
			Kind: "cachebox:" + fs.Mode, Detail: detail, StartedAt: time.Now(),
		}
		if err := o.faultEvents.Create(ctx, ev); err != nil {
			o.logger.Warn("orchestrator: record cachebox event failed",
				"phase_id", p.ID, "service", fs.Service, "error", err)
		}
	}
}

// recordRuleEvents opens a rule fault event per phase rule.
func (o *Orchestrator) recordRuleEvents(ctx context.Context, p *model.ExperimentPhase) {
	if o.faultEvents == nil {
		return
	}
	prs, err := o.experiments.ListPhaseRules(ctx, p.ID)
	if err != nil {
		o.logger.Warn("orchestrator: record rule events: list rules failed", "phase_id", p.ID, "error", err)
		return
	}
	for _, pr := range prs {
		r, err := o.rules.Get(ctx, pr.RuleID)
		if err != nil {
			o.logger.Warn("orchestrator: record rule event: load rule failed",
				"phase_id", p.ID, "rule_id", pr.RuleID, "error", err)
			continue
		}
		ev := &model.PhaseFaultEvent{
			ID: id.New("fevt"), PhaseID: p.ID, Source: "rule", Service: r.Service,
			Kind: o.ruleKind(ctx, r), StartedAt: time.Now(),
		}
		if err := o.faultEvents.Create(ctx, ev); err != nil {
			o.logger.Warn("orchestrator: record rule event failed",
				"phase_id", p.ID, "rule_id", r.ID, "error", err)
		}
	}
}

// ruleKind derives "category:fault_type" from a rule's fault spec; falls back
// to "rule" when the action is not a fault spec or the spec can't be loaded.
func (o *Orchestrator) ruleKind(ctx context.Context, r *model.Rule) string {
	if r.Action.FaultSpecID == "" {
		return "rule"
	}
	spec, err := o.faults.GetSpec(ctx, r.Action.FaultSpecID)
	if err != nil {
		return "rule"
	}
	return spec.Category + ":" + spec.FaultType
}

// =========================================================================
// Poller handle bookkeeping
// =========================================================================

// spawnPoller starts the zeus supervision goroutine for the phase and
// records its cancel handle, replacing (and cancelling) any previous one.
func (o *Orchestrator) spawnPoller(phaseID string, phaseDuration time.Duration) {
	pollCtx, cancel := context.WithCancel(context.Background())
	o.mu.Lock()
	if prev, ok := o.running[phaseID]; ok {
		prev()
	}
	o.running[phaseID] = cancel
	o.mu.Unlock()
	go o.pollPhaseAttacks(pollCtx, phaseID, phaseDuration)
}

// cancelPoller stops the phase's poller goroutine, if any.
func (o *Orchestrator) cancelPoller(phaseID string) {
	o.mu.Lock()
	if cancel, ok := o.running[phaseID]; ok {
		cancel()
		delete(o.running, phaseID)
	}
	o.mu.Unlock()
}
