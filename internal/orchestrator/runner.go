package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/zeus"
)

// StartPhase claims a phase (pending → running for a fresh start, paused →
// running for a resume) and runs the enter sequence: cache preload (fresh
// non-baseline only) → freeze frozen_services → push phase rules →
// materialize workflows into zeus → start attacks → spawn the poller.
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
		return fmt.Errorf("orchestrator: phase %q not startable from status %q", phaseID, p.Status)
	}

	claimed, err := o.experiments.TransitionPhase(ctx, phaseID, "running", p.Status)
	if err != nil {
		return fmt.Errorf("orchestrator: claim phase: %w", err)
	}
	if !claimed {
		// A concurrent start/pause/finish won the race.
		return fmt.Errorf("orchestrator: phase %q is no longer %s", phaseID, p.Status)
	}

	return o.enterPhase(ctx, p, fresh)
}

// PausePhase suspends a running phase: the poller and zeus attacks stop, but
// rules and cache-box freezes stay applied so a resume continues the same
// experimental condition. Resume via StartPhase.
func (o *Orchestrator) PausePhase(ctx context.Context, phaseID string) error {
	paused, err := o.experiments.TransitionPhase(ctx, phaseID, "paused", "running")
	if err != nil {
		return fmt.Errorf("orchestrator: pause phase: %w", err)
	}
	if !paused {
		return fmt.Errorf("orchestrator: phase %q is not running", phaseID)
	}

	o.cancelPoller(phaseID)
	o.stopPhaseAttacks(ctx, phaseID)
	// k6 runs aren't resumable; pause kills them and resume re-launches
	// (StartPhase fresh=false re-runs startPhaseRuns), matching attacks.
	if pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID); err == nil {
		o.stopPhaseRuns(ctx, pws)
	}
	o.logger.Info("orchestrator: phase paused", "phase_id", phaseID)
	return nil
}

// StopPhase is the operator terminal entry point: finalize a phase as
// completed/failed/skipped (default completed). Idempotent — a no-op if the
// phase already reached a terminal state.
func (o *Orchestrator) StopPhase(ctx context.Context, phaseID, finalStatus string) error {
	from := []string{"running", "paused", "draining"}
	switch finalStatus {
	case "":
		finalStatus = "completed"
	case "completed", "failed":
	case "skipped":
		from = append(from, "pending") // skipping a never-started phase is legal
	default:
		return fmt.Errorf("orchestrator: invalid phase final status %q", finalStatus)
	}
	o.finishPhase(ctx, phaseID, finalStatus, from...)
	return nil
}

// enterPhase runs the phase enter sequence after the running claim. Data
// errors (broken rules, unloadable workflows, zero startable attacks) fail
// the phase via finishPhase; fanout errors against an empty/struggling SDK
// fleet are warnings, matching the legacy FSM.
func (o *Orchestrator) enterPhase(ctx context.Context, p *model.ExperimentPhase, fresh bool) error {
	fail := func(err error) error {
		o.logger.Error("orchestrator: phase enter failed",
			"phase_id", p.ID, "error", err)
		o.finishPhase(ctx, p.ID, "failed", "running")
		return err
	}

	exp, err := o.experiments.Get(ctx, p.ExperimentID)
	if err != nil {
		return fail(fmt.Errorf("orchestrator: load experiment: %w", err))
	}

	// A cache-box phase becoming active changes the rule set SDKs synthesize on
	// poll: a baseline (persist_cache) adds record rules, an isolation phase
	// (frozen_services) adds replay rules. Bump the rule version so SDKs re-poll
	// and pick them up (the phase is already 'running' here). Cleared on finish.
	//
	// This fires on EVERY enter of a cache-box phase, not just a fresh start (M5):
	// on resume (fresh=false) the paused phase had dropped out of the synthesized
	// set, and an unrelated rule edit may have bumped the version past what a
	// frozen SDK last saw. Without a bump here that SDK 304s forever — the frozen
	// service never receives its replay rule and runs live (no synthetic delay,
	// live downstream calls, zero counters) for the rest of the phase.
	if p.PersistCache || len(p.FrozenServices) > 0 {
		if err := o.rules.BumpVersion(ctx); err != nil {
			o.logger.Warn("orchestrator: bump version for cache-box phase start failed",
				"phase_id", p.ID, "error", err)
		}
	}

	// Cache preload is a HARD GATE on a fresh start of a frozen phase (INV-4):
	// load may only begin against a replay set that every live instance has
	// verify-committed (byte-exact count + checksum). A missing baseline, a
	// zero-entry frozen service, or any instance failing/mismatching aborts the
	// phase here — before freeze and before any load. A resume continues with
	// whatever the boxes already hold.
	if fresh && len(p.FrozenServices) > 0 {
		if err := o.preloadCacheEntries(ctx, p); err != nil {
			return fail(fmt.Errorf("orchestrator: preload gate: %w", err))
		}
	}

	// Freeze is idempotent (intent-tracked), so re-freezing on resume re-asserts
	// desired state. For a frozen service an un-asserted freeze is a leak
	// (INV-4), so freeze-fanout failure is phase-fatal.
	if err := o.freezeServices(ctx, p); err != nil {
		return fail(fmt.Errorf("orchestrator: freeze gate: %w", err))
	}
	if fresh {
		o.recordFreezeEvents(ctx, p)
	}

	if err := o.pushPhaseRules(ctx, p); err != nil {
		return fail(fmt.Errorf("orchestrator: push phase rules: %w", err))
	}
	if fresh {
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
	runsStarted, runsConfigured, runsMaxDur, err := o.startPhaseRuns(ctx, exp, p)
	if err != nil {
		return fail(err)
	}

	atkStarted, atkConfigured, atkMaxDur, err := o.startPhaseAttacks(ctx, exp, p)
	if err != nil {
		return fail(err)
	}

	// Fail only if drivers were configured but NONE of either kind started --
	// a workflow may be run-only, attack-only, or both, and losing one kind
	// while the other runs is a warning, not a phase failure.
	configured := runsConfigured + atkConfigured
	started := runsStarted + atkStarted
	if configured > 0 && started == 0 {
		return fail(fmt.Errorf("orchestrator: no load driver could be started (%d configured)", configured))
	}

	maxDur := runsMaxDur
	if atkMaxDur > maxDur {
		maxDur = atkMaxDur
	}

	if started > 0 {
		o.spawnPoller(p.ID, maxDur)
		o.logger.Info("orchestrator: phase started",
			"phase_id", p.ID, "runs", runsStarted, "attacks", atkStarted, "fresh", fresh)
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

// finishPhase is the single race-safe terminal path for a phase. It
// atomically transitions phaseID to `status` only if the phase's current
// status is one of `from`; ONLY the caller that wins that transition runs
// the side effects: tear down the poller, clear injected rules, thaw frozen
// services, stop zeus attacks, harvest once for "completed", recompute the
// experiment rollup, and re-walk the scheduler. Losers are a no-op, which is
// what makes auto-complete, the poller, StopPhase, and cancellation safe to
// race. Returns whether this call performed the transition.
//
// Callers on a context that the poller teardown is about to cancel (i.e. the
// poller itself) must pass context.Background().
func (o *Orchestrator) finishPhase(ctx context.Context, phaseID, status string, from ...string) bool {
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

	// A recording (persist_cache) phase completes through the drain barrier
	// (INV-3): running → draining (drain gate) → completed. The CAS winner of
	// running→draining owns the drain AND the completion; losers no-op — the
	// winner-only discipline holds across BOTH hops. A failed/cancelled/skipped
	// recording phase skips the barrier (it never drains).
	if status == "completed" && p.PersistCache {
		won, err := o.experiments.TransitionPhase(ctx, phaseID, "draining", from...)
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
	}

	transitioned, err := o.experiments.TransitionPhase(ctx, phaseID, status, from...)
	if err != nil {
		o.logger.Error("orchestrator: finish phase: transition failed",
			"phase_id", phaseID, "status", status, "error", err)
		return false
	}
	if !transitioned {
		return false // lost the race / not in an allowed source state
	}

	o.cancelPoller(phaseID)

	// M1: stop load BEFORE clearing rules and thawing the frozen service. On an
	// operator StopPhase of a still-firing phase, tail traffic must not hit the
	// service after it is un-frozen / its replay rule is cleared, or that traffic
	// is harvested as frozen data (silent, plausible numbers). Both stops are
	// idempotent — the recording-phase drain barrier already called them; this is
	// the non-draining operator-stop path, which reaches teardown directly.
	o.stopZeusAttacks(ctx, pws)
	o.stopPhaseRuns(ctx, pws)

	// A finished cache-box phase drops out of the synthesized rule set — bump
	// the rule version so SDKs re-poll and stop recording/replaying. For a
	// recording (persist_cache) phase this rule-set removal is what the SDK's
	// drain tracker keys off (MANT-2): the phase is terminal here, so the poll
	// no longer synthesizes its cache-box rule.
	if p.PersistCache || len(p.FrozenServices) > 0 {
		if err := o.rules.BumpVersion(ctx); err != nil {
			o.logger.Warn("orchestrator: bump version for cache-box phase stop failed",
				"phase_id", phaseID, "error", err)
		}
	}

	for _, svc := range o.phaseServices(ctx, p) {
		if _, err := o.controller.PushRules(ctx, svc, nil); err != nil {
			o.logger.Warn("orchestrator: clear rules failed on finish",
				"phase_id", phaseID, "service", svc, "error", err)
		}
	}

	// Fidelity verdict BEFORE thaw (INV-6): pull each frozen instance's W6
	// snapshot while its counters are intact, compute + persist the verdict, and
	// carry the per-service replay aggregates (hits/misses/age) into harvest —
	// the same snapshots the verdict trusted, not the SDK's store counters.
	var fidelity map[string]replayFidelity
	if status == "completed" && len(p.FrozenServices) > 0 {
		fidelity = o.collectFidelityVerdict(ctx, p)
	}

	o.thawServices(ctx, p)

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

	o.logger.Info("orchestrator: phase finished", "phase_id", phaseID, "status", status)
	go o.advanceExperiment(context.Background(), p.ExperimentID)
	return true
}

// =========================================================================
// Enter-sequence pieces
// =========================================================================

// pushPhaseRules compiles the phase's rules and pushes each to its own
// target service. Compile/load errors are fatal (bad experiment data);
// fanout errors are warnings (an empty SDK fleet must not fail the phase).
func (o *Orchestrator) pushPhaseRules(ctx context.Context, p *model.ExperimentPhase) error {
	prs, err := o.experiments.ListPhaseRules(ctx, p.ID)
	if err != nil {
		return fmt.Errorf("list phase rules: %w", err)
	}
	if len(prs) == 0 {
		return nil
	}

	// Group rule IDs by their target service, preserving position order.
	byService := make(map[string][]string)
	var services []string
	for _, pr := range prs {
		r, err := o.rules.Get(ctx, pr.RuleID)
		if err != nil {
			return fmt.Errorf("load rule %q: %w", pr.RuleID, err)
		}
		// Push is a projection of the poll predicate (ForService: enabled AND
		// attached-to-running — the phase is already 'running' here), never a
		// second opinion. Pushing a disabled attached rule fired it for up to
		// one poll interval until the reconciler wiped it (ghost activation).
		if !r.Enabled {
			continue
		}
		if _, ok := byService[r.Service]; !ok {
			services = append(services, r.Service)
		}
		byService[r.Service] = append(byService[r.Service], r.ID)
	}

	for _, svc := range services {
		compiled, err := o.loadCompiledRules(ctx, byService[svc])
		if err != nil {
			return err
		}
		if _, err := o.controller.PushRules(ctx, svc, compiled); err != nil {
			o.logger.Warn("orchestrator: push rules failed",
				"phase_id", p.ID, "service", svc, "error", err)
		}
	}
	return nil
}

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
			return fmt.Errorf("register workflow %q in zeus: %w", wf.ID, err)
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
// knows). Returns (started, configured, max attack duration).
func (o *Orchestrator) startPhaseAttacks(ctx context.Context, exp *model.Experiment, p *model.ExperimentPhase) (started, configured int, maxDur time.Duration, err error) {
	if o.zeusClient == nil {
		return 0, 0, 0, nil
	}
	pws, err := o.experiments.ListPhaseWorkflows(ctx, p.ID)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("orchestrator: list phase workflows: %w", err)
	}

	for _, pw := range pws {
		if pw.TargetURL == "" {
			o.logger.Info("orchestrator: phase workflow has no target_url; not attack-driven",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID)
			continue
		}
		configured++

		attackID := id.New("atk")
		if err := o.experiments.UpdatePhaseWorkflowZeusAttack(ctx, p.ID, pw.WorkflowID, attackID); err != nil {
			o.logger.Error("orchestrator: persist attack id failed",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID, "error", err)
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

		started++
		if d := time.Duration(pw.DurationSec) * time.Second; d > maxDur {
			maxDur = d
		}
	}
	return started, configured, maxDur, nil
}

// startPhaseRuns launches one k6 workflow run per phase_workflows row,
// executing the workflow's DSL v2 DAG. The run is scoped to the phase:
// experiment_id + meta_trace_id=phase_id tag the traffic so records and
// traces slice by phase. The run id is persisted BEFORE StartRun so crash
// recovery can find the handle even if the response is lost; zeus echoes the
// id back (manteion mints it). Returns (started, configured, max duration).
//
// The workflow is already validated-and-registered in zeus by
// materializePhaseWorkflows, so a StartRun failure here is a live-fleet
// problem (zeus down, run rejected), logged per-row rather than fatal — the
// phase can still be driven by additive attacks, and a fully driver-less
// phase is handled by the caller.
func (o *Orchestrator) startPhaseRuns(ctx context.Context, exp *model.Experiment, p *model.ExperimentPhase) (started, configured int, maxDur time.Duration, err error) {
	if o.zeusClient == nil {
		return 0, 0, 0, nil
	}
	pws, err := o.experiments.ListPhaseWorkflows(ctx, p.ID)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("orchestrator: list phase workflows: %w", err)
	}

	for _, pw := range pws {
		configured++
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

		started++
		if d := time.Duration(pw.DurationSec) * time.Second; d > maxDur {
			maxDur = d
		}
	}
	return started, configured, maxDur, nil
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

// =========================================================================
// Cache-box freeze / thaw / preload
// =========================================================================

// freezeServices applies the phase's frozen_services cache-box config to
// each service (the cache-box decomposition primitive). Mu/Sigma come from
// the service's fitted synthetic-delay distribution. FreezeService persists
// intent so a re-registering SDK inherits the freeze.
func (o *Orchestrator) freezeServices(ctx context.Context, p *model.ExperimentPhase) error {
	for _, fs := range p.FrozenServices {
		result, err := o.controller.FreezeService(ctx, fs.Service, freezeDelayRequest(p, fs))
		if err != nil {
			return fmt.Errorf("freeze %q: %w", fs.Service, err)
		}
		if len(result.Failed) > 0 {
			return fmt.Errorf("freeze %q: %d of %d instances failed to freeze",
				fs.Service, len(result.Failed), len(result.Targeted))
		}
	}
	return nil
}

// freezeDelayRequest builds the SDK freeze command for one frozen service: its
// fitted synthetic-delay distribution plus the authoritative CacheBoxContext
// (§W1) scoping the freeze to (experiment_id, phase_id) with the service's key
// strategy. The context is provenance/forward-compat — replay itself is driven
// by the poll-synthesized replay rule (MANT-4).
func freezeDelayRequest(p *model.ExperimentPhase, fs model.CacheBoxConfig) atroposdk.DelayRequest {
	delay := atroposdk.DelayRequest{}
	if fs.SyntheticDelay != nil {
		if fs.SyntheticDelay.FitMu != nil {
			delay.Mu = *fs.SyntheticDelay.FitMu
		}
		if fs.SyntheticDelay.FitSigma != nil {
			delay.Sigma = *fs.SyntheticDelay.FitSigma
		}
	}
	strat := model.ResolveKeyStrategy(fs.KeyStrategy)
	delay.Context = &atroposdk.CacheBoxContext{
		ExperimentID:    p.ExperimentID,
		PhaseID:         p.ID,
		KeyStrategy:     strat,
		StrategyVersion: model.KeyStrategyVersion(strat),
		KeyHeaders:      fs.KeyHeaders,
	}
	return delay
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

// thawServices clears the phase's frozen_services cache-box freeze. Called
// at the phase's terminal cleanup, symmetric to freezeServices.
func (o *Orchestrator) thawServices(ctx context.Context, p *model.ExperimentPhase) {
	for _, fs := range p.FrozenServices {
		if _, err := o.controller.ClearService(ctx, fs.Service); err != nil {
			o.logger.Warn("orchestrator: clear service (thaw) failed",
				"phase_id", p.ID, "service", fs.Service, "error", err)
		}
	}
}

// preloadCacheEntries installs each frozen service's baseline recording onto
// every live SDK instance via the staged, verified §W4 protocol, and returns an
// error (the phase-abort trigger) unless every instance verify-commits the
// byte-exact set. The baseline is the experiment's most recently completed
// phase that persisted its cache and froze nothing.
//
// This is a hard gate (INV-4), replacing the old warn-and-continue: a missing
// baseline, a zero-entry frozen service, a checksum/count mismatch, or any
// instance failure aborts the isolation phase before freeze and before load —
// so load never runs all-miss (all-leak) against an unverified replay set.
func (o *Orchestrator) preloadCacheEntries(ctx context.Context, p *model.ExperimentPhase) error {
	baseline, err := o.baselinePhase(ctx, p.ExperimentID)
	if err != nil {
		return fmt.Errorf("resolve baseline: %w", err)
	}
	if baseline == nil {
		return fmt.Errorf("no completed baseline recording for experiment %q; cannot preload frozen services", p.ExperimentID)
	}

	// Refuse to build a replay set from a degraded recording (INV-3): its
	// coverage is unverified, so any isolation run over it is suspect. The
	// operator can override with MANTEION_ALLOW_DEGRADED_BASELINE.
	drain, err := o.experiments.GetPhaseDrain(ctx, baseline.ID)
	if err != nil {
		return fmt.Errorf("read baseline drain result: %w", err)
	}
	if drain.Degraded() && !o.allowDegradedBaseline {
		return fmt.Errorf("baseline recording %q is degraded (%d missing instances, %d entry shortfall); "+
			"refusing to preload — set MANTEION_ALLOW_DEGRADED_BASELINE=true to override",
			baseline.ID, len(drain.MissingInstances), drain.ShortfallEntries)
	}

	for _, fs := range p.FrozenServices {
		entries, err := o.cacheStore.Read(baseline.ExperimentID, baseline.ID, fs.Service)
		if err != nil {
			return fmt.Errorf("read baseline entries for %q: %w", fs.Service, err)
		}
		if len(entries) == 0 {
			return fmt.Errorf("baseline recorded zero entries for frozen service %q; nothing to replay", fs.Service)
		}

		strat := model.ResolveKeyStrategy(fs.KeyStrategy)
		if err := verifyRecordedStrategy(fs.Service, entries, strat); err != nil {
			return err
		}
		checksum := cachestore.SetChecksum(entries)
		results, err := o.controller.PreloadService(ctx, fs.Service, atrocontrol.PreloadSpec{
			ExperimentID:    p.ExperimentID,
			PhaseID:         p.ID,
			SourcePhaseID:   baseline.ID,
			KeyStrategy:     strat,
			StrategyVersion: model.KeyStrategyVersion(strat),
			KeyHeaders:      fs.KeyHeaders,
			MaxBytes:        atrocontrol.DefaultPreloadMaxBytes,
			Entries:         entries,
			Checksum:        checksum,
		})
		if err != nil {
			return fmt.Errorf("preload %q: %w", fs.Service, err)
		}
		if err := verifyPreload(fs.Service, len(entries), checksum, results); err != nil {
			return err
		}
		o.logger.Info("orchestrator: preload verified",
			"phase_id", p.ID, "service", fs.Service,
			"entries", len(entries), "instances", len(results))
	}
	return nil
}

// verifyRecordedStrategy is the preload strategy preflight (MANT-5/INV-2): the
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

// verifyPreload is the per-instance completeness gate (INV-4): every live
// instance must have verify-committed the exact expected set. Any transport
// failure, rejected commit, wrong count, or checksum mismatch — or no live
// instances at all — fails the phase.
func verifyPreload(service string, expectedCount int, expectedChecksum string, results []atrocontrol.InstancePreloadResult) error {
	if len(results) == 0 {
		return fmt.Errorf("preload %q: no live instances to install the replay set", service)
	}
	for _, r := range results {
		switch {
		case r.Err != nil:
			return fmt.Errorf("preload %q instance %s: %w", service, r.InstanceID, r.Err)
		case !r.Committed:
			return fmt.Errorf("preload %q instance %s: commit rejected (loaded=%d checksum=%s, want %d/%s)",
				service, r.InstanceID, r.Loaded, r.Checksum, expectedCount, expectedChecksum)
		case r.Loaded != expectedCount:
			return fmt.Errorf("preload %q instance %s: loaded %d entries, want %d", service, r.InstanceID, r.Loaded, expectedCount)
		case r.Checksum != expectedChecksum:
			return fmt.Errorf("preload %q instance %s: checksum %s, want %s", service, r.InstanceID, r.Checksum, expectedChecksum)
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

// phaseServices returns the de-duplicated union of the phase's frozen
// services and its rules' target services — every service the phase may
// have touched, for rule-clearing at teardown.
func (o *Orchestrator) phaseServices(ctx context.Context, p *model.ExperimentPhase) []string {
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
		o.logger.Warn("orchestrator: phase services: list rules failed",
			"phase_id", p.ID, "error", err)
		return svcs
	}
	for _, pr := range prs {
		r, err := o.rules.Get(ctx, pr.RuleID)
		if err != nil {
			o.logger.Warn("orchestrator: phase services: load rule failed",
				"rule_id", pr.RuleID, "error", err)
			continue
		}
		add(r.Service)
	}
	return svcs
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
