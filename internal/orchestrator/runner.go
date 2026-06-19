package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

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
	o.logger.Info("orchestrator: phase paused", "phase_id", phaseID)
	return nil
}

// StopPhase is the operator terminal entry point: finalize a phase as
// completed/failed/skipped (default completed). Idempotent — a no-op if the
// phase already reached a terminal state.
func (o *Orchestrator) StopPhase(ctx context.Context, phaseID, finalStatus string) error {
	from := []string{"running", "paused"}
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

	// Cache preload only on a fresh start: a resume continues with whatever
	// the cache boxes already hold.
	if fresh && len(p.FrozenServices) > 0 {
		if err := o.preloadCacheEntries(ctx, p); err != nil {
			o.logger.Warn("orchestrator: cache preload failed",
				"phase_id", p.ID, "error", err)
		}
	}

	// Freeze is idempotent (intent-tracked), so re-freezing on resume is
	// safe and re-asserts the desired state.
	o.freezeServices(ctx, p)
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

	started, configured, maxDur, err := o.startPhaseAttacks(ctx, exp, p)
	if err != nil {
		return fail(err)
	}
	if configured > 0 && started == 0 {
		return fail(fmt.Errorf("orchestrator: no attack could be started (%d configured)", configured))
	}

	if started > 0 {
		o.spawnPoller(p.ID, maxDur)
		o.logger.Info("orchestrator: phase started",
			"phase_id", p.ID, "attacks", started, "fresh", fresh)
		return nil
	}

	// No attacks → no driver takes this phase to a terminal state.
	// Auto-complete (a driver-less phase is valid — e.g. rules-only against
	// externally generated load). Only from 'running', so a concurrent
	// pause/stop wins the race.
	if o.autoComplete {
		o.logger.Info("orchestrator: phase has no attack driver; auto-completing",
			"phase_id", p.ID)
		go o.finishPhase(context.Background(), p.ID, "completed", "running")
	} else {
		o.logger.Info("orchestrator: phase started without attack driver",
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

	for _, svc := range o.phaseServices(ctx, p) {
		if _, err := o.controller.PushRules(ctx, svc, nil); err != nil {
			o.logger.Warn("orchestrator: clear rules failed on finish",
				"phase_id", phaseID, "service", svc, "error", err)
		}
	}
	o.thawServices(ctx, p)

	if o.faultEvents != nil {
		if err := o.faultEvents.EndOpenForPhase(ctx, phaseID, time.Now()); err != nil {
			o.logger.Warn("orchestrator: close fault events failed", "phase_id", phaseID, "error", err)
		}
	}

	if o.zeusClient != nil {
		for _, pw := range pws {
			if pw.ZeusAttackID == "" {
				continue
			}
			if err := o.zeusClient.StopAttack(ctx, pw.ZeusAttackID); err != nil {
				o.logger.Warn("orchestrator: zeus attack stop failed",
					"phase_id", phaseID, "attack_id", pw.ZeusAttackID, "error", err)
			}
		}
	}

	if status == "completed" {
		o.harvestPhase(ctx, p, pws) // exactly once — only the transition winner reaches here
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
		if err := o.zeusClient.RegisterWorkflow(ctx, wf.DSL); err != nil {
			return fmt.Errorf("register workflow %q in zeus: %w", wf.ID, err)
		}
	}
	return nil
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
			RunRef:        p.ID,
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
func (o *Orchestrator) freezeServices(ctx context.Context, p *model.ExperimentPhase) {
	for _, fs := range p.FrozenServices {
		delay := atroposdk.DelayRequest{}
		if fs.SyntheticDelay != nil {
			if fs.SyntheticDelay.FitMu != nil {
				delay.Mu = *fs.SyntheticDelay.FitMu
			}
			if fs.SyntheticDelay.FitSigma != nil {
				delay.Sigma = *fs.SyntheticDelay.FitSigma
			}
		}
		if _, err := o.controller.FreezeService(ctx, fs.Service, delay); err != nil {
			o.logger.Warn("orchestrator: freeze service failed",
				"phase_id", p.ID, "service", fs.Service, "error", err)
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

// preloadCacheEntries reads the baseline phase's persisted cache files and
// fans them out to all frozen services' SDK instances before the isolation
// phase begins. The baseline is the experiment's most recently completed
// phase that persisted its cache and froze nothing.
func (o *Orchestrator) preloadCacheEntries(ctx context.Context, p *model.ExperimentPhase) error {
	baseline, err := o.baselinePhase(ctx, p.ExperimentID)
	if err != nil {
		return err
	}
	if baseline == nil {
		o.logger.Info("orchestrator: no completed baseline phase; skipping cache preload",
			"phase_id", p.ID)
		return nil
	}

	for _, fs := range p.FrozenServices {
		entries, err := o.cacheStore.Read(baseline.ID, fs.Service)
		if err != nil {
			o.logger.Warn("orchestrator: read cache entries failed",
				"phase_id", p.ID, "baseline_phase_id", baseline.ID,
				"service", fs.Service, "error", err)
			continue
		}
		if len(entries) == 0 {
			o.logger.Info("orchestrator: no cache entries for service",
				"baseline_phase_id", baseline.ID, "service", fs.Service)
			continue
		}
		if _, err := o.controller.PreloadEntries(ctx, fs.Service, entries); err != nil {
			o.logger.Warn("orchestrator: preload entries failed",
				"phase_id", p.ID, "service", fs.Service, "error", err)
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
