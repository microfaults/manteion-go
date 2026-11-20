package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// servingGateService is the service the fixture's fault rule targets.
const servingGateService = "productcatalogservice"

// seedServingGate persists what an experiment serves while it runs: an enabled
// fault rule on servingGateService attached to one phase that also freezes
// frozenSvc, with the experiment and the phase both 'running' (claimed through
// the same CAS transitions the orchestrator uses). Cleanups remove the
// experiment first (its phase and phase_rules go with it, releasing the rule),
// then the rule, then its spec.
func seedServingGate(t *testing.T, tag, frozenSvc string) (*ExperimentRepo, *model.Experiment, *model.ExperimentPhase, *model.Rule) {
	t.Helper()
	ctx := context.Background()
	specID := seedLatencySpec(t, tag)
	rule := &model.Rule{ID: "rule-" + tag, Name: "rule-" + tag, Service: servingGateService, Enabled: true, Priority: 10,
		Mode: "background", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: specID},
		CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := testRuleRepo.Create(ctx, rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rule.ID) })

	repo := NewExperimentRepo(testDB)
	exp := &model.Experiment{ID: "exp-" + tag, Name: "exp-" + tag, Status: "planned", CreatedAt: time.Now()}
	if err := repo.Create(ctx, exp); err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	t.Cleanup(func() { _, _ = testDB.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
	phase := &model.ExperimentPhase{ID: "phase-" + tag, ExperimentID: exp.ID, Name: "fault", Position: 0, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: frozenSvc, Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}}}
	if err := repo.CreatePhase(ctx, phase); err != nil {
		t.Fatalf("create phase: %v", err)
	}
	if err := repo.AttachPhaseRules(ctx, phase.ID, []string{rule.ID}); err != nil {
		t.Fatalf("attach rule: %v", err)
	}
	if ok, err := repo.TransitionExperiment(ctx, exp.ID, "running", "planned"); err != nil || !ok {
		t.Fatalf("start experiment: claimed=%v err=%v", ok, err)
	}
	if ok, err := repo.TransitionPhase(ctx, phase.ID, "running", "pending"); err != nil || !ok {
		t.Fatalf("start phase: claimed=%v err=%v", ok, err)
	}
	return repo, exp, phase, rule
}

// endExperimentLeavingPhaseRunning moves the experiment to a terminal status
// and leaves its phase 'running': the state a cancel or stop that fails after
// the experiment's terminal CAS (a crash, a finishPhase error) leaves behind,
// and that nothing revisits.
func endExperimentLeavingPhaseRunning(t *testing.T, repo *ExperimentRepo, exp *model.Experiment, phase *model.ExperimentPhase, terminal string) {
	t.Helper()
	ctx := context.Background()
	if ok, err := repo.TransitionExperiment(ctx, exp.ID, terminal, "running"); err != nil || !ok {
		t.Fatalf("experiment -> %s: transitioned=%v err=%v", terminal, ok, err)
	}
	if p, err := repo.GetPhase(ctx, phase.ID); err != nil || p.Status != "running" {
		t.Fatalf("phase after the experiment ended = %+v, %v; want it still running", p, err)
	}
}

// TestForService_PhaseUnderCancelledExperimentNotServed pins that a phase's
// rules are served only while its experiment runs too: a phase left 'running'
// under a cancelled experiment serves nothing, so a cancel that fails part-way
// cannot leave a fault served for ever.
func TestForService_PhaseUnderCancelledExperimentNotServed(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("servegate-%d", time.Now().UnixNano())
	repo, exp, phase, rule := seedServingGate(t, tag, "frozen-"+tag)

	served := func() []*model.Rule {
		t.Helper()
		rules, err := testRuleRepo.ForService(ctx, servingGateService)
		if err != nil {
			t.Fatalf("ForService: %v", err)
		}
		return rules
	}

	got := served()
	if len(got) != 1 || got[0].ID != rule.ID {
		t.Fatalf("experiment and phase running: ForService = %v, want [%s]", ruleIDs(got), rule.ID)
	}
	if got[0].ServedPhaseID != phase.ID || got[0].ServedExperimentID != exp.ID {
		t.Errorf("served through (%q, %q), want (%q, %q)", got[0].ServedExperimentID, got[0].ServedPhaseID, exp.ID, phase.ID)
	}

	endExperimentLeavingPhaseRunning(t, repo, exp, phase, "cancelled")
	if got := served(); len(got) != 0 {
		t.Errorf("experiment cancelled, phase still running: ForService = %v, want none", ruleIDs(got))
	}
}

// TestListRunningPhases_ExcludesTerminalExperiments pins the same gate on the
// running-phase set, and with it on everything derived from that set: the
// synthesized cache-box rule (ActiveCacheBoxPhaseForService) and a frozen
// service's freeze_cfg and replay_set (FrozenServiceForService). A phase left
// 'running' under an experiment that ended is not running.
func TestListRunningPhases_ExcludesTerminalExperiments(t *testing.T) {
	for _, terminal := range []string{"cancelled", "completed", "failed"} {
		t.Run(terminal, func(t *testing.T) {
			ctx := context.Background()
			tag := fmt.Sprintf("runphases-%s-%d", terminal, time.Now().UnixNano())
			frozenSvc := "frozen-" + tag
			repo, exp, phase, _ := seedServingGate(t, tag, frozenSvc)

			// Other suites can leave running phases behind; count this test's.
			ours := func() int {
				t.Helper()
				running, err := repo.ListRunningPhases(ctx)
				if err != nil {
					t.Fatalf("ListRunningPhases: %v", err)
				}
				n := 0
				for _, p := range running {
					if p.ExperimentID == exp.ID {
						n++
					}
				}
				return n
			}
			frozenIn := func() (string, string) {
				t.Helper()
				p, _, err := repo.FrozenServiceForService(ctx, frozenSvc)
				if err != nil {
					t.Fatalf("FrozenServiceForService: %v", err)
				}
				rc, err := repo.ActiveCacheBoxPhaseForService(ctx, frozenSvc)
				if err != nil {
					t.Fatalf("ActiveCacheBoxPhaseForService: %v", err)
				}
				var frozen, cacheBox string
				if p != nil {
					frozen = p.ID
				}
				if rc != nil {
					cacheBox = rc.PhaseID
				}
				return frozen, cacheBox
			}

			if n := ours(); n != 1 {
				t.Fatalf("experiment and phase running: %d of its phases in ListRunningPhases, want 1", n)
			}
			if frozen, cacheBox := frozenIn(); frozen != phase.ID || cacheBox != phase.ID {
				t.Fatalf("experiment and phase running: %s frozen in %q, cache-box role in %q; want both %q",
					frozenSvc, frozen, cacheBox, phase.ID)
			}

			endExperimentLeavingPhaseRunning(t, repo, exp, phase, terminal)
			if n := ours(); n != 0 {
				t.Errorf("experiment %s, phase still running: %d of its phases in ListRunningPhases, want 0", terminal, n)
			}
			if frozen, cacheBox := frozenIn(); frozen != "" || cacheBox != "" {
				t.Errorf("experiment %s, phase still running: %s frozen in %q, cache-box role in %q; want neither",
					terminal, frozenSvc, frozen, cacheBox)
			}
		})
	}
}

// servedCounts reads ForService for servingGateService and counts how often
// each of ids is served; the rules it serves through are returned by id.
func servedCounts(t *testing.T, ids ...string) (map[string]int, map[string]*model.Rule) {
	t.Helper()
	rules, err := testRuleRepo.ForService(context.Background(), servingGateService)
	if err != nil {
		t.Fatalf("ForService: %v", err)
	}
	counts := make(map[string]int, len(ids))
	byID := make(map[string]*model.Rule, len(ids))
	for _, id := range ids {
		counts[id] = 0
	}
	for _, r := range rules {
		if _, ours := counts[r.ID]; ours {
			counts[r.ID]++
			byID[r.ID] = r
		}
	}
	return counts, byID
}

// TestForService_UnattachedRuleStillServedAfterNeighbourExperimentEnds pins
// that the experiment gate applies only to phase-attached rules: an enabled
// rule on the same service that no phase attaches is live on its own, before
// and after a neighbouring experiment is cancelled, while that experiment's
// attached rule stops being served.
func TestForService_UnattachedRuleStillServedAfterNeighbourExperimentEnds(t *testing.T) {
	tag := fmt.Sprintf("servegate-free-%d", time.Now().UnixNano())
	repo, exp, phase, attached := seedServingGate(t, tag, "frozen-"+tag)
	free := createOrderedRule(t, attached.Action.FaultSpecID, servingGateService, "free-"+tag, 5, time.Now())

	counts, _ := servedCounts(t, attached.ID, free.ID)
	if counts[attached.ID] != 1 || counts[free.ID] != 1 {
		t.Fatalf("experiment running: served %v, want each of %s and %s once", counts, attached.ID, free.ID)
	}

	endExperimentLeavingPhaseRunning(t, repo, exp, phase, "cancelled")
	counts, _ = servedCounts(t, attached.ID, free.ID)
	if counts[attached.ID] != 0 {
		t.Errorf("experiment cancelled: attached rule %s served %d time(s), want 0", attached.ID, counts[attached.ID])
	}
	if counts[free.ID] != 1 {
		t.Errorf("experiment cancelled: unattached rule %s served %d time(s), want 1", free.ID, counts[free.ID])
	}
}

// TestForService_DualAttachmentServedOnceViaHealthyPhase pins the pick among
// several running phases that attach one rule: a phase left running under a
// cancelled experiment is not a candidate, so the rule is served once, through
// the running phase of the running experiment. The stranded phase is made the
// most recently started, which is the one the pick prefers among candidates:
// only the experiment gate can pass it over.
func TestForService_DualAttachmentServedOnceViaHealthyPhase(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("servegate-dual-%d", time.Now().UnixNano())
	repo, exp, stranded, rule := seedServingGate(t, tag, "frozen-"+tag)

	healthyExp := &model.Experiment{ID: "exp-healthy-" + tag, Name: "exp-healthy-" + tag, Status: "planned", CreatedAt: time.Now()}
	if err := repo.Create(ctx, healthyExp); err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	t.Cleanup(func() { _, _ = testDB.Exec("DELETE FROM experiments WHERE id = $1", healthyExp.ID) })
	healthy := &model.ExperimentPhase{ID: "phase-healthy-" + tag, ExperimentID: healthyExp.ID, Name: "fault", Position: 0, Status: "pending"}
	if err := repo.CreatePhase(ctx, healthy); err != nil {
		t.Fatalf("create phase: %v", err)
	}
	if err := repo.AttachPhaseRules(ctx, healthy.ID, []string{rule.ID}); err != nil {
		t.Fatalf("attach rule: %v", err)
	}
	if ok, err := repo.TransitionExperiment(ctx, healthyExp.ID, "running", "planned"); err != nil || !ok {
		t.Fatalf("start experiment: claimed=%v err=%v", ok, err)
	}
	if ok, err := repo.TransitionPhase(ctx, healthy.ID, "running", "pending"); err != nil || !ok {
		t.Fatalf("start phase: claimed=%v err=%v", ok, err)
	}
	if _, err := testDB.Exec(`UPDATE experiment_phases SET started_at = now() + interval '1 hour' WHERE id = $1`, stranded.ID); err != nil {
		t.Fatalf("make the first phase the most recently started: %v", err)
	}

	counts, byID := servedCounts(t, rule.ID)
	if counts[rule.ID] != 1 || byID[rule.ID].ServedPhaseID != stranded.ID {
		t.Fatalf("both experiments running: served %d time(s) through %v, want once through %s (the most recently started)",
			counts[rule.ID], byID[rule.ID], stranded.ID)
	}

	endExperimentLeavingPhaseRunning(t, repo, exp, stranded, "cancelled")
	counts, byID = servedCounts(t, rule.ID)
	if counts[rule.ID] != 1 {
		t.Fatalf("one experiment cancelled: rule %s served %d time(s), want exactly once", rule.ID, counts[rule.ID])
	}
	if got := byID[rule.ID]; got.ServedPhaseID != healthy.ID || got.ServedExperimentID != healthyExp.ID {
		t.Errorf("served through (%q, %q), want the healthy phase (%q, %q)",
			got.ServedExperimentID, got.ServedPhaseID, healthyExp.ID, healthy.ID)
	}
}

// TestForService_PhaseUnderPlannedExperimentNotServed pins that "running"
// means the experiment too, not only "ended": a phase forced to running under
// an experiment that is planned (what a phase started through the phase-level
// API before its experiment looked like) serves none of its rules and is not
// in the running-phase set (so no freeze or replay set either).
func TestForService_PhaseUnderPlannedExperimentNotServed(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("servegate-planned-%d", time.Now().UnixNano())
	repo, exp, phase, rule := seedServingGate(t, tag, "frozen-"+tag)

	if counts, _ := servedCounts(t, rule.ID); counts[rule.ID] != 1 {
		t.Fatalf("experiment and phase running: rule %s served %d time(s), want 1", rule.ID, counts[rule.ID])
	}
	// Force the experiment back to planned, its phase left running.
	if ok, err := repo.TransitionExperiment(ctx, exp.ID, "planned", "running"); err != nil || !ok {
		t.Fatalf("experiment -> planned: transitioned=%v err=%v", ok, err)
	}
	if p, err := repo.GetPhase(ctx, phase.ID); err != nil || p.Status != "running" {
		t.Fatalf("phase = %+v, %v; want running", p, err)
	}

	if counts, _ := servedCounts(t, rule.ID); counts[rule.ID] != 0 {
		t.Errorf("experiment planned, phase running: rule %s served %d time(s), want 0", rule.ID, counts[rule.ID])
	}
	running, err := repo.ListRunningPhases(ctx)
	if err != nil {
		t.Fatalf("ListRunningPhases: %v", err)
	}
	for _, p := range running {
		if p.ID == phase.ID {
			t.Errorf("experiment planned: phase %s is in ListRunningPhases, want it left out", phase.ID)
		}
	}
}
