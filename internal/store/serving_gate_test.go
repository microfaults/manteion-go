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
