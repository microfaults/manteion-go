package demo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// These tests pin the fake itself: later tasks rely on it to catch the bug this
// package exists to prevent. fakes_db_test.go checks it against the real store.

// TestFakeLab_InvariantRecorder: the recorder flags a demo-run-* rule that is
// enabled while attached to no phase, whichever mutation causes it, and
// ignores operator rules.
func TestFakeLab_InvariantRecorder(t *testing.T) {
	ctx := context.Background()

	t.Run("deleting the experiment of an enabled rule", func(t *testing.T) {
		f := newFakeLab(t, testNow)
		f.expectViolations()
		built, err := BuildRun(ctx, f.deps(), "demo-inv", DefaultScenario(), testRunCap)
		if err != nil {
			t.Fatal(err)
		}
		if err := SetRuleEnabled(ctx, fakeRules{f}, built.RuleIDs[0], true); err != nil {
			t.Fatal(err)
		}
		if v := f.violations(); len(v) != 0 {
			t.Fatalf("violations before the delete: %v", v)
		}
		if err := (fakeExperiments{f}).Delete(ctx, built.ExperimentID); err != nil {
			t.Fatal(err)
		}
		v := f.violations()
		if len(v) != 1 || !strings.Contains(v[0], "experiments.Delete") || !strings.Contains(v[0], built.RuleIDs[0]) {
			t.Fatalf("violations = %v; want one, after experiments.Delete, naming the rule", v)
		}
	})

	t.Run("creating or switching on an unattached demo rule", func(t *testing.T) {
		f := newFakeLab(t, testNow)
		f.expectViolations()
		f.specs = append(f.specs, &model.FaultSpec{ID: "spec-x"})
		r := &model.Rule{ID: "rule-a", Name: RunNamePrefix + "a", Service: testTarget, Enabled: true,
			Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-x"}}
		if err := (fakeRules{f}).Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		r2 := *r
		r2.ID, r2.Name, r2.Enabled = "rule-b", RunNamePrefix+"b", false
		if err := (fakeRules{f}).Create(ctx, &r2); err != nil {
			t.Fatal(err)
		}
		if err := (fakeRules{f}).SetEnabled(ctx, r2.ID, true); err != nil {
			t.Fatal(err)
		}
		v := strings.Join(f.violations(), "\n")
		if !strings.Contains(v, "after rules.Create: rule rule-a") || !strings.Contains(v, "after rules.SetEnabled: rule rule-b") {
			t.Fatalf("violations = %s; want both rules flagged", v)
		}
	})

	t.Run("operator rules are not the demo's", func(t *testing.T) {
		f := newFakeLab(t, testNow)
		f.specs = append(f.specs, &model.FaultSpec{ID: "spec-x"})
		r := &model.Rule{ID: "rule-op", Name: "smoke-x", Service: testTarget, Enabled: true,
			Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-x"}}
		if err := (fakeRules{f}).Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		if v := f.violations(); len(v) != 0 {
			t.Fatalf("violations = %v; want none for an operator rule", v)
		}
	})
}

// TestFakeLab_Served: the fake serves a rule as RuleRepo.ForService does: a
// run's memory rule only while the memory phase runs under a running
// experiment, its CPU rule only while the cpu phase does.
func TestFakeLab_Served(t *testing.T) {
	ctx := context.Background()
	f := newFakeLab(t, testNow)
	built, err := BuildRun(ctx, f.deps(), "demo-srv", DefaultScenario(), testRunCap)
	if err != nil {
		t.Fatal(err)
	}
	mem, cpu, exp := built.RuleIDs[0], built.RuleIDs[1], built.ExperimentID
	memPhase, cpuPhase := built.PhaseIDs[1], built.PhaseIDs[2]
	step := func(what string, wantMem, wantCPU bool) {
		t.Helper()
		if got := f.served(mem); got != wantMem {
			t.Errorf("%s: memory rule served = %v, want %v", what, got, wantMem)
		}
		if got := f.served(cpu); got != wantCPU {
			t.Errorf("%s: cpu rule served = %v, want %v", what, got, wantCPU)
		}
	}
	step("built: disabled, planned", false, false)
	for _, id := range built.RuleIDs {
		if err := SetRuleEnabled(ctx, fakeRules{f}, id, true); err != nil {
			t.Fatal(err)
		}
	}
	step("enabled, experiment planned, phases pending", false, false)
	f.setExperimentStatus(exp, "running")
	step("enabled, experiment running, phases pending", false, false)
	f.setPhaseStatus(memPhase, "running")
	step("experiment running, memory phase running", true, false)
	f.setPhaseStatus(memPhase, "draining")
	step("memory phase draining", false, false)
	f.setPhaseStatus(memPhase, "completed")
	f.setPhaseStatus(cpuPhase, "running")
	step("cpu phase running", false, true)
	f.setExperimentStatus(exp, "cancelled")
	step("cpu phase running under a cancelled experiment", false, false)
	f.setExperimentStatus(exp, "running")
	if err := SetRuleEnabled(ctx, fakeRules{f}, cpu, false); err != nil {
		t.Fatal(err)
	}
	step("cpu rule disabled while its phase runs", false, false)
}

// TestFakeLab_ListPages: List pages, orders and counts like ExperimentRepo.List.
func TestFakeLab_ListPages(t *testing.T) {
	ctx := context.Background()
	f := newFakeLab(t, testNow)
	f.clock.SetStep(time.Millisecond)
	var ids []string // newest first
	for i := 0; i < 5; i++ {
		built, err := BuildRun(ctx, f.deps(), fmt.Sprintf("demo-%d", i), DefaultScenario(), testRunCap)
		if err != nil {
			t.Fatal(err)
		}
		ids = append([]string{built.ExperimentID}, ids...)
	}
	// Two experiments created in the same instant order by id.
	tie := testNow.Add(time.Hour)
	for _, id := range []string{"exp-tie-b", "exp-tie-a"} {
		if err := (fakeExperiments{f}).Create(ctx, &model.Experiment{ID: id, Name: id, Status: "completed",
			CreatedBy: CreatedBy, CreatedAt: tie}); err != nil {
			t.Fatal(err)
		}
	}
	ids = append([]string{"exp-tie-a", "exp-tie-b"}, ids...)
	if err := (fakeExperiments{f}).Create(ctx, &model.Experiment{ID: "exp-op", Name: "op", Status: "planned",
		CreatedBy: "lab-smoke", CreatedAt: tie}); err != nil {
		t.Fatal(err)
	}

	page := func(flt store.ExperimentFilter, p store.Page) ([]string, int) {
		t.Helper()
		exps, total, err := (fakeExperiments{f}).List(ctx, flt, p)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range exps {
			out = append(out, e.ID)
		}
		return out, total
	}
	demo := store.ExperimentFilter{CreatedBy: CreatedBy}
	cases := []struct {
		name  string
		flt   store.ExperimentFilter
		p     store.Page
		want  []string
		total int
	}{
		{"first page", demo, store.Page{Limit: 3}, ids[:3], 7},
		{"second page", demo, store.Page{Limit: 3, Offset: 3}, ids[3:6], 7},
		{"last page", demo, store.Page{Limit: 3, Offset: 6}, ids[6:], 7},
		{"past the end", demo, store.Page{Limit: 3, Offset: 7}, nil, 0},
		{"zero limit is 20", demo, store.Page{}, ids, 7},
		{"negative offset is 0", demo, store.Page{Limit: 2, Offset: -4}, ids[:2], 7},
		{"status and creator", store.ExperimentFilter{CreatedBy: CreatedBy, Status: "planned"}, store.Page{Limit: 10}, ids[2:], 5},
		{"no filter", store.ExperimentFilter{}, store.Page{Limit: 1}, []string{"exp-op"}, 8},
	}
	for _, tc := range cases {
		got, total := page(tc.flt, tc.p)
		if !slices.Equal(got, tc.want) || total != tc.total {
			t.Errorf("%s: %v, total %d; want %v, total %d", tc.name, got, total, tc.want, tc.total)
		}
	}
	// Above 200 is 200.
	for i := 0; i < 205; i++ {
		id := fmt.Sprintf("exp-bulk-%03d", i)
		if err := (fakeExperiments{f}).Create(ctx, &model.Experiment{ID: id, Name: id, Status: "completed",
			CreatedBy: "bulk", CreatedAt: testNow}); err != nil {
			t.Fatal(err)
		}
	}
	if got, total := page(store.ExperimentFilter{CreatedBy: "bulk"}, store.Page{Limit: 1000}); len(got) != 200 || total != 205 {
		t.Errorf("limit 1000: %d rows, total %d; want 200, 205", len(got), total)
	}
}

// TestFakeLab_HooksMayCallBack: a hook runs without the fake's lock held, so it
// may call into the fake; a hang waits without it too.
func TestFakeLab_HooksMayCallBack(t *testing.T) {
	ctx := context.Background()
	f := newFakeLab(t, testNow)
	hooked := false
	f.onCall("experiments.Create", func() {
		_, _ = (fakeRules{f}).List(ctx) // would deadlock if the hook ran under f.mu
		hooked = true
	})
	built := make(chan error, 1)
	go func() {
		_, err := BuildRun(ctx, f.deps(), "demo-hook", DefaultScenario(), testRunCap)
		built <- err
	}()
	select {
	case err := <-built:
		if err != nil || !hooked {
			t.Fatalf("BuildRun: %v, hook ran %v", err, hooked)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: a hook ran with the fake's lock held")
	}

	f.hang("zeus.Healthy")
	hctx, cancel := context.WithCancel(ctx)
	answered := make(chan bool, 1)
	go func() { answered <- (fakeZeus{f}).Healthy(hctx) }()
	for len(f.callList("zeus.Healthy")) == 0 {
		time.Sleep(time.Millisecond)
	}
	listed := make(chan struct{})
	go func() { _, _ = (fakeRules{f}).List(ctx); close(listed) }()
	select {
	case <-listed:
	case <-time.After(5 * time.Second):
		t.Fatal("a hang holds the fake's lock")
	}
	cancel()
	if <-answered {
		t.Errorf("a hung zeus answered healthy")
	}
}

// TestFakeLab_RuleNeedsKnownSpec: rules.fault_spec_id is a foreign key.
func TestFakeLab_RuleNeedsKnownSpec(t *testing.T) {
	f := newFakeLab(t, testNow)
	r := &model.Rule{ID: "rule-x", Name: RunNamePrefix + "x", Service: testTarget, Mode: "inline",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-unknown"}}
	err := (fakeRules{f}).Create(context.Background(), r)
	if err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v; want a foreign-key refusal", err)
	}
	if f.rule("rule-x") != nil {
		t.Errorf("rule stored despite the unknown spec")
	}
}
