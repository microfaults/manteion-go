package demo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// ---- helpers ----

// seeded is one run seeded into a fake lab.
type seeded struct {
	runID string
	Built
}

// seed builds a run in f, one second (of f's clock) after the previous one,
// switches its rule on when on, and moves its experiment to status: planned
// (as built), running (its fault phase running, so an enabled rule is served),
// or completed, failed or cancelled (phases not yet ended are skipped).
func seed(t *testing.T, f *fakeLab, status string, on bool) seeded {
	t.Helper()
	ctx := context.Background()
	s := seeded{runID: id.New(RunIDPrefix)}
	built, err := BuildRun(ctx, f.deps(), s.runID, testRunCap)
	if err != nil {
		t.Fatalf("seed: build: %v", err)
	}
	s.Built = built
	f.clock.Advance(time.Second)
	if on {
		if err := SetRuleEnabled(ctx, fakeRules{f}, built.RuleID, true); err != nil {
			t.Fatalf("seed: switch on: %v", err)
		}
	}
	switch status {
	case "planned":
	case "running":
		f.setExperimentStatus(built.ExperimentID, "running")
		f.setPhaseStatus(built.PhaseIDs[0], "completed")
		f.setPhaseStatus(built.PhaseIDs[FaultPhaseIndex], "running")
	case "completed", "failed", "cancelled":
		f.endExperiment(built.ExperimentID, status, testNow, testNow)
	default:
		t.Fatalf("seed: unknown status %q", status)
	}
	return s
}

// uninject removes what was injected for op.
func (f *fakeLab) uninject(op string) {
	f.mu.Lock()
	delete(f.injections, op)
	delete(f.hooks, op)
	f.mu.Unlock()
}

// hasExperiment and hasRule report whether the row still exists.
func (f *fakeLab) hasExperiment(id string) bool { return f.experimentStatus(id) != "" }
func (f *fakeLab) hasRule(id string) bool       { return f.rule(id) != nil }

// operatorSpec makes sure the operator's fault spec exists and returns its id.
func operatorSpec(f *fakeLab) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !slices.ContainsFunc(f.specs, func(s *model.FaultSpec) bool { return s.ID == "spec-op" }) {
		f.specs = append(f.specs, &model.FaultSpec{ID: "spec-op", Name: "op"})
	}
	return "spec-op"
}

func addRule(t *testing.T, f *fakeLab, ruleID, name string, enabled bool) {
	t.Helper()
	r := &model.Rule{ID: ruleID, Name: name, Service: TargetService, Enabled: enabled, Priority: 10, Mode: "inline",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: operatorSpec(f)}, CreatedAt: f.clock.Now()}
	if err := (fakeRules{f}).Create(context.Background(), r); err != nil {
		t.Fatalf("add rule %s: %v", name, err)
	}
}

func addExperiment(t *testing.T, f *fakeLab, expID, name, createdBy, status string, at time.Time) {
	t.Helper()
	e := &model.Experiment{ID: expID, Name: name, Status: status, CreatedBy: createdBy, CreatedAt: at}
	if err := (fakeExperiments{f}).Create(context.Background(), e); err != nil {
		t.Fatalf("add experiment %s: %v", name, err)
	}
}

func addPhase(t *testing.T, f *fakeLab, phaseID, expID string, pos int, status string, ruleIDs ...string) {
	t.Helper()
	ctx := context.Background()
	p := &model.ExperimentPhase{ID: phaseID, ExperimentID: expID, Name: phaseID, Position: pos, Status: status,
		FrozenServices: []model.CacheBoxConfig{}}
	if err := (fakeExperiments{f}).CreatePhase(ctx, p); err != nil {
		t.Fatalf("add phase %s: %v", phaseID, err)
	}
	if len(ruleIDs) > 0 {
		if err := (fakeExperiments{f}).AttachPhaseRules(ctx, phaseID, ruleIDs); err != nil {
			t.Fatalf("attach to %s: %v", phaseID, err)
		}
	}
}

// callsFor lists the recorded calls that name id.
func callsFor(f *fakeLab, id string) []string { return callsForSince(f, id, 0) }

// callsForSince lists the recorded calls from the mark-th on that name id.
func callsForSince(f *fakeLab, id string, mark int) []string {
	var out []string
	for _, c := range f.callList("")[mark:] {
		if strings.HasSuffix(c, " "+id) || strings.Contains(c, " "+id+" ") {
			out = append(out, c)
		}
	}
	return out
}

// requireRemovedInOrder checks that a run's experiment and rule are gone and
// that the rule was switched off before the experiment was deleted, and the
// experiment deleted before the rule.
func requireRemovedInOrder(t *testing.T, f *fakeLab, s seeded) {
	t.Helper()
	if f.hasExperiment(s.ExperimentID) || f.hasRule(s.RuleID) {
		t.Errorf("run %s: experiment kept %v, rule kept %v; want both gone",
			s.runID, f.hasExperiment(s.ExperimentID), f.hasRule(s.RuleID))
		return
	}
	calls := f.callList("")
	off := slices.Index(calls, "rules.SetEnabled "+s.RuleID+" false")
	delExp := slices.Index(calls, "experiments.Delete "+s.ExperimentID)
	delRule := slices.Index(calls, "rules.Delete "+s.RuleID)
	if off < 0 || delExp < 0 || delRule < 0 || !(off < delExp && delExp < delRule) {
		t.Errorf("run %s: switch-off at %d, experiment delete at %d, rule delete at %d; want off < experiment < rule",
			s.runID, off, delExp, delRule)
	}
}

// requireKept checks that a run's experiment and rule are still there.
func requireKept(t *testing.T, f *fakeLab, s seeded) {
	t.Helper()
	if !f.hasExperiment(s.ExperimentID) || !f.hasRule(s.RuleID) {
		t.Errorf("run %s: experiment kept %v, rule kept %v; want both kept",
			s.runID, f.hasExperiment(s.ExperimentID), f.hasRule(s.RuleID))
	}
	for _, c := range append(callsFor(f, s.ExperimentID), callsFor(f, s.RuleID)...) {
		if strings.HasPrefix(c, "experiments.Delete") || strings.HasPrefix(c, "rules.Delete") ||
			strings.HasPrefix(c, "orch.CancelExperiment") {
			t.Errorf("run %s: unexpected %q", s.runID, c)
		}
	}
}

// quickTidy are short deadlines for tests of hung calls.
var quickTidy = tidyTimeouts{call: 40 * time.Millisecond, cancel: 40 * time.Millisecond}

// ---- the start-up sweep ----

// TestSweep_DisablesBeforeCancelling: a crash left rules on: one on a planned
// experiment (between the switch-on and the start), one served by a running
// fault phase, one under a running experiment's plain phase. Every switch-off
// precedes the first cancel, no demo rule is on at any cancel, and afterwards
// every rule is off and every experiment cancelled.
func TestSweep_DisablesBeforeCancelling(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	planned := seed(t, f, "planned", true)
	served := seed(t, f, "running", true)
	plain := seed(t, f, "planned", true)
	f.setExperimentStatus(plain.ExperimentID, "running")
	f.setPhaseStatus(plain.PhaseIDs[0], "running")
	if !f.served(served.RuleID) {
		t.Fatal("precondition: the running fault phase's rule is not served")
	}
	onAtCancel := offBeforeCancel(f)

	if err := Sweep(context.Background(), f.deps(), 20); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	calls := f.callList("")
	firstCancel := indexOfPrefix(calls, "orch.CancelExperiment ")
	if firstCancel < 0 {
		t.Fatal("nothing was cancelled")
	}
	for _, s := range []seeded{planned, served, plain} {
		off := slices.Index(calls, "rules.SetEnabled "+s.RuleID+" false")
		if off < 0 || off > firstCancel {
			t.Errorf("run %s: switch-off at %d, first cancel at %d; want the switch-off first", s.runID, off, firstCancel)
		}
		if st := f.experimentStatus(s.ExperimentID); st != "cancelled" {
			t.Errorf("run %s: experiment %s; want cancelled", s.runID, st)
		}
	}
	for i, c := range calls {
		if strings.HasPrefix(c, "rules.SetEnabled ") && i > firstCancel {
			t.Errorf("a switch-off after the first cancel: %q", c)
		}
	}
	if n := onAtCancel.Load(); n != 0 {
		t.Errorf("%d demo rules were on at a cancel", n)
	}
	if n := len(f.callList("orch.CancelExperiment ")); n != 3 {
		t.Errorf("%d cancels; want 3", n)
	}
	f.requireDemoRulesOff(t)
}

// TestSweep_LeavesOperatorRules: the sweep switches off only demo-run-* rules
// and acts only on manteion-demo experiments named demo-run-*. An operator's
// enabled rule, names that merely contain "demo", and operator experiments are
// never touched. A rule named demo-run-x attached to an experiment someone
// else created is switched off and kept (rules carry no creator, and "x" is
// not a run id this package mints); its experiment is not touched.
func TestSweep_LeavesOperatorRules(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	addRule(t, f, "rule-smoke", "smoke-x", true)
	addRule(t, f, "rule-contains", "my-demo-run-latency", true)
	addRule(t, f, "rule-demoish", "demo-catalog", true)
	addRule(t, f, "rule-loose", "demo-run-y", false) // not a minted run id, attached to nothing
	addExperiment(t, f, "exp-op", "smoke", "lab-smoke", "running", testNow.Add(-time.Hour))
	addPhase(t, f, "phase-op", "exp-op", 0, "running", "rule-smoke")
	foreign := RunNamePrefix + id.New(RunIDPrefix) // a demo-looking experiment someone else created
	addExperiment(t, f, "exp-foreign", foreign, "lab-smoke", "planned", testNow.Add(-2*time.Hour))
	addRule(t, f, "rule-x", "demo-run-x", false)
	addExperiment(t, f, "exp-x", "demo-run-x", "alice", "running", testNow.Add(-3*time.Hour))
	addPhase(t, f, "phase-x", "exp-x", 0, "running", "rule-x")
	if err := (fakeRules{f}).SetEnabled(context.Background(), "rule-x", true); err != nil {
		t.Fatal(err)
	}
	addExperiment(t, f, "exp-showcase", "showcase", CreatedBy, "completed", testNow.Add(-4*time.Hour))
	if !f.served("rule-smoke") || !f.served("rule-x") {
		t.Fatal("precondition: the operator rules are not served")
	}
	ours := seed(t, f, "completed", false)
	mark := len(f.callList(""))

	if err := Sweep(context.Background(), f.deps(), 1); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	for _, id := range []string{"rule-smoke", "rule-contains", "rule-demoish", "rule-loose", "rule-x", "exp-op",
		"exp-foreign", "exp-x", "exp-showcase", "phase-op", "phase-x"} {
		for _, c := range callsForSince(f, id, mark) {
			if strings.HasPrefix(c, "rules.") || strings.HasPrefix(c, "orch.") ||
				strings.HasPrefix(c, "experiments.Delete") {
				if !(id == "rule-x" && c == "rules.SetEnabled rule-x false") {
					t.Errorf("%s was touched: %q", id, c)
				}
			}
		}
	}
	for _, id := range []string{"rule-smoke", "rule-contains", "rule-demoish"} {
		if r := f.rule(id); r == nil || !r.Enabled {
			t.Errorf("operator rule %s: %+v; want kept, enabled", id, r)
		}
	}
	if r := f.rule("rule-x"); r == nil || r.Enabled {
		t.Errorf("rule demo-run-x: %+v; want kept, switched off", r)
	}
	if !f.hasRule("rule-loose") {
		t.Error("rule demo-run-y was deleted")
	}
	for id, want := range map[string]string{"exp-op": "running", "exp-foreign": "planned", "exp-x": "running",
		"exp-showcase": "completed"} {
		if st := f.experimentStatus(id); st != want {
			t.Errorf("experiment %s: %q; want %q (untouched)", id, st, want)
		}
	}
	requireKept(t, f, ours)
}

// TestSweep_SwitchesOffWhenExperimentListFails: with the experiments unreadable
// the sweep still switches every enabled demo rule off, deletes nothing, and
// says it is incomplete without carrying any internal detail.
func TestSweep_SwitchesOffWhenExperimentListFails(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	runs := []seeded{seed(t, f, "planned", true), seed(t, f, "running", true), seed(t, f, "completed", true)}
	f.fail("experiments.List", errLeaky)

	err := Sweep(context.Background(), f.deps(), 1)
	if !errors.Is(err, ErrSweepIncomplete) || !errors.Is(err, errLeaky) {
		t.Fatalf("Sweep: %v; want ErrSweepIncomplete wrapping the cause", err)
	}
	assertNoLeak(t, err.Error())
	f.requireDemoRulesOff(t)
	for _, s := range runs {
		if !f.hasExperiment(s.ExperimentID) || !f.hasRule(s.RuleID) {
			t.Errorf("run %s: something was deleted with the experiments unreadable", s.runID)
		}
	}
	if n := len(f.callList("orch.")) + len(f.callList("experiments.Delete")) + len(f.callList("rules.Delete")); n != 0 {
		t.Errorf("%d cancels or deletes; want none", n)
	}
}

// failRuleOff is the fake's Rules, except that switching rule id off always
// fails.
type failRuleOff struct {
	fakeRules
	id string
}

func (r failRuleOff) SetEnabled(ctx context.Context, id string, enabled bool) error {
	if id == r.id && !enabled {
		r.f.mu.Lock()
		r.f.calls = append(r.f.calls, "rules.SetEnabled "+id+" false (refused)")
		r.f.mu.Unlock()
		return errLeaky
	}
	return r.fakeRules.SetEnabled(ctx, id, enabled)
}

// TestSweep_OneSwitchOffFails: one rule's switch-off fails; the others are
// still switched off, the experiments are still cancelled, the sweep tries
// the failed one again later, and it says it is incomplete.
func TestSweep_OneSwitchOffFails(t *testing.T) {
	t.Parallel()

	t.Run("persistent", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		a, b, c := seed(t, f, "running", true), seed(t, f, "running", true), seed(t, f, "planned", true)
		d := f.deps()
		d.Rules = failRuleOff{fakeRules{f}, b.RuleID}

		err := Sweep(context.Background(), d, 20)
		if !errors.Is(err, ErrSweepIncomplete) || !errors.Is(err, ErrRuleState) {
			t.Fatalf("Sweep: %v; want ErrSweepIncomplete with a rule-state failure", err)
		}
		assertNoLeak(t, err.Error())
		for _, s := range []seeded{a, c} {
			if r := f.rule(s.RuleID); r == nil || r.Enabled {
				t.Errorf("run %s: rule %+v; want switched off", s.runID, r)
			}
		}
		if n := len(f.callList("rules.SetEnabled " + b.RuleID + " false (refused)")); n < 2 {
			t.Errorf("the failed switch-off was tried %d times; want a second try", n)
		}
		for _, s := range []seeded{a, b, c} {
			if st := f.experimentStatus(s.ExperimentID); st != "cancelled" {
				t.Errorf("run %s: experiment %s; want cancelled", s.runID, st)
			}
		}
		if f.served(b.RuleID) {
			t.Error("the rule that could not be switched off is still served after its experiment was cancelled")
		}
		// The rule stays on, attached to a cancelled experiment: switch it off by
		// hand so the lab ends clean.
		if err := (fakeRules{f}).SetEnabled(context.Background(), b.RuleID, false); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("transient", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		runs := []seeded{seed(t, f, "running", true), seed(t, f, "running", true), seed(t, f, "planned", true)}
		f.fail("rules.SetEnabled#1", errLeaky)

		err := Sweep(context.Background(), f.deps(), 20)
		if !errors.Is(err, ErrSweepIncomplete) {
			t.Fatalf("Sweep: %v; want ErrSweepIncomplete", err)
		}
		if !strings.Contains(err.Error(), "disable_rule") {
			t.Errorf("error %q does not name the failed step", err)
		}
		f.requireDemoRulesOff(t)
		for _, s := range runs {
			if st := f.experimentStatus(s.ExperimentID); st != "cancelled" {
				t.Errorf("run %s: experiment %s; want cancelled", s.runID, st)
			}
		}
	})
}

// TestSweep_CollectsWorkerLeftovers: what the worker or a failed build can
// leave behind is cleaned up: a planned experiment and a running one (each
// with its rule off) are cancelled; a run's disabled rule whose experiment was
// never written is deleted; an experiment the build left without all its
// phases is cancelled and kept with its rule until retention takes both. An
// already terminal experiment's cancel refusal is not a failure.
func TestSweep_CollectsWorkerLeftovers(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	plannedLeft := seed(t, f, "planned", false)
	runningLeft := seed(t, f, "running", false)
	finished := seed(t, f, "completed", false)

	loneRun := id.New(RunIDPrefix)
	addRule(t, f, "rule-lone", RunNamePrefix+loneRun, false)
	halfRun := id.New(RunIDPrefix)
	addRule(t, f, "rule-half", RunNamePrefix+halfRun, false)
	addExperiment(t, f, "exp-half", RunNamePrefix+halfRun, CreatedBy, "planned", testNow.Add(-time.Hour))
	addPhase(t, f, "phase-half", "exp-half", 0, "pending")

	if err := Sweep(context.Background(), f.deps(), 20); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	for _, s := range []seeded{plannedLeft, runningLeft} {
		if st := f.experimentStatus(s.ExperimentID); st != "cancelled" {
			t.Errorf("run %s: experiment %s; want cancelled", s.runID, st)
		}
		if r := f.rule(s.RuleID); r == nil || r.Enabled {
			t.Errorf("run %s: rule %+v; want kept, off", s.runID, r)
		}
	}
	if f.hasRule("rule-lone") {
		t.Error("the rule whose experiment was never written is still there")
	}
	if st := f.experimentStatus("exp-half"); st != "cancelled" || !f.hasRule("rule-half") {
		t.Errorf("half-built run: experiment %q, rule kept %v; want cancelled and kept", st, f.hasRule("rule-half"))
	}
	if st := f.experimentStatus(finished.ExperimentID); st != "completed" {
		t.Errorf("finished run: %s; want completed, untouched", st)
	}
	f.requireDemoRulesOff(t)

	// Retention then takes the cancelled leftovers, rules and all.
	if err := Retain(context.Background(), f.deps(), 1); err != nil {
		t.Fatalf("Retain: %v", err)
	}
	if f.hasExperiment("exp-half") || f.hasRule("rule-half") {
		t.Error("retention kept the half-built run")
	}
	requireRemovedInOrder(t, f, plannedLeft)
}

// TestSweep_AlreadyEndedIsNotAFailure: an experiment that ends between the
// sweep's listing and its cancel is refused as already terminal; that is
// success.
func TestSweep_AlreadyEndedIsNotAFailure(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	s := seed(t, f, "running", false)
	f.onCall("orch.CancelExperiment", func() {
		f.endExperiment(s.ExperimentID, "completed", testNow, testNow)
	})
	if err := Sweep(context.Background(), f.deps(), 20); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if st := f.experimentStatus(s.ExperimentID); st != "completed" {
		t.Errorf("experiment %s; want completed", st)
	}
}

// TestSweep_HungCallsAreBounded: each call has a deadline, and a cancel stuck
// on the orchestrator's lock (which ignores contexts) is not waited for past
// its own: the sweep returns, incomplete, with every rule already off.
func TestSweep_HungCallsAreBounded(t *testing.T) {
	t.Parallel()

	t.Run("store", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		seed(t, f, "running", true)
		f.hang("experiments.List")
		began := time.Now()
		err := newTidier(f.deps(), ErrSweepIncomplete, quickTidy).sweep(context.Background(), 20)
		if took := time.Since(began); took > time.Second {
			t.Errorf("sweep took %s with a hung listing", took)
		}
		if !errors.Is(err, ErrSweepIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("sweep: %v; want incomplete, deadline exceeded", err)
		}
		f.requireDemoRulesOff(t)
	})

	t.Run("orchestrator lock", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		s := seed(t, f, "running", true)
		release := make(chan struct{})
		var once sync.Once
		free := func() { once.Do(func() { close(release) }) }
		defer time.AfterFunc(2*time.Second, free).Stop() // a sweep that waits for the stuck call ends late, not never
		f.onCall("orch.CancelExperiment", func() { <-release })
		began := time.Now()
		err := newTidier(f.deps(), ErrSweepIncomplete, quickTidy).sweep(context.Background(), 20)
		took := time.Since(began)
		free()
		if took > time.Second {
			t.Errorf("sweep took %s with a stuck cancel", took)
		}
		if !errors.Is(err, ErrSweepIncomplete) || !errors.Is(err, context.DeadlineExceeded) ||
			!strings.Contains(err.Error(), "cancel_experiment") {
			t.Fatalf("sweep: %v; want incomplete at cancel_experiment, deadline exceeded", err)
		}
		if r := f.rule(s.RuleID); r == nil || r.Enabled {
			t.Errorf("rule %+v; want off before the cancel", r)
		}
	})

	t.Run("caller gave up", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		seed(t, f, "running", true)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := Sweep(ctx, f.deps(), 20)
		if !errors.Is(err, ErrSweepIncomplete) || !errors.Is(err, context.Canceled) {
			t.Fatalf("Sweep: %v; want incomplete, canceled", err)
		}
		f.requireDemoRulesOff(t)
	})
}

// ---- retention ----

// TestRetain_OrderAndKeep: 25 finished runs, keep 20: the 5 oldest go, each
// as rule off, experiment deleted, rule deleted, rules that were on included.
// A running experiment and a planned one, older than all of them, are never
// touched; Retain never cancels anything.
func TestRetain_OrderAndKeep(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	running := seed(t, f, "running", true)
	planned := seed(t, f, "planned", false)
	statuses := []string{"completed", "failed", "cancelled"}
	var finished []seeded // oldest first
	for i := 0; i < 25; i++ {
		finished = append(finished, seed(t, f, statuses[i%3], i < 3))
	}

	if err := Retain(context.Background(), f.deps(), 20); err != nil {
		t.Fatalf("Retain: %v", err)
	}
	for _, s := range finished[:5] {
		requireRemovedInOrder(t, f, s)
	}
	for _, s := range finished[5:] {
		requireKept(t, f, s)
	}
	requireKept(t, f, running)
	requireKept(t, f, planned)
	if !f.served(running.RuleID) {
		t.Error("the running experiment's rule was switched off")
	}
	if c := f.callList("orch."); len(c) != 0 {
		t.Errorf("Retain called the orchestrator: %v", c)
	}
	// A second pass has nothing to do.
	before := len(f.callList("experiments.Delete")) + len(f.callList("rules.Delete"))
	if err := Retain(context.Background(), f.deps(), 20); err != nil {
		t.Fatalf("second Retain: %v", err)
	}
	if after := len(f.callList("experiments.Delete")) + len(f.callList("rules.Delete")); after != before {
		t.Errorf("a second pass deleted %d more rows", after-before)
	}
	// Turn the running run's rule off so the lab ends clean.
	if err := SetRuleEnabled(context.Background(), fakeRules{f}, running.RuleID, false); err != nil {
		t.Fatal(err)
	}
}

// TestRetain_PagesPastTheStoreLimit: with more finished runs than one page of
// the store's list (200), retention pages through all of them and keeps
// exactly the newest keep. When a later page cannot be read, it deletes only
// what the pages read show to be beyond keep, and the next pass finishes.
func TestRetain_PagesPastTheStoreLimit(t *testing.T) {
	t.Parallel()
	const n, keep = 230, 20

	setup := func(t *testing.T, withRunning bool) (*fakeLab, []string, []string) {
		f := newFakeLab(t, testNow)
		spec := operatorSpec(f)
		var exps, rules []string // oldest first
		for i := 0; i < n; i++ {
			runID := id.New(RunIDPrefix)
			at := testNow.Add(time.Duration(i) * time.Minute)
			rule := &model.Rule{ID: fmt.Sprintf("rule-%03d", i), Name: RunNamePrefix + runID, Service: TargetService,
				Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: spec}, CreatedAt: at}
			if err := (fakeRules{f}).Create(context.Background(), rule); err != nil {
				t.Fatal(err)
			}
			addExperiment(t, f, fmt.Sprintf("exp-%03d", i), RunNamePrefix+runID, CreatedBy, "completed", at)
			exps, rules = append(exps, fmt.Sprintf("exp-%03d", i)), append(rules, rule.ID)
		}
		if withRunning {
			// Two running ones among them: never counted, never deleted.
			addExperiment(t, f, "exp-run-1", RunNamePrefix+id.New(RunIDPrefix), CreatedBy, "running",
				testNow.Add(100*time.Minute+time.Second))
			addExperiment(t, f, "exp-run-2", RunNamePrefix+id.New(RunIDPrefix), CreatedBy, "running",
				testNow.Add(225*time.Minute+time.Second))
		}
		return f, exps, rules
	}
	check := func(t *testing.T, f *fakeLab, exps, rules []string) {
		t.Helper()
		for i := range exps {
			wantKept := i >= n-keep
			if f.hasExperiment(exps[i]) != wantKept || f.hasRule(rules[i]) != wantKept {
				t.Errorf("run %d of %d: experiment kept %v, rule kept %v; want %v",
					i, n, f.hasExperiment(exps[i]), f.hasRule(rules[i]), wantKept)
			}
		}
	}

	t.Run("all pages", func(t *testing.T) {
		t.Parallel()
		f, exps, rules := setup(t, true)
		if err := Retain(context.Background(), f.deps(), keep); err != nil {
			t.Fatalf("Retain: %v", err)
		}
		check(t, f, exps, rules)
		for _, id := range []string{"exp-run-1", "exp-run-2"} {
			if f.experimentStatus(id) != "running" {
				t.Errorf("%s was touched", id)
			}
		}
		if c := f.callList("experiments.List "); len(c) < 2 {
			t.Errorf("%d list calls; want paging", len(c))
		}
	})

	t.Run("a later page fails", func(t *testing.T) {
		t.Parallel()
		f, exps, rules := setup(t, false)
		f.fail("experiments.List#2", errLeaky)
		err := Retain(context.Background(), f.deps(), keep)
		if !errors.Is(err, ErrRetainIncomplete) {
			t.Fatalf("Retain: %v; want incomplete", err)
		}
		// Only the first page (the newest 200) was read: 180 of them beyond keep.
		for i := range exps {
			if wantKept := i < n-200 || i >= n-keep; f.hasExperiment(exps[i]) != wantKept {
				t.Fatalf("after the first pass, run %d of %d kept %v; want %v", i, n, f.hasExperiment(exps[i]), wantKept)
			}
		}
		if err := Retain(context.Background(), f.deps(), keep); err != nil {
			t.Fatalf("second Retain: %v", err)
		}
		check(t, f, exps, rules)
	})
}

// TestRetain_RefusedRuleDeleteRetried: a rule whose delete is refused (a
// phase still links it) or never attempted (the experiment delete reported a
// failure after it committed) is left off, and the next pass deletes it.
func TestRetain_RefusedRuleDeleteRetried(t *testing.T) {
	t.Parallel()

	t.Run("still linked", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		old := seed(t, f, "completed", true)
		seed(t, f, "completed", false)
		addExperiment(t, f, "exp-op", "op", "lab-smoke", "completed", testNow)
		addPhase(t, f, "phase-op", "exp-op", 0, "completed", old.RuleID)

		err := Retain(context.Background(), f.deps(), 1)
		if !errors.Is(err, ErrRetainIncomplete) || !strings.Contains(err.Error(), "delete_rule") {
			t.Fatalf("Retain: %v; want incomplete at delete_rule", err)
		}
		assertNoLeak(t, err.Error())
		if f.hasExperiment(old.ExperimentID) {
			t.Error("the old run's experiment was kept")
		}
		if r := f.rule(old.RuleID); r == nil || r.Enabled || f.served(old.RuleID) {
			t.Fatalf("rule %+v (served %v); want kept, off", r, f.served(old.RuleID))
		}
		if err := Retain(context.Background(), f.deps(), 1); !errors.Is(err, ErrRetainIncomplete) {
			t.Fatalf("Retain while still linked: %v; want incomplete", err)
		}
		if err := (fakeExperiments{f}).Delete(context.Background(), "exp-op"); err != nil {
			t.Fatal(err)
		}
		if err := Retain(context.Background(), f.deps(), 1); err != nil {
			t.Fatalf("Retain once unlinked: %v", err)
		}
		if f.hasRule(old.RuleID) {
			t.Error("the rule was not deleted on the next pass")
		}
	})

	t.Run("experiment delete committed but failed", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		old := seed(t, f, "completed", true)
		seed(t, f, "completed", false)
		f.failAfterCommit("experiments.Delete", errLeaky)
		if err := Retain(context.Background(), f.deps(), 1); !errors.Is(err, ErrRetainIncomplete) {
			t.Fatalf("Retain: %v; want incomplete", err)
		}
		if n := len(f.callList("rules.Delete")); n != 0 {
			t.Errorf("%d rule deletes after a failed experiment delete; want none", n)
		}
		if r := f.rule(old.RuleID); r == nil || r.Enabled {
			t.Fatalf("rule %+v; want kept, off", r)
		}
		f.uninject("experiments.Delete")
		if err := Retain(context.Background(), f.deps(), 1); err != nil {
			t.Fatalf("second Retain: %v", err)
		}
		if f.hasRule(old.RuleID) {
			t.Error("the orphaned rule was not deleted on the next pass")
		}
	})

	t.Run("experiment delete failed", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		old := seed(t, f, "completed", true)
		seed(t, f, "completed", false)
		f.fail("experiments.Delete", errLeaky)
		if err := Retain(context.Background(), f.deps(), 1); !errors.Is(err, ErrRetainIncomplete) {
			t.Fatalf("Retain: %v; want incomplete", err)
		}
		if !f.hasExperiment(old.ExperimentID) || !f.hasRule(old.RuleID) || f.rule(old.RuleID).Enabled {
			t.Fatal("want the experiment and its rule kept, the rule off")
		}
		f.uninject("experiments.Delete")
		if err := Retain(context.Background(), f.deps(), 1); err != nil {
			t.Fatalf("second Retain: %v", err)
		}
		requireRemovedInOrder(t, f, old)
	})
}

// TestRetain_SwitchOffFailsKeepsExperiment: when a rule cannot be switched
// off, its experiment is not deleted (deleting it would leave the rule on and
// attached to nothing).
func TestRetain_SwitchOffFailsKeepsExperiment(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	old := seed(t, f, "completed", true)
	seed(t, f, "completed", false)
	d := f.deps()
	d.Rules = failRuleOff{fakeRules{f}, old.RuleID}
	err := Retain(context.Background(), d, 1)
	if !errors.Is(err, ErrRetainIncomplete) || !errors.Is(err, ErrRuleState) {
		t.Fatalf("Retain: %v; want incomplete with a rule-state failure", err)
	}
	if !f.hasExperiment(old.ExperimentID) || !f.hasRule(old.RuleID) {
		t.Fatal("the experiment or rule was deleted although the rule could not be switched off")
	}
	if n := len(f.callList("experiments.Delete")); n != 0 {
		t.Errorf("%d experiment deletes; want none", n)
	}
	if err := Retain(context.Background(), f.deps(), 1); err != nil {
		t.Fatalf("Retain once the switch works: %v", err)
	}
	requireRemovedInOrder(t, f, old)
}

// TestRetain_RefusesBadKeep: keep must be at least 1; nothing is touched.
func TestRetain_RefusesBadKeep(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	s := seed(t, f, "completed", false)
	for _, keep := range []int{0, -1} {
		if err := Retain(context.Background(), f.deps(), keep); !errors.Is(err, ErrRetainIncomplete) {
			t.Errorf("Retain(keep %d): %v; want refused", keep, err)
		}
	}
	requireKept(t, f, s)
}

// ---- the worker's retention ----

// retainWorker is a worker over f whose current run is cur (none when cur's
// runID is ""), never started.
func retainWorker(f *fakeLab, cfg Config, curRunID, curExpID string) *Worker {
	w := NewWorker(f.deps(), cfg, nil, nil)
	if curRunID != "" {
		w.mu.Lock()
		w.cur = &active{runID: curRunID, expID: curExpID}
		w.mu.Unlock()
	}
	return w
}

// TestWorkerRetain_CancelsStalePlanned: a planned demo experiment older than
// twice the run cap (by the lab's clock) that is not the run in progress is
// cancelled, and on a later pass deleted with its rule; a younger planned one
// and the run in progress's are left alone.
func TestWorkerRetain_CancelsStalePlanned(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	cfg := workerConfig()
	cfg.RunCap, cfg.Retain = time.Minute, 1
	stale := seed(t, f, "planned", false)
	current := seed(t, f, "planned", false)
	f.clock.Advance(2*cfg.RunCap + time.Second)
	young := seed(t, f, "planned", false)
	w := retainWorker(f, cfg, current.runID, current.ExperimentID)

	if err := w.Retain(context.Background()); err != nil {
		t.Fatalf("Retain: %v", err)
	}
	if got := f.callList("orch.CancelExperiment "); !slices.Equal(got, []string{"orch.CancelExperiment " + stale.ExperimentID}) {
		t.Errorf("cancels %v; want only the stale one", got)
	}
	if st := f.experimentStatus(stale.ExperimentID); st != "cancelled" {
		t.Errorf("stale planned experiment: %s; want cancelled", st)
	}
	for _, s := range []seeded{current, young} {
		if st := f.experimentStatus(s.ExperimentID); st != "planned" {
			t.Errorf("run %s: %s; want planned, untouched", s.runID, st)
		}
	}

	// The run in progress ends and a newer run finishes: the cancelled leftover
	// is now beyond keep and goes with its rule.
	f.endExperiment(young.ExperimentID, "completed", testNow, testNow)
	w = retainWorker(f, cfg, current.runID, current.ExperimentID)
	if err := w.Retain(context.Background()); err != nil {
		t.Fatalf("second Retain: %v", err)
	}
	requireRemovedInOrder(t, f, stale)
	requireKept(t, f, young)
}

// TestWorkerRetain_SparesTheRunInProgress: the run in progress's experiment is
// never deleted, even when finished and beyond keep, nor cancelled; and its
// rule, written before its experiment, is never taken for an orphan.
func TestWorkerRetain_SparesTheRunInProgress(t *testing.T) {
	t.Parallel()

	t.Run("finished, beyond keep", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		cfg := workerConfig()
		cfg.Retain = 1
		current := seed(t, f, "completed", false)
		other := seed(t, f, "completed", false)
		seed(t, f, "completed", false)
		w := retainWorker(f, cfg, current.runID, current.ExperimentID)
		if err := w.Retain(context.Background()); err != nil {
			t.Fatalf("Retain: %v", err)
		}
		requireKept(t, f, current)
		requireRemovedInOrder(t, f, other)
	})

	t.Run("planned and old, known by name before its id", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		cfg := workerConfig()
		current := seed(t, f, "planned", false)
		f.clock.Advance(time.Hour)
		w := retainWorker(f, cfg, current.runID, "")
		if err := w.Retain(context.Background()); err != nil {
			t.Fatalf("Retain: %v", err)
		}
		requireKept(t, f, current)
		if st := f.experimentStatus(current.ExperimentID); st != "planned" {
			t.Errorf("the run in progress's experiment is %s; want planned", st)
		}
	})

	t.Run("rule written, experiment not yet", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		cfg := workerConfig()
		curRun, loneRun := id.New(RunIDPrefix), id.New(RunIDPrefix)
		addRule(t, f, "rule-cur", RunNamePrefix+curRun, false)
		addRule(t, f, "rule-lone", RunNamePrefix+loneRun, false)
		w := retainWorker(f, cfg, curRun, "")
		if err := w.Retain(context.Background()); err != nil {
			t.Fatalf("Retain: %v", err)
		}
		if !f.hasRule("rule-cur") || len(callsFor(f, "rule-cur")) != 0 {
			t.Errorf("the run in progress's rule was touched: %v", callsFor(f, "rule-cur"))
		}
		if f.hasRule("rule-lone") {
			t.Error("an orphaned run rule was kept")
		}
	})
}

// TestWorker_RetainCollectsWatchdogLeftover: end to end with OnRunEnded set
// to the worker's Retain. A run whose switch-on hangs past the cap ends
// timed out before its start, leaving a planned experiment with its rule off.
// Once that is older than twice the cap, the pass after the next run cancels
// it, and the pass after that deletes it with its rule.
func TestWorker_RetainCollectsWatchdogLeftover(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(c *Config) { c.Retain = 1 })
	passes := make(chan error, 8)
	x.w.OnRunEnded = func(ctx context.Context) { passes <- x.w.Retain(ctx) }
	x.f.hang("rules.EnableIfAttached")
	first, ch := x.enqueue("a")
	x.start()
	_, end := awaitEnd(t, ch)
	requireEnded(t, end, RunCancelled, ReasonTimedOut)
	if err := <-passes; err != nil {
		t.Fatalf("pass after the first run: %v", err)
	}
	leftover, leftRule := x.expID(first.RunID), x.f.ruleIDOfRun(first.RunID)
	if st := x.f.experimentStatus(leftover); st != "planned" {
		t.Fatalf("the timed-out run's experiment is %s; want planned (left behind)", st)
	}
	x.f.requireDemoRulesOff(t)

	x.f.uninject("rules.EnableIfAttached")
	x.f.clock.Advance(2*x.cfg.RunCap + time.Second)
	x.f.mu.Lock()
	x.f.autopilot = true
	x.f.mu.Unlock()
	_, ch2 := x.enqueue("b")
	_, end2 := awaitEnd(t, ch2)
	requireEnded(t, end2, RunCompleted, Reason{})
	if err := <-passes; err != nil {
		t.Fatalf("pass after the second run: %v", err)
	}
	if st := x.f.experimentStatus(leftover); st != "cancelled" {
		t.Fatalf("the leftover is %s; want cancelled", st)
	}
	if err := x.w.Retain(context.Background()); err != nil {
		t.Fatalf("a later pass: %v", err)
	}
	if x.f.hasExperiment(leftover) || x.f.hasRule(leftRule) {
		t.Error("the leftover and its rule were not deleted on the later pass")
	}
}

// ---- the worker's shutdown ----

// TestWorker_ShutdownDisablesCurrentRule: while the fault phase is served,
// Shutdown switches the rule off, cancels nothing, ends the run in progress
// and the waiting one interrupted, and Run returns; the experiment is left
// running for the next start's sweep. No run is picked up afterwards.
func TestWorker_ShutdownDisablesCurrentRule(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(c *Config) { c.RunCap = 10 * time.Second })
	r, ch := x.enqueue("a")
	_, chB := x.enqueue("b")
	x.start()
	awaitPhase(t, ch, 0, PhaseRunning)
	exp := x.expID(r.RunID)
	phases := x.f.phasesOf(exp)
	x.f.setPhaseStatus(phases[0].ID, "completed")
	x.f.setPhaseStatus(phases[1].ID, "running")
	rule := x.f.ruleIDOfRun(r.RunID)
	if !x.f.served(rule) {
		t.Fatal("precondition: the fault phase runs but its rule is not served")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := x.w.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	x.f.requireDemoRulesOff(t)
	select {
	case <-x.w.done:
	default:
		t.Fatal("Shutdown returned before Run")
	}
	_, end := awaitEnd(t, ch)
	requireEnded(t, end, RunCancelled, ReasonInterrupted)
	_, endB := awaitEnd(t, chB)
	requireEnded(t, endB, RunCancelled, ReasonInterrupted)
	if c := x.f.callList("orch.CancelExperiment"); len(c) != 0 {
		t.Errorf("cancels %v; want none on a shutdown", c)
	}
	if st := x.f.experimentStatus(exp); st != "running" {
		t.Errorf("experiment %s; want running, left for the sweep", st)
	}
	if err := x.w.Shutdown(ctx); err != nil {
		t.Errorf("second Shutdown: %v", err)
	}
	c, _ := x.enqueue("c")
	time.Sleep(20 * time.Millisecond)
	if x.f.ruleIDOfRun(c.RunID) != "" {
		t.Error("a run was picked up after Shutdown")
	}
}

// TestWorker_ShutdownReportsSwitchOffFailure: a switch-off that fails is
// reported; Shutdown still returns at once and Run still ends. The rule then
// stays on, and the next start's sweep switches it off.
func TestWorker_ShutdownReportsSwitchOffFailure(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(c *Config) { c.RunCap = 10 * time.Second })
	r, ch := x.enqueue("a")
	x.start()
	awaitPhase(t, ch, 0, PhaseRunning)
	rule := x.f.ruleIDOfRun(r.RunID)
	x.f.fail("rules.SetEnabled", errLeaky)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	began := time.Now()
	err := x.w.Shutdown(ctx)
	if took := time.Since(began); took > time.Second {
		t.Errorf("Shutdown took %s", took)
	}
	if !errors.Is(err, ErrRuleState) || !errors.Is(err, errLeaky) {
		t.Fatalf("Shutdown: %v; want the switch-off's failure", err)
	}
	assertNoLeak(t, err.Error())
	waitClosed(t, x.w.done, "Run to return")
	if r := x.f.rule(rule); r == nil || !r.Enabled {
		t.Fatalf("precondition of the rest: rule %+v; want still on", r)
	}
	x.f.uninject("rules.SetEnabled")
	if err := Sweep(context.Background(), x.f.deps(), 20); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	x.f.requireDemoRulesOff(t)
	if st := x.f.experimentStatus(x.expID(r.RunID)); st != "cancelled" {
		t.Errorf("experiment %s after the sweep; want cancelled", st)
	}
}

// TestWorker_ShutdownBoundedByContext: Shutdown returns by its context's
// deadline when the worker goroutine is stuck, with the rule already off; and
// when the switch-off itself is stuck, it returns and says so.
func TestWorker_ShutdownBoundedByContext(t *testing.T) {
	t.Parallel()

	// stuck blocks op until release, or for 2 s at most, so a Shutdown that
	// waits for it ends late instead of never.
	stuck := func(x *harness, op string) (entered <-chan struct{}, release func()) {
		in, out := make(chan struct{}), make(chan struct{})
		var once, done sync.Once
		x.f.onCall(op, func() {
			once.Do(func() { close(in) })
			<-out
		})
		release = func() { done.Do(func() { close(out) }) }
		time.AfterFunc(2*time.Second, release)
		return in, release
	}

	t.Run("worker goroutine", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, func(c *Config) { c.RunCap = 10 * time.Second })
		r, ch := x.enqueue("a")
		x.start()
		awaitPhase(t, ch, 0, PhaseRunning)
		entered, release := stuck(x, "experiments.Get")
		defer release()
		waitClosed(t, entered, "the worker to block")

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		began := time.Now()
		err := x.w.Shutdown(ctx)
		if took := time.Since(began); took > 400*time.Millisecond {
			t.Errorf("Shutdown took %s with a 100 ms deadline", took)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown: %v; want the deadline", err)
		}
		x.f.requireRunRuleOff(t, r.RunID)
		release()
		waitClosed(t, x.w.done, "Run to return once unblocked")
	})

	t.Run("switch-off", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, func(c *Config) { c.RunCap = 10 * time.Second })
		r, ch := x.enqueue("a")
		x.start()
		awaitPhase(t, ch, 0, PhaseRunning)
		entered, release := stuck(x, "rules.SetEnabled")
		defer release()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		began := time.Now()
		var err error
		done := make(chan struct{})
		go func() { err = x.w.Shutdown(ctx); close(done) }()
		waitClosed(t, entered, "the switch-off to block")
		waitClosed(t, done, "Shutdown to return")
		if took := time.Since(began); took > 400*time.Millisecond {
			t.Errorf("Shutdown took %s with a 100 ms deadline", took)
		}
		if !errors.Is(err, ErrRuleState) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown: %v; want an unfinished switch-off", err)
		}
		release()
		waitFor(t, "the switch-off to land", func() bool { r := x.f.rule(x.f.ruleIDOfRun(r.RunID)); return r != nil && !r.Enabled })
		waitClosed(t, x.w.done, "Run to return")
	})
}

// TestWorker_ShutdownIsSafeAnyTime: before Run (Run then returns at once and
// the waiting runs end interrupted), twice at once, and after Run returned.
func TestWorker_ShutdownIsSafeAnyTime(t *testing.T) {
	t.Parallel()

	t.Run("before Run", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, nil)
		_, chA := x.enqueue("a")
		_, chB := x.enqueue("b")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := x.w.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		_, endA := awaitEnd(t, chA)
		_, endB := awaitEnd(t, chB)
		requireEnded(t, endA, RunCancelled, ReasonInterrupted)
		requireEnded(t, endB, RunCancelled, ReasonInterrupted)
		returned := make(chan struct{})
		go func() { x.w.Run(context.Background()); close(returned) }()
		waitClosed(t, returned, "Run to return at once")
		waitClosed(t, x.w.done, "done to be closed")
		if c := x.f.callList("rules.Create"); len(c) != 0 {
			t.Errorf("a run was built after Shutdown: %v", c)
		}
		if n := len(x.q.Snapshot()); n != 0 {
			t.Errorf("%d runs in line", n)
		}
		if err := x.w.Shutdown(context.Background()); err != nil {
			t.Errorf("second Shutdown: %v", err)
		}
	})

	t.Run("twice at once", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, func(c *Config) { c.RunCap = 10 * time.Second })
		_, ch := x.enqueue("a")
		x.start()
		awaitPhase(t, ch, 0, PhaseRunning)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		var failed atomic.Int32
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := x.w.Shutdown(ctx); err != nil {
					failed.Add(1)
				}
			}()
		}
		wg.Wait()
		if failed.Load() != 0 {
			t.Errorf("%d Shutdown calls failed", failed.Load())
		}
		x.f.requireDemoRulesOff(t)
		waitClosed(t, x.w.done, "Run to return")
	})

	t.Run("after Run returned", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, nil)
		x.start()
		x.stop()
		began := time.Now()
		if err := x.w.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if took := time.Since(began); took > 100*time.Millisecond {
			t.Errorf("Shutdown after Run took %s", took)
		}
	})
}
