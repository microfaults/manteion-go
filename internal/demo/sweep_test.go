package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

// seeded is one run of build 2's shape seeded into a lab (buildOldRun).
type seeded struct {
	runID        string
	ExperimentID string
	RuleID       string
	PhaseIDs     [3]string
}

// Build 2's run shape: three phases, the middle one carrying the run's one
// rule, demo-run-<run id>, an inline +50 ms latency on productcatalogservice.
var oldPhaseNames = [3]string{"plain", "catalog-latency-50ms", "plain-end"}

const (
	oldFaultPhase = 1
	oldSpecName   = "demo-catalog-latency-50ms"
)

// buildOldRun writes run runID as build 2's BuildRun did, through d's repos:
// the shared spec demo-catalog-latency-50ms (created once), one disabled rule
// demo-run-<run id> on productcatalogservice, and a planned experiment
// demo-run-<run id> whose phases plain, catalog-latency-50ms and plain-end
// each run demo-browse at 30 VUs and 3 rps for 40 s, the rule attached to the
// middle phase only. Such runs are what the sweep and retention know how to
// group (one rule named like its experiment) and what a manteion upgraded from
// build 2 still finds in its database.
func buildOldRun(ctx context.Context, d Deps, runID string) (seeded, error) {
	now := d.Clock.Now()
	s := seeded{runID: runID}
	specs, err := d.Specs.ListSpecs(ctx)
	if err != nil {
		return s, err
	}
	var specID string
	for _, sp := range specs {
		if sp.Name == oldSpecName {
			specID = sp.ID
		}
	}
	if specID == "" {
		spec := &model.FaultSpec{ID: id.New("spec"), Name: oldSpecName, Category: "inline", FaultType: "latency",
			Params: json.RawMessage(`{"delay":"50ms"}`), DurationMs: 60000, CreatedAt: now}
		if err := d.Specs.CreateSpec(ctx, spec); err != nil {
			return s, err
		}
		specID = spec.ID
	}
	rule := &model.Rule{ID: id.New("rule"), Name: RunNamePrefix + runID, Service: testTarget, Priority: 100, Mode: "inline",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: specID}, CreatedAt: now, UpdatedAt: now}
	if err := d.Rules.Create(ctx, rule); err != nil {
		return s, err
	}
	s.RuleID = rule.ID
	exp := &model.Experiment{ID: id.New("exp"), Name: RunNamePrefix + runID, Status: "planned", CreatedBy: CreatedBy, CreatedAt: now}
	if err := d.Experiments.Create(ctx, exp); err != nil {
		return s, err
	}
	s.ExperimentID = exp.ID
	for i, name := range oldPhaseNames {
		p := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: name, Position: i, Status: "pending",
			FrozenServices: []model.CacheBoxConfig{}}
		if err := d.Experiments.CreatePhase(ctx, p); err != nil {
			return s, err
		}
		s.PhaseIDs[i] = p.ID
		if err := d.Experiments.AttachPhaseWorkflows(ctx, p.ID, []model.PhaseWorkflow{{PhaseID: p.ID,
			WorkflowID: "demo-browse", VUs: 30, RateRPS: 3, DurationSec: 40, DatasetID: "ds-ok"}}); err != nil {
			return s, err
		}
		if i == oldFaultPhase {
			if err := d.Experiments.AttachPhaseRules(ctx, p.ID, []string{rule.ID}); err != nil {
				return s, err
			}
		}
	}
	return s, nil
}

// seed writes a run of build 2's shape in f (buildOldRun), one second (of f's
// clock) after the previous one, switches its rule on when on, and moves its
// experiment to status: planned (as built), running (its fault phase running,
// so an enabled rule is served), or completed, failed or cancelled (phases not
// yet ended are skipped).
func seed(t *testing.T, f *fakeLab, status string, on bool) seeded {
	t.Helper()
	ctx := context.Background()
	s, err := buildOldRun(ctx, f.deps(), id.New(RunIDPrefix))
	if err != nil {
		t.Fatalf("seed: build: %v", err)
	}
	f.clock.Advance(time.Second)
	if on {
		if err := SetRuleEnabled(ctx, fakeRules{f}, s.RuleID, true); err != nil {
			t.Fatalf("seed: switch on: %v", err)
		}
	}
	switch status {
	case "planned":
	case "running":
		f.setExperimentStatus(s.ExperimentID, "running")
		f.setPhaseStatus(s.PhaseIDs[0], "completed")
		f.setPhaseStatus(s.PhaseIDs[oldFaultPhase], "running")
	case "completed", "failed", "cancelled":
		f.endExperiment(s.ExperimentID, status, testNow, testNow)
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
	r := &model.Rule{ID: ruleID, Name: name, Service: testTarget, Enabled: enabled, Priority: 10, Mode: "inline",
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
	// The guard read every phase's links between the switch-off and the delete.
	for _, pid := range s.PhaseIDs {
		if pid == "" {
			continue
		}
		at := slices.Index(calls, "experiments.ListPhaseRules "+pid)
		if at < 0 || at < off || at > delExp {
			t.Errorf("run %s: phase %s's links read at %d; want between the switch-off (%d) and the delete (%d)",
				s.runID, pid, at, off, delExp)
		}
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
var quickTidy = tidyTimeouts{call: 40 * time.Millisecond, off: 40 * time.Millisecond, offAll: 100 * time.Millisecond, cancel: 40 * time.Millisecond}

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
			rule := &model.Rule{ID: fmt.Sprintf("rule-%03d", i), Name: RunNamePrefix + runID, Service: testTarget,
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

		// The refusal is explained (a phase of another experiment links the
		// rule): no failure, one Warn per pass, no Error.
		logs := &syncBuffer{}
		d := f.deps()
		d.Logger = slog.New(slog.NewTextHandler(logs, nil))
		warned := func() int { return strings.Count(logs.String(), "a phase of another experiment still links them") }

		if err := Retain(context.Background(), d, 1); err != nil {
			t.Fatalf("Retain: %v; want no failure for a rule a phase still links", err)
		}
		if f.hasExperiment(old.ExperimentID) {
			t.Error("the old run's experiment was kept")
		}
		if r := f.rule(old.RuleID); r == nil || r.Enabled || f.served(old.RuleID) {
			t.Fatalf("rule %+v (served %v); want kept, off", r, f.served(old.RuleID))
		}
		if n := warned(); n != 1 || strings.Contains(logs.String(), "level=ERROR") ||
			!strings.Contains(logs.String(), old.RuleID) {
			t.Errorf("after one pass: %d Warn lines naming the rule, logs:\n%s\nwant one Warn, no Error", n, logs)
		}
		if err := Retain(context.Background(), d, 1); err != nil {
			t.Fatalf("Retain while still linked: %v", err)
		}
		if n := warned(); n != 2 || strings.Contains(logs.String(), "level=ERROR") {
			t.Errorf("after two passes: %d Warn lines; want 2, no Error", n)
		}
		if err := (fakeExperiments{f}).Delete(context.Background(), "exp-op"); err != nil {
			t.Fatal(err)
		}
		if err := Retain(context.Background(), d, 1); err != nil {
			t.Fatalf("Retain once unlinked: %v", err)
		}
		if f.hasRule(old.RuleID) {
			t.Error("the rule was not deleted on the next pass")
		}
		if n := warned(); n != 2 {
			t.Errorf("%d Warn lines after the rule went; want still 2", n)
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

	t.Run("an ended run whose switch-off is pending, beyond keep", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		cfg := workerConfig()
		cfg.Retain = 1
		pending := seed(t, f, "completed", true)
		other := seed(t, f, "completed", false)
		seed(t, f, "completed", false)
		w := retainWorker(f, cfg, "", "")
		w.mu.Lock()
		w.pending = &active{runID: pending.runID, expID: pending.ExperimentID}
		w.mu.Unlock()
		if err := w.Retain(context.Background()); err != nil {
			t.Fatalf("Retain: %v", err)
		}
		requireKept(t, f, pending)
		for _, c := range callsFor(f, pending.RuleID) {
			if strings.HasPrefix(c, "rules.SetEnabled") {
				t.Errorf("the pending run's rule was written to: %q (its switch is the worker's)", c)
			}
		}
		requireRemovedInOrder(t, f, other)
		// The pending rule is on, under a finished experiment: off by hand.
		if err := (fakeRules{f}).SetEnabled(context.Background(), pending.RuleID, false); err != nil {
			t.Fatal(err)
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
	leftover, leftRules := x.expID(first.RunID), x.f.rulesOfRun(t, first.RunID)
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
	if x.f.hasExperiment(leftover) || x.f.hasRule(leftRules[0]) || x.f.hasRule(leftRules[1]) {
		t.Error("the leftover and its rules were not deleted on the later pass")
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
	if rules := x.f.rulesOfRun(t, r.RunID); !x.f.served(rules[0]) {
		t.Fatal("precondition: the memory phase runs but its rule is not served")
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
	// No run can be queued, let alone picked up, after Shutdown.
	if _, err := x.q.Enqueue("c", DefaultScenario(), testNow); !errors.Is(err, ErrQueueClosed) {
		t.Errorf("enqueue after Shutdown: %v; want ErrQueueClosed", err)
	}
	if n := len(x.f.callList("rules.Create")); n != 2 {
		t.Errorf("%d rules created; want only the first run's two", n)
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
	rules := x.f.rulesOfRun(t, r.RunID)
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
	for _, rule := range rules {
		if r := x.f.rule(rule); r == nil || !r.Enabled {
			t.Fatalf("precondition of the rest: rule %+v; want still on", r)
		}
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
		x.f.requireRunRulesOff(t, r.RunID)
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
		for _, rule := range x.f.rulesOfRun(t, r.RunID) {
			waitFor(t, "the switch-off to land", func() bool { r := x.f.rule(rule); return r != nil && !r.Enabled })
		}
		waitClosed(t, x.w.done, "Run to return")
	})

	// G3 (M3): an ended run's switch-off that Shutdown starts and that is
	// still held when Shutdown and Run have returned is counted in the
	// worker's group until it returns.
	t.Run("a pending run's switch-off outlives Run", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, noCap)
		ctx := context.Background()
		runID := id.New(RunIDPrefix)
		built, err := BuildRun(ctx, x.f.deps(), runID, DefaultScenario(), testRunCap)
		if err != nil {
			t.Fatal(err)
		}
		a := &active{runID: runID, expID: built.ExperimentID, sw: newRuleSwitch(runID, fakeRules{x.f}, x.w.log, x.f.clock)}
		a.sw.attach(ctx, built.RuleIDs[:]...)
		if err := a.sw.on(ctx); err != nil {
			t.Fatal(err)
		}
		x.start() // the line is empty: Run waits for a run
		waitFor(t, "Run to start", x.w.running.Load)
		x.w.mu.Lock()
		x.w.pending = a
		x.w.mu.Unlock()
		entered, release := stuck(x, "rules.SetEnabled")
		defer release()

		sctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		if err := x.w.Shutdown(sctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown: %v; want an unfinished switch-off", err)
		}
		waitClosed(t, entered, "the switch-off to block")
		waitClosed(t, x.w.done, "Run to return")
		done := groupDone(&x.w.bg)
		requireNotDone(t, done, 30*time.Millisecond, "the worker's group, with Shutdown's switch-off held,")
		release()
		waitClosed(t, done, "Shutdown's switch-off to return")
		x.f.requireRunRulesOff(t, runID)
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

// ---- fix round 1: the guard, the start-up switch-off, the sweep's deadline ----

// TestRetain_ForeignRuleKeepsExperiment: an operator linked their own rule to
// a phase of a finished demo experiment and switched it on. Retention must not
// delete that experiment (the delete would cascade the link away and leave the
// rule served to every instance, for ever): it keeps it, with its run's own
// rule switched off, logs the ids at Error and says the pass is incomplete,
// on every pass. The foreign rule is never served: the fake's invariant
// watches it after every write, and the test checks it before every write.
func TestRetain_ForeignRuleKeepsExperiment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFakeLab(t, testNow)
	old := seed(t, f, "completed", true)
	newer := seed(t, f, "completed", false)
	addRule(t, f, "rule-foreign", "op-latency", false)
	f.watchRule("rule-foreign")
	if err := (fakeExperiments{f}).AttachPhaseRules(ctx, old.PhaseIDs[0], []string{"rule-foreign"}); err != nil {
		t.Fatal(err)
	}
	if err := (fakeRules{f}).SetEnabled(ctx, "rule-foreign", true); err != nil {
		t.Fatal(err)
	}
	var servedAtWrite atomic.Int32
	for _, op := range []string{"rules.SetEnabled", "rules.Delete", "experiments.Delete"} {
		f.onCall(op, func() {
			if f.served("rule-foreign") {
				servedAtWrite.Add(1)
			}
		})
	}
	logs := &syncBuffer{}
	d := f.deps()
	d.Logger = slog.New(slog.NewTextHandler(logs, nil))

	for pass := 1; pass <= 2; pass++ {
		err := Retain(ctx, d, 1)
		if !errors.Is(err, ErrRetainIncomplete) || !errors.Is(err, errForeignRule) ||
			!strings.Contains(err.Error(), "foreign_rule_linked") {
			t.Fatalf("pass %d: %v; want incomplete at foreign_rule_linked", pass, err)
		}
		assertNoLeak(t, err.Error())
		if !f.hasExperiment(old.ExperimentID) || !f.hasRule(old.RuleID) {
			t.Fatalf("pass %d: the experiment carrying a foreign rule, or its run's rule, was deleted", pass)
		}
		if r := f.rule(old.RuleID); r.Enabled {
			t.Errorf("pass %d: the run's own rule is still on", pass)
		}
		if r := f.rule("rule-foreign"); r == nil || !r.Enabled {
			t.Errorf("pass %d: the foreign rule was touched: %+v", pass, r)
		}
		if f.served("rule-foreign") {
			t.Errorf("pass %d: the foreign rule is served", pass)
		}
	}
	if n := servedAtWrite.Load(); n != 0 {
		t.Errorf("the foreign rule was served at %d writes", n)
	}
	if c := callsFor(f, old.ExperimentID); slices.Contains(c, "experiments.Delete "+old.ExperimentID) {
		t.Errorf("the experiment's delete was attempted: %v", c)
	}
	for _, c := range callsFor(f, "rule-foreign") {
		if strings.HasPrefix(c, "rules.") && c != "rules.SetEnabled rule-foreign true" { // the test's own switch-on
			t.Errorf("the foreign rule was written to: %q", c)
		}
	}
	line := ""
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, "foreign_rule_linked") {
			line = l
			break
		}
	}
	if !strings.Contains(line, "level=ERROR") || !strings.Contains(line, old.ExperimentID) ||
		!strings.Contains(line, "rule-foreign") || !strings.Contains(line, old.PhaseIDs[0]) {
		t.Errorf("the refusal's log line %q; want an Error naming the experiment, the rule and the phase", line)
	}
	requireKept(t, f, newer)
}

// TestRetain_PhaseLinkReadFailureKeepsExperiment: when the phases or a
// phase's links cannot be read, the guard fails closed: the experiment is
// kept (its rule off) and the pass is incomplete; the next pass, with reads
// working, deletes it in the usual order.
func TestRetain_PhaseLinkReadFailureKeepsExperiment(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"experiments.ListPhasesForExperiment", "experiments.ListPhaseRules"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			f := newFakeLab(t, testNow)
			old := seed(t, f, "completed", true)
			seed(t, f, "completed", false)
			f.fail(op, errLeaky)
			err := Retain(context.Background(), f.deps(), 1)
			if !errors.Is(err, ErrRetainIncomplete) || !errors.Is(err, errLeaky) ||
				!strings.Contains(err.Error(), "read_phase_links") {
				t.Fatalf("Retain: %v; want incomplete at read_phase_links", err)
			}
			assertNoLeak(t, err.Error())
			if !f.hasExperiment(old.ExperimentID) || len(f.callList("experiments.Delete")) != 0 {
				t.Fatal("the experiment was deleted although its links could not be read")
			}
			if r := f.rule(old.RuleID); r == nil || r.Enabled {
				t.Errorf("rule %+v; want kept, off", r)
			}
			f.uninject(op)
			if err := Retain(context.Background(), f.deps(), 1); err != nil {
				t.Fatalf("second Retain: %v", err)
			}
			requireRemovedInOrder(t, f, old)
		})
	}
}

// TestSwitchOffAll: the start-up switch-off switches every enabled demo-run-*
// rule off and does nothing else: no listing of experiments, no cancel, no
// delete; it is detached from the caller, bounded as a whole, and says when
// it is incomplete.
func TestSwitchOffAll(t *testing.T) {
	t.Parallel()

	t.Run("switches off, touches nothing else", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		runs := []seeded{seed(t, f, "planned", true), seed(t, f, "running", true), seed(t, f, "completed", true),
			seed(t, f, "cancelled", false)}
		addRule(t, f, "rule-smoke", "smoke-x", true)
		addRule(t, f, "rule-lone", RunNamePrefix+id.New(RunIDPrefix), false)
		mark := len(f.callList(""))
		if err := SwitchOffAll(context.Background(), f.deps()); err != nil {
			t.Fatalf("SwitchOffAll: %v", err)
		}
		f.requireDemoRulesOff(t)
		for _, c := range f.callList("")[mark:] {
			if c != "rules.List " && !strings.HasPrefix(c, "rules.SetEnabled ") || strings.HasSuffix(c, " true") {
				t.Errorf("SwitchOffAll made %q; want only the rule list and switch-offs", c)
			}
		}
		if n := len(f.callList("rules.SetEnabled ")); n != 3 {
			t.Errorf("%d switch-offs; want one per enabled demo rule (3)", n)
		}
		if r := f.rule("rule-smoke"); r == nil || !r.Enabled {
			t.Error("the operator's rule was switched off")
		}
		for i, want := range []string{"planned", "running", "completed", "cancelled"} {
			if st := f.experimentStatus(runs[i].ExperimentID); st != want {
				t.Errorf("run %d: experiment %s; want %s, untouched", i, st, want)
			}
		}
		if !f.hasRule("rule-lone") {
			t.Error("SwitchOffAll deleted a rule")
		}
	})

	t.Run("detached from the caller", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		seed(t, f, "running", true)
		seed(t, f, "planned", true)
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // the caller gave up: the switch-off still happens
		if err := SwitchOffAll(ctx, f.deps()); err != nil {
			t.Fatalf("SwitchOffAll on a cancelled context: %v", err)
		}
		f.requireDemoRulesOff(t)
	})

	t.Run("incomplete when the listing or a switch-off fails", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		a, b := seed(t, f, "running", true), seed(t, f, "running", true)
		f.fail("rules.List", errLeaky)
		err := SwitchOffAll(context.Background(), f.deps())
		if !errors.Is(err, ErrSwitchOffIncomplete) || !errors.Is(err, errLeaky) {
			t.Fatalf("listing fails: %v; want ErrSwitchOffIncomplete", err)
		}
		assertNoLeak(t, err.Error())
		f.uninject("rules.List")
		d := f.deps()
		d.Rules = failRuleOff{fakeRules{f}, a.RuleID}
		err = SwitchOffAll(context.Background(), d)
		if !errors.Is(err, ErrSwitchOffIncomplete) || !errors.Is(err, ErrRuleState) {
			t.Fatalf("one switch-off fails: %v; want ErrSwitchOffIncomplete, ErrRuleState", err)
		}
		if r := f.rule(b.RuleID); r.Enabled {
			t.Error("the other rule was not switched off")
		}
		if err := SwitchOffAll(context.Background(), f.deps()); err != nil {
			t.Fatalf("once it works: %v", err)
		}
		f.requireDemoRulesOff(t)
	})

	t.Run("bounded as a whole", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		for i := 0; i < 6; i++ {
			seed(t, f, "running", true)
		}
		f.hang("rules.SetEnabled")
		lim := tidyTimeouts{call: time.Second, off: 100 * time.Millisecond, offAll: 250 * time.Millisecond, cancel: time.Second}
		tid := newTidier(f.deps(), ErrSwitchOffIncomplete, lim)
		began := time.Now()
		tid.switchOffAll(context.Background())
		err := tid.result()
		took := time.Since(began)
		if took > 450*time.Millisecond {
			t.Errorf("took %s; want about the 250 ms bound, not 6 x 100 ms", took)
		}
		if n := len(f.callList("rules.SetEnabled ")); n >= 6 {
			t.Errorf("%d switch-offs started; want the bound to stop them", n)
		}
		if !errors.Is(err, ErrSwitchOffIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("result: %v; want incomplete, deadline exceeded", err)
		}
		f.uninject("rules.SetEnabled")
		if err := SwitchOffAll(context.Background(), f.deps()); err != nil {
			t.Fatal(err)
		}
	})
}

// TestSweep_HonoursDeadline: six running demo experiments whose cancels each
// wait (on a lock that ignores contexts) past their own bound. The sweep stops
// starting cancels once its context's deadline passes and returns then,
// incomplete, with every rule already off: the deadline, not 6 x the cancel
// bound, is what bounds it.
func TestSweep_HonoursDeadline(t *testing.T) {
	t.Parallel()
	f := newFakeLab(t, testNow)
	for i := 0; i < 6; i++ {
		seed(t, f, "running", true)
	}
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	defer time.AfterFunc(3*time.Second, free).Stop()
	var inCall, returned atomic.Int32
	f.onCall("orch.CancelExperiment", func() {
		inCall.Add(1)
		<-release
		returned.Add(1)
	})
	lim := tidyTimeouts{call: time.Second, off: time.Second, offAll: time.Second, cancel: 100 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	began := time.Now()
	err := newTidier(f.deps(), ErrSweepIncomplete, lim).sweep(ctx, 20)
	took := time.Since(began)
	if took > 600*time.Millisecond {
		t.Errorf("sweep took %s with a 250 ms deadline", took)
	}
	if n := len(f.callList("orch.CancelExperiment ")); n >= 6 {
		t.Errorf("%d cancels started; want none after the deadline", n)
	}
	if !errors.Is(err, ErrSweepIncomplete) || !errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), "out_of_time") {
		t.Fatalf("sweep: %v; want incomplete, out of time", err)
	}
	f.requireDemoRulesOff(t)
	free()
	// Each orchestrator call ends once it gets past the lock; nothing waits on.
	waitFor(t, "the stuck cancels to return", func() bool { return returned.Load() == inCall.Load() })
}

// TestWorkerRetain_WhileSwitchOffPending: OnRunEnded runs before the worker
// waits for a failed switch-off, so the worker's Retain runs while the ended
// run's rule may still be on. It deletes the older runs beyond keep in the
// usual order and leaves the pending run alone: its experiment (the newest
// finished one) is kept and its rule is neither written nor deleted by the
// pass. Shutdown then switches it off.
func TestWorkerRetain_WhileSwitchOffPending(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(c *Config) { noCap(c); c.Retain = 1 })
	// The run's own first switch-off of its memory rule fails.
	x.rebuild(func(d *Deps) {
		d.Rules = newFlakyOff(x.f, func(name string, n int) bool { return strings.HasSuffix(name, "-memory") && n == 1 })
	})
	x.w.offRetry = backoff{first: time.Hour, max: time.Hour} // stays pending until Shutdown
	older := []seeded{seed(t, x.f, "completed", true), seed(t, x.f, "completed", false)}
	passes := make(chan error, 4)
	var pendingAtPass atomic.Bool
	x.w.OnRunEnded = func(ctx context.Context) {
		pendingAtPass.Store(x.w.SwitchOffPending())
		passes <- x.w.Retain(ctx)
	}
	x.f.mu.Lock()
	x.f.autopilot = true
	x.f.mu.Unlock()
	r, ch := x.enqueue("a")
	x.start()
	_, end := awaitEnd(t, ch)
	requireEnded(t, end, RunCompleted, Reason{})
	if err := <-passes; err != nil {
		t.Fatalf("the pass: %v", err)
	}
	if !pendingAtPass.Load() {
		t.Fatal("precondition: the switch-off was not pending during the pass")
	}
	rules, exp := x.f.rulesOfRun(t, r.RunID), x.expID(r.RunID)
	rule := rules[0]
	if rr := x.f.rule(rule); rr == nil || !rr.Enabled {
		t.Fatalf("precondition: the pending run's memory rule %+v; want still on", rr)
	}
	if rr := x.f.rule(rules[1]); rr == nil || rr.Enabled {
		t.Fatalf("precondition: the pending run's cpu rule %+v; want kept, off", rr)
	}
	if st := x.f.experimentStatus(exp); st != "completed" {
		t.Errorf("the pending run's experiment is %q; want completed, kept", st)
	}
	var writes []string
	for _, c := range callsFor(x.f, rule) {
		if strings.HasPrefix(c, "rules.SetEnabled ") || strings.HasPrefix(c, "rules.Delete ") {
			writes = append(writes, c)
		}
	}
	if !slices.Equal(writes, []string{"rules.SetEnabled " + rule + " false (refused)"}) {
		t.Errorf("writes to the pending run's memory rule %v; want only the worker's one failed switch-off", writes)
	}
	writes = nil
	for _, c := range callsFor(x.f, rules[1]) {
		if strings.HasPrefix(c, "rules.SetEnabled ") || strings.HasPrefix(c, "rules.Delete ") {
			writes = append(writes, c)
		}
	}
	if !slices.Equal(writes, []string{"rules.SetEnabled " + rules[1] + " false"}) {
		t.Errorf("writes to the pending run's cpu rule %v; want only the worker's one switch-off", writes)
	}
	for _, s := range older {
		requireRemovedInOrder(t, x.f, s)
	}
	if !x.w.SwitchOffPending() {
		t.Error("the switch-off is no longer pending")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := x.w.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	x.f.requireDemoRulesOff(t)
}

// TestSweep_FailedSwitchOffLoggedOnce: a failed switch-off is logged once per
// attempt, through the pass's logger, and not again through slog's default.
// Not parallel: it swaps slog's default logger.
func TestSweep_FailedSwitchOffLoggedOnce(t *testing.T) {
	def := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(def, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	f := newFakeLab(t, testNow)
	s := seed(t, f, "running", true)
	logs := &syncBuffer{}
	d := f.deps()
	d.Logger = slog.New(slog.NewTextHandler(logs, nil))
	d.Rules = failRuleOff{fakeRules{f}, s.RuleID}
	if err := Sweep(context.Background(), d, 20); !errors.Is(err, ErrRuleState) {
		t.Fatalf("Sweep: %v; want the rule-state failure", err)
	}
	attempts := len(f.callList("rules.SetEnabled " + s.RuleID + " false (refused)"))
	lines := 0
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, "level=ERROR") && strings.Contains(l, "rule_id="+s.RuleID) {
			lines++
		}
	}
	if attempts == 0 || lines != attempts {
		t.Errorf("%d failed attempts, %d Error lines naming the rule; want one line per attempt", attempts, lines)
	}

	// The worker's switch goes through the same unlogged write.
	sw := newRuleSwitch("demo-test", failRuleOff{fakeRules{f}, s.RuleID}, d.Logger, f.clock)
	sw.attach(context.Background(), s.RuleID)
	if err := sw.off(context.Background(), offCompleted); !errors.Is(err, ErrRuleState) {
		t.Fatalf("ruleSwitch.off: %v; want the failure", err)
	}
	if strings.Contains(def.String(), s.RuleID) {
		t.Errorf("a failed switch-off was also logged through slog.Default:\n%s", def)
	}
	if err := (fakeRules{f}).SetEnabled(context.Background(), s.RuleID, false); err != nil {
		t.Fatal(err)
	}
}

// ---- runs of scenarios, and a lab of both shapes ----

// pair is one run of a scenario seeded into a lab through BuildRun: its
// experiment, its two rules (memory, then CPU) and its phases.
type pair struct {
	runID        string
	ExperimentID string
	RuleIDs      [2]string
	PhaseIDs     [3]string
}

// seedPair writes a run of scenario sc in f through BuildRun, one second (of
// f's clock) after the previous one, switches both its rules on when on, and
// moves its experiment to status as seed does: planned (as built), running
// (its memory phase running, so an enabled memory rule is served), or
// completed, failed or cancelled (phases not yet ended are skipped).
func seedPair(t *testing.T, f *fakeLab, sc Scenario, status string, on bool) pair {
	t.Helper()
	ctx := context.Background()
	runID := id.New(RunIDPrefix)
	built, err := BuildRun(ctx, f.deps(), runID, sc, testRunCap)
	if err != nil {
		t.Fatalf("seed pair: build: %v", err)
	}
	p := pair{runID: runID, ExperimentID: built.ExperimentID, RuleIDs: built.RuleIDs, PhaseIDs: built.PhaseIDs}
	f.clock.Advance(time.Second)
	if on {
		for _, rule := range p.RuleIDs {
			if err := SetRuleEnabled(ctx, fakeRules{f}, rule, true); err != nil {
				t.Fatalf("seed pair: switch on: %v", err)
			}
		}
	}
	switch status {
	case "planned":
	case "running":
		f.setExperimentStatus(p.ExperimentID, "running")
		f.setPhaseStatus(p.PhaseIDs[0], "completed")
		f.setPhaseStatus(p.PhaseIDs[1], "running")
	case "completed", "failed", "cancelled":
		f.endExperiment(p.ExperimentID, status, testNow, testNow)
	default:
		t.Fatalf("seed pair: unknown status %q", status)
	}
	return p
}

// dropRule deletes rule i of the pair (0 memory, 1 CPU) as an operator would:
// switched off, unlinked from its phase, then deleted.
func dropRule(t *testing.T, f *fakeLab, p pair, i int) {
	t.Helper()
	ctx := context.Background()
	if err := (fakeRules{f}).SetEnabled(ctx, p.RuleIDs[i], false); err != nil {
		t.Fatalf("switch off rule %s: %v", p.RuleIDs[i], err)
	}
	if err := (fakeExperiments{f}).AttachPhaseRules(ctx, p.PhaseIDs[i+1], nil); err != nil {
		t.Fatalf("unlink rule %s: %v", p.RuleIDs[i], err)
	}
	if err := (fakeRules{f}).Delete(ctx, p.RuleIDs[i]); err != nil {
		t.Fatalf("delete rule %s: %v", p.RuleIDs[i], err)
	}
}

// requirePairRemovedInOrder checks that a pair's experiment and rules are gone,
// and that every rule it still had (all but those numbered in gone) was
// switched off before the experiment was deleted, every phase's links read
// between the last of those switch-offs and the delete, and the experiment
// deleted before each rule.
func requirePairRemovedInOrder(t *testing.T, f *fakeLab, p pair, gone ...int) {
	t.Helper()
	if f.hasExperiment(p.ExperimentID) || f.hasRule(p.RuleIDs[0]) || f.hasRule(p.RuleIDs[1]) {
		t.Errorf("run %s: experiment kept %v, rules kept %v %v; want all gone", p.runID,
			f.hasExperiment(p.ExperimentID), f.hasRule(p.RuleIDs[0]), f.hasRule(p.RuleIDs[1]))
		return
	}
	calls := f.callList("")
	delExp := slices.Index(calls, "experiments.Delete "+p.ExperimentID)
	if delExp < 0 {
		t.Errorf("run %s: its experiment was never deleted", p.runID)
		return
	}
	lastOff := -1
	for i, rule := range p.RuleIDs {
		if slices.Contains(gone, i) {
			continue
		}
		off := slices.Index(calls, "rules.SetEnabled "+rule+" false")
		del := slices.Index(calls, "rules.Delete "+rule)
		if off < 0 || del < 0 || !(off < delExp && delExp < del) {
			t.Errorf("run %s, rule %s: switch-off at %d, experiment delete at %d, rule delete at %d; want off < experiment < rule",
				p.runID, rule, off, delExp, del)
		}
		lastOff = max(lastOff, off)
	}
	for _, pid := range p.PhaseIDs {
		if at := slices.Index(calls, "experiments.ListPhaseRules "+pid); at < lastOff || at > delExp {
			t.Errorf("run %s: phase %s's links read at %d; want between the last switch-off (%d) and the delete (%d)",
				p.runID, pid, at, lastOff, delExp)
		}
	}
}

// requirePairKept checks that a pair's experiment and both rules are still
// there, and that nothing deleted or cancelled any of them.
func requirePairKept(t *testing.T, f *fakeLab, p pair) {
	t.Helper()
	if !f.hasExperiment(p.ExperimentID) || !f.hasRule(p.RuleIDs[0]) || !f.hasRule(p.RuleIDs[1]) {
		t.Errorf("run %s: experiment kept %v, rules kept %v %v; want all kept", p.runID,
			f.hasExperiment(p.ExperimentID), f.hasRule(p.RuleIDs[0]), f.hasRule(p.RuleIDs[1]))
	}
	for _, rowID := range []string{p.ExperimentID, p.RuleIDs[0], p.RuleIDs[1]} {
		for _, c := range callsFor(f, rowID) {
			if strings.HasPrefix(c, "experiments.Delete") || strings.HasPrefix(c, "rules.Delete") ||
				strings.HasPrefix(c, "orch.CancelExperiment") {
				t.Errorf("run %s: unexpected %q", p.runID, c)
			}
		}
	}
}

// scenario returns the catalog's scenario id, failing t if there is none.
func scenario(t *testing.T, id string) Scenario {
	t.Helper()
	sc, ok := ScenarioByID(id)
	if !ok {
		t.Fatalf("no scenario %q in the catalog", id)
	}
	return sc
}

// TestRetain_ScenarioRuns: retention over runs built through BuildRun, two
// rules each. A finished run beyond keep goes as a set: both rules off, then
// the experiment, then each rule; a pair with one rule already gone goes the
// same way, the remaining rule alone; when one rule's switch-off fails, the
// run is kept (its experiment not even tried), the other rule is off, and
// the next pass removes it.
func TestRetain_ScenarioRuns(t *testing.T) {
	t.Parallel()

	t.Run("a pair", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		old := seedPair(t, f, scenario(t, "cart-heavy"), "completed", true)
		older := seedPair(t, f, scenario(t, "catalog-moderate"), "failed", false)
		newer := seedPair(t, f, scenario(t, "catalog-heavy"), "cancelled", false)
		if err := Retain(context.Background(), f.deps(), 1); err != nil {
			t.Fatalf("Retain: %v", err)
		}
		requirePairRemovedInOrder(t, f, old)
		requirePairRemovedInOrder(t, f, older)
		requirePairKept(t, f, newer)
		f.requireDemoRulesOff(t)
	})

	t.Run("one rule of the pair already gone", func(t *testing.T) {
		t.Parallel()
		for i, kind := range []FaultKind{FaultMemory, FaultCPU} {
			t.Run(string(kind), func(t *testing.T) {
				t.Parallel()
				f := newFakeLab(t, testNow)
				old := seedPair(t, f, DefaultScenario(), "completed", true)
				dropRule(t, f, old, i)
				newer := seedPair(t, f, DefaultScenario(), "completed", false)
				if err := Retain(context.Background(), f.deps(), 1); err != nil {
					t.Fatalf("Retain: %v", err)
				}
				requirePairRemovedInOrder(t, f, old, i)
				requirePairKept(t, f, newer)
				f.requireDemoRulesOff(t)
			})
		}
	})

	t.Run("one rule's switch-off fails", func(t *testing.T) {
		t.Parallel()
		for i, kind := range []FaultKind{FaultMemory, FaultCPU} {
			t.Run(string(kind), func(t *testing.T) {
				t.Parallel()
				f := newFakeLab(t, testNow)
				old := seedPair(t, f, DefaultScenario(), "completed", true)
				newer := seedPair(t, f, DefaultScenario(), "completed", false)
				d := f.deps()
				d.Rules = failRuleOff{fakeRules{f}, old.RuleIDs[i]}
				err := Retain(context.Background(), d, 1)
				if !errors.Is(err, ErrRetainIncomplete) || !errors.Is(err, ErrRuleState) {
					t.Fatalf("Retain: %v; want incomplete with a rule-state failure", err)
				}
				requirePairKept(t, f, old)
				if n := len(f.callList("experiments.Delete")); n != 0 {
					t.Errorf("%d experiment deletes; want none while a rule of the run could not be switched off", n)
				}
				other := old.RuleIDs[1-i]
				if r := f.rule(other); r == nil || r.Enabled {
					t.Errorf("the other rule %s %+v; want switched off although this one's write failed", other, r)
				}
				if r := f.rule(old.RuleIDs[i]); r == nil || !r.Enabled {
					t.Fatalf("precondition of the rest: rule %+v; want still on", r)
				}
				if err := Retain(context.Background(), f.deps(), 1); err != nil {
					t.Fatalf("Retain once the switch works: %v", err)
				}
				requirePairRemovedInOrder(t, f, old)
				requirePairKept(t, f, newer)
				f.requireDemoRulesOff(t)
			})
		}
	})
}

// TestTidy_MixedLab: a lab holding runs of build 2's shape (one rule,
// demo-run-<run id>) beside runs of scenarios (two rules each), some of each
// left on. The start-up switch-off switches every enabled one off and does
// nothing else; the sweep switches them all off before its first cancel,
// cancels both kinds, and removes both kinds' orphaned rules; retention
// removes the oldest of both kinds, each as its own set and in order, and
// keeps the newest.
func TestTidy_MixedLab(t *testing.T) {
	t.Parallel()

	t.Run("start-up switch-off", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		olds := []seeded{seed(t, f, "running", true), seed(t, f, "completed", true), seed(t, f, "planned", false)}
		pairs := []pair{seedPair(t, f, DefaultScenario(), "running", true), seedPair(t, f, scenario(t, "cart-moderate"), "planned", true),
			seedPair(t, f, scenario(t, "cart-heavy"), "completed", false)}
		addRule(t, f, "rule-smoke", "smoke-x", true)
		mark := len(f.callList(""))
		if err := SwitchOffAll(context.Background(), f.deps()); err != nil {
			t.Fatalf("SwitchOffAll: %v", err)
		}
		f.requireDemoRulesOff(t)
		for _, c := range f.callList("")[mark:] {
			if c != "rules.List " && !strings.HasPrefix(c, "rules.SetEnabled ") || strings.HasSuffix(c, " true") {
				t.Errorf("SwitchOffAll made %q; want only the rule list and switch-offs", c)
			}
		}
		want := []string{olds[0].RuleID, olds[1].RuleID, pairs[0].RuleIDs[0], pairs[0].RuleIDs[1], pairs[1].RuleIDs[0], pairs[1].RuleIDs[1]}
		for _, rule := range want {
			if n := len(f.callList("rules.SetEnabled " + rule + " false")); n != 1 {
				t.Errorf("%d switch-offs of %s; want 1", n, rule)
			}
		}
		if n := len(f.callList("rules.SetEnabled ")); n != len(want) {
			t.Errorf("%d switch-offs; want one per enabled demo rule (%d)", n, len(want))
		}
		if r := f.rule("rule-smoke"); r == nil || !r.Enabled {
			t.Error("the operator's rule was switched off")
		}
		for _, s := range olds {
			requireKept(t, f, s)
		}
		for _, p := range pairs {
			requirePairKept(t, f, p)
		}
	})

	t.Run("sweep", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		olds := []seeded{seed(t, f, "running", true), seed(t, f, "planned", true)}
		pairs := []pair{seedPair(t, f, DefaultScenario(), "running", true), seedPair(t, f, scenario(t, "cart-heavy"), "planned", true)}
		// Rules a build wrote before it died, its experiment never written.
		lone := id.New(RunIDPrefix)
		addRule(t, f, "rule-lone-memory", RuleName(lone, FaultMemory), false)
		addRule(t, f, "rule-lone-cpu", RuleName(lone, FaultCPU), false)
		addRule(t, f, "rule-lone-old", RunNamePrefix+id.New(RunIDPrefix), false)
		onAtCancel := offBeforeCancel(f)
		if err := Sweep(context.Background(), f.deps(), 20); err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if n := onAtCancel.Load(); n != 0 {
			t.Errorf("%d demo rules were on at a cancel", n)
		}
		calls := f.callList("")
		firstCancel := indexOfPrefix(calls, "orch.CancelExperiment ")
		var exps, rules []string
		for _, s := range olds {
			exps, rules = append(exps, s.ExperimentID), append(rules, s.RuleID)
		}
		for _, p := range pairs {
			exps, rules = append(exps, p.ExperimentID), append(rules, p.RuleIDs[:]...)
		}
		for _, rule := range rules {
			if off := slices.Index(calls, "rules.SetEnabled "+rule+" false"); off < 0 || off > firstCancel {
				t.Errorf("rule %s switched off at %d, first cancel at %d; want the switch-off first", rule, off, firstCancel)
			}
		}
		for _, e := range exps {
			if st := f.experimentStatus(e); st != "cancelled" {
				t.Errorf("experiment %s: %s; want cancelled", e, st)
			}
		}
		for _, rule := range []string{"rule-lone-memory", "rule-lone-cpu", "rule-lone-old"} {
			if f.hasRule(rule) {
				t.Errorf("the orphaned rule %s was kept", rule)
			}
		}
		f.requireDemoRulesOff(t)
	})

	t.Run("sweep, a rule gone and a switch-off failing", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		gone := seedPair(t, f, DefaultScenario(), "running", true)
		dropRule(t, f, gone, 1)
		failing := seedPair(t, f, scenario(t, "cart-moderate"), "running", true)
		old := seed(t, f, "running", true)
		d := f.deps()
		d.Rules = failRuleOff{fakeRules{f}, failing.RuleIDs[0]}
		err := Sweep(context.Background(), d, 20)
		if !errors.Is(err, ErrSweepIncomplete) || !errors.Is(err, ErrRuleState) {
			t.Fatalf("Sweep: %v; want incomplete with a rule-state failure", err)
		}
		for _, rule := range []string{gone.RuleIDs[0], failing.RuleIDs[1], old.RuleID} {
			if r := f.rule(rule); r == nil || r.Enabled {
				t.Errorf("rule %s %+v; want switched off", rule, r)
			}
		}
		for _, e := range []string{gone.ExperimentID, failing.ExperimentID, old.ExperimentID} {
			if st := f.experimentStatus(e); st != "cancelled" {
				t.Errorf("experiment %s: %s; want cancelled", e, st)
			}
		}
		if f.served(failing.RuleIDs[0]) {
			t.Error("the rule that could not be switched off is served after its experiment was cancelled")
		}
		// It stays on under a cancelled experiment: the next start's sweep
		// switches it off.
		if err := Sweep(context.Background(), f.deps(), 20); err != nil {
			t.Fatalf("the next sweep: %v", err)
		}
		f.requireDemoRulesOff(t)
	})

	t.Run("retention", func(t *testing.T) {
		t.Parallel()
		f := newFakeLab(t, testNow)
		// Oldest first, the two kinds alternating; some left on.
		o1 := seed(t, f, "completed", true)
		p1 := seedPair(t, f, scenario(t, "cart-moderate"), "failed", true)
		o2 := seed(t, f, "cancelled", false)
		p2 := seedPair(t, f, DefaultScenario(), "completed", false)
		dropRule(t, f, p2, 1)
		o3 := seed(t, f, "completed", false)
		p3 := seedPair(t, f, scenario(t, "catalog-heavy"), "completed", true)
		running := seedPair(t, f, DefaultScenario(), "running", true) // never counted, never touched
		if err := Retain(context.Background(), f.deps(), 2); err != nil {
			t.Fatalf("Retain: %v", err)
		}
		requireRemovedInOrder(t, f, o1)
		requirePairRemovedInOrder(t, f, p1)
		requireRemovedInOrder(t, f, o2)
		requirePairRemovedInOrder(t, f, p2, 1)
		requireKept(t, f, o3)
		requirePairKept(t, f, p3)
		requirePairKept(t, f, running)
		if !f.served(running.RuleIDs[0]) {
			t.Error("the running run's memory rule was switched off")
		}
		before := len(f.callList("experiments.Delete")) + len(f.callList("rules.Delete"))
		if err := Retain(context.Background(), f.deps(), 2); err != nil {
			t.Fatalf("second Retain: %v", err)
		}
		if after := len(f.callList("experiments.Delete")) + len(f.callList("rules.Delete")); after != before {
			t.Errorf("a second pass deleted %d more rows", after-before)
		}
		// Switch the kept runs' rules off so the lab ends clean.
		for _, rule := range append(running.RuleIDs[:], p3.RuleIDs[:]...) {
			if err := SetRuleEnabled(context.Background(), fakeRules{f}, rule, false); err != nil {
				t.Fatal(err)
			}
		}
	})
}
