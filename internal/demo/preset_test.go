package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

var testNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

const testRunCap = 5 * time.Minute

// onlyRule returns the single rule the lab holds.
func onlyRule(t *testing.T, f *fakeLab) *model.Rule {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.rules) != 1 {
		t.Fatalf("lab holds %d rules, want 1", len(f.rules))
	}
	for _, r := range f.rules {
		cp := *r
		return &cp
	}
	return nil
}

func TestBuildRun_Shape(t *testing.T) {
	f := newFakeLab(testNow)
	const runID = "demo-0001"

	built, err := BuildRun(context.Background(), f.deps(), runID, testRunCap)
	if err != nil {
		t.Fatalf("BuildRun: %v", err)
	}

	rule := onlyRule(t, f)
	if rule.Name != "demo-run-"+runID || rule.Service != "productcatalogservice" || rule.Enabled ||
		rule.Priority != 100 || rule.Mode != "inline" || rule.Action.Type != "fault_spec" {
		t.Errorf("rule = %+v; want demo-run-%s on productcatalogservice, disabled, priority 100, inline, fault_spec", rule, runID)
	}
	if len(rule.Match.Labels) != 0 || rule.Match.InjectionPoint != "" {
		t.Errorf("rule match = %+v; want empty (every request)", rule.Match)
	}
	if len(f.specs) != 1 || rule.Action.FaultSpecID != f.specs[0].ID {
		t.Errorf("rule spec = %q, lab specs %d; want the one demo spec", rule.Action.FaultSpecID, len(f.specs))
	}
	if built.RuleID != rule.ID {
		t.Errorf("Built.RuleID = %q, want %q", built.RuleID, rule.ID)
	}

	exp, ok := f.experiments[built.ExperimentID]
	if !ok || len(f.experiments) != 1 {
		t.Fatalf("experiment %q not stored (lab holds %d)", built.ExperimentID, len(f.experiments))
	}
	if exp.Name != "demo-run-"+runID || exp.CreatedBy != "manteion-demo" || exp.Status != "planned" {
		t.Errorf("experiment = %+v; want demo-run-%s by manteion-demo, planned", exp, runID)
	}
	if !exp.CreatedAt.Equal(testNow) {
		t.Errorf("experiment created_at = %v, want the clock's %v", exp.CreatedAt, testNow)
	}

	phases := f.phasesOf(built.ExperimentID)
	wantNames := []string{"plain", "catalog-latency-50ms", "plain-end"}
	if len(phases) != 3 {
		t.Fatalf("experiment has %d phases, want 3", len(phases))
	}
	for i, p := range phases {
		if p.Name != wantNames[i] || p.Position != i || p.Status != "pending" || len(p.FrozenServices) != 0 || p.PersistCache {
			t.Errorf("phase %d = %+v; want %s at %d, pending, nothing frozen", i, p, wantNames[i], i)
		}
		if built.PhaseIDs[i] != p.ID {
			t.Errorf("Built.PhaseIDs[%d] = %q, want %q", i, built.PhaseIDs[i], p.ID)
		}
		wantPW := model.PhaseWorkflow{PhaseID: p.ID, WorkflowID: "demo-browse", VUs: 30, RateRPS: 3, DurationSec: 40, DatasetID: "ds-ok"}
		if got := f.phaseWFs[p.ID]; len(got) != 1 || got[0] != wantPW {
			t.Errorf("phase %d workflows = %+v; want [%+v]", i, got, wantPW)
		}
	}

	attach := f.callList("experiments.AttachPhaseRules")
	if want := []string{"experiments.AttachPhaseRules " + phases[1].ID + " " + rule.ID}; !slices.Equal(attach, want) {
		t.Errorf("AttachPhaseRules calls = %v; want only %v", attach, want)
	}
	if len(f.phaseRules) != 1 || !slices.Equal(f.phaseRules[phases[1].ID], []string{rule.ID}) {
		t.Errorf("phase rule links = %v; want only phase 1 -> %s", f.phaseRules, rule.ID)
	}
	if upd := f.callList("rules.Update"); len(upd) != 0 || f.enabledEver[rule.ID] {
		t.Errorf("the builder touched the rule's enabled flag: updates %v, enabled ever %v", upd, f.enabledEver[rule.ID])
	}
}

func TestBuildRun_ReusesSpec(t *testing.T) {
	f := newFakeLab(testNow)
	d := f.deps()

	first, err := BuildRun(context.Background(), d, "demo-a", testRunCap)
	if err != nil {
		t.Fatalf("first build: %v", err)
	}
	second, err := BuildRun(context.Background(), d, "demo-b", testRunCap)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}

	if got := f.callList("specs.CreateSpec"); len(got) != 1 {
		t.Fatalf("CreateSpec calls = %v; want exactly one over two builds", got)
	}
	spec := f.specs[0]
	if spec.Name != "demo-catalog-latency-50ms" || spec.Category != "inline" || spec.FaultType != "latency" || spec.DurationMs != 60000 {
		t.Errorf("spec = %+v; want demo-catalog-latency-50ms, inline latency, duration_ms 60000", spec)
	}
	var params map[string]any
	if err := json.Unmarshal(spec.Params, &params); err != nil || len(params) != 1 || params["delay"] != "50ms" {
		t.Errorf("spec params = %s (%v); want {\"delay\":\"50ms\"}", spec.Params, err)
	}
	if a, b := f.rules[first.RuleID], f.rules[second.RuleID]; a.Action.FaultSpecID != spec.ID || b.Action.FaultSpecID != spec.ID {
		t.Errorf("rules use specs %q and %q; want both %q", a.Action.FaultSpecID, b.Action.FaultSpecID, spec.ID)
	}
	if first.RuleID == second.RuleID || first.ExperimentID == second.ExperimentID {
		t.Errorf("two builds share ids: %+v and %+v", first, second)
	}
}

// TestBuildRun_IgnoresAlteredSpec: a spec that carries the demo's name but no
// longer says +50 ms latency is never reused; the builder makes a correct one.
func TestBuildRun_IgnoresAlteredSpec(t *testing.T) {
	f := newFakeLab(testNow)
	altered := &model.FaultSpec{ID: "spec-altered", Name: SpecName, Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{"delay":"500ms"}`), DurationMs: 60000, CreatedAt: testNow.Add(-time.Hour)}
	f.specs = append(f.specs, altered)

	built, err := BuildRun(context.Background(), f.deps(), "demo-x", testRunCap)
	if err != nil {
		t.Fatalf("BuildRun: %v", err)
	}
	if got := f.rules[built.RuleID].Action.FaultSpecID; got == altered.ID {
		t.Fatalf("rule uses the altered spec %q", got)
	}
	if len(f.callList("specs.CreateSpec")) != 1 {
		t.Errorf("CreateSpec calls = %v; want one fresh spec", f.callList("specs.CreateSpec"))
	}

	// A correct spec that already exists is reused as it is.
	g := newFakeLab(testNow)
	good := &model.FaultSpec{ID: "spec-good", Name: SpecName, Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{ "delay": "50ms" }`), DurationMs: 60000, CreatedAt: testNow.Add(-time.Hour)}
	g.specs = append(g.specs, good)
	built, err = BuildRun(context.Background(), g.deps(), "demo-y", testRunCap)
	if err != nil {
		t.Fatalf("BuildRun with a stored spec: %v", err)
	}
	if got := g.rules[built.RuleID].Action.FaultSpecID; got != good.ID || len(g.callList("specs.CreateSpec")) != 0 {
		t.Errorf("rule spec = %q, CreateSpec calls %v; want the stored %q and no create", got, g.callList("specs.CreateSpec"), good.ID)
	}
}

func TestBuildRun_PicksUsableDataset(t *testing.T) {
	runCap := testRunCap
	edge := testNow.Add(runCap + time.Minute) // must expire strictly after this
	ds := func(id string, size int64, created time.Time, ttl int) zeus.Dataset {
		return zeus.Dataset{ID: id, Name: DatasetName, SizeBytes: size, TTLS: ttl, CreatedAt: created}
	}

	cases := []struct {
		name     string
		datasets []zeus.Dataset
		want     string // "" means ErrNoDataset
	}{
		{"brief: empty, expiring in 1 min, never expiring", []zeus.Dataset{
			ds("ds-empty", 0, testNow, 0),
			ds("ds-soon", 10, testNow, 60),
			ds("ds-forever", 10, testNow, 0),
		}, "ds-forever"},
		{"other names never count", []zeus.Dataset{
			{ID: "ds-other", Name: "other-products", SizeBytes: 10, CreatedAt: testNow},
		}, ""},
		{"none at all", nil, ""},
		{"expires exactly at now+cap+1m is not enough", []zeus.Dataset{
			ds("ds-edge", 10, edge.Add(-time.Hour), 3600),
		}, ""},
		{"expires one second after now+cap+1m", []zeus.Dataset{
			ds("ds-edge1", 10, edge.Add(-time.Hour), 3601),
		}, "ds-edge1"},
		{"negative ttl never expires", []zeus.Dataset{
			ds("ds-neg", 10, testNow.Add(-48*time.Hour), -1),
		}, "ds-neg"},
		{"several usable: the newest", []zeus.Dataset{
			ds("ds-old", 10, testNow.Add(-2*time.Hour), 0),
			ds("ds-new", 10, testNow.Add(-time.Hour), 86400),
			ds("ds-mid", 10, testNow.Add(-90*time.Minute), 0),
		}, "ds-new"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeLab(testNow)
			f.datasets = tc.datasets
			built, err := BuildRun(context.Background(), f.deps(), "demo-ds", runCap)
			if tc.want == "" {
				if !errors.Is(err, ErrNoDataset) {
					t.Fatalf("err = %v; want ErrNoDataset", err)
				}
				if writes := writeCalls(f); len(writes) != 0 {
					t.Errorf("writes before the dataset check failed: %v", writes)
				}
				return
			}
			if err != nil {
				t.Fatalf("BuildRun: %v", err)
			}
			for _, pid := range built.PhaseIDs {
				if got := f.phaseWFs[pid][0].DatasetID; got != tc.want {
					t.Errorf("phase %s dataset = %q, want %q", pid, got, tc.want)
				}
			}
		})
	}
}

// writeCalls lists every recorded call that writes.
func writeCalls(f *fakeLab) []string {
	var out []string
	for _, c := range f.callList("") {
		op := strings.SplitN(c, " ", 2)[0]
		switch {
		case strings.HasSuffix(op, ".Create"), strings.HasSuffix(op, ".CreatePhase"), strings.HasSuffix(op, ".CreateSpec"),
			strings.HasPrefix(op, "experiments.Attach"), strings.HasSuffix(op, ".Update"), strings.HasSuffix(op, ".Delete"):
			out = append(out, c)
		}
	}
	return out
}

// TestBuildRun_LoadPreconditions: no workflow or an unreachable zeus is "no
// dataset" (the load generator is not ready); a database error reading the
// workflow is a build error. None of them writes anything.
func TestBuildRun_LoadPreconditions(t *testing.T) {
	cases := []struct {
		name  string
		setup func(f *fakeLab)
		want  error
	}{
		{"workflow demo-browse missing", func(f *fakeLab) { delete(f.workflows, WorkflowID) }, ErrNoDataset},
		{"zeus unreachable", func(f *fakeLab) { f.fail("zeus.ListDatasets", errLeaky) }, ErrNoDataset},
		{"workflow read fails", func(f *fakeLab) { f.fail("workflows.Get", errLeaky) }, ErrBuild},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeLab(testNow)
			tc.setup(f)
			_, err := BuildRun(context.Background(), f.deps(), "demo-pre", testRunCap)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v; want %v", err, tc.want)
			}
			if writes := writeCalls(f); len(writes) != 0 {
				t.Errorf("writes: %v; want none", writes)
			}
			assertNoLeak(t, err.Error())
		})
	}
}

func TestBuildRun_FailureCleansUp(t *testing.T) {
	f := newFakeLab(testNow)
	f.fail("experiments.AttachPhaseRules", errLeaky)

	_, err := BuildRun(context.Background(), f.deps(), "demo-fail", testRunCap)
	if !errors.Is(err, ErrBuild) {
		t.Fatalf("err = %v; want ErrBuild", err)
	}

	var expID, ruleID string
	created := f.callList("rules.Create")
	if len(created) != 1 {
		t.Fatalf("rules.Create calls = %v; want 1", created)
	}
	deletes := append(f.callList("experiments.Delete"), f.callList("rules.Delete")...)
	if len(deletes) != 2 {
		t.Fatalf("delete calls = %v; want one experiment and one rule delete", deletes)
	}
	expID = strings.TrimPrefix(deletes[0], "experiments.Delete ")
	ruleID = strings.TrimPrefix(deletes[1], "rules.Delete ")
	order := indexOf(f.callList(""), "experiments.Delete "+expID) < indexOf(f.callList(""), "rules.Delete "+ruleID)
	if !order {
		t.Errorf("calls = %v; want experiments.Delete before rules.Delete", f.callList(""))
	}
	if !strings.HasPrefix(expID, "exp-") || !strings.HasPrefix(ruleID, "rule-") {
		t.Errorf("deleted %q and %q; want the run's experiment and rule", expID, ruleID)
	}
	if upd := f.callList("rules.Update"); len(upd) != 0 || f.enabledEver[ruleID] {
		t.Errorf("rule enabled during a failed build: updates %v", upd)
	}
	if left := f.leftovers(); len(left) != 0 {
		t.Errorf("left behind: %v", left)
	}
	assertNoLeak(t, err.Error())
}

func indexOf(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
}

// TestBuildRun_FailureAtEveryStepLeavesNothing injects a failure at each write
// of the build in turn: whatever step fails, no experiment, phase, link or
// rule is left behind, the rule is never enabled, and the error is ErrBuild
// with none of the cause's text.
func TestBuildRun_FailureAtEveryStepLeavesNothing(t *testing.T) {
	steps := []string{
		"specs.ListSpecs",
		"specs.CreateSpec",
		"rules.Create",
		"experiments.Create",
		"experiments.CreatePhase#1",
		"experiments.AttachPhaseWorkflows#1",
		"experiments.CreatePhase#2",
		"experiments.AttachPhaseWorkflows#2",
		"experiments.AttachPhaseRules",
		"experiments.CreatePhase#3",
		"experiments.AttachPhaseWorkflows#3",
	}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			f := newFakeLab(testNow)
			f.fail(step, errLeaky)
			_, err := BuildRun(context.Background(), f.deps(), "demo-step", testRunCap)
			if !errors.Is(err, ErrBuild) || errors.Is(err, ErrNoDataset) {
				t.Fatalf("err = %v; want ErrBuild only", err)
			}
			if !errors.Is(err, errLeaky) {
				t.Errorf("err = %v; want the cause reachable through errors.Is", err)
			}
			if left := f.leftovers(); len(left) != 0 {
				t.Errorf("left behind: %v (calls %v)", left, f.callList(""))
			}
			if upd := f.callList("rules.Update"); len(upd) != 0 {
				t.Errorf("rule updated during a failed build: %v", upd)
			}
			for id, on := range f.enabledEver {
				if on {
					t.Errorf("rule %s was enabled", id)
				}
			}
			assertNoLeak(t, err.Error())
		})
	}
}

// TestBuildRun_CleanupOutlivesCancelledContext: the build's context is
// cancelled mid-way (a shutdown, say); the cleanup still runs to completion.
func TestBuildRun_CleanupOutlivesCancelledContext(t *testing.T) {
	f := newFakeLab(testNow)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.fail("experiments.AttachPhaseWorkflows#3", context.Canceled)
	f.onCall("experiments.AttachPhaseWorkflows#3", cancel)

	_, err := BuildRun(ctx, f.deps(), "demo-cancel", testRunCap)
	if !errors.Is(err, ErrBuild) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want ErrBuild wrapping context.Canceled", err)
	}
	if left := f.leftovers(); len(left) != 0 {
		t.Errorf("left behind after a cancelled build: %v", left)
	}
}

// TestBuildRun_CleanupFailureIsLogged: when the cleanup itself fails, the build
// still returns ErrBuild, the rule stays disabled, and the log names what was
// left behind by id without the cause's text.
func TestBuildRun_CleanupFailureIsLogged(t *testing.T) {
	f := newFakeLab(testNow)
	f.fail("experiments.AttachPhaseWorkflows#3", errLeaky)
	f.fail("experiments.Delete", errLeaky)
	var buf bytes.Buffer
	d := f.deps()
	d.Logger = slog.New(slog.NewTextHandler(&buf, nil))

	_, err := BuildRun(context.Background(), d, "demo-stuck", testRunCap)
	if !errors.Is(err, ErrBuild) {
		t.Fatalf("err = %v; want ErrBuild", err)
	}
	rule := onlyRule(t, f)
	if rule.Enabled || f.enabledEver[rule.ID] {
		t.Errorf("rule left enabled: %+v", rule)
	}
	var expID string
	for id := range f.experiments {
		expID = id
	}
	logs := buf.String()
	if !strings.Contains(logs, expID) || !strings.Contains(logs, rule.ID) {
		t.Errorf("log does not name the experiment %q and rule %q left behind:\n%s", expID, rule.ID, logs)
	}
	for _, bad := range []string{"pq:", "192.0.2.10"} {
		if strings.Contains(logs, bad) {
			t.Errorf("log carries cause text %q:\n%s", bad, logs)
		}
	}
}

func TestBuildRun_RefusesBadInput(t *testing.T) {
	f := newFakeLab(testNow)
	if _, err := BuildRun(context.Background(), f.deps(), "", testRunCap); !errors.Is(err, ErrBuild) {
		t.Errorf("empty run id: err = %v; want ErrBuild", err)
	}
	d := f.deps()
	d.Clock = nil
	if _, err := BuildRun(context.Background(), d, "demo-nil", testRunCap); !errors.Is(err, ErrBuild) {
		t.Errorf("missing clock: err = %v; want ErrBuild", err)
	}
	if writes := writeCalls(f); len(writes) != 0 {
		t.Errorf("writes: %v; want none", writes)
	}
}

// assertNoLeak fails when s carries anything the public contract forbids in
// a body: the injected cause's text or an internal id.
func assertNoLeak(t *testing.T, s string) {
	t.Helper()
	for _, bad := range []string{"pq:", "192.0.2.10", "exp-", "phase-", "rule-", "spec-"} {
		if strings.Contains(s, bad) {
			t.Errorf("%q carries %q", s, bad)
		}
	}
}

func TestSetRuleEnabled(t *testing.T) {
	newLabWithRule := func(t *testing.T) (*fakeLab, *model.Rule) {
		t.Helper()
		f := newFakeLab(testNow)
		built, err := BuildRun(context.Background(), f.deps(), "demo-sw", testRunCap)
		if err != nil {
			t.Fatalf("BuildRun: %v", err)
		}
		return f, f.rules[built.RuleID]
	}

	t.Run("on then off, everything else kept", func(t *testing.T) {
		f, before := newLabWithRule(t)
		r := fakeRules{f}
		if err := SetRuleEnabled(context.Background(), r, before.ID, true); err != nil {
			t.Fatalf("enable: %v", err)
		}
		after := *f.rules[before.ID]
		if !after.Enabled {
			t.Fatalf("rule not enabled")
		}
		after.Enabled = false
		if after.Name != before.Name || after.Service != before.Service || after.Priority != before.Priority ||
			after.Mode != before.Mode || after.Action != before.Action {
			t.Errorf("enable changed more than the flag: %+v -> %+v", before, after)
		}
		if err := SetRuleEnabled(context.Background(), r, before.ID, false); err != nil {
			t.Fatalf("disable: %v", err)
		}
		if f.rules[before.ID].Enabled {
			t.Errorf("rule still enabled")
		}
		if got := f.callList("rules.Update"); len(got) != 2 {
			t.Errorf("updates = %v; want 2", got)
		}
	})

	t.Run("already in the wanted state writes nothing", func(t *testing.T) {
		f, rule := newLabWithRule(t)
		if err := SetRuleEnabled(context.Background(), fakeRules{f}, rule.ID, false); err != nil {
			t.Fatalf("disable a disabled rule: %v", err)
		}
		if got := f.callList("rules.Update"); len(got) != 0 {
			t.Errorf("updates = %v; want none", got)
		}
	})

	t.Run("never enables a rule that is not a demo run's", func(t *testing.T) {
		f, rule := newLabWithRule(t)
		operator := *rule
		operator.ID, operator.Name = "rule-operator", "smoke-x"
		f.rules[operator.ID] = &operator
		err := SetRuleEnabled(context.Background(), fakeRules{f}, operator.ID, true)
		if !errors.Is(err, ErrNotDemoRule) {
			t.Errorf("enable an operator rule: err = %v; want ErrNotDemoRule", err)
		}
		if got := f.callList("rules.Update"); len(got) != 0 || f.rules[operator.ID].Enabled {
			t.Errorf("updates = %v; want none", got)
		}

		// Disabling is never refused: it can only stop a fault.
		f.rules[operator.ID].Enabled = true
		if err := SetRuleEnabled(context.Background(), fakeRules{f}, operator.ID, false); err != nil {
			t.Errorf("disable an enabled operator rule: %v", err)
		}
		if f.rules[operator.ID].Enabled {
			t.Errorf("operator rule still enabled")
		}
	})

	t.Run("errors keep their class and drop their text", func(t *testing.T) {
		f, rule := newLabWithRule(t)
		err := SetRuleEnabled(context.Background(), fakeRules{f}, "rule-missing", true)
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("missing rule: err = %v; want store.ErrNotFound", err)
		}
		assertNoLeak(t, err.Error())

		f.fail("rules.Update", errLeaky)
		err = SetRuleEnabled(context.Background(), fakeRules{f}, rule.ID, true)
		if err == nil || !errors.Is(err, errLeaky) {
			t.Fatalf("update failure: err = %v; want the cause wrapped", err)
		}
		assertNoLeak(t, err.Error())
		if f.rules[rule.ID].Enabled {
			t.Errorf("rule enabled although the update failed")
		}
	})
}
