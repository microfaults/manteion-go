package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

var testNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

const testRunCap = 5 * time.Minute

// switchCalls lists every call that switches a rule's enabled flag.
func switchCalls(f *fakeLab) []string {
	return append(f.callList("rules.SetEnabled"), f.callList("rules.EnableIfAttached")...)
}

// specByID returns a copy of the lab's fault spec id, or nil.
func specByID(f *fakeLab, id string) *model.FaultSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.specs {
		if s.ID == id {
			cp := *s
			return &cp
		}
	}
	return nil
}

// decodeParams decodes a spec's params as a JSON object, nil when they are not
// one.
func decodeParams(raw json.RawMessage) map[string]any {
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil
	}
	return params
}

// TestBuildRun_Scenario: for every catalog scenario, the run's phases are
// baseline, memory and cpu at 40 s, each running the scenario's workflow at 30
// VUs and 3 rps; its two rules, demo-run-<id>-memory and demo-run-<id>-cpu,
// run in their step's target, are disabled, and are each attached to exactly
// their own phase; each rule's fault spec says exactly the step's fault and
// target_load. Nothing is switched and nothing is served.
func TestBuildRun_Scenario(t *testing.T) {
	for _, sc := range Catalog() {
		t.Run(sc.ID, func(t *testing.T) {
			f := newFakeLab(t, testNow)
			runID := id.New(RunIDPrefix)

			built, err := BuildRun(context.Background(), f.deps(), runID, sc, testRunCap)
			if err != nil {
				t.Fatalf("BuildRun: %v", err)
			}

			if n := len(f.rules); n != 2 {
				t.Fatalf("lab holds %d rules, want 2", n)
			}
			steps := [2]FaultStep{sc.Memory, sc.CPU}
			wantRule := [2]string{"demo-run-" + runID + "-memory", "demo-run-" + runID + "-cpu"}
			wantSpec := [2]string{
				"demo-memory-" + sc.Memory.Target + "-" + sc.Memory.Intensity,
				"demo-cpu-" + sc.CPU.Target + "-" + sc.CPU.Intensity,
			}
			for i, st := range steps {
				rule := f.rule(built.RuleIDs[i])
				if rule == nil {
					t.Fatalf("Built.RuleIDs[%d] = %q names no rule", i, built.RuleIDs[i])
				}
				if rule.Name != wantRule[i] || rule.Service != st.Target || rule.Enabled || rule.Priority != 100 ||
					rule.Mode != "background" || rule.Action.Type != "fault_spec" {
					t.Errorf("%s rule = %+v; want %s on %s, disabled, priority 100, background, fault_spec",
						st.Kind, rule, wantRule[i], st.Target)
				}
				if len(rule.Match.Labels) != 0 || rule.Match.InjectionPoint != "" {
					t.Errorf("%s rule match = %+v; want empty (every request)", st.Kind, rule.Match)
				}
				spec := specByID(f, rule.Action.FaultSpecID)
				if spec == nil {
					t.Fatalf("%s rule names spec %q, which the lab does not hold", st.Kind, rule.Action.FaultSpecID)
				}
				if spec.Name != wantSpec[i] || spec.Category != "resource" || spec.FaultType != string(st.Kind) ||
					spec.Host != "" || spec.Network != nil || spec.DurationMs != 60000 || spec.RampUpMs != 0 || spec.RampDownMs != 0 {
					t.Errorf("%s spec = %+v; want %s, resource/%s, duration_ms 60000, no host, network or ramp",
						st.Kind, spec, wantSpec[i], st.Kind)
				}
				if params := decodeParams(spec.Params); len(params) != 1 || params["target_load"] != st.TargetLoad {
					t.Errorf("%s spec params = %s; want exactly {\"target_load\":%v}", st.Kind, spec.Params, st.TargetLoad)
				}
			}
			if len(f.specs) != 2 || built.RuleIDs[0] == built.RuleIDs[1] ||
				f.rule(built.RuleIDs[0]).Action.FaultSpecID == f.rule(built.RuleIDs[1]).Action.FaultSpecID {
				t.Errorf("specs %d, rules %v; want one spec per fault step, two distinct rules", len(f.specs), built.RuleIDs)
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
			wantNames := []string{"baseline", "memory", "cpu"}
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
				wantPW := model.PhaseWorkflow{PhaseID: p.ID, WorkflowID: sc.Workload.WorkflowID, VUs: 30, RateRPS: 3,
					DurationSec: 40, DatasetID: "ds-ok"}
				if got := f.phaseWFs[p.ID]; len(got) != 1 || got[0] != wantPW {
					t.Errorf("phase %d workflows = %+v; want [%+v]", i, got, wantPW)
				}
			}

			wantLinks := map[string][]string{phases[1].ID: {built.RuleIDs[0]}, phases[2].ID: {built.RuleIDs[1]}}
			if !maps.EqualFunc(f.phaseRules, wantLinks, slices.Equal) {
				t.Errorf("phase rule links = %v; want only memory -> %s and cpu -> %s", f.phaseRules, built.RuleIDs[0], built.RuleIDs[1])
			}
			attach := f.callList("experiments.AttachPhaseRules")
			if want := []string{
				"experiments.AttachPhaseRules " + phases[1].ID + " " + built.RuleIDs[0],
				"experiments.AttachPhaseRules " + phases[2].ID + " " + built.RuleIDs[1],
			}; !slices.Equal(attach, want) {
				t.Errorf("AttachPhaseRules calls = %v; want only %v", attach, want)
			}

			// buildWrites, which the failure tables walk, is every write, in order.
			var ops []string
			for _, c := range writeCalls(f) {
				ops = append(ops, strings.SplitN(c, " ", 2)[0])
			}
			var wantOps []string
			for _, w := range buildWrites {
				wantOps = append(wantOps, strings.SplitN(w, "#", 2)[0])
			}
			if !slices.Equal(ops, wantOps) {
				t.Errorf("writes = %v; want %v", ops, wantOps)
			}

			for _, rid := range built.RuleIDs {
				if f.enabledEver[rid] || f.served(rid) {
					t.Errorf("rule %s was enabled or is served", rid)
				}
			}
			if sw := switchCalls(f); len(sw) != 0 {
				t.Errorf("the builder touched a rule's enabled flag: %v", sw)
			}
		})
	}
}

// TestBuildRun_DB_Scenario: every catalog scenario builds on the real store
// (it skips without the test database). The store accepts both resource fault
// specs and both background rules; a second build reuses each spec; each rule
// is linked to its own phase only; and the store serves each rule, to its
// step's target, only while its own phase runs.
func TestBuildRun_DB_Scenario(t *testing.T) {
	l := newDBLab(t)
	ctx := context.Background()
	for _, sc := range Catalog() {
		t.Run(sc.ID, func(t *testing.T) {
			runID := id.New(RunIDPrefix)
			first, err := BuildRun(ctx, l.d, runID, sc, testRunCap)
			if err != nil {
				t.Fatalf("BuildRun: %v", err)
			}
			second, err := BuildRun(ctx, l.d, id.New(RunIDPrefix), sc, testRunCap)
			if err != nil {
				t.Fatalf("second BuildRun: %v", err)
			}
			specs, err := l.d.Specs.ListSpecs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			steps := [2]FaultStep{sc.Memory, sc.CPU}
			for i, st := range steps {
				r1, err1 := l.rules.Get(ctx, first.RuleIDs[i])
				r2, err2 := l.rules.Get(ctx, second.RuleIDs[i])
				if err1 != nil || err2 != nil {
					t.Fatalf("read %s rules: %v, %v", st.Kind, err1, err2)
				}
				if r1.Name != RuleName(runID, st.Kind) || r1.Service != st.Target || r1.Enabled || r1.Mode != "background" ||
					r1.Priority != 100 || r1.Action.Type != "fault_spec" {
					t.Errorf("%s rule = %+v; want %s on %s, disabled, background, priority 100", st.Kind, r1, RuleName(runID, st.Kind), st.Target)
				}
				if r1.Action.FaultSpecID != r2.Action.FaultSpecID {
					t.Errorf("%s rules of two runs use specs %s and %s; want the same", st.Kind, r1.Action.FaultSpecID, r2.Action.FaultSpecID)
				}
				at := slices.IndexFunc(specs, func(s *model.FaultSpec) bool { return s.ID == r1.Action.FaultSpecID })
				if at < 0 {
					t.Fatalf("%s rule's spec %s not stored", st.Kind, r1.Action.FaultSpecID)
				}
				if s := specs[at]; s.Name != SpecNameFor(st) || !specMatches(s, st) {
					t.Errorf("%s spec as stored = %+v (params %s); want %s, exactly the step's fault", st.Kind, s, s.Params, SpecNameFor(st))
				}
			}
			for i, pid := range first.PhaseIDs {
				links, err := l.exps.ListPhaseRules(ctx, pid)
				if err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, k := range links {
					got = append(got, k.RuleID)
				}
				if want := [][]string{nil, {first.RuleIDs[0]}, {first.RuleIDs[1]}}[i]; !slices.Equal(got, want) {
					t.Errorf("phase %d links %v; want %v", i, got, want)
				}
			}

			// Served only while its own phase runs, under a running experiment.
			served := func(svc string) []string {
				rules, err := l.rules.ForService(ctx, svc)
				if err != nil {
					t.Fatalf("ForService(%s): %v", svc, err)
				}
				var out []string
				for _, r := range rules {
					if slices.Contains(first.RuleIDs[:], r.ID) {
						out = append(out, r.ID)
					}
				}
				return out
			}
			for _, rule := range first.RuleIDs {
				if err := SetRuleEnabled(ctx, l.d.Rules, rule, true); err != nil {
					t.Fatalf("switch on: %v", err)
				}
			}
			l.moveExperiment(first.ExperimentID, "running", "planned")
			l.movePhase(first.PhaseIDs[0], "running", "pending")
			if a, b := served(sc.Memory.Target), served(sc.CPU.Target); len(a) != 0 || len(b) != 0 {
				t.Errorf("baseline running: served %v, %v; want none", a, b)
			}
			l.movePhase(first.PhaseIDs[0], "completed", "running")
			l.movePhase(first.PhaseIDs[1], "running", "pending")
			if got := served(sc.Memory.Target); !slices.Equal(got, first.RuleIDs[:1]) {
				t.Errorf("memory phase running: %s serves %v; want only the memory rule", sc.Memory.Target, got)
			}
			l.movePhase(first.PhaseIDs[1], "completed", "running")
			l.movePhase(first.PhaseIDs[2], "running", "pending")
			if got := served(sc.CPU.Target); !slices.Equal(got, first.RuleIDs[1:]) {
				t.Errorf("cpu phase running: %s serves %v; want only the cpu rule", sc.CPU.Target, got)
			}
			for _, rule := range first.RuleIDs {
				if err := SetRuleEnabled(ctx, l.d.Rules, rule, false); err != nil {
					t.Fatalf("switch off: %v", err)
				}
			}
			if a, b := served(sc.Memory.Target), served(sc.CPU.Target); len(a) != 0 || len(b) != 0 {
				t.Errorf("after the switch-off: served %v, %v; want none", a, b)
			}
		})
	}
}

// TestBuildRun_UsesTheScenariosWorkload: the workflow and the dataset come
// from the scenario's workload, not from a fixed name.
func TestBuildRun_UsesTheScenariosWorkload(t *testing.T) {
	sc := DefaultScenario()
	sc.Workload = Workload{WorkflowID: "wf-other", DatasetName: "other-products", Description: "other"}
	f := newFakeLab(t, testNow)
	f.workflows["wf-other"] = true
	f.datasets = append(f.datasets, zeus.Dataset{ID: "ds-other", Name: "other-products", SizeBytes: 10, CreatedAt: testNow.Add(-2 * time.Hour)})

	built, err := BuildRun(context.Background(), f.deps(), "demo-wl", sc, testRunCap)
	if err != nil {
		t.Fatalf("BuildRun: %v", err)
	}
	if got := f.callList("workflows.Get"); !slices.Equal(got, []string{"workflows.Get wf-other"}) {
		t.Errorf("workflow reads = %v; want only wf-other", got)
	}
	for _, pid := range built.PhaseIDs {
		if pw := f.phaseWFs[pid]; len(pw) != 1 || pw[0].WorkflowID != "wf-other" || pw[0].DatasetID != "ds-other" {
			t.Errorf("phase %s workflows = %+v; want wf-other on ds-other", pid, pw)
		}
	}
}

// TestBuildRun_SpecReused: one fault spec per distinct fault step, created by
// the first build that needs it and reused by every later one; a stored spec
// that says exactly the step, however its params are written, is used as it
// is.
func TestBuildRun_SpecReused(t *testing.T) {
	f := newFakeLab(t, testNow)
	d := f.deps()
	sc := DefaultScenario()

	first, err := BuildRun(context.Background(), d, "demo-a", sc, testRunCap)
	if err != nil {
		t.Fatalf("first build: %v", err)
	}
	second, err := BuildRun(context.Background(), d, "demo-b", sc, testRunCap)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}
	if got, want := f.callList("specs.CreateSpec"), []string{
		"specs.CreateSpec demo-memory-productcatalogservice-moderate",
		"specs.CreateSpec demo-cpu-productcatalogservice-moderate",
	}; !slices.Equal(got, want) {
		t.Fatalf("CreateSpec calls = %v; want %v over two builds", got, want)
	}
	for i := range first.RuleIDs {
		a, b := f.rules[first.RuleIDs[i]], f.rules[second.RuleIDs[i]]
		if a.Action.FaultSpecID != b.Action.FaultSpecID {
			t.Errorf("rule %d of the two runs uses specs %q and %q; want the same", i, a.Action.FaultSpecID, b.Action.FaultSpecID)
		}
	}
	if first.ExperimentID == second.ExperimentID || slices.ContainsFunc(first.RuleIDs[:], func(r string) bool {
		return slices.Contains(second.RuleIDs[:], r)
	}) {
		t.Errorf("two builds share ids: %+v and %+v", first, second)
	}

	// Every scenario, twice: one spec per distinct step, never more.
	distinct := map[string]bool{}
	for _, other := range Catalog() {
		distinct[SpecNameFor(other.Memory)], distinct[SpecNameFor(other.CPU)] = true, true
		for _, run := range []string{"demo-c-" + other.ID, "demo-d-" + other.ID} {
			if _, err := BuildRun(context.Background(), d, run, other, testRunCap); err != nil {
				t.Fatalf("build %s: %v", run, err)
			}
		}
	}
	if n := len(f.callList("specs.CreateSpec")); n != len(distinct) || len(f.specs) != len(distinct) {
		t.Errorf("CreateSpec calls %d, specs %d; want %d, one per distinct step", n, len(f.specs), len(distinct))
	}

	// A stored spec that says exactly the step is reused as it is.
	g := newFakeLab(t, testNow)
	heavy, ok := ScenarioByID("catalog-heavy")
	if !ok {
		t.Fatal("no catalog-heavy scenario")
	}
	stored := &model.FaultSpec{ID: "spec-stored", Name: SpecNameFor(heavy.CPU), Category: "resource", FaultType: "cpu",
		Params: json.RawMessage(`{ "target_load": 1.0 }`), DurationMs: 60000, CreatedAt: testNow.Add(-time.Hour)}
	g.specs = append(g.specs, stored)
	built, err := BuildRun(context.Background(), g.deps(), "demo-y", heavy, testRunCap)
	if err != nil {
		t.Fatalf("BuildRun with a stored spec: %v", err)
	}
	if got := g.rules[built.RuleIDs[1]].Action.FaultSpecID; got != stored.ID {
		t.Errorf("cpu rule spec = %q; want the stored %q", got, stored.ID)
	}
	if got := g.callList("specs.CreateSpec"); !slices.Equal(got, []string{"specs.CreateSpec " + SpecNameFor(heavy.Memory)}) {
		t.Errorf("CreateSpec calls = %v; want only the memory step's", got)
	}
}

// TestBuildRun_SpecDrifted: a stored spec that carries a step's name but says
// anything else than the catalog (another target_load, a mechanism or any
// other params key, another category, type, duration, a host or a ramp) is an
// error before anything is written: no spec is created beside it, no rule, no
// experiment, and the drifted spec is left as it is.
func TestBuildRun_SpecDrifted(t *testing.T) {
	sc := DefaultScenario()
	exact := func(st FaultStep) *model.FaultSpec {
		params, err := json.Marshal(map[string]float64{"target_load": st.TargetLoad})
		if err != nil {
			t.Fatal(err)
		}
		return &model.FaultSpec{ID: "spec-drifted", Name: SpecNameFor(st), Category: "resource", FaultType: string(st.Kind),
			Params: params, DurationMs: 60000, CreatedAt: testNow.Add(-time.Hour)}
	}
	raw := func(s string) func(*model.FaultSpec) {
		return func(f *model.FaultSpec) { f.Params = json.RawMessage(s) }
	}
	cases := []struct {
		name string
		step FaultStep
		edit func(*model.FaultSpec)
	}{
		{"memory: another target_load", sc.Memory, raw(`{"target_load":0.2}`)},
		{"cpu: another target_load", sc.CPU, raw(`{"target_load":0.51}`)},
		{"a mechanism", sc.Memory, raw(`{"target_load":0.1,"mechanism":"balloon"}`)},
		{"another params key", sc.CPU, raw(`{"target_load":0.5,"cores":1}`)},
		{"target_load as a string", sc.CPU, raw(`{"target_load":"0.5"}`)},
		{"no target_load", sc.Memory, raw(`{}`)},
		{"params not an object", sc.Memory, raw(`[0.1]`)},
		{"params null", sc.CPU, raw(`null`)},
		{"another category", sc.CPU, func(s *model.FaultSpec) { s.Category = "inline" }},
		{"another type", sc.Memory, func(s *model.FaultSpec) { s.FaultType = "cpu" }},
		{"another duration", sc.CPU, func(s *model.FaultSpec) { s.DurationMs = 30000 }},
		{"a host", sc.Memory, func(s *model.FaultSpec) { s.Host = "process" }},
		{"a ramp", sc.CPU, func(s *model.FaultSpec) { s.RampUpMs = 1000 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeLab(t, testNow)
			drifted := exact(tc.step)
			tc.edit(drifted)
			f.specs = append(f.specs, drifted)
			before := *drifted
			var buf bytes.Buffer
			d := f.deps()
			d.Logger = slog.New(slog.NewTextHandler(&buf, nil))

			_, err := BuildRun(context.Background(), d, "demo-drift", sc, testRunCap)
			if !errors.Is(err, ErrBuild) || errors.Is(err, ErrNoDataset) || errors.Is(err, ErrCleanupIncomplete) {
				t.Fatalf("err = %v; want ErrBuild", err)
			}
			if writes := writeCalls(f); len(writes) != 0 {
				t.Errorf("writes: %v; want none", writes)
			}
			if left := f.leftovers(); len(left) != 0 {
				t.Errorf("left behind: %v", left)
			}
			if len(f.specs) != 1 || string(f.specs[0].Params) != string(before.Params) || f.specs[0].Category != before.Category ||
				f.specs[0].FaultType != before.FaultType || f.specs[0].DurationMs != before.DurationMs {
				t.Errorf("specs = %d, first %+v; want only the drifted one, untouched", len(f.specs), f.specs[0])
			}
			if !strings.Contains(buf.String(), drifted.ID) {
				t.Errorf("log does not name the drifted spec %s:\n%s", drifted.ID, buf.String())
			}
			assertNoLeak(t, err.Error())
		})
	}

	t.Run("a drifted spec beside an exact one", func(t *testing.T) {
		f := newFakeLab(t, testNow)
		good := exact(sc.Memory)
		good.ID = "spec-exact"
		bad := exact(sc.Memory)
		raw(`{"target_load":0.2}`)(bad)
		f.specs = append(f.specs, good, bad)
		if _, err := BuildRun(context.Background(), f.deps(), "demo-drift", sc, testRunCap); !errors.Is(err, ErrBuild) {
			t.Fatalf("err = %v; want ErrBuild", err)
		}
		if writes := writeCalls(f); len(writes) != 0 {
			t.Errorf("writes: %v; want none", writes)
		}
	})

	t.Run("control: the exact spec is used", func(t *testing.T) {
		f := newFakeLab(t, testNow)
		f.specs = append(f.specs, exact(sc.Memory))
		built, err := BuildRun(context.Background(), f.deps(), "demo-exact", sc, testRunCap)
		if err != nil {
			t.Fatalf("BuildRun: %v", err)
		}
		if got := f.rules[built.RuleIDs[0]].Action.FaultSpecID; got != "spec-drifted" {
			t.Errorf("memory rule spec = %q; want the stored one", got)
		}
	})
}

func TestBuildRun_PicksUsableDataset(t *testing.T) {
	runCap := testRunCap
	edge := testNow.Add(runCap + time.Minute) // must expire strictly after this
	ds := func(id string, size int64, created time.Time, ttl int) zeus.Dataset {
		return zeus.Dataset{ID: id, Name: "demo-products", SizeBytes: size, TTLS: ttl, CreatedAt: created}
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
			f := newFakeLab(t, testNow)
			f.datasets = tc.datasets
			built, err := BuildRun(context.Background(), f.deps(), "demo-ds", DefaultScenario(), runCap)
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
			strings.HasPrefix(op, "experiments.Attach"), strings.HasSuffix(op, ".SetEnabled"),
			strings.HasSuffix(op, ".EnableIfAttached"), strings.HasSuffix(op, ".Delete"):
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
		{"workflow demo-browse missing", func(f *fakeLab) { delete(f.workflows, "demo-browse") }, ErrNoDataset},
		{"zeus unreachable", func(f *fakeLab) { f.fail("zeus.ListDatasets", errLeaky) }, ErrNoDataset},
		{"workflow read fails", func(f *fakeLab) { f.fail("workflows.Get", errLeaky) }, ErrBuild},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeLab(t, testNow)
			tc.setup(f)
			_, err := BuildRun(context.Background(), f.deps(), "demo-pre", DefaultScenario(), testRunCap)
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

// TestBuildRun_DoneContextIsNotNoDataset: a build whose context is cancelled
// or expired while it looks for the load is an interruption, never "no
// dataset" and never a build error, so a later task cannot tell a visitor the
// load generator is down when manteion was shutting down.
func TestBuildRun_DoneContextIsNotNoDataset(t *testing.T) {
	for _, step := range []string{"workflows.Get", "zeus.ListDatasets", "specs.ListSpecs"} {
		t.Run("cancelled during "+step, func(t *testing.T) {
			f := newFakeLab(t, testNow)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.onCall(step, cancel)
			_, err := BuildRun(ctx, f.deps(), "demo-ctx", DefaultScenario(), testRunCap)
			if !errors.Is(err, context.Canceled) || errors.Is(err, ErrNoDataset) || errors.Is(err, ErrBuild) {
				t.Fatalf("err = %v; want context.Canceled and neither ErrNoDataset nor ErrBuild", err)
			}
			if writes := writeCalls(f); len(writes) != 0 {
				t.Errorf("writes: %v; want none", writes)
			}
		})
	}
	t.Run("expired", func(t *testing.T) {
		f := newFakeLab(t, testNow)
		ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
		defer cancel()
		_, err := BuildRun(ctx, f.deps(), "demo-ctx", DefaultScenario(), testRunCap)
		if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrNoDataset) {
			t.Fatalf("err = %v; want context.DeadlineExceeded, not ErrNoDataset", err)
		}
	})
	t.Run("pickDataset itself", func(t *testing.T) {
		// A load generator that answers whatever the context: an empty list,
		// or an error that does not wrap the context's.
		for _, z := range []stubZeus{{}, {err: errors.New("connection reset by peer")}} {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := pickDataset(ctx, z, "demo-products", testNow, testRunCap); !errors.Is(err, context.Canceled) {
				t.Errorf("pickDataset with %+v on a cancelled context: err = %v; want context.Canceled", z, err)
			}
		}
	})
}

// stubZeus answers ListDatasets with datasets and err, ignoring the context.
type stubZeus struct {
	datasets []zeus.Dataset
	err      error
}

func (s stubZeus) Healthy(context.Context) bool { return s.err == nil }
func (s stubZeus) ListDatasets(context.Context) ([]zeus.Dataset, error) {
	return s.datasets, s.err
}

// TestBuildRun_FailureCleansUp: a failure after both rules and the experiment
// were written deletes the experiment first, then each rule, and switches
// nothing.
func TestBuildRun_FailureCleansUp(t *testing.T) {
	f := newFakeLab(t, testNow)
	f.fail("experiments.AttachPhaseRules", errLeaky)

	_, err := BuildRun(context.Background(), f.deps(), "demo-fail", DefaultScenario(), testRunCap)
	if !errors.Is(err, ErrBuild) || errors.Is(err, ErrCleanupIncomplete) {
		t.Fatalf("err = %v; want ErrBuild with a complete cleanup", err)
	}

	if created := f.callList("rules.Create"); len(created) != 2 {
		t.Fatalf("rules.Create calls = %v; want 2", created)
	}
	expDeletes, ruleDeletes := f.callList("experiments.Delete"), f.callList("rules.Delete")
	if len(expDeletes) != 1 || len(ruleDeletes) != 2 {
		t.Fatalf("delete calls = %v, %v; want one experiment and two rule deletes", expDeletes, ruleDeletes)
	}
	expID := strings.TrimPrefix(expDeletes[0], "experiments.Delete ")
	all := f.callList("")
	if !strings.HasPrefix(expID, "exp-") {
		t.Errorf("deleted %q; want the run's experiment", expID)
	}
	for _, del := range ruleDeletes {
		ruleID := strings.TrimPrefix(del, "rules.Delete ")
		if indexOf(all, "experiments.Delete "+expID) > indexOf(all, del) {
			t.Errorf("calls = %v; want experiments.Delete before %s", all, del)
		}
		if !strings.HasPrefix(ruleID, "rule-") || f.enabledEver[ruleID] {
			t.Errorf("deleted %q (enabled ever %v); want one of the run's rules, never switched", ruleID, f.enabledEver[ruleID])
		}
	}
	if sw := switchCalls(f); len(sw) != 0 {
		t.Errorf("rule switched during a failed build: %v", sw)
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

// buildWrites are the build's writes in order, as fake operation keys:
// TestBuildRun_Scenario checks that this is every write a build makes.
var buildWrites = []string{
	"specs.CreateSpec#1",
	"specs.CreateSpec#2",
	"rules.Create#1",
	"rules.Create#2",
	"experiments.Create",
	"experiments.CreatePhase#1",
	"experiments.AttachPhaseWorkflows#1",
	"experiments.CreatePhase#2",
	"experiments.AttachPhaseWorkflows#2",
	"experiments.AttachPhaseRules#1",
	"experiments.CreatePhase#3",
	"experiments.AttachPhaseWorkflows#3",
	"experiments.AttachPhaseRules#2",
}

// checkFailedBuild asserts what every failed build guarantees: ErrBuild, a
// complete cleanup, nothing per run left (neither rule, no experiment), no
// rule ever switched, and an error string without the cause's text or any id.
func checkFailedBuild(t *testing.T, f *fakeLab, err error) {
	t.Helper()
	if !errors.Is(err, ErrBuild) || errors.Is(err, ErrNoDataset) || errors.Is(err, ErrCleanupIncomplete) {
		t.Fatalf("err = %v; want ErrBuild only, cleanup complete", err)
	}
	if !errors.Is(err, errLeaky) {
		t.Errorf("err = %v; want the cause reachable through errors.Is", err)
	}
	if left := f.leftovers(); len(left) != 0 {
		t.Errorf("left behind: %v (calls %v)", left, f.callList(""))
	}
	if sw := switchCalls(f); len(sw) != 0 {
		t.Errorf("rule switched during a failed build: %v", sw)
	}
	for id, on := range f.enabledEver {
		if on {
			t.Errorf("rule %s was enabled", id)
		}
	}
	assertNoLeak(t, err.Error())
}

// TestBuildRun_FailsHalfWay injects a failure, with nothing written, at each
// write of the build in turn (and at the spec listing): both rules' creates
// and both attaches included. Whichever fails, no rule and no experiment is
// left.
func TestBuildRun_FailsHalfWay(t *testing.T) {
	for _, step := range append([]string{"specs.ListSpecs"}, buildWrites...) {
		t.Run(step, func(t *testing.T) {
			f := newFakeLab(t, testNow)
			f.fail(step, errLeaky)
			_, err := BuildRun(context.Background(), f.deps(), "demo-step", DefaultScenario(), testRunCap)
			checkFailedBuild(t, f, err)
		})
	}
}

// TestBuildRun_CommittedButFailedWriteIsCleanedUp: at each write in turn, the
// write commits but its answer is an error (a connection lost after COMMIT).
// The build still removes the row it believed failed, which it can do only
// because it records each row before writing it.
func TestBuildRun_CommittedButFailedWriteIsCleanedUp(t *testing.T) {
	for _, step := range buildWrites {
		t.Run(step, func(t *testing.T) {
			f := newFakeLab(t, testNow)
			f.failAfterCommit(step, errLeaky)
			_, err := BuildRun(context.Background(), f.deps(), "demo-commit", DefaultScenario(), testRunCap)
			checkFailedBuild(t, f, err)
		})
	}
}

// TestBuildRun_CleanupOutlivesCancelledContext: the build's context is
// cancelled mid-way (a shutdown, say); the cleanup still runs to completion,
// and the error is the interruption, not a build failure.
func TestBuildRun_CleanupOutlivesCancelledContext(t *testing.T) {
	f := newFakeLab(t, testNow)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.onCall("experiments.AttachPhaseWorkflows#3", cancel)

	_, err := BuildRun(ctx, f.deps(), "demo-cancel", DefaultScenario(), testRunCap)
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrBuild) || errors.Is(err, ErrCleanupIncomplete) {
		t.Fatalf("err = %v; want context.Canceled with a complete cleanup", err)
	}
	if left := f.leftovers(); len(left) != 0 {
		t.Errorf("left behind after a cancelled build: %v", left)
	}
}

// TestBuildRun_CleanupFailureIsReported: when the cleanup itself fails, the
// build returns ErrBuild and ErrCleanupIncomplete, and the log names what was
// left behind with the full cause. The failure comes after the memory rule
// was attached and before the CPU rule was: the experiment's delete fails, so
// the store refuses the memory rule's delete (its phase still links it) and
// that rule is left, disabled; the CPU rule, linked nowhere, is deleted.
func TestBuildRun_CleanupFailureIsReported(t *testing.T) {
	f := newFakeLab(t, testNow)
	f.fail("experiments.AttachPhaseWorkflows#3", errLeaky)
	f.fail("experiments.Delete", errors.New("pq: delete timed out"))
	var buf bytes.Buffer
	d := f.deps()
	d.Logger = slog.New(slog.NewTextHandler(&buf, nil))

	_, err := BuildRun(context.Background(), d, "demo-stuck", DefaultScenario(), testRunCap)
	var se *StepError
	if !errors.Is(err, ErrBuild) || !errors.Is(err, ErrCleanupIncomplete) || !errors.As(err, &se) || !se.CleanupIncomplete {
		t.Fatalf("err = %v; want ErrBuild with ErrCleanupIncomplete", err)
	}
	if !strings.Contains(err.Error(), "cleanup incomplete") {
		t.Errorf("err text %q does not say the cleanup is incomplete", err.Error())
	}
	assertNoLeak(t, err.Error())
	if n := len(f.rules); n != 1 {
		t.Fatalf("lab holds %d rules; want the run's memory rule only", n)
	}
	var expID string
	for id := range f.experiments {
		expID = id
	}
	want := []string{expID, "pq: delete timed out", "pq: dial 192.0.2.10", "cleanup_incomplete=true"}
	for id, rule := range f.rules {
		if rule.Name != RuleName("demo-stuck", FaultMemory) || rule.Enabled || f.enabledEver[id] || f.served(id) {
			t.Errorf("rule left = %+v; want the memory rule, never enabled, not served", rule)
		}
		want = append(want, id)
	}
	if dels := f.callList("rules.Delete"); len(dels) != 2 {
		t.Errorf("rule deletes = %v; want both rules' attempted", dels)
	}
	logs := buf.String()
	for _, w := range want {
		if !strings.Contains(logs, w) {
			t.Errorf("log lacks %q:\n%s", w, logs)
		}
	}
}

// TestBuildRun_HungCleanupDeleteDoesNotStarveTheOther: the experiment delete
// of a cleanup hangs; it is cut off by its own timeout, each rule delete is
// still attempted on a fresh context of its own, the build returns within the
// timeouts, and the error says the cleanup is incomplete.
func TestBuildRun_HungCleanupDeleteDoesNotStarveTheOther(t *testing.T) {
	f := newFakeLab(t, testNow)
	f.fail("experiments.AttachPhaseWorkflows#2", errLeaky)
	f.hang("experiments.Delete")
	const timeout = 50 * time.Millisecond

	start := time.Now()
	_, err := buildRun(context.Background(), f.deps(), "demo-hang", DefaultScenario(), testRunCap, timeout)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrBuild) || !errors.Is(err, ErrCleanupIncomplete) {
		t.Fatalf("err = %v; want ErrBuild with ErrCleanupIncomplete", err)
	}
	// The failure came before either rule was attached, so their deletes, each
	// on a fresh context of its own, succeed although the first delete hung.
	dels := f.callList("rules.Delete")
	if len(dels) != 2 {
		t.Fatalf("rules.Delete calls = %v; want both attempted after the hung experiment delete", dels)
	}
	for _, del := range dels {
		if id := strings.TrimPrefix(del, "rules.Delete "); f.rule(id) != nil {
			t.Errorf("rule %s still there: its delete ran on the hung delete's expired context", id)
		}
	}
	if elapsed > 3*timeout+500*time.Millisecond {
		t.Errorf("build returned after %v; want within the cleanup timeouts", elapsed)
	}
	if len(f.experiments) != 1 {
		t.Errorf("experiments = %d; want the planned one the hung delete left", len(f.experiments))
	}
}

// TestBuildRun_RefusesBadInput: a missing run id, run cap or dependency, or a
// scenario whose steps are not a memory then a CPU fault with a target, an
// intensity and a target_load in (0, 1], or that names no workload, writes
// nothing.
func TestBuildRun_RefusesBadInput(t *testing.T) {
	f := newFakeLab(t, testNow)
	sc := DefaultScenario()
	if _, err := BuildRun(context.Background(), f.deps(), "", sc, testRunCap); !errors.Is(err, ErrBuild) {
		t.Errorf("empty run id: err = %v; want ErrBuild", err)
	}
	if _, err := BuildRun(context.Background(), f.deps(), "demo-cap", sc, 0); !errors.Is(err, ErrBuild) {
		t.Errorf("no run cap: err = %v; want ErrBuild", err)
	}
	d := f.deps()
	d.Clock = nil
	if _, err := BuildRun(context.Background(), d, "demo-nil", sc, testRunCap); !errors.Is(err, ErrBuild) {
		t.Errorf("missing clock: err = %v; want ErrBuild", err)
	}
	bad := map[string]func(*Scenario){
		"zero scenario":         func(s *Scenario) { *s = Scenario{} },
		"steps swapped":         func(s *Scenario) { s.Memory, s.CPU = s.CPU, s.Memory },
		"memory step of cpu":    func(s *Scenario) { s.Memory.Kind = FaultCPU },
		"no memory target":      func(s *Scenario) { s.Memory.Target = "" },
		"no cpu intensity":      func(s *Scenario) { s.CPU.Intensity = "" },
		"target_load 0":         func(s *Scenario) { s.Memory.TargetLoad = 0 },
		"target_load above 1":   func(s *Scenario) { s.CPU.TargetLoad = 1.5 },
		"target_load NaN":       func(s *Scenario) { s.CPU.TargetLoad = math.NaN() },
		"target_load negative":  func(s *Scenario) { s.Memory.TargetLoad = -0.1 },
		"no workflow":           func(s *Scenario) { s.Workload.WorkflowID = "" },
		"no dataset":            func(s *Scenario) { s.Workload.DatasetName = "" },
		"target_load +Infinity": func(s *Scenario) { s.CPU.TargetLoad = math.Inf(1) },
	}
	for name, edit := range bad {
		s := DefaultScenario()
		edit(&s)
		if _, err := BuildRun(context.Background(), f.deps(), "demo-bad", s, testRunCap); !errors.Is(err, ErrBuild) {
			t.Errorf("%s: err = %v; want ErrBuild", name, err)
		}
	}
	if calls := f.callList(""); len(calls) != 0 {
		t.Errorf("calls: %v; want none", calls)
	}
}

// TestRuleAndSpecNames pins the names the build gives a run's rules and the
// shared fault specs.
func TestRuleAndSpecNames(t *testing.T) {
	if got := RuleName("demo-x", FaultMemory); got != "demo-run-demo-x-memory" {
		t.Errorf("RuleName(memory) = %q", got)
	}
	if got := RuleName("demo-x", FaultCPU); got != "demo-run-demo-x-cpu" {
		t.Errorf("RuleName(cpu) = %q", got)
	}
	if got := SpecNameFor(DefaultScenario().Memory); got != "demo-memory-productcatalogservice-moderate" {
		t.Errorf("SpecNameFor(default memory) = %q", got)
	}
	heavy, _ := ScenarioByID("cart-heavy")
	if got := SpecNameFor(heavy.CPU); got != "demo-cpu-cartservice-heavy" {
		t.Errorf("SpecNameFor(cart-heavy cpu) = %q", got)
	}
}

// TestRunIDOfRule: a run's rule names, the new two (demo-run-<id>-memory,
// demo-run-<id>-cpu) and build 2's one (demo-run-<id>), give the run id back;
// anything else is not a run's rule. A run id is demo- and a lower-case
// canonical UUID, so no run id ends in -memory or -cpu and each name has one
// reading.
func TestRunIDOfRule(t *testing.T) {
	for i := 0; i < 50; i++ {
		runID := id.New(RunIDPrefix)
		for _, name := range []string{RuleName(runID, FaultMemory), RuleName(runID, FaultCPU), RunNamePrefix + runID} {
			if got, ok := RunIDOfRule(name); !ok || got != runID {
				t.Fatalf("RunIDOfRule(%q) = %q, %v; want %q", name, got, ok, runID)
			}
		}
		for _, k := range []FaultKind{FaultMemory, FaultCPU} {
			if validRunID(runID + "-" + string(k)) {
				t.Fatalf("%s-%s passes as a run id", runID, k)
			}
		}
	}
	runID := id.New(RunIDPrefix)
	for _, name := range []string{
		"", "smoke-x", "demo-run-", "demo-run-x", "demo-run-x-memory", "demo-run--memory", "demo-run--cpu",
		runID, runID + "-memory", // no demo-run- prefix
		"x" + RunNamePrefix + runID,
		RunNamePrefix + runID + "-latency",
		RunNamePrefix + runID + "-MEMORY",
		RunNamePrefix + runID + "-memory-cpu",
		RunNamePrefix + runID + "-cpu-memory",
		RunNamePrefix + runID + "-memory ",
		RunNamePrefix + runID + "memory",
		RunNamePrefix + strings.ToUpper(runID),
		RunNamePrefix + strings.ToUpper(runID) + "-cpu",
		RunNamePrefix + runID[:len(runID)-1] + "-cpu",
		RunNamePrefix + "demo-run-" + runID,
	} {
		if got, ok := RunIDOfRule(name); ok || got != "" {
			t.Errorf("RunIDOfRule(%q) = %q, %v; want not a run's rule", name, got, ok)
		}
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

// ctxSpyRules records the context each SetEnabled call ran on.
type ctxSpyRules struct {
	fakeRules
	errAtCall      error
	deadlineIn     time.Duration
	hadDeadline    bool
	setEnabledSeen bool
}

func (s *ctxSpyRules) SetEnabled(ctx context.Context, id string, enabled bool) error {
	s.setEnabledSeen = true
	s.errAtCall = ctx.Err()
	if dl, ok := ctx.Deadline(); ok {
		s.hadDeadline, s.deadlineIn = true, time.Until(dl)
	}
	return s.fakeRules.SetEnabled(ctx, id, enabled)
}

func TestSetRuleEnabled(t *testing.T) {
	// newLabWithRule builds a run and returns its memory rule, attached to the
	// memory phase.
	newLabWithRule := func(t *testing.T) (*fakeLab, string) {
		t.Helper()
		f := newFakeLab(t, testNow)
		built, err := BuildRun(context.Background(), f.deps(), "demo-sw", DefaultScenario(), testRunCap)
		if err != nil {
			t.Fatalf("BuildRun: %v", err)
		}
		return f, built.RuleIDs[0]
	}

	t.Run("on enables an attached demo rule, off disables it", func(t *testing.T) {
		f, id := newLabWithRule(t)
		before := f.rule(id)
		if err := SetRuleEnabled(context.Background(), fakeRules{f}, id, true); err != nil {
			t.Fatalf("enable: %v", err)
		}
		after := f.rule(id)
		if !after.Enabled {
			t.Fatalf("rule not enabled")
		}
		if after.Name != before.Name || after.Service != before.Service || after.Priority != before.Priority ||
			after.Mode != before.Mode || after.Action != before.Action || len(after.Match.Labels) != 0 {
			t.Errorf("enable changed more than the flag: %+v -> %+v", before, after)
		}
		if got := f.callList("rules.EnableIfAttached"); len(got) != 1 {
			t.Errorf("EnableIfAttached calls = %v; want 1", got)
		}
		if err := SetRuleEnabled(context.Background(), fakeRules{f}, id, false); err != nil {
			t.Fatalf("disable: %v", err)
		}
		if f.rule(id).Enabled {
			t.Errorf("rule still enabled")
		}
	})

	t.Run("off always writes and never reads first", func(t *testing.T) {
		f, id := newLabWithRule(t)
		for i := 0; i < 3; i++ { // the rule is already off every time
			if err := SetRuleEnabled(context.Background(), fakeRules{f}, id, false); err != nil {
				t.Fatalf("disable #%d: %v", i, err)
			}
		}
		if got := f.callList("rules.SetEnabled"); len(got) != 3 {
			t.Errorf("SetEnabled calls = %v; want one write per switch-off, even when already off", got)
		}
		if got := f.callList("rules.Get"); len(got) != 0 {
			t.Errorf("switch-off read the rule first: %v", got)
		}
	})

	t.Run("off on a cancelled context still writes, on its own deadline", func(t *testing.T) {
		f, id := newLabWithRule(t)
		if err := SetRuleEnabled(context.Background(), fakeRules{f}, id, true); err != nil {
			t.Fatalf("enable: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		spy := &ctxSpyRules{fakeRules: fakeRules{f}}
		if err := SetRuleEnabled(ctx, spy, id, false); err != nil {
			t.Fatalf("disable on a cancelled context: %v", err)
		}
		if f.rule(id).Enabled || f.served(id) {
			t.Errorf("rule still enabled after a switch-off on a cancelled context")
		}
		if !spy.setEnabledSeen || spy.errAtCall != nil || !spy.hadDeadline ||
			spy.deadlineIn <= 9*time.Second || spy.deadlineIn > 10*time.Second {
			t.Errorf("switch-off context: seen %v, err %v, deadline %v in %v; want live, about 10 s",
				spy.setEnabledSeen, spy.errAtCall, spy.hadDeadline, spy.deadlineIn)
		}
	})

	t.Run("off is never refused and a missing rule is success", func(t *testing.T) {
		f, _ := newLabWithRule(t)
		operator := &model.Rule{ID: "rule-operator", Name: "smoke-x", Service: "productcatalogservice", Enabled: true,
			Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: f.specs[0].ID}}
		if err := (fakeRules{f}).Create(context.Background(), operator); err != nil {
			t.Fatal(err)
		}
		if err := SetRuleEnabled(context.Background(), fakeRules{f}, operator.ID, false); err != nil {
			t.Errorf("disable an operator rule: %v", err)
		}
		if f.rule(operator.ID).Enabled {
			t.Errorf("operator rule still enabled")
		}
		if err := SetRuleEnabled(context.Background(), fakeRules{f}, "rule-gone", false); err != nil {
			t.Errorf("disable a missing rule: %v; want success", err)
		}
	})

	t.Run("off failure keeps its class and drops its text", func(t *testing.T) {
		f, id := newLabWithRule(t)
		f.fail("rules.SetEnabled", errLeaky)
		err := SetRuleEnabled(context.Background(), fakeRules{f}, id, false)
		if !errors.Is(err, ErrRuleState) || !errors.Is(err, errLeaky) {
			t.Fatalf("err = %v; want ErrRuleState wrapping the cause", err)
		}
		assertNoLeak(t, err.Error())
	})

	t.Run("on refuses an unattached rule and leaves it disabled", func(t *testing.T) {
		f, _ := newLabWithRule(t)
		loose := &model.Rule{ID: "rule-loose", Name: RunNamePrefix + "loose", Service: "productcatalogservice",
			Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: f.specs[0].ID}}
		if err := (fakeRules{f}).Create(context.Background(), loose); err != nil {
			t.Fatal(err)
		}
		err := SetRuleEnabled(context.Background(), fakeRules{f}, loose.ID, true)
		if !errors.Is(err, store.ErrRuleUnattached) || !errors.Is(err, ErrRuleState) {
			t.Fatalf("err = %v; want store.ErrRuleUnattached", err)
		}
		if f.rule(loose.ID).Enabled || f.served(loose.ID) {
			t.Errorf("unattached rule enabled or served")
		}
		assertNoLeak(t, err.Error())
	})

	t.Run("on refuses a rule with another name without a write", func(t *testing.T) {
		f, id := newLabWithRule(t)
		f.mu.Lock()
		f.rules[id].Name = "smoke-x"
		f.mu.Unlock()
		err := SetRuleEnabled(context.Background(), fakeRules{f}, id, true)
		if !errors.Is(err, ErrNotDemoRule) {
			t.Errorf("err = %v; want ErrNotDemoRule", err)
		}
		if sw := switchCalls(f); len(sw) != 0 || f.rule(id).Enabled {
			t.Errorf("switch calls = %v; want none", sw)
		}
	})

	t.Run("on with a cancelled context enables nothing", func(t *testing.T) {
		f, id := newLabWithRule(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := SetRuleEnabled(ctx, fakeRules{f}, id, true)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v; want context.Canceled", err)
		}
		if f.rule(id).Enabled {
			t.Errorf("rule enabled on a cancelled context")
		}
	})

	t.Run("on for a missing rule is not found", func(t *testing.T) {
		f, _ := newLabWithRule(t)
		err := SetRuleEnabled(context.Background(), fakeRules{f}, "rule-missing", true)
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("err = %v; want store.ErrNotFound", err)
		}
		assertNoLeak(t, err.Error())
	})
}
