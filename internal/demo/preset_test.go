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

// switchCalls lists every call that switches a rule's enabled flag.
func switchCalls(f *fakeLab) []string {
	return append(f.callList("rules.SetEnabled"), f.callList("rules.EnableIfAttached")...)
}

func TestBuildRun_Shape(t *testing.T) {
	f := newFakeLab(t, testNow)
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
	if sw := switchCalls(f); len(sw) != 0 || f.enabledEver[rule.ID] {
		t.Errorf("the builder touched the rule's enabled flag: %v, enabled ever %v", sw, f.enabledEver[rule.ID])
	}
	if f.served(rule.ID) {
		t.Errorf("a freshly built rule is served")
	}
}

func TestBuildRun_ReusesSpec(t *testing.T) {
	f := newFakeLab(t, testNow)
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
	f := newFakeLab(t, testNow)
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
	g := newFakeLab(t, testNow)
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
			f := newFakeLab(t, testNow)
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
		{"workflow demo-browse missing", func(f *fakeLab) { delete(f.workflows, WorkflowID) }, ErrNoDataset},
		{"zeus unreachable", func(f *fakeLab) { f.fail("zeus.ListDatasets", errLeaky) }, ErrNoDataset},
		{"workflow read fails", func(f *fakeLab) { f.fail("workflows.Get", errLeaky) }, ErrBuild},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeLab(t, testNow)
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
			_, err := BuildRun(ctx, f.deps(), "demo-ctx", testRunCap)
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
		_, err := BuildRun(ctx, f.deps(), "demo-ctx", testRunCap)
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
			if _, err := pickDataset(ctx, z, testNow, testRunCap); !errors.Is(err, context.Canceled) {
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

func TestBuildRun_FailureCleansUp(t *testing.T) {
	f := newFakeLab(t, testNow)
	f.fail("experiments.AttachPhaseRules", errLeaky)

	_, err := BuildRun(context.Background(), f.deps(), "demo-fail", testRunCap)
	if !errors.Is(err, ErrBuild) || errors.Is(err, ErrCleanupIncomplete) {
		t.Fatalf("err = %v; want ErrBuild with a complete cleanup", err)
	}

	if created := f.callList("rules.Create"); len(created) != 1 {
		t.Fatalf("rules.Create calls = %v; want 1", created)
	}
	deletes := append(f.callList("experiments.Delete"), f.callList("rules.Delete")...)
	if len(deletes) != 2 {
		t.Fatalf("delete calls = %v; want one experiment and one rule delete", deletes)
	}
	expID := strings.TrimPrefix(deletes[0], "experiments.Delete ")
	ruleID := strings.TrimPrefix(deletes[1], "rules.Delete ")
	all := f.callList("")
	if indexOf(all, "experiments.Delete "+expID) > indexOf(all, "rules.Delete "+ruleID) {
		t.Errorf("calls = %v; want experiments.Delete before rules.Delete", all)
	}
	if !strings.HasPrefix(expID, "exp-") || !strings.HasPrefix(ruleID, "rule-") {
		t.Errorf("deleted %q and %q; want the run's experiment and rule", expID, ruleID)
	}
	if sw := switchCalls(f); len(sw) != 0 || f.enabledEver[ruleID] {
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

// buildWrites are the build's writes in order, as fake operation keys.
var buildWrites = []string{
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

// checkFailedBuild asserts what every failed build guarantees: ErrBuild, a
// complete cleanup, nothing per run left, the rule never switched, and an
// error string without the cause's text or any id.
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

// TestBuildRun_FailureAtEveryStepLeavesNothing injects a failure, with nothing
// written, at each write of the build in turn (and at the spec listing).
func TestBuildRun_FailureAtEveryStepLeavesNothing(t *testing.T) {
	for _, step := range append([]string{"specs.ListSpecs"}, buildWrites...) {
		t.Run(step, func(t *testing.T) {
			f := newFakeLab(t, testNow)
			f.fail(step, errLeaky)
			_, err := BuildRun(context.Background(), f.deps(), "demo-step", testRunCap)
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
			_, err := BuildRun(context.Background(), f.deps(), "demo-commit", testRunCap)
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

	_, err := BuildRun(ctx, f.deps(), "demo-cancel", testRunCap)
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrBuild) || errors.Is(err, ErrCleanupIncomplete) {
		t.Fatalf("err = %v; want context.Canceled with a complete cleanup", err)
	}
	if left := f.leftovers(); len(left) != 0 {
		t.Errorf("left behind after a cancelled build: %v", left)
	}
}

// TestBuildRun_CleanupFailureIsReported: when the cleanup itself fails, the
// build returns ErrBuild and ErrCleanupIncomplete, the rule stays disabled,
// and the log names what was left behind with the full cause.
func TestBuildRun_CleanupFailureIsReported(t *testing.T) {
	f := newFakeLab(t, testNow)
	f.fail("experiments.AttachPhaseWorkflows#3", errLeaky)
	f.fail("experiments.Delete", errors.New("pq: delete timed out"))
	var buf bytes.Buffer
	d := f.deps()
	d.Logger = slog.New(slog.NewTextHandler(&buf, nil))

	_, err := BuildRun(context.Background(), d, "demo-stuck", testRunCap)
	var se *StepError
	if !errors.Is(err, ErrBuild) || !errors.Is(err, ErrCleanupIncomplete) || !errors.As(err, &se) || !se.CleanupIncomplete {
		t.Fatalf("err = %v; want ErrBuild with ErrCleanupIncomplete", err)
	}
	if !strings.Contains(err.Error(), "cleanup incomplete") {
		t.Errorf("err text %q does not say the cleanup is incomplete", err.Error())
	}
	assertNoLeak(t, err.Error())
	rule := onlyRule(t, f)
	if rule.Enabled || f.enabledEver[rule.ID] || f.served(rule.ID) {
		t.Errorf("rule left enabled or served: %+v", rule)
	}
	var expID string
	for id := range f.experiments {
		expID = id
	}
	logs := buf.String()
	for _, want := range []string{expID, rule.ID, "pq: delete timed out", "pq: dial 192.0.2.10", "cleanup_incomplete=true"} {
		if !strings.Contains(logs, want) {
			t.Errorf("log lacks %q:\n%s", want, logs)
		}
	}
}

// TestBuildRun_HungCleanupDeleteDoesNotStarveTheOther: the experiment delete
// of a cleanup hangs; it is cut off by its own timeout, the rule delete is
// still attempted on a fresh one, the build returns within the two timeouts,
// and the error says the cleanup is incomplete.
func TestBuildRun_HungCleanupDeleteDoesNotStarveTheOther(t *testing.T) {
	f := newFakeLab(t, testNow)
	f.fail("experiments.AttachPhaseWorkflows#2", errLeaky)
	f.hang("experiments.Delete")
	const timeout = 50 * time.Millisecond

	start := time.Now()
	_, err := buildRun(context.Background(), f.deps(), "demo-hang", testRunCap, timeout)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrBuild) || !errors.Is(err, ErrCleanupIncomplete) {
		t.Fatalf("err = %v; want ErrBuild with ErrCleanupIncomplete", err)
	}
	// The failure came before the rule was attached, so its delete, on a
	// fresh context of its own, succeeds although the first delete hung.
	dels := f.callList("rules.Delete")
	if len(dels) != 1 {
		t.Fatalf("rules.Delete calls = %v; want it attempted after the hung experiment delete", dels)
	}
	if id := strings.TrimPrefix(dels[0], "rules.Delete "); f.rule(id) != nil {
		t.Errorf("rule %s still there: its delete ran on the hung delete's expired context", id)
	}
	if elapsed > 2*timeout+500*time.Millisecond {
		t.Errorf("build returned after %v; want within two cleanup timeouts", elapsed)
	}
	if len(f.experiments) != 1 {
		t.Errorf("experiments = %d; want the planned one the hung delete left", len(f.experiments))
	}
}

func TestBuildRun_RefusesBadInput(t *testing.T) {
	f := newFakeLab(t, testNow)
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
	// newLabWithRule builds a run, so its rule is attached to the fault phase.
	newLabWithRule := func(t *testing.T) (*fakeLab, string) {
		t.Helper()
		f := newFakeLab(t, testNow)
		built, err := BuildRun(context.Background(), f.deps(), "demo-sw", testRunCap)
		if err != nil {
			t.Fatalf("BuildRun: %v", err)
		}
		return f, built.RuleID
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
		operator := &model.Rule{ID: "rule-operator", Name: "smoke-x", Service: TargetService, Enabled: true,
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
		loose := &model.Rule{ID: "rule-loose", Name: RunNamePrefix + "loose", Service: TargetService,
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
