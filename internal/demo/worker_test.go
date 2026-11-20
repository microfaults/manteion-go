package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/orchestrator"
)

// ---- harness ----

// testOffRetry is the switch-off retry schedule of every test worker: the
// production 1 s doubling to 30 s, shrunk a thousandfold.
var testOffRetry = backoff{first: time.Millisecond, max: 30 * time.Millisecond}

// noCap gives a test whose run must not end by the watchdog a cap it cannot
// reach, so a slow machine cannot turn its ending into timed_out.
func noCap(c *Config) { c.RunCap = 10 * time.Second }

// workerConfig is the brief's test configuration: StatusPoll 5 ms, RunCap
// 200 ms, the rest default.
func workerConfig() Config {
	cfg := DefaultConfig()
	cfg.StatusPoll = 5 * time.Millisecond
	cfg.RunCap = 200 * time.Millisecond
	return cfg
}

// harness is one worker over one fake lab.
type harness struct {
	t      *testing.T
	f      *fakeLab
	cfg    Config
	q      *Queue
	h      *Hub
	w      *Worker
	cancel context.CancelFunc
	began  time.Time
	once   sync.Once
}

func newHarness(t *testing.T, tweak func(*Config)) *harness {
	t.Helper()
	f := newFakeLab(t, testNow)
	cfg := workerConfig()
	if tweak != nil {
		tweak(&cfg)
	}
	q, h := NewQueue(cfg.Capacity), NewHub()
	w := NewWorker(f.deps(), cfg, q, h)
	w.offRetry = testOffRetry
	return &harness{t: t, f: f, cfg: cfg, q: q, h: h, w: w}
}

// enqueue queues a run for client and subscribes to its events.
func (x *harness) enqueue(client string) (Run, <-chan Event) {
	x.t.Helper()
	r, err := x.q.Enqueue(client, x.f.clock.Now())
	if err != nil {
		x.t.Fatal(err)
	}
	ch, unsub := x.h.Subscribe(r.RunID)
	x.t.Cleanup(unsub)
	return r, ch
}

// start runs the worker; the test's cleanup stops it.
func (x *harness) start() {
	ctx, cancel := context.WithCancel(context.Background())
	x.cancel = cancel
	x.began = time.Now()
	go x.w.Run(ctx)
	x.t.Cleanup(x.stop)
}

// stop ends Run and waits for it and for every goroutine the worker started;
// then no demo rule may be on.
func (x *harness) stop() {
	x.once.Do(func() {
		if x.cancel == nil {
			return
		}
		x.cancel()
		waitClosed(x.t, x.w.done, "Run to return")
		bg := make(chan struct{})
		go func() { x.w.bg.Wait(); close(bg) }()
		waitClosed(x.t, bg, "the worker's goroutines to end")
		x.f.requireDemoRulesOff(x.t)
	})
}

// expID returns the id of the experiment run runID started, waiting for it.
func (x *harness) expID(runID string) string {
	x.t.Helper()
	var id string
	waitFor(x.t, "the run's experiment", func() bool { id = x.f.experimentIDOfRun(runID); return id != "" })
	return id
}

// waitPolls waits until the worker has read its experiment n more times.
func (x *harness) waitPolls(n int) {
	x.t.Helper()
	base := len(x.f.callList("experiments.Get "))
	waitFor(x.t, fmt.Sprintf("%d more polls", n), func() bool { return len(x.f.callList("experiments.Get ")) >= base+n })
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Errorf("timed out waiting for %s", what)
	}
}

// waitFor polls cond every millisecond for up to 3 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// readUntil reads ch until match holds for an event, returning every event read.
func readUntil(t *testing.T, ch <-chan Event, what string, match func(Event) bool) []Event {
	t.Helper()
	var evs []Event
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("stream closed before %s; got %v", what, eventTypes(evs))
			}
			evs = append(evs, ev)
			if match(ev) {
				return evs
			}
		case <-timer.C:
			t.Fatalf("no %s within 3 s; got %v", what, eventTypes(evs))
		}
	}
}

// awaitEnd reads ch until the end event; it returns the events read, end last,
// and the ended Run.
func awaitEnd(t *testing.T, ch <-chan Event) ([]Event, Run) {
	t.Helper()
	evs := readUntil(t, ch, "the end event", func(ev Event) bool { return ev.Type == EventEnd })
	return evs, evs[len(evs)-1].Data.(Run)
}

// awaitPhase reads ch until phase idx reports status.
func awaitPhase(t *testing.T, ch <-chan Event, idx int, status PhaseStatus) []Event {
	t.Helper()
	return readUntil(t, ch, fmt.Sprintf("phase %d %s", idx, status), func(ev Event) bool {
		pe, ok := ev.Data.(PhaseEvent)
		return ok && pe.PhaseIndex == idx && pe.Status == status
	})
}

// requireNoMore fails if ch holds another event (call once the worker stopped).
func requireNoMore(t *testing.T, ch <-chan Event) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if ok {
			t.Errorf("an event after the end: %s %+v", ev.Type, ev.Data)
		}
	default:
	}
}

func eventTypes(evs []Event) []string {
	var out []string
	for _, ev := range evs {
		out = append(out, ev.Type)
	}
	return out
}

// phaseTrail lists the phase events as "index:status".
func phaseTrail(evs []Event) []string {
	var out []string
	for _, ev := range evs {
		if pe, ok := ev.Data.(PhaseEvent); ok {
			out = append(out, fmt.Sprintf("%d:%s", pe.PhaseIndex, pe.Status))
		}
	}
	return out
}

// requireEnded checks an ended run's public shape and that nothing internal
// reaches its JSON.
func requireEnded(t *testing.T, r Run, status RunStatus, reason Reason) {
	t.Helper()
	if r.Status != status || r.Reason != reason {
		t.Errorf("run ended %s/%q; want %s/%q", r.Status, r.Reason, status, reason)
	}
	if r.Position != nil || r.EndedAt == nil {
		t.Errorf("ended run: position %v, ended_at %v; want null, set", r.Position, r.EndedAt)
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("the ended run does not marshal: %v", err)
	}
	assertNoLeak(t, string(b))
}

func indexOfPrefix(calls []string, prefix string) int {
	return slices.IndexFunc(calls, func(c string) bool { return strings.HasPrefix(c, prefix) })
}

// offBeforeCancel records, at each CancelExperiment, whether the run's rule
// was already off.
func offBeforeCancel(f *fakeLab) *atomic.Int32 {
	var ruleOnAtCancel atomic.Int32
	f.onCall("orch.CancelExperiment", func() {
		for _, id := range f.demoRuleIDs() {
			if r := f.rule(id); r != nil && r.Enabled {
				ruleOnAtCancel.Add(1)
			}
		}
	})
	return &ruleOnAtCancel
}

// ---- happy path and preflight ----

// TestWorker_HappyPath: the rule is switched on before the start and off
// after the experiment completes; the run ends completed with results.
func TestWorker_HappyPath(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	x.f.autopilot = true
	var expAtDisable atomic.Value
	x.f.onCall("rules.SetEnabled", func() {
		for _, e := range x.f.callList("orch.StartExperiment ") {
			expAtDisable.Store(x.f.experimentStatus(strings.TrimPrefix(e, "orch.StartExperiment ")))
		}
	})
	var ended atomic.Int32
	var hadDeadline atomic.Bool
	x.w.OnRunEnded = func(ctx context.Context) {
		_, ok := ctx.Deadline()
		hadDeadline.Store(ok)
		ended.Add(1)
	}
	r, ch := x.enqueue("a")
	x.start()
	evs, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)

	requireEnded(t, end, RunCompleted, Reason{})
	if end.StartedAt == nil || end.RunID != r.RunID {
		t.Errorf("end = %+v", end)
	}
	for i, p := range end.Phases {
		if p.Status != PhaseCompleted || p.Result == nil || p.Result.Requests != 290+int64(i) ||
			p.Result.HTTPErrors != int64(i) || p.Result.P95Ms != 250.5 || p.StartedAt == nil || p.EndedAt == nil {
			t.Errorf("phase %d = %+v result %+v", i, p, p.Result)
		}
	}
	want := []string{"0:running", "0:completed", "1:running", "1:completed", "2:running", "2:completed"}
	if got := phaseTrail(evs); !slices.Equal(got, want) {
		t.Errorf("phase events %v; want %v", got, want)
	}

	calls := x.f.callList("")
	enable, start := indexOfPrefix(calls, "rules.EnableIfAttached "), indexOfPrefix(calls, "orch.StartExperiment ")
	disable := indexOfPrefix(calls, "rules.SetEnabled ")
	if enable < 0 || start < 0 || disable < 0 || !(enable < start && start < disable) {
		t.Errorf("enable at %d, start at %d, disable at %d; want enable < start < disable", enable, start, disable)
	}
	if got := expAtDisable.Load(); got != "completed" {
		t.Errorf("experiment was %v when the rule was switched off; want completed", got)
	}
	if n := len(x.f.callList("orch.CancelExperiment")); n != 0 {
		t.Errorf("%d cancels; want none", n)
	}
	if n := len(x.q.Snapshot()); n != 0 {
		t.Errorf("%d runs still in line", n)
	}
	if v, err := x.w.View(context.Background(), r.RunID); err != nil || v.Status != RunCompleted {
		t.Errorf("View = %+v, %v", v, err)
	}
	if n := len(x.f.callList("rules.SetEnabled ")); n != 1 || x.w.SwitchOffPending() {
		t.Errorf("%d switch-off writes, pending %v; want exactly 1 and none pending", n, x.w.SwitchOffPending())
	}
	if id, _, _ := x.w.Current(); id != "" {
		t.Errorf("Current() = %q after the run ended", id)
	}
	x.stop()
	requireNoMore(t, ch)
	if ended.Load() != 1 || !hadDeadline.Load() {
		t.Errorf("OnRunEnded: %d calls, deadline %v; want 1, true", ended.Load(), hadDeadline.Load())
	}
}

// TestWorker_PreflightRefusals: each refusal ends the run failed with its
// reason at once, creates nothing, never calls the orchestrator and frees
// the slot.
func TestWorker_PreflightRefusals(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(f *fakeLab)
		want  Reason
	}{
		{"zeus unhealthy", func(f *fakeLab) { f.fail("zeus.Healthy", errLeaky) }, ReasonLoadGeneratorUnavailable},
		{"no dataset", func(f *fakeLab) { f.datasets = nil }, ReasonLoadGeneratorUnavailable},
		{"datasets unreadable", func(f *fakeLab) { f.fail("zeus.ListDatasets", errLeaky) }, ReasonLoadGeneratorUnavailable},
		{"only a stale instance", func(f *fakeLab) {
			f.instances = []*model.SDKInstance{{ID: "inst-s", Service: TargetService, Status: "stale"}}
		}, ReasonTargetUnavailable},
		{"no instance", func(f *fakeLab) { f.instances = nil }, ReasonTargetUnavailable},
		{"instances unreadable", func(f *fakeLab) { f.fail("instances.InstancesForService", errLeaky) }, ReasonTargetUnavailable},
		{"another experiment running", func(f *fakeLab) {
			f.experiments["exp-op"] = &model.Experiment{ID: "exp-op", Name: "op", Status: "running",
				CreatedBy: "lab-smoke", CreatedAt: testNow}
		}, ReasonLabBusy},
		{"running experiments unreadable", func(f *fakeLab) { f.fail("experiments.RunningExperimentIDs", errLeaky) }, ReasonInternalError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newHarness(t, nil)
			tc.setup(x.f)
			_, ch := x.enqueue("a")
			x.start()
			_, end := awaitEnd(t, ch)
			took := time.Since(x.began)
			x.f.requireDemoRulesOff(t)
			requireEnded(t, end, RunFailed, tc.want)
			if took > 50*time.Millisecond {
				t.Errorf("ended after %s; want within 50 ms", took)
			}
			if end.StartedAt != nil {
				t.Errorf("a refused run has started_at %v", end.StartedAt)
			}
			for _, op := range []string{"experiments.Create", "rules.Create", "orch."} {
				if c := x.f.callList(op); len(c) != 0 {
					t.Errorf("calls %v; want none", c)
				}
			}
			if left := x.f.leftovers(); len(left) != 0 && tc.want != ReasonLabBusy {
				t.Errorf("left behind: %v", left)
			}
			if n := len(x.q.Snapshot()); n != 0 {
				t.Errorf("%d runs in line; want the slot freed", n)
			}
			if _, err := x.q.Enqueue("a", testNow); err != nil {
				t.Errorf("same client after the refusal: %v", err)
			}
		})
	}
}

// TestPreflight_OwnExperimentIsNotBusy: the caller's own running experiment
// does not make the lab busy.
func TestPreflight_OwnExperimentIsNotBusy(t *testing.T) {
	f := newFakeLab(t, testNow)
	f.experiments["exp-own"] = &model.Experiment{ID: "exp-own", Name: "own", Status: "running", CreatedBy: CreatedBy}
	if reason, ok := Preflight(context.Background(), f.deps(), "exp-own"); !ok || !reason.IsZero() {
		t.Fatalf("Preflight = %q, %v; want ok", reason, ok)
	}
	if reason, ok := Preflight(context.Background(), f.deps(), ""); ok || reason != ReasonLabBusy {
		t.Fatalf("Preflight without own id = %q, %v; want lab_busy", reason, ok)
	}
}

// ---- endings ----

// TestWorker_PhaseFailed: a failed experiment ends the run failed with
// phase_failed; the rule goes off and nothing is cancelled.
func TestWorker_PhaseFailed(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	r, ch := x.enqueue("a")
	x.start()
	awaitPhase(t, ch, 0, PhaseRunning)
	exp := x.expID(r.RunID)
	phases := x.f.phasesOf(exp)
	x.f.setPhaseStatus(phases[0].ID, "failed")
	x.f.endExperiment(exp, "failed", testNow, testNow.Add(time.Minute))
	evs, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunFailed, ReasonPhaseFailed)
	if got := phaseTrail(evs); !slices.Contains(got, "0:failed") || !slices.Contains(got, "1:skipped") {
		t.Errorf("phase events %v; want 0:failed and 1:skipped", got)
	}
	if n := len(x.f.callList("orch.CancelExperiment")); n != 0 {
		t.Errorf("%d cancels; want none", n)
	}
}

// TestWorker_ExternalCancel: an experiment cancelled by someone else ends the
// run cancelled/interrupted without a cancel of the worker's own.
func TestWorker_ExternalCancel(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	r, ch := x.enqueue("a")
	x.start()
	awaitPhase(t, ch, 0, PhaseRunning)
	x.f.endExperiment(x.expID(r.RunID), "cancelled", testNow, testNow.Add(time.Minute))
	_, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunCancelled, ReasonInterrupted)
	if n := len(x.f.callList("orch.CancelExperiment")); n != 0 {
		t.Errorf("%d cancels; want none", n)
	}
}

// TestWorker_WatchdogWhileStartBlocks: the start blocks past the cap. The
// rule goes off at the cap, the cancel waits for the start to return, and a
// start that then succeeds is still cancelled.
func TestWorker_WatchdogWhileStartBlocks(t *testing.T) {
	t.Parallel()
	x := newHarness(t, nil)
	release := make(chan struct{})
	var released atomic.Bool
	x.f.onCall("orch.StartExperiment", func() { <-release })
	var cancelsBefore, cancelsAfter atomic.Int32
	x.f.onCall("orch.CancelExperiment", func() {
		if released.Load() {
			cancelsAfter.Add(1)
		} else {
			cancelsBefore.Add(1)
		}
	})
	ruleOn := offBeforeCancel(x.f)
	var offAt atomic.Int64
	x.f.onCall("rules.SetEnabled", func() { offAt.CompareAndSwap(0, int64(time.Since(x.began))) })
	r, ch := x.enqueue("a")
	x.start()

	waitFor(t, "the switch-off", func() bool { return offAt.Load() != 0 })
	at := time.Duration(offAt.Load())
	if at < x.cfg.RunCap || at > x.cfg.RunCap+50*time.Millisecond {
		t.Errorf("rule switched off %s after start; want within 50 ms of the %s cap", at, x.cfg.RunCap)
	}
	rule := x.f.ruleIDOfRun(r.RunID)
	if !x.f.ruleEnabledEver(rule) {
		t.Fatal("the rule was never switched on")
	}
	waitFor(t, "the rule to be off", func() bool { return !x.f.rule(rule).Enabled })
	x.f.requireDemoRulesOff(t)
	time.Sleep(30 * time.Millisecond) // the worker is now waiting for the start
	if n := cancelsBefore.Load() + cancelsAfter.Load(); n != 0 {
		t.Fatalf("%d cancels while the start was still blocked", n)
	}
	released.Store(true)
	close(release)

	_, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunCancelled, ReasonTimedOut)
	if cancelsBefore.Load() != 0 || cancelsAfter.Load() != 1 {
		t.Errorf("cancels before release %d, after %d; want 0, 1", cancelsBefore.Load(), cancelsAfter.Load())
	}
	requireOffBeforeCancel(t, x.f, rule, ruleOn)
	if st := x.f.experimentStatus(x.expID(r.RunID)); st != "cancelled" {
		t.Errorf("experiment %s; want cancelled after the late start", st)
	}
	x.stop()
	requireNoMore(t, ch)
}

// requireOffBeforeCancel: the rule's first switch-off precedes the first
// cancel in the call log, and no demo rule was on at any cancel.
func requireOffBeforeCancel(t *testing.T, f *fakeLab, rule string, ruleOn *atomic.Int32) {
	t.Helper()
	calls := f.callList("")
	off := slices.Index(calls, "rules.SetEnabled "+rule+" false")
	cancel := indexOfPrefix(calls, "orch.CancelExperiment ")
	if off < 0 || cancel < 0 || off > cancel {
		t.Errorf("switch-off at %d, cancel at %d; want the switch-off first", off, cancel)
	}
	if ruleOn.Load() != 0 {
		t.Error("a demo rule was still on when the cancel was issued")
	}
}

// TestWorker_CancelErrorStillDisablesRule: the watchdog's cancel fails while
// the fault phase runs; the rule is off, the run ends cancelled/timed_out and
// the queue is empty.
func TestWorker_CancelErrorStillDisablesRule(t *testing.T) {
	t.Parallel()
	x := newHarness(t, nil)
	x.f.fail("orch.CancelExperiment", errLeaky)
	ruleOn := offBeforeCancel(x.f)
	r, ch := x.enqueue("a")
	x.start()
	awaitPhase(t, ch, 0, PhaseRunning)
	phases := x.f.phasesOf(x.expID(r.RunID))
	x.f.setPhaseStatus(phases[0].ID, "completed")
	x.f.setPhaseStatus(phases[1].ID, "running")
	rule := x.f.ruleIDOfRun(r.RunID)
	if !x.f.served(rule) {
		t.Fatal("the fault phase runs but its rule is not served")
	}
	_, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunCancelled, ReasonTimedOut)
	if n := len(x.f.callList("orch.CancelExperiment")); n != 1 {
		t.Errorf("%d cancels; want 1", n)
	}
	requireOffBeforeCancel(t, x.f, rule, ruleOn)
	if n := len(x.q.Snapshot()); n != 0 {
		t.Errorf("%d runs in line; want none", n)
	}
	if st := x.f.experimentStatus(x.expID(r.RunID)); st != "running" {
		t.Errorf("experiment %s; the failed cancel should have left it running", st)
	}
}

// TestWorker_StartNeverReturns: a start that never returns cannot hold the
// worker: the run ends after the cap plus the teardown wait, the slot frees,
// and the cancel follows once the start returns, exactly once.
func TestWorker_StartNeverReturns(t *testing.T) {
	t.Parallel()
	x := newHarness(t, nil)
	release := make(chan struct{})
	x.f.onCall("orch.StartExperiment", func() { <-release })
	r, ch := x.enqueue("a")
	x.start()
	_, end := awaitEnd(t, ch)
	took := time.Since(x.began)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunCancelled, ReasonTimedOut)
	if took > 2*x.cfg.RunCap+100*time.Millisecond {
		t.Errorf("ended after %s; want within cap + teardown", took)
	}
	if n := len(x.q.Snapshot()); n != 0 {
		t.Errorf("%d runs in line", n)
	}
	if n := len(x.f.callList("orch.CancelExperiment")); n != 0 {
		t.Fatalf("%d cancels before the start returned", n)
	}
	close(release)
	waitFor(t, "the late cancel", func() bool { return len(x.f.callList("orch.CancelExperiment")) == 1 })
	x.stop()
	if n := len(x.f.callList("orch.CancelExperiment")); n != 1 {
		t.Errorf("%d cancels; want 1", n)
	}
	if st := x.f.experimentStatus(x.expID(r.RunID)); st != "cancelled" {
		t.Errorf("experiment %s; want cancelled", st)
	}
	x.f.requireDemoRulesOff(t)
}

// TestWorker_ShutdownDuringTeardown: a shutdown while an ending waits for a
// start that does not return lets Run return at once; the rule is already off
// and the cancel still follows the start.
func TestWorker_ShutdownDuringTeardown(t *testing.T) {
	t.Parallel()
	x := newHarness(t, func(c *Config) { c.RunCap = 100 * time.Millisecond })
	release := make(chan struct{})
	x.f.onCall("orch.StartExperiment", func() { <-release })
	var offAt atomic.Int64
	x.f.onCall("rules.SetEnabled", func() { offAt.CompareAndSwap(0, int64(time.Since(x.began))) })
	r, ch := x.enqueue("a")
	x.start()
	waitFor(t, "the watchdog's switch-off", func() bool { return offAt.Load() != 0 })
	time.Sleep(20 * time.Millisecond) // the ending now waits for the start
	stopped := time.Now()
	x.cancel()
	waitClosed(t, x.w.done, "Run to return")
	if took := time.Since(stopped); took > 50*time.Millisecond {
		t.Errorf("Run returned %s after the shutdown; want at once", took)
	}
	_, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunCancelled, ReasonTimedOut)
	close(release)
	waitFor(t, "the late cancel", func() bool { return len(x.f.callList("orch.CancelExperiment")) == 1 })
	if st := x.f.experimentStatus(x.expID(r.RunID)); st != "cancelled" {
		t.Errorf("experiment %s; want cancelled", st)
	}
}

// TestWorker_StartError: a start that fails ends the run failed; the rule is
// off before the cancel, which ends what the start may have left.
func TestWorker_StartError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		inject func(f *fakeLab)
		want   Reason
	}{
		{"error", func(f *fakeLab) { f.fail("orch.StartExperiment", errLeaky) }, ReasonInternalError},
		{"service overlap", func(f *fakeLab) {
			f.fail("orch.StartExperiment", &orchestrator.ErrServiceOverlap{ExperimentID: "exp-1", RunningExperimentID: "exp-2"})
		}, ReasonLabBusy},
		{"panic", func(f *fakeLab) { f.onCall("orch.StartExperiment", func() { panic("start exploded at exp-123") }) }, ReasonInternalError},
		{"claimed, then failed", func(f *fakeLab) { f.failAfterCommit("orch.StartExperiment", errLeaky) }, ReasonInternalError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newHarness(t, noCap)
			tc.inject(x.f)
			ruleOn := offBeforeCancel(x.f)
			r, ch := x.enqueue("a")
			x.start()
			_, end := awaitEnd(t, ch)
			x.f.requireDemoRulesOff(t)
			requireEnded(t, end, RunFailed, tc.want)
			if n := len(x.f.callList("orch.CancelExperiment")); n != 1 {
				t.Errorf("%d cancels; want 1", n)
			}
			if ruleOn.Load() != 0 {
				t.Error("the rule was still on when the cancel was issued")
			}
			if st := x.f.experimentStatus(x.expID(r.RunID)); st != "cancelled" {
				t.Errorf("experiment %s; want cancelled", st)
			}
		})
	}
}

// TestWorker_BuildErrors: build failures map to their reasons and start
// nothing.
func TestWorker_BuildErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		inject func(x *harness)
		status RunStatus
		want   Reason
	}{
		{"no workflow", func(x *harness) { x.f.workflows = map[string]bool{} }, RunFailed, ReasonLoadGeneratorUnavailable},
		{"write fails", func(x *harness) { x.f.fail("experiments.Create", errLeaky) }, RunFailed, ReasonInternalError},
		{"shutdown during the build", func(x *harness) {
			x.f.onCall("experiments.Create", func() { x.cancel() })
		}, RunCancelled, ReasonInterrupted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newHarness(t, noCap)
			tc.inject(x)
			_, ch := x.enqueue("a")
			x.start()
			_, end := awaitEnd(t, ch)
			x.f.requireDemoRulesOff(t)
			requireEnded(t, end, tc.status, tc.want)
			if c := x.f.callList("orch."); len(c) != 0 {
				t.Errorf("orchestrator calls %v; want none", c)
			}
			if c := x.f.callList("rules.EnableIfAttached"); len(c) != 0 {
				t.Errorf("switch-on calls %v; want none", c)
			}
			if left := x.f.leftovers(); len(left) != 0 {
				t.Errorf("left behind: %v", left)
			}
		})
	}
}

// TestWorker_SwitchOnRefusedWhenUnattached: the store refuses to enable a rule
// no phase links; the run fails internal_error, nothing starts, the rule stays
// off and the build's rows are left for the start-up sweep.
func TestWorker_SwitchOnRefusedWhenUnattached(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	x.f.onCall("rules.EnableIfAttached", func() {
		x.f.mu.Lock()
		clear(x.f.phaseRules)
		x.f.mu.Unlock()
	})
	r, ch := x.enqueue("a")
	x.start()
	_, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunFailed, ReasonInternalError)
	if c := x.f.callList("orch."); len(c) != 0 {
		t.Errorf("orchestrator calls %v; want none", c)
	}
	rule := x.f.ruleIDOfRun(r.RunID)
	if rule == "" || x.f.ruleEnabledEver(rule) {
		t.Errorf("rule %q: enabled ever %v; want it kept and never on", rule, x.f.ruleEnabledEver(rule))
	}
	if st := x.f.experimentStatus(x.expID(r.RunID)); st != "planned" {
		t.Errorf("experiment %s; want planned, left for the sweep", st)
	}
}

// TestWorker_WatchdogDuringSwitchOn: the watchdog fires while the switch-on is
// in flight; the rule is never enabled and nothing starts.
func TestWorker_WatchdogDuringSwitchOn(t *testing.T) {
	t.Parallel()
	x := newHarness(t, nil)
	x.f.onCall("rules.Get", func() {
		x.w.mu.Lock()
		a := x.w.cur
		x.w.mu.Unlock()
		<-a.watchdog
	})
	r, ch := x.enqueue("a")
	x.start()
	_, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunCancelled, ReasonTimedOut)
	if rule := x.f.ruleIDOfRun(r.RunID); rule == "" || x.f.ruleEnabledEver(rule) {
		t.Errorf("rule %q was switched on after the watchdog fired", rule)
	}
	if c := x.f.callList("orch."); len(c) != 0 {
		t.Errorf("orchestrator calls %v; want none", c)
	}
}

// TestWorker_HungStatusRead: a status read that hangs does not stop the
// watchdog; one that honours its deadline ends the run at the cap, one that
// ignores it still has its rule switched off at the cap.
func TestWorker_HungStatusRead(t *testing.T) {
	t.Parallel()
	t.Run("honours its deadline", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, func(c *Config) { c.QueryTimeout = 20 * time.Millisecond })
		x.f.hang("experiments.Get")
		_, ch := x.enqueue("a")
		x.start()
		_, end := awaitEnd(t, ch)
		took := time.Since(x.began)
		x.f.requireDemoRulesOff(t)
		requireEnded(t, end, RunCancelled, ReasonTimedOut)
		if took > x.cfg.RunCap+200*time.Millisecond {
			t.Errorf("ended after %s; want shortly after the %s cap", took, x.cfg.RunCap)
		}
	})
	t.Run("ignores its deadline", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, nil)
		release := make(chan struct{})
		x.f.onCall("experiments.Get", func() { <-release })
		var offAt atomic.Int64
		x.f.onCall("rules.SetEnabled", func() { offAt.CompareAndSwap(0, int64(time.Since(x.began))) })
		r, ch := x.enqueue("a")
		x.start()
		waitFor(t, "the rule to be switched on", func() bool {
			id := x.f.ruleIDOfRun(r.RunID)
			return id != "" && x.f.ruleEnabledEver(id)
		})
		waitFor(t, "the switch-off", func() bool { return offAt.Load() != 0 })
		if at := time.Duration(offAt.Load()); at < x.cfg.RunCap || at > x.cfg.RunCap+50*time.Millisecond {
			t.Errorf("rule switched off %s after start; want within 50 ms of the %s cap", at, x.cfg.RunCap)
		}
		waitFor(t, "the rule to be off", func() bool { return !x.f.rule(x.f.ruleIDOfRun(r.RunID)).Enabled })
		x.f.requireDemoRulesOff(t)
		close(release)
		_, end := awaitEnd(t, ch)
		x.f.requireDemoRulesOff(t)
		requireEnded(t, end, RunCancelled, ReasonTimedOut)
	})
}

// TestWorker_UnknownCoreStatus: a phase or experiment status the contract does
// not know ends the run failed/internal_error, rule off first, and the status
// never reaches a client.
func TestWorker_UnknownCoreStatus(t *testing.T) {
	t.Parallel()
	for _, which := range []string{"phase", "experiment"} {
		t.Run(which, func(t *testing.T) {
			t.Parallel()
			x := newHarness(t, noCap)
			ruleOn := offBeforeCancel(x.f)
			r, ch := x.enqueue("a")
			x.start()
			awaitPhase(t, ch, 0, PhaseRunning)
			exp := x.expID(r.RunID)
			if which == "phase" {
				x.f.setPhaseStatus(x.f.phasesOf(exp)[0].ID, "bogus-exp-123")
			} else {
				x.f.setExperimentStatus(exp, "weird-exp-123")
			}
			evs, end := awaitEnd(t, ch)
			x.f.requireDemoRulesOff(t)
			requireEnded(t, end, RunFailed, ReasonInternalError)
			for _, ev := range evs {
				b, err := json.Marshal(ev.Data)
				if err != nil {
					t.Fatalf("%s event does not marshal: %v", ev.Type, err)
				}
				if strings.Contains(string(b), "bogus") || strings.Contains(string(b), "weird") {
					t.Errorf("%s event carries the core status: %s", ev.Type, b)
				}
			}
			if n := len(x.f.callList("orch.CancelExperiment")); n != 1 {
				t.Errorf("%d cancels; want 1", n)
			}
			if ruleOn.Load() != 0 {
				t.Error("the rule was still on when the cancel was issued")
			}
		})
	}
}

// TestWorker_ExperimentGone: an experiment deleted under a running run (its
// enabled rule is then attached to nothing, which the fake records) ends the
// run at the next poll with the rule switched off.
func TestWorker_ExperimentGone(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	x.f.expectViolations()
	r, ch := x.enqueue("a")
	x.start()
	awaitPhase(t, ch, 0, PhaseRunning)
	if err := (fakeExperiments{x.f}).Delete(context.Background(), x.expID(r.RunID)); err != nil {
		t.Fatal(err)
	}
	_, end := awaitEnd(t, ch)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunFailed, ReasonInternalError)
}

// TestWorker_PanicsAreContained: a panic in preflight, in a status read and in
// OnRunEnded each ends only its own run (rule off, failed/internal_error); the
// worker lives on and completes the next run.
func TestWorker_PanicsAreContained(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	x.f.autopilot = true
	x.f.onCall("zeus.Healthy#1", func() { panic("preflight exploded") })
	x.f.onCall("experiments.Get#1", func() { panic("status read exploded") })
	ruleOn := offBeforeCancel(x.f)
	var ended atomic.Int32
	x.w.OnRunEnded = func(context.Context) {
		if ended.Add(1) == 1 {
			panic("retention exploded")
		}
	}
	a, chA := x.enqueue("a")
	b, chB := x.enqueue("b")
	c, chC := x.enqueue("c")
	x.start()

	_, endA := awaitEnd(t, chA)
	x.f.requireRunRuleOff(t, a.RunID)
	requireEnded(t, endA, RunFailed, ReasonInternalError)

	_, endB := awaitEnd(t, chB)
	x.f.requireRunRuleOff(t, b.RunID)
	requireEnded(t, endB, RunFailed, ReasonInternalError)
	if n := len(x.f.callList("orch.CancelExperiment")); n != 1 {
		t.Errorf("%d cancels for the run whose read panicked; want 1", n)
	}
	if ruleOn.Load() != 0 {
		t.Error("the rule was still on when the cancel was issued")
	}

	_, endC := awaitEnd(t, chC)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, endC, RunCompleted, Reason{})
	if endC.RunID != c.RunID {
		t.Fatal("wrong run")
	}
	x.stop()
	if ended.Load() != 3 {
		t.Errorf("OnRunEnded called %d times; want 3", ended.Load())
	}
}

// TestWorker_EveryEndingFreesOnceAndEndsLast: three runs ending three ways
// (refused, start error, completed) each free their slot once, get exactly
// one end event, last, and move the runs behind them up.
func TestWorker_EveryEndingFreesOnceAndEndsLast(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	x.f.autopilot = true
	x.f.fail("zeus.Healthy#1", errLeaky)
	x.f.fail("orch.StartExperiment#1", errLeaky)
	a, chA := x.enqueue("a")
	b, chB := x.enqueue("b")
	c, chC := x.enqueue("c")
	if *a.Position != 0 || *b.Position != 1 || *c.Position != 2 {
		t.Fatalf("positions %d %d %d", *a.Position, *b.Position, *c.Position)
	}
	x.start()

	_, endA := awaitEnd(t, chA)
	x.f.requireRunRuleOff(t, a.RunID)
	requireEnded(t, endA, RunFailed, ReasonLoadGeneratorUnavailable)

	evsB, endB := awaitEnd(t, chB)
	x.f.requireRunRuleOff(t, b.RunID)
	requireEnded(t, endB, RunFailed, ReasonInternalError)

	evsC, endC := awaitEnd(t, chC)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, endC, RunCompleted, Reason{})
	if n := len(x.q.Snapshot()); n != 0 {
		t.Errorf("%d runs in line; want none", n)
	}
	// Exactly the three slots are free again (the worker takes the first of
	// these at once; it still counts).
	for _, cl := range []string{"a", "b", "c"} {
		if _, err := x.q.Enqueue(cl, testNow); err != nil {
			t.Errorf("client %s after its run ended: %v", cl, err)
		}
	}
	if _, err := x.q.Enqueue("d", testNow); !errors.Is(err, ErrQueueFull) {
		t.Errorf("fourth enqueue: %v; want ErrQueueFull", err)
	}
	x.stop()
	for _, ch := range []<-chan Event{chA, chB, chC} {
		requireNoMore(t, ch)
	}

	queueMoves := func(evs []Event) []int {
		var out []int
		for _, ev := range evs {
			if qe, ok := ev.Data.(QueueEvent); ok {
				out = append(out, qe.Position)
			}
		}
		return out
	}
	if got := queueMoves(evsB); !slices.Equal(got, []int{0}) {
		t.Errorf("run b moved to %v; want [0]", got)
	}
	if got := queueMoves(evsC); !slices.Equal(got, []int{1, 0}) {
		t.Errorf("run c moved to %v; want [1 0]", got)
	}
	if n := len(x.q.Snapshot()); n != 0 {
		t.Errorf("%d runs in line after the stop; want none", n)
	}
}

// TestWorker_ShutdownEndsRuns: stopping Run switches the running run's rule
// off without a cancel, ends it and every waiting run cancelled/interrupted,
// and returns.
func TestWorker_ShutdownEndsRuns(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	a, chA := x.enqueue("a")
	b, chB := x.enqueue("b")
	x.start()
	awaitPhase(t, chA, 0, PhaseRunning)
	if !x.f.ruleEnabledEver(x.f.ruleIDOfRun(a.RunID)) {
		t.Fatal("the rule was never switched on")
	}
	x.stop()
	_, endA := awaitEnd(t, chA)
	_, endB := awaitEnd(t, chB)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, endA, RunCancelled, ReasonInterrupted)
	requireEnded(t, endB, RunCancelled, ReasonInterrupted)
	if c := x.f.callList("orch.CancelExperiment"); len(c) != 0 {
		t.Errorf("cancels %v; want none on a shutdown", c)
	}
	if x.f.ruleIDOfRun(b.RunID) != "" {
		t.Error("the waiting run was built")
	}
	if n := len(x.q.Snapshot()); n != 0 {
		t.Errorf("%d runs in line; want none", n)
	}
}

// TestWorker_SwitchOffCurrentAndHalt: the pieces Shutdown is built from. The
// current run's rule goes off at once and stays off; halt then ends the run
// without a cancel and Run returns.
func TestWorker_SwitchOffCurrentAndHalt(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	a, ch := x.enqueue("a")
	x.start()
	awaitPhase(t, ch, 0, PhaseRunning)
	if err := x.w.switchOffCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	x.f.requireDemoRulesOff(t)
	x.w.mu.Lock()
	sw := x.w.cur.sw
	x.w.mu.Unlock()
	if err := sw.on(context.Background()); err == nil {
		t.Fatal("a switch-on after switchOffCurrent was accepted")
	}
	x.f.requireDemoRulesOff(t)
	x.w.halt()
	x.w.halt()
	waitClosed(t, x.w.done, "Run to return after halt")
	_, end := awaitEnd(t, ch)
	requireEnded(t, end, RunCancelled, ReasonInterrupted)
	x.f.requireRunRuleOff(t, a.RunID)
	if c := x.f.callList("orch.CancelExperiment"); len(c) != 0 {
		t.Errorf("cancels %v; want none", c)
	}
	if err := x.w.switchOffCurrent(context.Background()); err != nil {
		t.Errorf("switchOffCurrent with no run: %v", err)
	}
}

// TestWorker_PhaseEventsOnChangeOnly: core paused and draining read as
// running and publish nothing; each real change publishes once; Current
// follows the phases.
func TestWorker_PhaseEventsOnChangeOnly(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	r, ch := x.enqueue("a")
	x.start()
	evs := awaitPhase(t, ch, 0, PhaseRunning)
	exp := x.expID(r.RunID)
	ph := x.f.phasesOf(exp)
	if id, phase, running := x.w.Current(); id != r.RunID || phase != PhaseNames[0] || !running {
		t.Errorf("Current() = %q %q %v; want the run in phase plain, running", id, phase, running)
	}
	if got := x.w.currentExperimentID(); got != exp {
		t.Errorf("currentExperimentID() = %q", got)
	}
	for _, st := range []string{"paused", "draining", "running"} {
		x.f.setPhaseStatus(ph[0].ID, st)
		x.waitPolls(3)
	}
	x.f.setPhaseStatus(ph[0].ID, "completed")
	evs = append(evs, awaitPhase(t, ch, 0, PhaseCompleted)...)
	if id, phase, running := x.w.Current(); id != r.RunID || phase != PhaseNames[1] || running {
		t.Errorf("Current() between phases = %q %q %v; want the next phase, not running", id, phase, running)
	}
	x.f.setPhaseStatus(ph[1].ID, "running")
	evs = append(evs, awaitPhase(t, ch, 1, PhaseRunning)...)
	x.f.setPhaseStatus(ph[1].ID, "completed")
	x.f.setPhaseStatus(ph[2].ID, "running")
	evs = append(evs, awaitPhase(t, ch, 2, PhaseRunning)...)
	x.f.setPhaseStatus(ph[2].ID, "completed")
	x.f.endExperiment(exp, "completed", testNow, testNow.Add(2*time.Minute))
	more, end := awaitEnd(t, ch)
	evs = append(evs, more...)
	x.f.requireDemoRulesOff(t)
	requireEnded(t, end, RunCompleted, Reason{})
	want := []string{"0:running", "0:completed", "1:running", "1:completed", "2:running", "2:completed"}
	if got := phaseTrail(evs); !slices.Equal(got, want) {
		t.Errorf("phase events %v; want %v", got, want)
	}
}

// ---- a switch-off that fails ----

// syncBuffer is a log sink safe for concurrent writers.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// rebuild replaces the worker, before start, with one over the fake's deps as
// edit changes them.
func (x *harness) rebuild(edit func(*Deps)) {
	d := x.f.deps()
	edit(&d)
	x.w = NewWorker(d, x.cfg, x.q, x.h)
	x.w.offRetry = testOffRetry
}

// captureLogs rebuilds the worker with its log in a buffer.
func (x *harness) captureLogs() *syncBuffer {
	buf := &syncBuffer{}
	x.rebuild(func(d *Deps) { d.Logger = slog.New(slog.NewTextHandler(buf, nil)) })
	return buf
}

// TestWorker_FailedSwitchOffHoldsTheNextRun: the ending's switch-off fails three
// times (on the watchdog path, the watchdog's own try is the first). It is
// retried with backoff, the first failure logged at Warn with the rule and the
// attempt; SwitchOffPending holds meanwhile; the next run is not picked up
// until the switch-off has landed; then it runs, and pending clears.
func TestWorker_FailedSwitchOffHoldsTheNextRun(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"completed", "watchdog"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			cap := 10 * time.Second // the completed path must not race the watchdog
			if path == "watchdog" {
				cap = 150 * time.Millisecond
			}
			x := newHarness(t, func(c *Config) { c.RunCap = cap })
			logs := x.captureLogs()
			x.f.autopilot = path == "completed"
			for i := 1; i <= 3; i++ {
				x.f.fail(fmt.Sprintf("rules.SetEnabled#%d", i), errLeaky)
			}
			fourth, release := make(chan struct{}), make(chan struct{})
			x.f.onCall("rules.SetEnabled#4", func() { close(fourth); <-release })
			a, chA := x.enqueue("a")
			b, chB := x.enqueue("b")
			var ruleAOnAtB atomic.Int32
			x.f.onCall("zeus.Healthy#2", func() { // the next run's first call
				if r := x.f.rule(x.f.ruleIDOfRun(a.RunID)); r == nil || r.Enabled {
					ruleAOnAtB.Add(1)
				}
			})
			x.start()

			_, endA := awaitEnd(t, chA)
			want := RunCompleted
			if path == "watchdog" {
				want = RunCancelled
			}
			if endA.Status != want {
				t.Fatalf("run a ended %s; want %s", endA.Status, want)
			}
			waitClosed(t, fourth, "the fourth switch-off")
			if !x.w.SwitchOffPending() {
				t.Error("SwitchOffPending is false while the switch-off has not landed")
			}
			time.Sleep(20 * time.Millisecond)
			if n := len(x.f.callList("zeus.Healthy")); n != 1 || x.f.ruleIDOfRun(b.RunID) != "" {
				t.Fatalf("the next run was picked up while the switch-off was pending (%d preflights)", n)
			}
			if v, err := x.w.View(context.Background(), b.RunID); err != nil || v.Status != RunQueued || *v.Position != 0 {
				t.Errorf("waiting run = %+v, %v; want queued at 0", v, err)
			}
			close(release)

			waitFor(t, "the next run's pickup", func() bool { return x.f.ruleIDOfRun(b.RunID) != "" })
			if x.w.SwitchOffPending() {
				t.Error("SwitchOffPending still true after the switch-off landed")
			}
			if ruleAOnAtB.Load() != 0 {
				t.Error("the next run started while the ended run's rule was still on")
			}
			ruleA := x.f.ruleIDOfRun(a.RunID)
			calls := x.f.callList("")
			var offs []int
			for i, c := range calls {
				if c == "rules.SetEnabled "+ruleA+" false" {
					offs = append(offs, i)
				}
			}
			built := slices.Index(calls, "rules.Create "+RunNamePrefix+b.RunID)
			if len(offs) != 4 || built < 0 || built < offs[3] {
				t.Errorf("switch-offs of a at %v, run b built at %d; want 4 switch-offs, all before b", offs, built)
			}
			// The first failure is logged at Warn (on the watchdog path, the
			// watchdog's or the ending's, whichever wrote first), and no
			// other: the clock stands still, so no minute passes for a second
			// one. Then the success, at Info, with the attempt that landed.
			first := logLineHas(logs.String(), "level=WARN", "switching the run's rule off failed; retrying", "rule_id="+ruleA, "attempt=1")
			if path == "watchdog" && !first {
				first = logLineHas(logs.String(), "level=WARN", "watchdog could not switch", "rule_id="+ruleA, "attempt=1")
			}
			if !first {
				t.Errorf("no Warn line for the first failed switch-off with the rule id:\n%s", logs)
			}
			if n := countLines(logs.String(), "level=WARN", "rule_id="+ruleA, "attempt="); n != 1 {
				t.Errorf("%d Warn lines for the failed switch-offs; want 1, the first", n)
			}
			if n := countLines(logs.String(), "level=INFO", "rule switched off", "rule_id="+ruleA, "attempt=4"); n != 1 {
				t.Errorf("%d Info lines for the switch-off that landed at attempt 4; want 1:\n%s", n, logs)
			}
			if path == "completed" {
				_, endB := awaitEnd(t, chB)
				requireEnded(t, endB, RunCompleted, Reason{})
			}
			x.stop()
			if x.w.SwitchOffPending() {
				t.Error("SwitchOffPending after the stop")
			}
		})
	}
}

// countLines is how many lines of logs hold every part.
func countLines(logs string, parts ...string) int {
	n := 0
	for _, line := range strings.Split(logs, "\n") {
		if logLineHas(line, parts...) {
			n++
		}
	}
	return n
}

// logLineHas reports whether one line of logs holds every part.
func logLineHas(logs string, parts ...string) bool {
	for _, line := range strings.Split(logs, "\n") {
		all := true
		for _, p := range parts {
			if !strings.Contains(line, p) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// TestWorker_SwitchOffFailsForEver: the next run never starts; the retries go
// on, spaced out by the backoff; a shutdown returns at once, tries the pending
// rule itself, and leaves no goroutine behind; the next start's sweep then
// switches the rule off.
func TestWorker_SwitchOffFailsForEver(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	x.f.autopilot = true
	x.w.offRetry = backoff{first: 2 * time.Millisecond, max: 8 * time.Millisecond}
	var offTimes []time.Time
	var offMu sync.Mutex
	x.f.onCall("rules.SetEnabled", func() {
		offMu.Lock()
		offTimes = append(offTimes, time.Now())
		offMu.Unlock()
	})
	x.f.fail("rules.SetEnabled", errLeaky)
	_, chA := x.enqueue("a")
	b, chB := x.enqueue("b")
	x.start()
	awaitEnd(t, chA)
	waitFor(t, "six switch-off attempts", func() bool { return len(x.f.callList("rules.SetEnabled ")) >= 6 })
	if !x.w.SwitchOffPending() {
		t.Error("SwitchOffPending is false")
	}
	if x.f.ruleIDOfRun(b.RunID) != "" || len(x.f.callList("zeus.Healthy")) != 1 {
		t.Fatal("the next run was picked up with the ended run's rule still on")
	}
	offMu.Lock()
	gaps := make([]time.Duration, 0, len(offTimes))
	for i := 1; i < len(offTimes); i++ {
		gaps = append(gaps, offTimes[i].Sub(offTimes[i-1]))
	}
	offMu.Unlock()
	// The first write is the ending's own; then 2, 4, 8, 8 ms apart at least.
	for i, min := range []time.Duration{2, 4, 8, 8} {
		if i+1 < len(gaps) && gaps[i+1] < min*time.Millisecond {
			t.Errorf("retry gap %d is %s; want at least %s", i+1, gaps[i+1], min*time.Millisecond)
		}
	}

	before := len(x.f.callList("rules.SetEnabled "))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	began := time.Now()
	err := x.w.Shutdown(ctx)
	if took := time.Since(began); took > 200*time.Millisecond {
		t.Errorf("Shutdown took %s", took)
	}
	if !errors.Is(err, ErrRuleState) {
		t.Errorf("Shutdown: %v; want the pending switch-off's failure", err)
	}
	if len(x.f.callList("rules.SetEnabled ")) <= before {
		t.Error("Shutdown did not try the pending rule's switch-off")
	}
	waitClosed(t, x.w.done, "Run to return")
	bg := make(chan struct{})
	go func() { x.w.bg.Wait(); close(bg) }()
	waitClosed(t, bg, "the retry goroutine to end")
	_, endB := awaitEnd(t, chB)
	requireEnded(t, endB, RunCancelled, ReasonInterrupted)

	x.f.uninject("rules.SetEnabled")
	if err := Sweep(context.Background(), x.f.deps(), 20); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	x.f.requireDemoRulesOff(t)
}

// TestWorker_FirstSwitchOffSucceeds: no retry, no extra write, no wait. With
// a retry schedule of an hour, any wait would stall the next run.
func TestWorker_FirstSwitchOffSucceeds(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	x.f.autopilot = true
	x.w.offRetry = backoff{first: time.Hour, max: time.Hour}
	a, chA := x.enqueue("a")
	b, chB := x.enqueue("b")
	x.start()
	awaitEnd(t, chA)
	if x.w.SwitchOffPending() {
		t.Error("SwitchOffPending after a switch-off that succeeded")
	}
	_, endB := awaitEnd(t, chB)
	requireEnded(t, endB, RunCompleted, Reason{})
	for _, r := range []Run{a, b} {
		rule := x.f.ruleIDOfRun(r.RunID)
		if n := len(x.f.callList("rules.SetEnabled " + rule + " false")); n != 1 {
			t.Errorf("run %s: %d switch-off writes; want 1", r.RunID, n)
		}
	}
}

// TestWorker_PanicInEndStillFreesTheSlot: the clock panics inside the first
// run's ending, before its slot would be freed; the run still leaves the line
// with its end event, and the next run is picked up and completes.
func TestWorker_PanicInEndStillFreesTheSlot(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	x.f.autopilot = true
	x.f.fail("zeus.Healthy#1", errLeaky) // refused before any clock call: end's is the first
	x.rebuild(func(d *Deps) { d.Clock = &panicOnceClock{Clock: d.Clock} })
	a, chA := x.enqueue("a")
	_, chB := x.enqueue("b")
	x.start()
	_, endA := awaitEnd(t, chA)
	if endA.Status != RunFailed || endA.Reason != ReasonLoadGeneratorUnavailable || endA.Position != nil {
		t.Errorf("run a ended %+v", endA)
	}
	if v, err := x.w.View(context.Background(), a.RunID); err != nil || v.Status != RunFailed {
		t.Errorf("run a's view = %+v, %v", v, err)
	}
	_, endB := awaitEnd(t, chB)
	requireEnded(t, endB, RunCompleted, Reason{})
}

// panicOnceClock panics on its first call.
type panicOnceClock struct {
	Clock
	calls atomic.Int32
}

func (c *panicOnceClock) Now() time.Time {
	if c.calls.Add(1) == 1 {
		panic("clock exploded")
	}
	return c.Clock.Now()
}

// TestWorker_EndedRunNeverShownInLine: while runs end, a reader hammering
// Snapshot and View never sees an ended run in line, never sees one with a
// position, and never sees a run go back from ended.
func TestWorker_EndedRunNeverShownInLine(t *testing.T) {
	// Not parallel: its busy loops would starve the timing tests' timers
	// when GOMAXPROCS is 1.
	for round := 0; round < 10; round++ {
		x := newHarness(t, nil)
		x.f.fail("zeus.Healthy", errLeaky) // every run ends at once, refused
		var ids []string
		var chans []<-chan Event
		for _, c := range []string{"a", "b", "c"} {
			r, ch := x.enqueue(c)
			ids = append(ids, r.RunID)
			chans = append(chans, ch)
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		var bad atomic.Value
		wg.Add(1)
		go func() {
			defer wg.Done()
			ended := map[string]bool{}
			for {
				select {
				case <-stop:
					return
				default:
				}
				runtime.Gosched()
				for _, r := range x.q.Snapshot() {
					if r.Status != RunQueued && r.Status != RunRunning {
						bad.Store(fmt.Sprintf("snapshot holds run %s %s", r.RunID, r.Status))
					}
				}
				for _, id := range ids {
					v, err := x.w.View(context.Background(), id)
					if err != nil {
						bad.Store(fmt.Sprintf("view of %s: %v", id, err))
						continue
					}
					done := v.Status != RunQueued && v.Status != RunRunning
					switch {
					case done && v.Position != nil:
						bad.Store(fmt.Sprintf("ended run %s has position %d", id, *v.Position))
					case !done && ended[id]:
						bad.Store(fmt.Sprintf("run %s went back to %s", id, v.Status))
					}
					ended[id] = ended[id] || done
				}
			}
		}()
		x.start()
		for _, ch := range chans {
			awaitEnd(t, ch)
		}
		close(stop)
		wg.Wait()
		x.stop()
		if msg := bad.Load(); msg != nil {
			t.Fatalf("round %d: %s", round, msg)
		}
	}
}

// TestWorker_EnqueueRefusedOnceStopped: once the worker is halted, while its
// run is still ending, after Run returned, and after a Shutdown before Run,
// Enqueue answers ErrQueueClosed.
func TestWorker_EnqueueRefusedOnceStopped(t *testing.T) {
	t.Parallel()
	t.Run("halted, Run still ending", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, nil)
		entered, release := make(chan struct{}), make(chan struct{})
		x.f.onCall("experiments.Get#1", func() { close(entered); <-release })
		_, ch := x.enqueue("a")
		x.start()
		waitClosed(t, entered, "the worker to block in a read")
		x.w.halt()
		if _, err := x.q.Enqueue("b", testNow); !errors.Is(err, ErrQueueClosed) {
			t.Errorf("enqueue after halt: %v; want ErrQueueClosed", err)
		}
		select {
		case <-x.w.done:
			t.Fatal("Run returned while its worker was blocked")
		default:
		}
		close(release)
		awaitEnd(t, ch)
		waitClosed(t, x.w.done, "Run to return")
	})
	t.Run("after Run returned", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, nil)
		x.start()
		x.stop()
		if _, err := x.q.Enqueue("a", testNow); !errors.Is(err, ErrQueueClosed) {
			t.Errorf("enqueue after Run returned: %v; want ErrQueueClosed", err)
		}
	})
	t.Run("Shutdown before Run", func(t *testing.T) {
		t.Parallel()
		x := newHarness(t, nil)
		if err := x.w.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := x.q.Enqueue("a", testNow); !errors.Is(err, ErrQueueClosed) {
			t.Errorf("enqueue after Shutdown: %v; want ErrQueueClosed", err)
		}
	})
}

// TestWorker_EnqueueRacingStopIsNeverOrphaned: runs enqueued while the worker
// stops are each either refused (ErrQueueClosed) or ended with an end event;
// none is left in line. Subscribing after the end still gets the end.
func TestWorker_EnqueueRacingStopIsNeverOrphaned(t *testing.T) {
	// Not parallel: its busy loops would starve the timing tests' timers
	// when GOMAXPROCS is 1.
	for round := 0; round < 20; round++ {
		x := newHarness(t, func(c *Config) { c.Capacity = 10; c.RunCap = 10 * time.Second })
		var (
			mu       sync.Mutex
			accepted []string
			stop     = make(chan struct{})
			wg       sync.WaitGroup
		)
		for g := 0; g < 6; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					runtime.Gosched()
					r, err := x.q.Enqueue(fmt.Sprintf("c%d-%d", g, i), testNow)
					switch {
					case err == nil:
						mu.Lock()
						accepted = append(accepted, r.RunID)
						mu.Unlock()
					case errors.Is(err, ErrQueueClosed), errors.Is(err, ErrQueueFull):
					default:
						t.Errorf("enqueue: %v", err)
					}
				}
			}()
		}
		x.start()
		time.Sleep(time.Duration(round%5) * time.Millisecond)
		x.cancel()
		waitClosed(t, x.w.done, "Run to return")
		time.Sleep(2 * time.Millisecond) // enqueuers keep trying after the close
		close(stop)
		wg.Wait()
		x.stop()
		if n := len(x.q.Snapshot()); n != 0 {
			t.Fatalf("round %d: %d runs orphaned in line", round, n)
		}
		mu.Lock()
		ids := slices.Clone(accepted)
		mu.Unlock()
		for _, id := range ids {
			ch, unsub := x.h.Subscribe(id)
			ev, ok := recvWithin(t, ch)
			unsub()
			if !ok || ev.Type != EventEnd {
				t.Fatalf("round %d: run %s has no end event (got %q, open %v)", round, id, ev.Type, ok)
			}
			if v, err := x.w.View(context.Background(), id); err != nil || v.Status == RunQueued || v.Status == RunRunning {
				t.Fatalf("round %d: run %s = %s, %v; want ended", round, id, v.Status, err)
			}
		}
	}
}

// ---- views ----

// TestWorker_ViewAndRecent: views come from memory first, then from the
// database with the restart mapping. An id never accepted is ErrRunNotFound
// without a database read; a database failure is ErrRunUnavailable, never
// not-found; Recent on a database failure returns what memory has and says it
// is partial. Nothing internal reaches a view or an error.
func TestWorker_ViewAndRecent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	x := newHarness(t, noCap)
	x.f.autopilot = true
	r, ch := x.enqueue("a")
	queued, err := x.w.View(ctx, r.RunID)
	if err != nil || queued.Status != RunQueued || *queued.Position != 0 {
		t.Fatalf("queued view = %+v, %v", queued, err)
	}
	x.start()
	awaitEnd(t, ch)
	x.stop()
	listed := len(x.f.callList("experiments.List"))
	if v, err := x.w.View(ctx, r.RunID); err != nil || v.Status != RunCompleted {
		t.Fatalf("finished view = %+v, %v", v, err)
	}
	if n := len(x.f.callList("experiments.List")); n != listed {
		t.Errorf("an in-memory view read the database (%d List calls)", n-listed)
	}
	// Recent fills up from the database, but never lists a run twice.
	if got, complete := x.w.Recent(ctx, 10); len(got) != 1 || got[0].RunID != r.RunID || !complete {
		t.Fatalf("Recent = %+v, complete %v", got, complete)
	}
	if got, complete := x.w.Recent(ctx, 1); len(got) != 1 || !complete || len(x.f.callList("experiments.List")) != listed+1 {
		t.Errorf("Recent(1) with one run in memory read the database again")
	}

	// Older runs, as a previous manteion left them.
	older := map[string]string{}
	for i, st := range []string{"failed", "cancelled", "completed", "running"} {
		id := fmt.Sprintf("demo-0190c2f4-95b4-7cc3-8000-00000000000%d", i)
		built, err := BuildRun(ctx, x.f.deps(), id, testRunCap)
		if err != nil {
			t.Fatal(err)
		}
		if st == "running" {
			x.f.setExperimentStatus(built.ExperimentID, "running")
		} else {
			ended := testNow.Add(-time.Duration(i+1) * time.Minute)
			x.f.endExperiment(built.ExperimentID, st, ended.Add(-2*time.Minute), ended)
		}
		older[st] = id
	}

	// After a restart nothing is in memory.
	w2 := NewWorker(x.f.deps(), x.cfg, NewQueue(3), NewHub())
	cases := []struct {
		id     string
		status RunStatus
		reason Reason
	}{
		{r.RunID, RunCompleted, Reason{}},
		{older["failed"], RunFailed, ReasonPhaseFailed},
		{older["cancelled"], RunCancelled, ReasonInterrupted},
		{older["completed"], RunCompleted, Reason{}},
		{older["running"], RunCancelled, ReasonInterrupted},
	}
	for _, tc := range cases {
		v, err := w2.View(ctx, tc.id)
		if err != nil {
			t.Errorf("View(%s): %v", tc.id, err)
			continue
		}
		if v.RunID != tc.id || v.Status != tc.status || v.Reason != tc.reason || v.Position != nil {
			t.Errorf("View(%s) = %s/%q position %v; want %s/%q", tc.id, v.Status, v.Reason, v.Position, tc.status, tc.reason)
		}
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		assertNoLeak(t, string(b))
	}
	if v, _ := w2.View(ctx, r.RunID); v.Phases[2].Result == nil || v.Phases[2].Result.Requests != 292 ||
		v.Phases[2].Status != PhaseCompleted || v.StartedAt == nil || v.EndedAt == nil {
		t.Errorf("view from the database = %+v, last phase %+v", v, v.Phases[2])
	}

	recent, complete := w2.Recent(ctx, 10)
	var ids []string
	for _, v := range recent {
		ids = append(ids, v.RunID)
	}
	want := []string{r.RunID, older["failed"], older["cancelled"], older["completed"]}
	if !slices.Equal(ids, want) || !complete {
		t.Errorf("Recent = %v, complete %v; want %v (terminal runs, newest ended first), complete", ids, complete, want)
	}
	if got, _ := w2.Recent(ctx, 2); len(got) != 2 {
		t.Errorf("Recent(2) returned %d", len(got))
	}

	// Never accepted: not found, and no database read at all.
	listed = len(x.f.callList("experiments.List"))
	for _, bad := range []string{"", "nope", "exp-123", "demo-../../x", RunNamePrefix + r.RunID} {
		if v, err := w2.View(ctx, bad); !errors.Is(err, ErrRunNotFound) || v.RunID != "" {
			t.Errorf("View(%q) = %+v, %v; want ErrRunNotFound", bad, v, err)
		}
	}
	if n := len(x.f.callList("experiments.List")); n != listed {
		t.Errorf("malformed ids read the database %d times", n-listed)
	}
	if _, err := w2.View(ctx, "demo-0190c2f4-95b4-7cc3-8000-0000000000ff"); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("View of a well-formed unknown id: %v; want ErrRunNotFound", err)
	}

	// A database failure is unavailable, not not-found, and says nothing. (A
	// worker that has not read the run yet: w2 keeps the ended views it read.)
	w3 := NewWorker(x.f.deps(), x.cfg, NewQueue(3), NewHub())
	x.f.fail("experiments.List", errLeaky)
	v, err := w3.View(ctx, older["failed"])
	if !errors.Is(err, ErrRunUnavailable) || errors.Is(err, ErrRunNotFound) || v.RunID != "" {
		t.Errorf("View on a database error = %+v, %v; want ErrRunUnavailable", v, err)
	}
	if err != nil {
		assertNoLeak(t, err.Error())
	}
	x.f.uninject("experiments.List")
	x.f.fail("experiments.ListPhasesForExperiment", errLeaky)
	if _, err := w3.View(ctx, older["failed"]); !errors.Is(err, ErrRunUnavailable) {
		t.Errorf("View when the phases cannot be read: %v; want ErrRunUnavailable", err)
	}
	x.f.uninject("experiments.ListPhasesForExperiment")
	// The views w2 read stay as they were, without the database.
	x.f.fail("experiments.List", errLeaky)
	if v, err := w2.View(ctx, older["failed"]); err != nil || v.Status != RunFailed {
		t.Errorf("an ended run w2 read before, the database down: %+v, %v; want its kept view", v, err)
	}
	x.f.uninject("experiments.List")

	// Recent on a database failure: what memory has, marked partial.
	x.f.fail("experiments.List", errLeaky)
	if got, complete := w2.Recent(ctx, 10); len(got) != 0 || complete {
		t.Errorf("Recent on a database error, nothing in memory = %d runs, complete %v; want 0, partial", len(got), complete)
	}
	if got, complete := x.w.Recent(ctx, 10); len(got) != 1 || got[0].RunID != r.RunID || complete {
		t.Errorf("Recent on a database error = %d runs, complete %v; want the one in memory, partial", len(got), complete)
	}
	if got, complete := x.w.Recent(ctx, 1); len(got) != 1 || !complete {
		t.Errorf("Recent(1) served from memory = %d runs, complete %v; want 1, complete", len(got), complete)
	}
}

// ---- the rule switch ----

// TestRuleSwitch: once off, a switch refuses every switch-on; off always
// writes and may be called from any goroutine any number of times; an off
// waits for a switch-on in flight, so its write lands last; a rule attached
// after the switch closed is switched off at once.
func TestRuleSwitch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	build := func(t *testing.T) (*fakeLab, string) {
		f := newFakeLab(t, testNow)
		built, err := BuildRun(ctx, f.deps(), "demo-sw", testRunCap)
		if err != nil {
			t.Fatal(err)
		}
		return f, built.RuleID
	}

	t.Run("on after off is refused", func(t *testing.T) {
		f, rule := build(t)
		sw := newRuleSwitch("demo-sw", fakeRules{f}, quiet, f.clock)
		sw.attach(ctx, rule)
		if err := sw.off(ctx, offCompleted); err != nil {
			t.Fatal(err)
		}
		if err := sw.on(ctx); err == nil {
			t.Fatal("switch-on after off accepted")
		}
		if c := f.callList("rules.EnableIfAttached"); len(c) != 0 || f.ruleEnabledEver(rule) {
			t.Fatalf("switch-on reached the store: %v", c)
		}
	})

	t.Run("off is idempotent and concurrent", func(t *testing.T) {
		f, rule := build(t)
		sw := newRuleSwitch("demo-sw", fakeRules{f}, quiet, f.clock)
		sw.attach(ctx, rule)
		if err := sw.on(ctx); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := sw.off(ctx, offCompleted); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if n := len(f.callList("rules.SetEnabled " + rule + " false")); n != 10 {
			t.Errorf("%d switch-off writes; want 10", n)
		}
		f.requireDemoRulesOff(t)
	})

	t.Run("off waits for an on in flight", func(t *testing.T) {
		f, rule := build(t)
		sw := newRuleSwitch("demo-sw", fakeRules{f}, quiet, f.clock)
		sw.attach(ctx, rule)
		entered, release := make(chan struct{}), make(chan struct{})
		var enableReturned atomic.Bool
		f.onCall("rules.EnableIfAttached", func() { close(entered); <-release; enableReturned.Store(true) })
		var offOvertook atomic.Bool
		f.onCall("rules.SetEnabled", func() {
			if !enableReturned.Load() {
				offOvertook.Store(true)
			}
		})
		onDone := make(chan error, 1)
		go func() { onDone <- sw.on(ctx) }()
		<-entered
		offDone := make(chan error, 1)
		go func() { offDone <- sw.off(ctx, offCompleted) }()
		time.Sleep(20 * time.Millisecond)
		close(release)
		<-onDone
		if err := <-offDone; err != nil {
			t.Fatal(err)
		}
		if offOvertook.Load() {
			t.Error("the switch-off wrote while a switch-on was in flight")
		}
		f.requireDemoRulesOff(t)
	})

	t.Run("a rule attached after close is switched off", func(t *testing.T) {
		f, rule := build(t)
		sw := newRuleSwitch("demo-sw", fakeRules{f}, quiet, f.clock)
		if err := sw.off(ctx, offCompleted); err != nil {
			t.Fatal(err)
		}
		sw.attach(ctx, rule)
		if n := len(f.callList("rules.SetEnabled " + rule + " false")); n != 1 {
			t.Errorf("%d switch-offs of the late rule; want 1", n)
		}
		if err := sw.on(ctx); err == nil {
			t.Error("switch-on accepted")
		}
	})
}
