package demo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"manteion-go/internal/id"
)

// ---- helpers ----

// viewReads are the store reads a run lookup makes (Worker.View's database
// path): the listing that finds the experiment, its phases, their results.
var viewReads = []string{"experiments.List", "experiments.ListPhasesForExperiment", "experiments.ListWorkflowResultsForPhase"}

// storeGate holds every call of the operations it gates until it is
// released, and records how many calls it held at once.
type storeGate struct {
	open    chan struct{}
	once    sync.Once
	held    atomic.Int32
	maxHeld atomic.Int32
}

// gateStore gates ops of f; the test's cleanup releases the gate.
func gateStore(t *testing.T, f *fakeLab, ops ...string) *storeGate {
	t.Helper()
	g := &storeGate{open: make(chan struct{})}
	for _, op := range ops {
		f.onCall(op, func() {
			n := g.held.Add(1)
			for m := g.maxHeld.Load(); n > m && !g.maxHeld.CompareAndSwap(m, n); m = g.maxHeld.Load() {
			}
			<-g.open
			g.held.Add(-1)
		})
	}
	t.Cleanup(g.release)
	return g
}

func (g *storeGate) release() { g.once.Do(func() { close(g.open) }) }

// storeCalls is how many store, zeus, Prometheus and orchestrator calls f has
// recorded.
func storeCalls(f *fakeLab) int { return len(f.callList("")) }

// sweep runs the demo's start-up sweep, which lists every demo experiment.
func (x *demoHarness) sweep() {
	x.t.Helper()
	if err := x.d.Sweep(context.Background()); err != nil {
		x.t.Fatalf("Sweep: %v", err)
	}
}

// getAll sends one GET of target(i) per i in 0..n-1 at once, each as client
// c<i>, and returns the channel their answers arrive on.
func (x *demoHarness) getAll(n int, target func(i int) string) <-chan *httptest.ResponseRecorder {
	results := make(chan *httptest.ResponseRecorder, n)
	for i := 0; i < n; i++ {
		go func() { results <- x.get(target(i), fmt.Sprintf("c%02d", i)) }()
	}
	return results
}

// requireBounded checks n requests sent at once while the gate holds the
// store: every request but two is answered 503 unavailable while those two
// are still held in the store (no request waits for a slot or for the store),
// exactly two calls are held at once, and once the gate opens the two are
// answered with lastStatus.
func requireBounded(t *testing.T, g *storeGate, results <-chan *httptest.ResponseRecorder, n, lastStatus int) []*httptest.ResponseRecorder {
	t.Helper()
	for i := 0; i < n-2; i++ {
		select {
		case rec := <-results:
			requireError(t, rec, http.StatusServiceUnavailable, "unavailable")
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d requests answered while two lookups are held in the store: the rest wait", i, n-2)
		}
	}
	waitFor(t, "two lookups held in the store", func() bool { return g.held.Load() == 2 })
	g.release()
	var last []*httptest.ResponseRecorder
	for i := 0; i < 2; i++ {
		select {
		case rec := <-results:
			if rec.Code != lastStatus {
				t.Errorf("a lookup the store answered: %d %s; want %d", rec.Code, rec.Body, lastStatus)
			}
			last = append(last, rec)
		case <-time.After(3 * time.Second):
			t.Fatal("a lookup held in the store did not answer once the store did")
		}
	}
	if m := g.maxHeld.Load(); m != 2 {
		t.Errorf("%d store calls at once; want at most 2 (and 2 reached)", m)
	}
	return last
}

// ---- R2: an id that cannot exist costs nothing ----

// TestLookups_UnknownIDsCostNothing: once a listing has completed, 10 000
// distinct well-formed ids the demo does not know are 404 not_found, through
// the run route and the event stream route, without a single store call. A
// run the listing found is still read.
func TestLookups_UnknownIDsCostNothing(t *testing.T) {
	t.Parallel()
	x := newUnsweptDemo(t, nil)
	old := seed(t, x.f, "completed", false)
	x.sweep()
	before := storeCalls(x.f)
	const n, clients = 10000, 100
	for i := 0; i < n; i++ {
		target := pathRun(id.New(RunIDPrefix))
		if i%10 == 0 {
			target = pathEvents(id.New(RunIDPrefix))
		}
		rec := x.get(target, fmt.Sprintf("c%03d", i%clients))
		if rec.Code != http.StatusNotFound || rec.Body.String() != `{"error":"not_found"}` {
			t.Fatalf("GET %s: %d %s; want 404 not_found", target, rec.Code, rec.Body)
		}
	}
	if got := storeCalls(x.f) - before; got != 0 {
		t.Errorf("%d unknown ids made %d store calls; want 0", n, got)
	}
	if rec := x.get(pathRun(old.runID), "c000"); rec.Code != http.StatusOK {
		t.Errorf("a run the listing found: %d %s; want 200", rec.Code, rec.Body)
	}
}

// TestLookups_BoundedWhileListingIncomplete: before any listing has completed,
// and after a listing that failed, an unknown id is looked up in the store,
// but at most 2 lookups at once: with two held in the store, every other
// request is 503 unavailable at once; once the store answers, the two are 404.
func TestLookups_BoundedWhileListingIncomplete(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, x *demoHarness)
	}{
		{"before any listing", func(*testing.T, *demoHarness) {}},
		{"after a failed listing", func(t *testing.T, x *demoHarness) {
			x.sweep()
			x.f.fail("experiments.List", errLeaky)
			if err := x.d.w.Retain(context.Background()); err == nil {
				t.Fatal("Retain with the listing failing returned nil")
			}
			x.f.uninject("experiments.List")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newUnsweptDemo(t, nil)
			tc.prepare(t, x)
			g := gateStore(t, x.f, viewReads...)
			const n = 40
			results := x.getAll(n, func(int) string { return pathRun(id.New(RunIDPrefix)) })
			requireBounded(t, g, results, n, http.StatusNotFound)
		})
	}
}

// ---- R1 and R3: old runs ----

// TestLookups_KnownOldRunsBounded: concurrent reads of old runs the listing
// found reach the store at most 2 at once; every other read is 503 at once.
// Once read, an ended run is served from memory: a second round of concurrent
// reads of every old run makes no store call.
func TestLookups_KnownOldRunsBounded(t *testing.T) {
	t.Parallel()
	x := newUnsweptDemo(t, nil)
	var olds []seeded
	for _, st := range []string{"completed", "failed", "cancelled", "completed", "failed", "cancelled"} {
		olds = append(olds, seed(t, x.f, st, false))
	}
	x.sweep()
	g := gateStore(t, x.f, viewReads...)
	const n = 30
	target := func(i int) string { return pathRun(olds[i%len(olds)].runID) }
	requireBounded(t, g, x.getAll(n, target), n, http.StatusOK)

	for _, s := range olds {
		if rec := x.get(pathRun(s.runID), "seq"); rec.Code != http.StatusOK {
			t.Fatalf("old run %s: %d %s", s.runID, rec.Code, rec.Body)
		}
	}
	before := storeCalls(x.f)
	results := x.getAll(n, target)
	for i := 0; i < n; i++ {
		if rec := <-results; rec.Code != http.StatusOK {
			t.Errorf("an old run read again: %d %s; want 200", rec.Code, rec.Body)
		}
	}
	if got := storeCalls(x.f) - before; got != 0 {
		t.Errorf("reading old runs again made %d store calls; want 0", got)
	}
}

// TestLookups_OldRunReadOnce: an ended run read from the store is read once: a
// second read answers the same body with no store call. A run whose
// experiment has not ended (an earlier process left it running) is read again
// each time, since its view will change.
func TestLookups_OldRunReadOnce(t *testing.T) {
	t.Parallel()
	x := newUnsweptDemo(t, nil)
	ended := seed(t, x.f, "completed", false)
	x.sweep()
	before := storeCalls(x.f)
	first := x.get(pathRun(ended.runID), "a")
	if first.Code != http.StatusOK {
		t.Fatalf("first read: %d %s", first.Code, first.Body)
	}
	if storeCalls(x.f) == before {
		t.Fatal("the first read of an old run made no store call")
	}
	mid := storeCalls(x.f)
	second := x.get(pathRun(ended.runID), "a")
	if second.Code != http.StatusOK || second.Body.String() != first.Body.String() {
		t.Errorf("second read: %d %s; want 200 %s", second.Code, second.Body, first.Body)
	}
	if got := storeCalls(x.f) - mid; got != 0 {
		t.Errorf("the second read made %d store calls; want 0", got)
	}

	// Not ended in the store: no sweep has run in this demo, so it stays running.
	y := newUnsweptDemo(t, nil)
	left := seed(t, y.f, "running", false)
	for i := 0; i < 2; i++ {
		before := storeCalls(y.f)
		if rec := y.get(pathRun(left.runID), "a"); rec.Code != http.StatusOK {
			t.Fatalf("read %d of a run left running: %d %s", i+1, rec.Code, rec.Body)
		}
		if storeCalls(y.f) == before {
			t.Errorf("read %d of a run left running made no store call: its view was kept", i+1)
		}
	}
}

// TestLookups_RetentionForgetsRun: once retention has removed a run, its id is
// 404 without a store call, and its kept view is gone: when a later listing
// fails (so ids are looked up again), the id is looked up in the store and is
// 404, not served from memory.
func TestLookups_RetentionForgetsRun(t *testing.T) {
	t.Parallel()
	x := newUnsweptDemo(t, func(c *Config, _ *Deps) { c.Retain, c.Recent = 2, 2 })
	oldest := seed(t, x.f, "completed", false)
	kept := seed(t, x.f, "failed", false)
	x.sweep()
	if rec := x.get(pathRun(oldest.runID), "a"); rec.Code != http.StatusOK {
		t.Fatalf("oldest run: %d %s", rec.Code, rec.Body)
	}
	newer := seed(t, x.f, "cancelled", false)
	if err := x.d.w.Retain(context.Background()); err != nil {
		t.Fatalf("Retain: %v", err)
	}
	if x.f.hasExperiment(oldest.ExperimentID) {
		t.Fatal("retention kept the oldest run's experiment")
	}
	before := storeCalls(x.f)
	requireError(t, x.get(pathRun(oldest.runID), "a"), http.StatusNotFound, "not_found")
	if got := storeCalls(x.f) - before; got != 0 {
		t.Errorf("a run retention removed made %d store calls; want 0", got)
	}
	for _, s := range []seeded{kept, newer} {
		if rec := x.get(pathRun(s.runID), "a"); rec.Code != http.StatusOK {
			t.Errorf("run %s that retention kept: %d %s; want 200", s.runID, rec.Code, rec.Body)
		}
	}

	x.f.fail("experiments.List", errLeaky)
	_ = x.d.w.Retain(context.Background())
	x.f.uninject("experiments.List")
	before = storeCalls(x.f)
	requireError(t, x.get(pathRun(oldest.runID), "a"), http.StatusNotFound, "not_found")
	if storeCalls(x.f) == before {
		t.Error("with the listing incomplete, a removed run was answered from memory: its kept view was not dropped")
	}
}

// ---- R2: runs of this process ----

// TestLookups_MintedRunFoundWithoutListing: a run queued after the latest
// listing is found from its enqueue to its end and after, once it has left
// the worker's memory of ended runs too, although no listing has seen it.
func TestLookups_MintedRunFoundWithoutListing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	x := newHarness(t, func(c *Config) { noCap(c); c.Retain = 1 })
	if err := x.w.Retain(ctx); err != nil { // the listing: nothing exists yet
		t.Fatalf("Retain: %v", err)
	}
	listed := len(x.f.callList("experiments.List "))
	a, ach := x.enqueue("a")
	if v, err := x.w.View(ctx, a.RunID); err != nil || v.Status != RunQueued {
		t.Fatalf("queued: %+v, %v", v, err)
	}
	x.start()
	awaitPhase(t, ach, 0, PhaseRunning) // and it stays there until the autopilot is on
	if v, err := x.w.View(ctx, a.RunID); err != nil || v.Status != RunRunning {
		t.Fatalf("running: %+v, %v", v, err)
	}
	x.f.mu.Lock()
	x.f.autopilot = true
	x.f.mu.Unlock()
	awaitEnd(t, ach)
	if v, err := x.w.View(ctx, a.RunID); err != nil || v.Status != RunCompleted {
		t.Fatalf("ended: %+v, %v", v, err)
	}
	if n := len(x.f.callList("experiments.List ")); n != listed {
		t.Errorf("%d listings while the run was in memory; want none", n-listed)
	}
	// A second run ends: the first leaves the worker's memory (Retain 1).
	_, bch := x.enqueue("b")
	awaitEnd(t, bch)
	x.w.mu.Lock()
	inMemory := len(x.w.finished) == 1 && x.w.finished[0].RunID == a.RunID
	x.w.mu.Unlock()
	if inMemory {
		t.Fatal("the first run is still the one ended run in memory")
	}
	if v, err := x.w.View(ctx, a.RunID); err != nil || v.RunID != a.RunID || v.Status != RunCompleted {
		t.Errorf("after it left memory: %+v, %v; want it read from the store, completed", v, err)
	}
}

// TestLookups_IndexStaysBounded: across more runs than retention keeps, the
// run index holds no more than the runs retention keeps and the line, and
// every run that retention kept is still found.
func TestLookups_IndexStaysBounded(t *testing.T) {
	t.Parallel()
	x := newUnsweptDemo(t, func(c *Config, _ *Deps) { c.Retain, c.Recent = 3, 3 })
	x.f.autopilot = true
	x.sweep()
	x.start()
	var runs []string
	for i := 0; i < 8; i++ {
		runID := x.postOK(fmt.Sprintf("c%d", i))
		waitFor(t, "the run to end", func() bool {
			r, err := x.d.view(context.Background(), runID)
			return err == nil && runEnded(r.Status)
		})
		runs = append(runs, runID)
	}
	bound := x.d.cfg.Retain + x.d.cfg.Capacity
	waitFor(t, "the last retention pass", func() bool { ids, _ := x.d.w.runs.size(); return ids <= bound })
	if _, views := x.d.w.runs.size(); views > maxCachedViews {
		t.Errorf("%d views kept; want at most %d", views, maxCachedViews)
	}
	for _, runID := range runs[len(runs)-x.d.cfg.Retain:] {
		if rec := x.get(pathRun(runID), "check"); rec.Code != http.StatusOK {
			t.Errorf("run %s, kept by retention: %d %s", runID, rec.Code, rec.Body)
		}
	}
}

// ---- the event stream route ----

// TestLookups_EventsRoute: a stream opened on an id the demo does not know is
// 404 without a store call once a listing has completed; before one, the
// stream's lookup takes a slot like any other, and with both slots taken it is
// 503 unavailable at once, its stream slot released.
func TestLookups_EventsRoute(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{})
	x.sweep()
	before := storeCalls(x.f)
	requireError(t, x.get(pathEvents(id.New(RunIDPrefix)), "a"), http.StatusNotFound, "not_found")
	resp, _ := x.request(t, id.New(RunIDPrefix), "a")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown run through the server: %d; want 404", resp.StatusCode)
	}
	if got := storeCalls(x.f) - before; got != 0 {
		t.Errorf("streams on unknown ids made %d store calls; want 0", got)
	}

	y := newStreamDemo(t, streamOpts{})
	g := gateStore(t, y.f, viewReads...)
	held := y.getAll(2, func(int) string { return pathRun(id.New(RunIDPrefix)) })
	waitFor(t, "two lookups held in the store", func() bool { return g.held.Load() == 2 })
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- y.get(pathEvents(id.New(RunIDPrefix)), "s") }()
	select {
	case rec := <-done:
		requireError(t, rec, http.StatusServiceUnavailable, "unavailable")
	case <-time.After(3 * time.Second):
		t.Fatal("a stream's lookup waited for a slot")
	}
	if n := openStreamCount(y.d.lim); n != 0 {
		t.Errorf("%d stream slots held after a 503; want 0", n)
	}
	g.release()
	for i := 0; i < 2; i++ {
		requireError(t, <-held, http.StatusNotFound, "not_found")
	}
}
