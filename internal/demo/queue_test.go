package demo

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestQueue_ConcurrentEnqueueForLastSlot: 64 distinct clients race for the
// last of three slots; exactly one wins.
func TestQueue_ConcurrentEnqueueForLastSlot(t *testing.T) {
	q := NewQueue(3)
	for _, c := range []string{"a", "b"} {
		if _, err := q.Enqueue(c, DefaultScenario(), testNow); err != nil {
			t.Fatal(err)
		}
	}
	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		wins, full, bad int
		start           = make(chan struct{})
	)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := q.Enqueue(fmt.Sprintf("client-%d", i), DefaultScenario(), testNow)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrQueueFull):
				full++
			default:
				bad++
			}
		}()
	}
	close(start)
	wg.Wait()
	if wins != 1 || full != 63 || bad != 0 {
		t.Fatalf("wins %d, queue_full %d, other %d; want 1, 63, 0", wins, full, bad)
	}
	if n := len(q.Snapshot()); n != 3 {
		t.Fatalf("%d entries; want 3", n)
	}
}

// TestQueue_OneRunPerClient: a client with a run queued or running gets
// ErrClientHasRun, even when the queue is also full; once its run has left
// the line it may enqueue again.
func TestQueue_OneRunPerClient(t *testing.T) {
	q := NewQueue(2)
	first, err := q.Enqueue("a", DefaultScenario(), testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("a", DefaultScenario(), testNow); !errors.Is(err, ErrClientHasRun) {
		t.Fatalf("second enqueue: %v; want ErrClientHasRun", err)
	}
	if _, err := q.Enqueue("b", DefaultScenario(), testNow); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("a", DefaultScenario(), testNow); !errors.Is(err, ErrClientHasRun) {
		t.Fatalf("full queue, same client: %v; want ErrClientHasRun", err)
	}
	if _, err := q.Enqueue("c", DefaultScenario(), testNow); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue, new client: %v; want ErrQueueFull", err)
	}
	// The worker taking the run does not free the client: running counts.
	if id, sc, ok := q.next(t.Context()); !ok || id != first.RunID || sc.ID != DefaultScenario().ID {
		t.Fatalf("next = %q, %q, %v; want %q of the default scenario", id, sc.ID, ok, first.RunID)
	}
	if _, err := q.Enqueue("a", DefaultScenario(), testNow); !errors.Is(err, ErrClientHasRun) {
		t.Fatalf("running run: %v; want ErrClientHasRun", err)
	}
	if _, ok := q.remove(first.RunID); !ok {
		t.Fatal("remove failed")
	}
	if _, err := q.Enqueue("a", DefaultScenario(), testNow); err != nil {
		t.Fatalf("after its run left the line: %v", err)
	}
}

// TestQueue_Closed: a closed queue refuses every enqueue first of all, keeps
// what waits for the worker to end, and next returns false at once.
func TestQueue_Closed(t *testing.T) {
	q := NewQueue(3)
	r, err := q.Enqueue("a", DefaultScenario(), testNow)
	if err != nil {
		t.Fatal(err)
	}
	q.closeLine()
	q.closeLine()
	for _, c := range []string{"a", "b"} {
		if _, err := q.Enqueue(c, DefaultScenario(), testNow); !errors.Is(err, ErrQueueClosed) {
			t.Errorf("enqueue %s on a closed queue: %v; want ErrQueueClosed", c, err)
		}
	}
	if w := q.waiting(); !slices.Equal(w, []string{r.RunID}) {
		t.Errorf("waiting = %v; want the run queued before the close", w)
	}
	if id, _, ok := q.next(t.Context()); ok {
		t.Errorf("next on a closed queue took %q", id)
	}
}

// TestQueue_Positions: position is the 0-based place in the line, 0 for the
// first run before the worker took it; removing the head moves the others up
// and reports their new positions; a second remove frees nothing.
func TestQueue_Positions(t *testing.T) {
	q := NewQueue(3)
	var ids []string
	for i, c := range []string{"a", "b", "c"} {
		r, err := q.Enqueue(c, DefaultScenario(), testNow)
		if err != nil {
			t.Fatal(err)
		}
		if r.Position == nil || *r.Position != i || r.Status != RunQueued {
			t.Fatalf("enqueue %d: position %v status %s; want %d queued", i, r.Position, r.Status, i)
		}
		ids = append(ids, r.RunID)
	}
	snap := q.Snapshot()
	for i, r := range snap {
		if r.RunID != ids[i] || r.Position == nil || *r.Position != i {
			t.Fatalf("snapshot[%d] = %s at %v; want %s at %d", i, r.RunID, r.Position, ids[i], i)
		}
	}
	if _, _, ok := q.next(t.Context()); !ok {
		t.Fatal("next failed")
	}
	moved, ok := q.remove(ids[0])
	if !ok {
		t.Fatal("remove failed")
	}
	want := []QueueEvent{{RunID: ids[1], Position: 0}, {RunID: ids[2], Position: 1}}
	if !slices.Equal(moved, want) {
		t.Fatalf("moved = %v; want %v", moved, want)
	}
	if _, ok := q.remove(ids[0]); ok {
		t.Fatal("a second remove of the same run succeeded")
	}
	if n := len(q.Snapshot()); n != 2 {
		t.Fatalf("%d entries after one remove; want 2", n)
	}
	if r, ok := q.get(ids[2]); !ok || *r.Position != 1 {
		t.Fatalf("get = %+v, %v; want position 1", r, ok)
	}
}

// TestQueue_RunView: a queued run's view is the contract's Run with its own
// scenario's id and phases (baseline with no fault, then memory and cpu with
// their fault, target and intensity), the fixed testbed and a run id of the
// demo's shape; the worker takes the run with that scenario, and every view
// of the run shows it.
func TestQueue_RunView(t *testing.T) {
	q := NewQueue(3)
	heavy, ok := ScenarioByID("cart-heavy")
	if !ok {
		t.Fatal("no cart-heavy in the catalog")
	}
	r, err := q.Enqueue("a", heavy, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if !validRunID(r.RunID) {
		t.Errorf("run id %q has not the demo's shape", r.RunID)
	}
	if r.Scenario != "cart-heavy" {
		t.Errorf("scenario = %q; want the queued one, cart-heavy", r.Scenario)
	}
	if snap := q.Snapshot(); len(snap) != 1 || snap[0].Scenario != "cart-heavy" {
		t.Errorf("snapshot = %+v; want the run of cart-heavy", snap)
	}
	if id, sc, ok := q.next(t.Context()); !ok || id != r.RunID || sc != heavy {
		t.Errorf("next = %q, %+v, %v; want the run with its scenario", id, sc, ok)
	}
	other, err := q.Enqueue("b", DefaultScenario(), testNow)
	if err != nil || other.Scenario != DefaultScenario().ID || *other.Phases[1].Target != "productcatalogservice" {
		t.Errorf("a run of the default scenario = %+v, %v", other, err)
	}
	str := func(p *string) any {
		if p == nil {
			return nil
		}
		return *p
	}
	wantFaults := [3][3]any{
		{nil, nil, nil},
		{"memory", "cartservice", "heavy"},
		{"cpu", "cartservice", "heavy"},
	}
	for i, p := range r.Phases {
		if p.Name != PhaseNames[i] || p.Status != PhasePending || p.DurationS != PhaseDurationS ||
			[3]any{str(p.Fault), str(p.Target), str(p.Intensity)} != wantFaults[i] {
			t.Errorf("phase %d = %+v; want %s pending with fault, target, intensity %v", i, p, PhaseNames[i], wantFaults[i])
		}
	}
	if r.Testbed != FixedTestbed() || !r.QueuedAt.Equal(testNow) || r.StartedAt != nil || r.EndedAt != nil {
		t.Errorf("run = %+v", r)
	}
	// Views are copies: changing one leaves the queue's alone.
	*r.Position = 7
	*r.Phases[1].Fault, *r.Phases[2].Target, *r.Phases[2].Intensity = "changed", "changed", "changed"
	again, _ := q.get(r.RunID)
	if *again.Position != 0 || *again.Phases[1].Fault != "memory" || *again.Phases[2].Target != "cartservice" ||
		*again.Phases[2].Intensity != "heavy" {
		t.Fatalf("the queue's view changed through a copy: %+v", again)
	}
	b, err := json.Marshal(again)
	if err != nil {
		t.Fatal(err)
	}
	assertNoLeak(t, string(b))

	for _, bad := range []string{"", "demo-", "exp-0190c2f4-95b4-7cc3-8000-000000000000",
		"demo-0190C2F4-95B4-7CC3-8000-000000000000", "demo-0190c2f4-95b4-7cc3-8000-00000000000",
		"demo-0190c2f4x95b4-7cc3-8000-000000000000", "demo-../../etc/passwd"} {
		if validRunID(bad) {
			t.Errorf("validRunID(%q) = true", bad)
		}
	}
}

// TestCloneRun_SharesNoPointer: a copy shares no pointer with its run, the
// fault phases' target, intensity, proof and measured figures included.
func TestCloneRun_SharesNoPointer(t *testing.T) {
	yes := true
	start, end := testNow, testNow
	r := Run{RunID: "demo-1", Scenario: "cart-heavy", Status: RunCompleted, Position: intp(0), QueuedAt: testNow,
		StartedAt: &start, EndedAt: &end, Testbed: FixedTestbed()}
	for i := range r.Phases {
		at := testNow
		r.Phases[i] = PhaseView{Name: fmt.Sprint("phase-", i), Fault: strp("cpu"), Target: strp("cartservice"),
			Intensity: strp("heavy"), Status: PhaseCompleted, StartedAt: &at, EndedAt: &at, DurationS: 40,
			Result: &Result{Requests: 1}, FaultObserved: &yes,
			Measure: &Measure{Metric: "target_cpu_cores", Value: f64p(0.2), Baseline: f64p(0.02)}}
	}
	before, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	c := cloneRun(r)
	*c.Position = 9
	for i := range c.Phases {
		p := &c.Phases[i]
		*p.Fault, *p.Target, *p.Intensity = "changed", "changed", "changed"
		*p.StartedAt = p.StartedAt.Add(time.Hour)
		p.Result.Requests = 9
		*p.FaultObserved = false
		*p.Measure.Value, *p.Measure.Baseline = 9, 9
		p.Measure.Metric = "target_memory_bytes"
	}
	if after, _ := json.Marshal(r); string(after) != string(before) {
		t.Errorf("the run changed through its copy:\n %s\nwas\n %s", after, before)
	}
}
