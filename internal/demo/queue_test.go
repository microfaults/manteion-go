package demo

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
)

// TestQueue_ConcurrentEnqueueForLastSlot: 64 distinct clients race for the
// last of three slots; exactly one wins.
func TestQueue_ConcurrentEnqueueForLastSlot(t *testing.T) {
	q := NewQueue(3)
	for _, c := range []string{"a", "b"} {
		if _, err := q.Enqueue(c, testNow); err != nil {
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
			_, err := q.Enqueue(fmt.Sprintf("client-%d", i), testNow)
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
	first, err := q.Enqueue("a", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("a", testNow); !errors.Is(err, ErrClientHasRun) {
		t.Fatalf("second enqueue: %v; want ErrClientHasRun", err)
	}
	if _, err := q.Enqueue("b", testNow); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("a", testNow); !errors.Is(err, ErrClientHasRun) {
		t.Fatalf("full queue, same client: %v; want ErrClientHasRun", err)
	}
	if _, err := q.Enqueue("c", testNow); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue, new client: %v; want ErrQueueFull", err)
	}
	// The worker taking the run does not free the client: running counts.
	if id, ok := q.next(t.Context()); !ok || id != first.RunID {
		t.Fatalf("next = %q, %v; want %q", id, ok, first.RunID)
	}
	if _, err := q.Enqueue("a", testNow); !errors.Is(err, ErrClientHasRun) {
		t.Fatalf("running run: %v; want ErrClientHasRun", err)
	}
	if _, ok := q.remove(first.RunID); !ok {
		t.Fatal("remove failed")
	}
	if _, err := q.Enqueue("a", testNow); err != nil {
		t.Fatalf("after its run left the line: %v", err)
	}
}

// TestQueue_Positions: position is the 0-based place in the line, 0 for the
// first run before the worker took it; removing the head moves the others up
// and reports their new positions; a second remove frees nothing.
func TestQueue_Positions(t *testing.T) {
	q := NewQueue(3)
	var ids []string
	for i, c := range []string{"a", "b", "c"} {
		r, err := q.Enqueue(c, testNow)
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
	if _, ok := q.next(t.Context()); !ok {
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

// TestQueue_RunView: a queued run's view is the contract's Run with the
// preset's phases, the fixed testbed and a run id of the demo's shape.
func TestQueue_RunView(t *testing.T) {
	q := NewQueue(3)
	r, err := q.Enqueue("a", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if !validRunID(r.RunID) {
		t.Errorf("run id %q has not the demo's shape", r.RunID)
	}
	for i, p := range r.Phases {
		if p.Name != PhaseNames[i] || p.Status != PhasePending || p.DurationS != PhaseDurationS ||
			(p.Fault != nil) != (i == FaultPhaseIndex) {
			t.Errorf("phase %d = %+v", i, p)
		}
	}
	if r.Testbed != FixedTestbed() || !r.QueuedAt.Equal(testNow) || r.StartedAt != nil || r.EndedAt != nil {
		t.Errorf("run = %+v", r)
	}
	// Views are copies: changing one leaves the queue's alone.
	*r.Position = 7
	*r.Phases[FaultPhaseIndex].Fault = "changed"
	again, _ := q.get(r.RunID)
	if *again.Position != 0 || *again.Phases[FaultPhaseIndex].Fault != FaultLabel {
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
