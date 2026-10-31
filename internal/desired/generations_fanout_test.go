package desired

import (
	"context"
	"sync"
	"testing"
	"time"
)

// countingStore counts the Gets that have completed per service. It counts
// after the inner read returns, so a test that sees count >= n knows n callers
// already hold the value they read.
type countingStore struct {
	Store
	mu   sync.Mutex
	gets map[string]int
}

func (s *countingStore) Get(ctx context.Context, service string) (uint64, error) {
	gen, err := s.Store.Get(ctx, service)
	s.mu.Lock()
	s.gets[service]++
	s.mu.Unlock()
	return gen, err
}

func (s *countingStore) count(service string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[service]
}

// awaitGets blocks until n Gets on service have completed.
func awaitGets(t *testing.T, st *countingStore, service string, n int) {
	t.Helper()
	for stop := time.Now().Add(2 * time.Second); st.count(service) < n; {
		if time.Now().After(stop) {
			t.Fatalf("only %d of %d Gets on %q completed", st.count(service), n, service)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestGenerations_NotifyWakesEveryWaiterThenWaitersDeregister pins the two
// properties the single-waiter tests cannot: one Notify wakes all N parked
// long-polls of the service at once (not on the recheck tick), and every
// Wait deregisters on return.
func TestGenerations_NotifyWakesEveryWaiterThenWaitersDeregister(t *testing.T) {
	st := &countingStore{Store: NewMemoryStore(), gets: map[string]int{}}
	g := NewGenerations(st, NewBroker())
	ctx := context.Background()
	const n = 4
	type result struct {
		gen uint64
		err error
		at  time.Time
	}
	results := make(chan result, n)
	for range n {
		go func() {
			gen, err := g.Wait(ctx, "cart", 1, 5*time.Second)
			results <- result{gen, err, time.Now()}
		}()
	}
	// Every waiter has subscribed and read generation 1 before the bump, so
	// each one needs the Notify, not a fresh read, to learn about generation 2.
	awaitGets(t, st, "cart", n)
	bumped := time.Now()
	if _, err := g.Bump(ctx, "cart"); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		select {
		case r := <-results:
			if r.err != nil || r.gen != 2 {
				t.Fatalf("waiter %d: Wait = %d, %v; want 2, nil", i, r.gen, r.err)
			}
			if d := r.at.Sub(bumped); d > time.Second {
				t.Fatalf("waiter %d woke %v after the bump; Notify did not wake it", i, d)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d waiters returned after the bump", i, n)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.waiters) != 0 {
		t.Fatalf("waiter registry not empty after every Wait returned: %v", g.waiters)
	}
}

// TestGenerations_NotifyLeavesOtherServicesWaitersAlone pins per-service
// isolation: bumping cart must not even wake a waiter parked on other.
func TestGenerations_NotifyLeavesOtherServicesWaitersAlone(t *testing.T) {
	st := &countingStore{Store: NewMemoryStore(), gets: map[string]int{}}
	g := NewGenerations(st, NewBroker())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = g.Wait(ctx, "other", 1, 5*time.Second) }()
	awaitGets(t, st, "other", 1)
	before := st.count("other")
	if _, err := g.Bump(ctx, "cart"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if after := st.count("other"); after != before {
		t.Fatalf("bumping cart woke a waiter on other: Gets %d -> %d", before, after)
	}
	cancel()
	<-done
}
