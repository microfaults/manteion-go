package desired

import (
	"context"
	"sync"
	"testing"
	"time"
)

// afterFirstGetStore runs hook once, right after the first Get has read its
// value and before that (now stale) value is returned to the caller.
type afterFirstGetStore struct {
	Store
	once sync.Once
	hook func()
}

func (s *afterFirstGetStore) Get(ctx context.Context, service string) (uint64, error) {
	gen, err := s.Store.Get(ctx, service)
	s.once.Do(s.hook)
	return gen, err
}

// TestGenerations_WaitDoesNotLoseAWakeupBetweenReadAndSleep pins Wait's
// subscribe-before-read order: a bump that lands after Wait's first read but
// before it blocks must still wake it at once, not on the waitRecheck tick.
func TestGenerations_WaitDoesNotLoseAWakeupBetweenReadAndSleep(t *testing.T) {
	st := &afterFirstGetStore{Store: NewMemoryStore()}
	g := NewGenerations(st, NewBroker())
	st.hook = func() { _, _ = g.Bump(context.Background(), "cart") }

	start := time.Now()
	gen, err := g.Wait(context.Background(), "cart", 1, 5*time.Second)
	if err != nil || gen != 2 {
		t.Fatalf("Wait = %d, %v; want 2, nil", gen, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Wait took %v; the bump between its read and its sleep was lost", d)
	}
}
