package desired

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// Store persists per-service generations. Bump increments (creating the row at
// 2 when absent, since absent = 1) inside tx when tx is non-nil.
type Store interface {
	Get(ctx context.Context, service string) (uint64, error)
	Bump(ctx context.Context, tx *sql.Tx, service string) (uint64, error)
}

// waitRecheck bounds how long a waiter can miss a wake-up: Wait re-reads the
// store on this cadence even without a Notify (insurance against a caller
// that bumped in a transaction and forgot to Notify after commit).
const waitRecheck = 2 * time.Second

// Generations is the per-service generation counter plus the wake-up fabric:
// long-polls Wait on it and SSE subscribers receive rules_changed from it.
type Generations struct {
	store  Store
	broker *Broker

	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
}

func NewGenerations(store Store, broker *Broker) *Generations {
	return &Generations{store: store, broker: broker, waiters: map[string]map[chan struct{}]struct{}{}}
}

func (g *Generations) Broker() *Broker { return g.broker }

func (g *Generations) Get(ctx context.Context, service string) (uint64, error) {
	return g.store.Get(ctx, service)
}

// Bump increments the service's generation in its own statement and wakes
// everyone. Use BumpTx + Notify when the bump must commit with other writes.
func (g *Generations) Bump(ctx context.Context, service string) (uint64, error) {
	gen, err := g.store.Bump(ctx, nil, service)
	if err != nil {
		return 0, err
	}
	g.Notify(service, gen)
	return gen, nil
}

// BumpTx increments inside tx and does NOT notify: a waiter woken before the
// commit would re-materialize the old state. Call Notify after Commit.
func (g *Generations) BumpTx(ctx context.Context, tx *sql.Tx, service string) (uint64, error) {
	return g.store.Bump(ctx, tx, service)
}

// Notify wakes the service's waiters and emits the SSE nudge.
func (g *Generations) Notify(service string, generation uint64) {
	g.mu.Lock()
	for ch := range g.waiters[service] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	g.mu.Unlock()
	g.broker.Broadcast(service, Event{Type: "rules_changed", Data: fmt.Sprintf(`{"generation":%d}`, generation)})
}

// Wait blocks until the service's generation exceeds after, ctx is done, or
// timeout elapses, and returns the generation it last read. A timeout is not
// an error (the caller answers 304); ctx.Err() is.
func (g *Generations) Wait(ctx context.Context, service string, after uint64, timeout time.Duration) (uint64, error) {
	ch := g.subscribe(service)
	defer g.unsubscribe(service, ch)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	recheck := time.NewTicker(waitRecheck)
	defer recheck.Stop()
	for {
		cur, err := g.store.Get(ctx, service)
		if err != nil {
			return 0, err
		}
		if cur > after {
			return cur, nil
		}
		select {
		case <-ctx.Done():
			return cur, ctx.Err()
		case <-deadline.C:
			return cur, nil
		case <-ch:
		case <-recheck.C:
		}
	}
}

func (g *Generations) subscribe(service string) chan struct{} {
	ch := make(chan struct{}, 1)
	g.mu.Lock()
	if g.waiters[service] == nil {
		g.waiters[service] = map[chan struct{}]struct{}{}
	}
	g.waiters[service][ch] = struct{}{}
	g.mu.Unlock()
	return ch
}

func (g *Generations) unsubscribe(service string, ch chan struct{}) {
	g.mu.Lock()
	delete(g.waiters[service], ch)
	if len(g.waiters[service]) == 0 {
		delete(g.waiters, service)
	}
	g.mu.Unlock()
}
