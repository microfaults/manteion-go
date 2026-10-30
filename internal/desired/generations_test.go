package desired

import (
	"context"
	"testing"
	"time"
)

func TestGenerations_GetDefaultsToOne(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	if gen, err := g.Get(context.Background(), "cart"); err != nil || gen != 1 {
		t.Fatalf("Get = %d, %v; want 1 (absent row)", gen, err)
	}
}

func TestGenerations_BumpIsMonotonicAndBroadcasts(t *testing.T) {
	broker := NewBroker()
	g := NewGenerations(NewMemoryStore(), broker)
	ch := broker.Subscribe("cart")
	defer broker.Unsubscribe("cart", ch)

	if gen, _ := g.Bump(context.Background(), "cart"); gen != 2 {
		t.Fatalf("first Bump = %d, want 2", gen)
	}
	if gen, _ := g.Bump(context.Background(), "cart"); gen != 3 {
		t.Fatalf("second Bump = %d, want 3", gen)
	}
	select {
	case e := <-ch:
		if e.Type != "rules_changed" || e.Data != `{"generation":2}` {
			t.Fatalf("event = %+v, want rules_changed {\"generation\":2}", e)
		}
	case <-time.After(time.Second):
		t.Fatal("no SSE event after Bump")
	}
}

func TestGenerations_WaitWakesOnBump(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	ctx := context.Background()
	done := make(chan uint64, 1)
	go func() {
		gen, err := g.Wait(ctx, "cart", 1, 5*time.Second)
		if err != nil {
			t.Errorf("Wait: %v", err)
		}
		done <- gen
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if _, err := g.Bump(ctx, "cart"); err != nil {
		t.Fatal(err)
	}
	select {
	case gen := <-done:
		if gen != 2 {
			t.Fatalf("Wait returned %d, want 2", gen)
		}
		if time.Since(start) > time.Second {
			t.Fatalf("Wait took %v after the bump; the waiter was not woken", time.Since(start))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Wait never returned after Bump")
	}
}

func TestGenerations_WaitReturnsImmediatelyWhenAlreadyAhead(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	_, _ = g.Bump(context.Background(), "cart") // now 2
	start := time.Now()
	gen, err := g.Wait(context.Background(), "cart", 1, 5*time.Second)
	if err != nil || gen != 2 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("Wait = %d, %v in %v; want 2 immediately", gen, err, time.Since(start))
	}
}

func TestGenerations_WaitTimesOutWithoutError(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	start := time.Now()
	gen, err := g.Wait(context.Background(), "cart", 1, 100*time.Millisecond)
	if err != nil || gen != 1 {
		t.Fatalf("Wait = %d, %v; want 1, nil", gen, err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatalf("Wait returned after %v, before the timeout", time.Since(start))
	}
}

func TestGenerations_WaitHonoursContext(t *testing.T) {
	g := NewGenerations(NewMemoryStore(), NewBroker())
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if _, err := g.Wait(ctx, "cart", 1, 5*time.Second); err == nil {
		t.Fatal("Wait returned nil error after context cancel")
	}
}

func TestGenerations_BumpTxDoesNotNotifyUntilNotify(t *testing.T) {
	broker := NewBroker()
	g := NewGenerations(NewMemoryStore(), broker)
	ch := broker.Subscribe("cart")
	defer broker.Unsubscribe("cart", ch)
	gen, err := g.BumpTx(context.Background(), nil, "cart")
	if err != nil || gen != 2 {
		t.Fatalf("BumpTx = %d, %v", gen, err)
	}
	select {
	case e := <-ch:
		t.Fatalf("BumpTx broadcast %+v before Notify", e)
	case <-time.After(50 * time.Millisecond):
	}
	g.Notify("cart", gen)
	select {
	case e := <-ch:
		if e.Data != `{"generation":2}` {
			t.Fatalf("event = %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Notify did not broadcast")
	}
}
