package api

import (
	"testing"
	"time"
)

func TestEventBroker_Subscribe_Receive(t *testing.T) {
	b := NewEventBroker()
	ch := b.Subscribe("svc")
	defer b.Unsubscribe("svc", ch)

	b.Broadcast("svc", Event{Type: "rules_changed", Data: `{"version":1}`})

	select {
	case e := <-ch:
		if e.Type != "rules_changed" {
			t.Fatalf("event type = %q, want rules_changed", e.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestEventBroker_Broadcast_MultipleSubscribers(t *testing.T) {
	b := NewEventBroker()
	ch1 := b.Subscribe("svc")
	ch2 := b.Subscribe("svc")
	defer b.Unsubscribe("svc", ch1)
	defer b.Unsubscribe("svc", ch2)

	b.Broadcast("svc", Event{Type: "rules_changed", Data: `{"version":2}`})

	for i, ch := range []chan Event{ch1, ch2} {
		select {
		case e := <-ch:
			if e.Type != "rules_changed" {
				t.Fatalf("subscriber %d: event type = %q, want rules_changed", i, e.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timed out waiting for event", i)
		}
	}
}

func TestEventBroker_SlowClient_Dropped(t *testing.T) {
	b := NewEventBroker()
	ch := b.Subscribe("svc")
	defer b.Unsubscribe("svc", ch)

	// Fill the buffer (capacity 16) plus one more — the extra must be dropped,
	// not block the broker.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 17 {
			b.Broadcast("svc", Event{Type: "rules_changed", Data: `{"version":` + string(rune('0'+i)) + `}`})
		}
	}()

	select {
	case <-done:
		// broadcast goroutine completed without blocking
	case <-time.After(time.Second):
		t.Fatal("Broadcast blocked on slow client")
	}

	if b.ClientCount() != 1 {
		t.Fatalf("ClientCount = %d, want 1", b.ClientCount())
	}
}

func TestEventBroker_Unsubscribe_Cleanup(t *testing.T) {
	b := NewEventBroker()
	ch := b.Subscribe("svc")
	b.Unsubscribe("svc", ch)

	if b.ClientCount() != 0 {
		t.Fatalf("ClientCount = %d after unsubscribe, want 0", b.ClientCount())
	}

	// Channel must be closed — a read should return the zero value immediately.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel still open after Unsubscribe")
		}
	default:
		t.Fatal("channel not closed after Unsubscribe")
	}
}

func TestEventBroker_Broadcast_WrongService(t *testing.T) {
	b := NewEventBroker()
	ch := b.Subscribe("svc-a")
	defer b.Unsubscribe("svc-a", ch)

	b.Broadcast("svc-b", Event{Type: "rules_changed", Data: `{"version":1}`})

	select {
	case <-ch:
		t.Fatal("received event for wrong service")
	case <-time.After(50 * time.Millisecond):
		// correct — no event delivered
	}
}
