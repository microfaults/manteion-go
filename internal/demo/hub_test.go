package demo

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestHub_DropsWhenFull: a subscriber that never reads holds 16 events; the
// rest are dropped and the publisher never blocks.
func TestHub_DropsWhenFull(t *testing.T) {
	h := NewHub()
	ch, unsub := h.Subscribe("run-a")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			h.Publish("run-a", Event{Type: EventQueue, Data: QueueEvent{RunID: "run-a", Position: i}})
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		// No unsubscribe here: it would wait for the blocked publisher.
		t.Fatal("Publish blocked on a subscriber that does not read")
	}
	defer unsub()
	if n := len(ch); n != 16 {
		t.Fatalf("%d events buffered; want 16", n)
	}
	// The first 16 were kept, in order.
	for i := 0; i < 16; i++ {
		ev := <-ch
		if ev.Data.(QueueEvent).Position != i {
			t.Fatalf("event %d = %+v", i, ev)
		}
	}
}

// TestHub_EndAlwaysLands: the end event is delivered even to a full
// subscriber, as its last event, at the cost of the oldest one buffered.
func TestHub_EndAlwaysLands(t *testing.T) {
	h := NewHub()
	ch, unsub := h.Subscribe("run-a")
	defer unsub()
	for i := 0; i < 20; i++ {
		h.Publish("run-a", Event{Type: EventPhase})
	}
	h.Publish("run-a", Event{Type: EventEnd})
	if n := len(ch); n != 16 {
		t.Fatalf("%d buffered; want 16", n)
	}
	var last Event
	for len(ch) > 0 {
		last = <-ch
	}
	if last.Type != EventEnd {
		t.Fatalf("last event %q; want end", last.Type)
	}
}

// TestHub_NothingAfterEnd: once a run's end is published, a late event or a
// second end for it is dropped; other runs are not affected; the memory of
// ended runs is bounded.
func TestHub_NothingAfterEnd(t *testing.T) {
	h := NewHub()
	a, ua := h.Subscribe("run-a")
	b, ub := h.Subscribe("run-b")
	defer ua()
	defer ub()
	h.Publish("run-a", Event{Type: EventEnd})
	h.Publish("run-a", Event{Type: EventSample})
	h.Publish("run-a", Event{Type: EventEnd})
	h.Publish("run-b", Event{Type: EventSample})
	if len(a) != 1 || (<-a).Type != EventEnd {
		t.Fatalf("run-a got %d events; want only its end", len(a)+1)
	}
	if len(b) != 1 {
		t.Fatalf("run-b got %d events; want 1", len(b))
	}
	for i := 0; i < 3*hubSealed; i++ {
		h.Publish(fmt.Sprintf("run-%d", i), Event{Type: EventEnd})
	}
	h.mu.Lock()
	n, m := len(h.sealed), len(h.order)
	h.mu.Unlock()
	if n != hubSealed || m != hubSealed {
		t.Fatalf("remembers %d/%d ended runs; want %d", n, m, hubSealed)
	}
}

// TestHub_SubscribeAfterEnd: a subscription to a run that already ended gets
// its end event at once and is then closed, registering nothing; a closed hub
// gives a closed, empty channel, which a caller tells apart. This is what makes
// "subscribe, then read the view" safe for a stream: an end published before
// the subscription is not lost.
func TestHub_SubscribeAfterEnd(t *testing.T) {
	h := NewHub()
	end := Event{Type: EventEnd, Data: Run{RunID: "run-a", Status: RunCompleted}}
	h.Publish("run-a", end)
	ch, unsub := h.Subscribe("run-a")
	ev, ok := recvWithin(t, ch)
	if !ok || ev.Type != EventEnd || ev.Data.(Run).RunID != "run-a" {
		t.Fatalf("late subscriber got %+v, open %v; want the end event", ev, ok)
	}
	if _, open := recvWithin(t, ch); open {
		t.Fatal("the late subscription stayed open after the end")
	}
	unsub()
	unsub()
	if h.Count() != 0 {
		t.Fatalf("Count = %d; a late subscription registered", h.Count())
	}
	h.CloseAll()
	c, _ := h.Subscribe("run-b")
	if ev, open := recvWithin(t, c); open || ev.Type != "" {
		t.Fatalf("after CloseAll got %+v, open %v; want closed and empty", ev, open)
	}
}

// recvWithin receives from ch, failing t after a second instead of hanging.
func recvWithin(t *testing.T, ch <-chan Event) (Event, bool) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(time.Second):
		t.Fatal("nothing received within a second")
		return Event{}, false
	}
}

// TestHub_SubscribersAreSeparate: an event reaches every subscriber of its
// run and no one else's.
func TestHub_SubscribersAreSeparate(t *testing.T) {
	h := NewHub()
	a1, u1 := h.Subscribe("run-a")
	a2, u2 := h.Subscribe("run-a")
	b, u3 := h.Subscribe("run-b")
	defer u1()
	defer u2()
	defer u3()
	h.Publish("run-a", Event{Type: EventPhase})
	if len(a1) != 1 || len(a2) != 1 || len(b) != 0 {
		t.Fatalf("buffered a1=%d a2=%d b=%d; want 1 1 0", len(a1), len(a2), len(b))
	}
	if h.Count() != 3 {
		t.Fatalf("Count = %d; want 3", h.Count())
	}
}

// TestHub_UnsubscribeIdempotent: unsubscribing closes the channel, removes
// the subscription and its run's map entry, and may be called again.
func TestHub_UnsubscribeIdempotent(t *testing.T) {
	h := NewHub()
	ch, unsub := h.Subscribe("run-a")
	_, other := h.Subscribe("run-b")
	unsub()
	unsub()
	if _, open := <-ch; open {
		t.Fatal("channel still open after unsubscribe")
	}
	if h.Count() != 1 {
		t.Fatalf("Count = %d; want 1", h.Count())
	}
	h.mu.Lock()
	_, kept := h.subs["run-a"]
	h.mu.Unlock()
	if kept {
		t.Fatal("the run's map entry outlived its last subscriber")
	}
	h.Publish("run-a", Event{Type: EventPhase}) // nobody left: no panic
	other()
	h.mu.Lock()
	entries := len(h.subs)
	h.mu.Unlock()
	if h.Count() != 0 || entries != 0 {
		t.Fatalf("Count = %d, %d map entries; want 0, 0", h.Count(), entries)
	}
}

// TestHub_CloseAll: every channel closes, nothing is left, later publishes
// are dropped and a later subscription is closed at once.
func TestHub_CloseAll(t *testing.T) {
	h := NewHub()
	a, ua := h.Subscribe("run-a")
	b, _ := h.Subscribe("run-b")
	h.CloseAll()
	for _, ch := range []<-chan Event{a, b} {
		if _, open := <-ch; open {
			t.Fatal("channel open after CloseAll")
		}
	}
	ua() // after CloseAll: no double close
	h.Publish("run-a", Event{Type: EventEnd})
	c, uc := h.Subscribe("run-c")
	if _, open := <-c; open {
		t.Fatal("a subscription after CloseAll is open")
	}
	uc()
	if h.Count() != 0 {
		t.Fatalf("Count = %d; want 0", h.Count())
	}
}

// TestHub_PublishRacesUnsubscribe: publishing while subscribers come and go
// never sends on a closed channel (run with -race).
func TestHub_PublishRacesUnsubscribe(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				h.Publish("run-a", Event{Type: EventPhase})
				h.Publish("run-a", Event{Type: EventSample})
			}
		}
	}()
	for i := 0; i < 500; i++ {
		_, unsub := h.Subscribe("run-a")
		unsub()
	}
	close(stop)
	wg.Wait()
	if h.Count() != 0 {
		t.Fatalf("Count = %d; want 0", h.Count())
	}
}
