package demo

import "sync"

// hubBuffer is how many events a subscriber holds unread before the hub drops
// new ones for it.
const hubBuffer = 16

// hubSealed is how many ended runs the hub remembers, to drop anything
// published for them after their end event.
const hubSealed = 256

// Hub fans a run's events out to its subscribers (the event streams). It
// never blocks a publisher: a subscriber whose buffer is full misses the
// event, except the end event, which takes the place of the oldest buffered
// one so that a slow subscriber still learns its run ended. Once a run's end
// event is published, anything else published for that run (a late sample, a
// second end) is dropped, so the end is always the last event. The hub runs no
// goroutine of its own.
type Hub struct {
	mu     sync.Mutex
	subs   map[string]map[*subscription]struct{} // by run id; an entry exists only while it has subscribers
	n      int
	closed bool
	sealed map[string]struct{} // runs whose end was published (the last hubSealed)
	order  []string            // sealed run ids, oldest first
}

type subscription struct {
	ch chan Event
}

// NewHub returns a hub with no subscribers.
func NewHub() *Hub {
	return &Hub{subs: map[string]map[*subscription]struct{}{}, sealed: map[string]struct{}{}}
}

// Publish sends ev to every subscriber of runID without blocking. It does
// nothing after CloseAll, or once runID's end event was published.
func (h *Hub) Publish(runID string, ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	if _, ok := h.sealed[runID]; ok {
		return
	}
	if ev.Type == EventEnd {
		h.sealed[runID] = struct{}{}
		h.order = append(h.order, runID)
		if len(h.order) > hubSealed {
			delete(h.sealed, h.order[0])
			h.order = h.order[1:]
		}
	}
	for s := range h.subs[runID] {
		select {
		case s.ch <- ev:
			continue
		default:
		}
		if ev.Type != EventEnd {
			continue // full: dropped
		}
		// Make room for the end: drop the oldest event. Only publishers send,
		// and they hold h.mu, so the send below cannot find the buffer full
		// again.
		select {
		case <-s.ch:
		default:
		}
		select {
		case s.ch <- ev:
		default:
		}
	}
}

// Subscribe returns a channel of runID's events and a function that ends the
// subscription. Ending it closes the channel and removes every trace of the
// subscription; it may be called any number of times, also after CloseAll.
// After CloseAll, Subscribe returns a closed channel.
func (h *Hub) Subscribe(runID string) (<-chan Event, func()) {
	s := &subscription{ch: make(chan Event, hubBuffer)}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		close(s.ch)
		return s.ch, func() {}
	}
	m := h.subs[runID]
	if m == nil {
		m = map[*subscription]struct{}{}
		h.subs[runID] = m
	}
	m[s] = struct{}{}
	h.n++
	var once sync.Once
	return s.ch, func() { once.Do(func() { h.unsubscribe(runID, s) }) }
}

func (h *Hub) unsubscribe(runID string, s *subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := h.subs[runID]
	if _, ok := m[s]; !ok {
		return // CloseAll got there first and closed the channel
	}
	delete(m, s)
	if len(m) == 0 {
		delete(h.subs, runID)
	}
	h.n--
	close(s.ch)
}

// Count returns the number of live subscriptions.
func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// CloseAll closes every subscriber's channel and drops them all; later
// publishes do nothing and later subscriptions are closed at once.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for _, m := range h.subs {
		for s := range m {
			close(s.ch)
		}
	}
	h.subs = map[string]map[*subscription]struct{}{}
	h.n = 0
}
