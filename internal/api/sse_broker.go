package api

import "sync"

// Event is a single SSE event with a type and JSON data payload.
type Event struct {
	Type string
	Data string
}

// EventBroker fans out events to per-service SSE subscribers.
// Drop semantics: slow clients miss events rather than blocking the broker.
type EventBroker struct {
	mu      sync.RWMutex
	clients map[string]map[chan Event]struct{}
}

// NewEventBroker creates an empty EventBroker.
func NewEventBroker() *EventBroker {
	return &EventBroker{
		clients: make(map[string]map[chan Event]struct{}),
	}
}

// Subscribe returns a buffered channel that will receive events for service.
func (b *EventBroker) Subscribe(service string) chan Event {
	ch := make(chan Event, 16)
	b.mu.Lock()
	if b.clients[service] == nil {
		b.clients[service] = make(map[chan Event]struct{})
	}
	b.clients[service][ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes ch from the subscriber set and closes it.
// Closing under the write lock serializes against Broadcast's RLock so no
// in-flight send races with the close.
func (b *EventBroker) Unsubscribe(service string, ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs, ok := b.clients[service]
	if !ok {
		return
	}
	if _, exists := subs[ch]; !exists {
		return
	}
	delete(subs, ch)
	if len(subs) == 0 {
		delete(b.clients, service)
	}
	close(ch)
}

// Broadcast sends e to all subscribers for service.
// Non-blocking: if a subscriber's buffer is full the event is dropped.
func (b *EventBroker) Broadcast(service string, e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients[service] {
		select {
		case ch <- e:
		default:
		}
	}
}

// ClientCount returns the total number of active SSE connections.
func (b *EventBroker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, subs := range b.clients {
		n += len(subs)
	}
	return n
}
