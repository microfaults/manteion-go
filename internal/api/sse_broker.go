package api

import "manteion-go/internal/desired"

// The SSE broker lives in internal/desired (it also wakes long-polls); these
// aliases keep the handler and its tests on their historical names.
type (
	Event       = desired.Event
	EventBroker = desired.Broker
)

func NewEventBroker() *EventBroker { return desired.NewBroker() }
