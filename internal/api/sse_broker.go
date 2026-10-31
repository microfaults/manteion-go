package api

import "manteion-go/internal/desired"

// The SSE broker lives in internal/desired, beside Generations, whose bumps it
// broadcasts; it does not wake long-polls (Generations does). These aliases
// keep the handler and its tests on their historical names.
type (
	Event       = desired.Event
	EventBroker = desired.Broker
)

func NewEventBroker() *EventBroker { return desired.NewBroker() }
