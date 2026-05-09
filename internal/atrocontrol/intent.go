package atrocontrol

import (
	"sync"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

type ServiceIntent struct {
	Rules       []atroposdk.StaticRule  `json:"rules,omitempty"`
	ActiveFaults map[string]*atroposdk.FaultRequest `json:"active_faults,omitempty"`
	FreezeCfg   *atroposdk.DelayRequest `json:"freeze_cfg,omitempty"`
	AppliedAt   time.Time               `json:"applied_at"`
	RunID       string                  `json:"run_id,omitempty"`
}

// IntentReader is the narrow read-only interface exposed to the register handler.
type IntentReader interface {
	Get(service string) (*ServiceIntent, bool)
}

type IntentTracker struct {
	mu    sync.RWMutex
	state map[string]*ServiceIntent
}

func newIntentTracker() *IntentTracker {
	return &IntentTracker{state: make(map[string]*ServiceIntent)}
}

func (t *IntentTracker) Set(service string, intent ServiceIntent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state[service] = &intent
}

func (t *IntentTracker) Clear(service string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.state, service)
}

func (t *IntentTracker) Get(service string) (*ServiceIntent, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	i, ok := t.state[service]
	return i, ok
}

func (t *IntentTracker) SetFaultSlot(service, category string, req *atroposdk.FaultRequest) {
	t.mu.Lock()
	defer t.mu.Unlock()
	intent, ok := t.state[service]
	if !ok {
		intent = &ServiceIntent{}
		t.state[service] = intent
	}
	if intent.ActiveFaults == nil {
		intent.ActiveFaults = make(map[string]*atroposdk.FaultRequest)
	}
	intent.ActiveFaults[category] = req
	intent.AppliedAt = time.Now()
}

func (t *IntentTracker) ClearFaultSlot(service, category string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	intent, ok := t.state[service]
	if !ok {
		return
	}
	delete(intent.ActiveFaults, category)
	intent.AppliedAt = time.Now()
}

func (t *IntentTracker) ClearAllFaultSlots(service string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	intent, ok := t.state[service]
	if !ok {
		return
	}
	intent.ActiveFaults = make(map[string]*atroposdk.FaultRequest)
	intent.AppliedAt = time.Now()
}
