package atrocontrol

import (
	"sync"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

type ServiceIntent struct {
	Rules       []atroposdk.StaticRule  `json:"rules,omitempty"`
	ActiveFault *atroposdk.FaultRequest `json:"active_fault,omitempty"`
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
