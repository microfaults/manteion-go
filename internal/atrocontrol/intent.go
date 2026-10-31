package atrocontrol

import (
	"context"
	"sync"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
	"manteion-go/internal/ruleconv"
)

type ServiceIntent struct {
	Rules       []ruleconv.CompiledRule `json:"rules,omitempty"`
	ActiveFault *atroposdk.FaultRequest `json:"active_fault,omitempty"`
	FreezeCfg   *atroposdk.DelayRequest `json:"freeze_cfg,omitempty"`
	AppliedAt   time.Time               `json:"applied_at"`
	RunID       string                  `json:"run_id,omitempty"`
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

// FreezeConfigForService is the plan-1 desired.FreezeSource: the freeze
// config the orchestrator last pushed for the service. Plan 2 replaces it
// with the frozen phase's config and deletes the tracker.
func (t *IntentTracker) FreezeConfigForService(_ context.Context, service string) (*atroposdk.DelayRequest, error) {
	if i, ok := t.Get(service); ok {
		return i.FreezeCfg, nil
	}
	return nil, nil
}
