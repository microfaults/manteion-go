package atrocontrol

import (
	"context"
	"sync"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
	"manteion-go/internal/ruleconv"
)

// ServiceIntent is what manteion last pushed service-wide. Each field has one
// writer, which replaces that field alone: the rules (and RunID) come from
// PushRules, the freeze config from FreezeService/ClearService, the active
// fault from InjectFault/ClearFault. So a rule push keeps the freeze config
// the materializer serves as freeze_cfg, and a thaw keeps the rules.
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

// SetRules records a service-wide rule push and its run id; nil clears the
// rules. The freeze config and active fault are kept.
func (t *IntentTracker) SetRules(service string, rules []ruleconv.CompiledRule, runID string) {
	t.update(service, func(i *ServiceIntent) { i.Rules, i.RunID = rules, runID })
}

// SetFreeze records the service's freeze config; nil clears it (thaw). The
// rules and active fault are kept.
func (t *IntentTracker) SetFreeze(service string, cfg *atroposdk.DelayRequest) {
	t.update(service, func(i *ServiceIntent) { i.FreezeCfg = cfg })
}

// SetActiveFault records the service's injected fault; nil clears it. The
// rules and freeze config are kept.
func (t *IntentTracker) SetActiveFault(service string, req *atroposdk.FaultRequest) {
	t.update(service, func(i *ServiceIntent) { i.ActiveFault = req })
}

// update applies f to a copy of the service's intent and stores the copy, so
// an intent handed out by Get never changes under its reader. A service whose
// intent holds no rules, fault or freeze config is dropped.
func (t *IntentTracker) update(service string, f func(*ServiceIntent)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var next ServiceIntent
	if cur := t.state[service]; cur != nil {
		next = *cur
	}
	f(&next)
	if len(next.Rules) == 0 && next.ActiveFault == nil && next.FreezeCfg == nil {
		delete(t.state, service)
		return
	}
	t.state[service] = &next
}

func (t *IntentTracker) Get(service string) (*ServiceIntent, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	i, ok := t.state[service]
	return i, ok
}

// FreezeConfigForService is the plan-1 desired.FreezeSource: the freeze
// config the orchestrator last pushed for the service (FreezeService, until
// ClearService). Plan 2 replaces it with the frozen phase's config and
// deletes the tracker.
func (t *IntentTracker) FreezeConfigForService(_ context.Context, service string) (*atroposdk.DelayRequest, error) {
	if i, ok := t.Get(service); ok {
		return i.FreezeCfg, nil
	}
	return nil, nil
}
