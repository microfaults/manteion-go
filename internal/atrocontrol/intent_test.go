package atrocontrol

import (
	"context"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/ruleconv"
)

// FreezeConfigForService is the plan-1 freeze source of the desired-state
// materializer: the freeze config last pushed for the service, nil when the
// service has none.
func TestIntentTracker_FreezeConfigForService(t *testing.T) {
	ctx := context.Background()
	tr := newIntentTracker()
	if cfg, err := tr.FreezeConfigForService(ctx, "cart"); cfg != nil || err != nil {
		t.Errorf("no intent: got %+v, %v; want nil, nil", cfg, err)
	}

	tr.SetFreeze("cart", &atroposdk.DelayRequest{Mu: 1.5, Sigma: 0.25})
	if cfg, err := tr.FreezeConfigForService(ctx, "cart"); err != nil || cfg == nil || cfg.Mu != 1.5 || cfg.Sigma != 0.25 {
		t.Errorf("frozen service: got %+v, %v; want mu=1.5 sigma=0.25", cfg, err)
	}
	if cfg, err := tr.FreezeConfigForService(ctx, "checkout"); cfg != nil || err != nil {
		t.Errorf("other service: got %+v, %v; want nil, nil", cfg, err)
	}

	tr.SetRules("cart", []ruleconv.CompiledRule{{Name: "r1"}}, "")
	if cfg, err := tr.FreezeConfigForService(ctx, "cart"); err != nil || cfg == nil || cfg.Mu != 1.5 {
		t.Errorf("after a rule push: got %+v, %v; want the freeze config kept", cfg, err)
	}
}

// Each writer owns one field of a service's intent: a rule push (SetRules)
// replaces the rules and the run id, freeze/thaw (SetFreeze) the freeze
// config, fault injection (SetActiveFault) the active fault; nil clears that
// field alone. A write never changes an intent read before it, and a service
// whose intent holds nothing is dropped.
func TestIntentTracker_WritesAreFieldScoped(t *testing.T) {
	tr := newIntentTracker()
	freeze := &atroposdk.DelayRequest{Mu: 1.5}
	fault := &atroposdk.FaultRequest{FaultType: "latency"}
	rules := []ruleconv.CompiledRule{{Name: "r1"}}

	tr.SetFreeze("cart", freeze)
	tr.SetRules("cart", rules, "run-1")
	tr.SetActiveFault("cart", fault)
	before, ok := tr.Get("cart")
	if !ok || before.FreezeCfg != freeze || before.ActiveFault != fault || len(before.Rules) != 1 || before.RunID != "run-1" {
		t.Fatalf("after freeze, rule push and fault: %+v", before)
	}

	tr.SetRules("cart", nil, "run-2") // the nil push endPhase makes before thaw
	if got, _ := tr.Get("cart"); got.Rules != nil || got.RunID != "run-2" || got.FreezeCfg != freeze || got.ActiveFault != fault {
		t.Errorf("nil rule push: %+v, want only the rules and run id replaced", got)
	}
	if len(before.Rules) != 1 || before.RunID != "run-1" {
		t.Errorf("a write changed an intent read before it: %+v", before)
	}

	tr.SetActiveFault("cart", nil)
	if got, _ := tr.Get("cart"); got.ActiveFault != nil || got.FreezeCfg != freeze {
		t.Errorf("fault cleared: %+v, want only the active fault cleared", got)
	}

	tr.SetRules("cart", rules, "run-3")
	tr.SetFreeze("cart", nil) // thaw
	if got, _ := tr.Get("cart"); got == nil || got.FreezeCfg != nil || len(got.Rules) != 1 || got.RunID != "run-3" {
		t.Errorf("thaw: %+v, want only the freeze config cleared", got)
	}

	tr.SetRules("cart", nil, "")
	if got, ok := tr.Get("cart"); ok {
		t.Errorf("an intent holding nothing was kept: %+v", got)
	}
}
