package atrocontrol

import (
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/ruleconv"
)

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
