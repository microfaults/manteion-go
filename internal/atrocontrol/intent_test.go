package atrocontrol

import (
	"context"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/ruleconv"
)

// FreezeConfigForService is the plan-1 freeze source of the desired-state
// materializer: the freeze config last pushed for the service, nil when the
// service has none (no intent, or an intent a later push replaced).
func TestIntentTracker_FreezeConfigForService(t *testing.T) {
	ctx := context.Background()
	tr := newIntentTracker()
	if cfg, err := tr.FreezeConfigForService(ctx, "cart"); cfg != nil || err != nil {
		t.Errorf("no intent: got %+v, %v; want nil, nil", cfg, err)
	}

	tr.Set("cart", ServiceIntent{FreezeCfg: &atroposdk.DelayRequest{Mu: 1.5, Sigma: 0.25}})
	if cfg, err := tr.FreezeConfigForService(ctx, "cart"); err != nil || cfg == nil || cfg.Mu != 1.5 || cfg.Sigma != 0.25 {
		t.Errorf("frozen service: got %+v, %v; want mu=1.5 sigma=0.25", cfg, err)
	}
	if cfg, err := tr.FreezeConfigForService(ctx, "checkout"); cfg != nil || err != nil {
		t.Errorf("other service: got %+v, %v; want nil, nil", cfg, err)
	}

	tr.Set("cart", ServiceIntent{Rules: []ruleconv.CompiledRule{{Name: "r1"}}})
	if cfg, err := tr.FreezeConfigForService(ctx, "cart"); cfg != nil || err != nil {
		t.Errorf("after a rule push replaced the intent: got %+v, %v; want nil, nil", cfg, err)
	}
}
