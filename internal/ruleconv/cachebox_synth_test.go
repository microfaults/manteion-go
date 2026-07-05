package ruleconv

import (
	"testing"

	"manteion-go/internal/model"
)

// TestSynthesizeCacheBoxRule pins the synthesized rule's match shape (MANT-4):
// a wildcard egress rule (empty labels ⇒ all egress) carrying the cache-box
// mode and authoritative CacheBoxContext, so the SDK records/replays all of the
// service's egress under the phase's (experiment_id, phase_id).
func TestSynthesizeCacheBoxRule(t *testing.T) {
	rc := model.CacheBoxRuleContext{
		ExperimentID:    "exp-1",
		PhaseID:         "phase-1",
		Mode:            "passthrough",
		KeyStrategy:     "canonical_v2",
		StrategyVersion: 2,
		KeyHeaders:      []string{"x-tenant"},
	}

	r := SynthesizeCacheBoxRule(rc)
	if r.InjectionPoint != "egress" {
		t.Fatalf("injection point = %q, want egress", r.InjectionPoint)
	}
	if len(r.Labels) != 0 {
		t.Fatalf("labels = %v, want empty (wildcard: all egress)", r.Labels)
	}
	if r.CacheBox == nil || r.CacheBox.Mode != "passthrough" {
		t.Fatalf("cachebox = %+v, want mode passthrough", r.CacheBox)
	}
	ctx := r.CacheBox.Context
	if ctx == nil || ctx.ExperimentID != "exp-1" || ctx.PhaseID != "phase-1" {
		t.Fatalf("context = %+v, want (exp-1, phase-1)", ctx)
	}
	if ctx.KeyStrategy != "canonical_v2" || ctx.StrategyVersion != 2 {
		t.Fatalf("context strategy = (%s, v%d), want (canonical_v2, v2)", ctx.KeyStrategy, ctx.StrategyVersion)
	}
	if len(ctx.KeyHeaders) != 1 || ctx.KeyHeaders[0] != "x-tenant" {
		t.Fatalf("context key_headers = %v, want [x-tenant]", ctx.KeyHeaders)
	}
}
