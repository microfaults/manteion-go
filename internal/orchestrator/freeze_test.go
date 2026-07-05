package orchestrator

import (
	"testing"

	"manteion-go/internal/model"
)

// TestFreezeCommand_CarriesContext pins MANT-4(b): the freeze command sent to a
// frozen service carries both the fitted delay distribution and the
// authoritative CacheBoxContext (§W1) scoping it to (experiment_id, phase_id)
// with the service's key strategy.
func TestFreezeCommand_CarriesContext(t *testing.T) {
	mu, sigma := 1.5, 0.4
	p := &model.ExperimentPhase{ID: "phase-iso", ExperimentID: "exp-1"}
	fs := model.CacheBoxConfig{
		Service: "frontend", Mode: "replay_with_delay", KeyStrategy: "exact",
		KeyHeaders:     []string{"x-tenant"},
		SyntheticDelay: &model.SyntheticDelayConfig{FitMu: &mu, FitSigma: &sigma},
	}

	req := freezeDelayRequest(p, fs)
	if req.Mu != mu || req.Sigma != sigma {
		t.Fatalf("delay = (mu=%v, sigma=%v), want (%v, %v)", req.Mu, req.Sigma, mu, sigma)
	}
	if req.Context == nil {
		t.Fatal("freeze command missing CacheBoxContext")
	}
	if req.Context.ExperimentID != "exp-1" || req.Context.PhaseID != "phase-iso" {
		t.Fatalf("context pair = (%s, %s), want (exp-1, phase-iso)", req.Context.ExperimentID, req.Context.PhaseID)
	}
	if req.Context.KeyStrategy != "exact" || req.Context.StrategyVersion != 1 {
		t.Fatalf("context strategy = (%s, v%d), want (exact, v1)", req.Context.KeyStrategy, req.Context.StrategyVersion)
	}
	if len(req.Context.KeyHeaders) != 1 || req.Context.KeyHeaders[0] != "x-tenant" {
		t.Fatalf("context key_headers = %v, want [x-tenant]", req.Context.KeyHeaders)
	}

	// Empty key_strategy defaults to canonical_v2 (wire version 2).
	def := freezeDelayRequest(p, model.CacheBoxConfig{Service: "cart"})
	if def.Context.KeyStrategy != "canonical_v2" || def.Context.StrategyVersion != 2 {
		t.Fatalf("default strategy = (%s, v%d), want (canonical_v2, v2)", def.Context.KeyStrategy, def.Context.StrategyVersion)
	}
}
