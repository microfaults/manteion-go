package desired

import (
	"context"
	"errors"
	"testing"

	"manteion-go/internal/model"
)

type fakeFrozen struct {
	phase *model.ExperimentPhase
	cfg   *model.CacheBoxConfig
}

func (f *fakeFrozen) FrozenServiceForService(context.Context, string) (*model.ExperimentPhase, *model.CacheBoxConfig, error) {
	return f.phase, f.cfg, nil
}

type fakeArtifacts struct {
	byKey map[string]*model.ReplayArtifact
}

func (f *fakeArtifacts) ForPhaseService(_ context.Context, phaseID, service string) (*model.ReplayArtifact, error) {
	return f.byKey[phaseID+"/"+service], nil
}

func TestPhaseSources_NotFrozen(t *testing.T) {
	s := &PhaseSources{Phases: &fakeFrozen{}, Artifacts: &fakeArtifacts{}}
	if cfg, err := s.FreezeConfigForService(context.Background(), "frontend"); err != nil || cfg != nil {
		t.Errorf("freeze = %v, %v; want nil", cfg, err)
	}
	if ref, err := s.ReplaySetForService(context.Background(), "frontend"); err != nil || ref != nil {
		t.Errorf("replay set = %v, %v; want nil", ref, err)
	}
}

func TestPhaseSources_FrozenPhase(t *testing.T) {
	mu, sigma := 1.5, 0.4
	p := &model.ExperimentPhase{ID: "phase-iso", ExperimentID: "exp-1"}
	cfg := &model.CacheBoxConfig{Service: "frontend", Mode: "replay_with_delay", KeyHeaders: []string{"x-tenant"},
		SyntheticDelay: &model.SyntheticDelayConfig{FitMu: &mu, FitSigma: &sigma}}
	s := &PhaseSources{Phases: &fakeFrozen{phase: p, cfg: cfg}, Artifacts: &fakeArtifacts{byKey: map[string]*model.ReplayArtifact{}}}

	fc, err := s.FreezeConfigForService(context.Background(), "frontend")
	if err != nil || fc == nil || fc.Mu != 1.5 || fc.Context == nil || fc.Context.PhaseID != "phase-iso" || fc.Context.KeyStrategy != "canonical_v2" {
		t.Fatalf("freeze = %+v, %v", fc, err)
	}
	if ref, _ := s.ReplaySetForService(context.Background(), "frontend"); ref != nil {
		t.Fatalf("replay set before the artifact is assigned = %+v, want nil", ref)
	}
	s.Artifacts.(*fakeArtifacts).byKey["phase-iso/frontend"] = &model.ReplayArtifact{ID: "sha256:aa", EntryCount: 12, W5SHA256: "w5", SizeBytes: 300}
	ref, err := s.ReplaySetForService(context.Background(), "frontend")
	if err != nil || ref == nil {
		t.Fatalf("replay set = %v, %v", ref, err)
	}
	if ref.ID != "sha256:aa" || ref.ExperimentID != "exp-1" || ref.PhaseID != "phase-iso" || ref.KeyStrategy != "canonical_v2" ||
		ref.Count != 12 || ref.SHA256 != "w5" || ref.SizeBytes != 300 || ref.URL != "/api/v1/cache/artifacts/sha256:aa" {
		t.Errorf("replay set = %+v", ref)
	}
}

// The replay set is installed under the same key strategy the freeze config's
// context names: the config's own, not a default, when it sets one.
func TestPhaseSources_ReplaySetCarriesTheConfiguredKeyStrategy(t *testing.T) {
	p := &model.ExperimentPhase{ID: "phase-iso", ExperimentID: "exp-1"}
	cfg := &model.CacheBoxConfig{Service: "frontend", Mode: "replay", KeyStrategy: "exact"}
	s := &PhaseSources{Phases: &fakeFrozen{phase: p, cfg: cfg}, Artifacts: &fakeArtifacts{byKey: map[string]*model.ReplayArtifact{
		"phase-iso/frontend": {ID: "sha256:bb"},
	}}}
	fc, err := s.FreezeConfigForService(context.Background(), "frontend")
	if err != nil || fc == nil || fc.Context == nil {
		t.Fatalf("freeze = %+v, %v", fc, err)
	}
	ref, err := s.ReplaySetForService(context.Background(), "frontend")
	if err != nil || ref == nil {
		t.Fatalf("replay set = %v, %v", ref, err)
	}
	if ref.KeyStrategy != "exact" || ref.KeyStrategy != fc.Context.KeyStrategy {
		t.Errorf("replay set strategy = %q, freeze context strategy = %q, want both exact", ref.KeyStrategy, fc.Context.KeyStrategy)
	}
}

// A failing lookup is an error, never an absent source: the materializer
// aborts on it, and an instance that was served "nothing desired" would drop
// the freeze it holds. The freeze config does not wait on the artifact, and a
// service that is not frozen never reaches the artifact lookup.
func TestPhaseSources_SourceErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	frozen := &fakeFrozen{phase: &model.ExperimentPhase{ID: "phase-iso", ExperimentID: "exp-1"},
		cfg: &model.CacheBoxConfig{Service: "frontend", Mode: "replay"}}

	s := &PhaseSources{Phases: failing{}, Artifacts: &fakeArtifacts{}}
	if cfg, err := s.FreezeConfigForService(ctx, "frontend"); !errors.Is(err, errBoom) || cfg != nil {
		t.Errorf("freeze with a failing phase lookup = %v, %v; want the error", cfg, err)
	}
	if ref, err := s.ReplaySetForService(ctx, "frontend"); !errors.Is(err, errBoom) || ref != nil {
		t.Errorf("replay set with a failing phase lookup = %v, %v; want the error", ref, err)
	}

	s = &PhaseSources{Phases: frozen, Artifacts: failing{}}
	if cfg, err := s.FreezeConfigForService(ctx, "frontend"); err != nil || cfg == nil {
		t.Errorf("freeze with a failing artifact lookup = %v, %v; want the config (it does not read artifacts)", cfg, err)
	}
	if ref, err := s.ReplaySetForService(ctx, "frontend"); !errors.Is(err, errBoom) || ref != nil {
		t.Errorf("replay set with a failing artifact lookup = %v, %v; want the error", ref, err)
	}

	s = &PhaseSources{Phases: &fakeFrozen{}, Artifacts: failing{}}
	if ref, err := s.ReplaySetForService(ctx, "frontend"); err != nil || ref != nil {
		t.Errorf("replay set of a service that is not frozen = %v, %v; want nil, nil without a lookup", ref, err)
	}
}
