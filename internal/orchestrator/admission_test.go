package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// frozenPhasePending creates a pending isolation phase freezing `service`.
func frozenPhasePending(t *testing.T, expID, service, keyStrategy string, pos int) *model.ExperimentPhase {
	t.Helper()
	p := &model.ExperimentPhase{
		ID: id.New("phase"), ExperimentID: expID, Name: fmt.Sprintf("iso-%d", pos), Position: pos, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: service, Mode: "replay", KeyStrategy: keyStrategy, MutationPolicy: "deny"}},
	}
	if err := testExpRepo.CreatePhase(context.Background(), p); err != nil {
		t.Fatalf("create frozen phase: %v", err)
	}
	return p
}

// TestPreload_StrategyMismatchFailsPreflight pins MANT-5(a)/INV-2: entries
// recorded under one key strategy cannot be preloaded into a freeze context
// using another — the mismatch is a preflight failure, not a silent all-miss run.
func TestPreload_StrategyMismatchFailsPreflight(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	base := mkCompletedBaseline(t, exp.ID, 0)
	// Recorded under "exact"...
	if err := o.cacheStore.Append(exp.ID, base.ID, "frontend",
		[]atroposdk.CacheBoxWireEntry{{Key: "k1", StatusCode: 200, Body: []byte("v1"), KeyStrategy: "exact"}}); err != nil {
		t.Fatal(err)
	}
	// ...but the isolation phase freezes with canonical_v2.
	iso := frozenPhasePending(t, exp.ID, "frontend", "canonical_v2", 1)
	if err := testExpRepo.UpdatePhaseStatus(ctx, iso.ID, "running"); err != nil {
		t.Fatal(err)
	}
	sdk := newFakeSDK(t, true) // cooperative — but preflight aborts before contact
	registerSDK(t, "frontend", sdk.server.URL)

	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "key_strategy_mismatch") {
		t.Fatalf("enterPhase err = %v, want key_strategy_mismatch", err)
	}
	if s := getPhase(t, iso.ID).Status; s != "failed" {
		t.Fatalf("phase status = %q, want failed", s)
	}
	if sdk.freezeHit.Load() {
		t.Fatal("freeze asserted despite a preflight failure")
	}
}

// TestExperimentStart_RejectsOverlappingServices pins MANT-5(b)/INV-5: an
// experiment whose service set overlaps a running experiment's is refused, with
// the shared services and both experiments named.
func TestExperimentStart_RejectsOverlappingServices(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	shared := "shared-" + id.New("s")

	exp1 := mkExp(t)
	frozenPhasePending(t, exp1.ID, shared, "exact", 0)
	if _, err := testExpRepo.TransitionExperiment(ctx, exp1.ID, "running", "planned"); err != nil {
		t.Fatalf("run exp1: %v", err)
	}

	exp2 := mkExp(t)
	frozenPhasePending(t, exp2.ID, shared, "exact", 0)

	err := o.StartExperiment(ctx, exp2.ID)
	if err == nil || !strings.Contains(err.Error(), shared) || !strings.Contains(err.Error(), exp1.ID) {
		t.Fatalf("StartExperiment err = %v, want overlap naming %q and %q", err, shared, exp1.ID)
	}
	if s := getExp(t, exp2.ID).Status; s != "planned" {
		t.Fatalf("exp2 status = %q, want planned (start refused)", s)
	}
}

// TestExperimentStart_OverrideAllowsOverlap pins the MANTEION_ALLOW_CONCURRENT_OVERLAP
// escape hatch: with it set, an overlapping experiment is admitted (claimed
// running) despite the shared service.
func TestExperimentStart_OverrideAllowsOverlap(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.WithAllowConcurrentOverlap(true)
	shared := "shared-" + id.New("s")

	exp1 := mkExp(t)
	frozenPhasePending(t, exp1.ID, shared, "exact", 0)
	if _, err := testExpRepo.TransitionExperiment(ctx, exp1.ID, "running", "planned"); err != nil {
		t.Fatalf("run exp1: %v", err)
	}

	exp2 := mkExp(t)
	frozenPhasePending(t, exp2.ID, shared, "exact", 0)

	if err := o.StartExperiment(ctx, exp2.ID); err != nil {
		t.Fatalf("with override, StartExperiment err = %v, want nil (admitted)", err)
	}
}
