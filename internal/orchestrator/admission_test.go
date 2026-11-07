package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

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
	sdk := newFakeSDK(t) // cooperative — but preflight aborts before any replay set exists
	registerSDK(t, "frontend", sdk.server.URL)

	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "key_strategy_mismatch") {
		t.Fatalf("enterPhase err = %v, want key_strategy_mismatch", err)
	}
	if s := getPhase(t, iso.ID).Status; s != "failed" {
		t.Fatalf("phase status = %q, want failed", s)
	}
	if a, err := o.artifacts.ForPhaseService(ctx, iso.ID, "frontend"); err != nil || a != nil {
		t.Fatalf("replay set assigned despite a preflight failure: %+v, %v", a, err)
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

// (The former MANTEION_ALLOW_CONCURRENT_OVERLAP escape hatch is deleted:
// the SDK is single-tenant per instance -- one replay set, one preload
// staging slot -- so shared-service concurrency silently serves one
// experiment the other's data. Overlap admission is unconditional; see
// TestExperimentStart_RejectsOverlappingServices.)

// A cache-box rule edited after the experiment started can disagree with its
// service's freeze by the time the scheduler reaches the phase it is attached
// to. That phase's start is refused before its claim, so the scheduler fails
// the still-pending phase with the refusal as its reason — otherwise nothing
// would ever start it — and the cascade fails the experiment.
func TestAdvanceExperiment_CacheBoxRuleDisagreementFailsThePendingPhase(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	svc := id.New("svc-cbagree")
	frozenPhasePending(t, exp.ID, svc, "exact", 1)
	base := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "base", Position: 0, Status: "pending"}
	if err := testExpRepo.CreatePhase(ctx, base); err != nil {
		t.Fatal(err)
	}
	r := &model.Rule{
		ID: id.New("rule"), Name: "cb-agree", Service: svc, Mode: "inline",
		Action:    model.RuleAction{Type: "cachebox", CacheBox: &model.CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: "canonical_v2"}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := testRuleRepo.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testDB.Exec("DELETE FROM phase_rules WHERE rule_id = $1", r.ID)
		_, _ = testDB.Exec("DELETE FROM rules WHERE id = $1", r.ID)
	})
	if err := testExpRepo.AttachPhaseRules(ctx, base.ID, []string{r.ID}); err != nil {
		t.Fatal(err)
	}
	if ok, err := testExpRepo.TransitionExperiment(ctx, exp.ID, "running", "planned"); err != nil || !ok {
		t.Fatalf("mark experiment running: %v %v", ok, err)
	}

	o.advanceExperiment(ctx, exp.ID)

	got := getPhase(t, base.ID)
	if got.Status != "failed" || !strings.Contains(got.FailureReason, `keys with "canonical_v2", but phase "iso-1" freezes`) {
		t.Fatalf("phase = %s / %q, want failed with the agreement refusal as its reason", got.Status, got.FailureReason)
	}
	waitFor(t, "the cascade fails the experiment", 5*time.Second, func() bool {
		e, err := testExpRepo.Get(ctx, exp.ID)
		return err == nil && e.Status == "failed"
	})
}
