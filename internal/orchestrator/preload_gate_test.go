package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// mkExp creates a planned experiment for the gate tests.
func mkExp(t *testing.T) *model.Experiment {
	t.Helper()
	exp := &model.Experiment{ID: id.New("exp"), Name: "gate-" + id.New("n"), Status: "planned", CreatedAt: time.Now()}
	if err := testExpRepo.Create(context.Background(), exp); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	return exp
}

// mkCompletedBaseline creates a completed persist_cache baseline phase, named
// "baseline" at position 0 and "baseline-<pos>" elsewhere (phase names are
// unique per experiment).
func mkCompletedBaseline(t *testing.T, expID string, pos int) *model.ExperimentPhase {
	t.Helper()
	ctx := context.Background()
	name := "baseline"
	if pos != 0 {
		name = fmt.Sprintf("baseline-%d", pos)
	}
	p := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: expID, Name: name, Position: pos, Status: "pending", PersistCache: true}
	if err := testExpRepo.CreatePhase(ctx, p); err != nil {
		t.Fatalf("create baseline: %v", err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, p.ID, "completed"); err != nil {
		t.Fatalf("complete baseline: %v", err)
	}
	return p
}

// mkRunningIsolation creates and starts (status=running) an isolation phase
// freezing `service`, ready to be handed to enterPhase directly.
func mkRunningIsolation(t *testing.T, expID, service string, pos int) *model.ExperimentPhase {
	t.Helper()
	ctx := context.Background()
	p := &model.ExperimentPhase{
		ID: id.New("phase"), ExperimentID: expID, Name: fmt.Sprintf("isolation-%d", pos), Position: pos, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: service, Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
	}
	if err := testExpRepo.CreatePhase(ctx, p); err != nil {
		t.Fatalf("create isolation: %v", err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, p.ID, "running"); err != nil {
		t.Fatalf("run isolation: %v", err)
	}
	return p
}

// TestEnterPhase_NoBaselineAbortsFrozenPhase pins MANT-1: a frozen phase with no
// completed baseline recording fails the preload gate — load never runs against
// an absent replay set (the old code INFO-skipped, yielding an all-leak run).
func TestEnterPhase_NoBaselineAbortsFrozenPhase(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 0)

	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "baseline") {
		t.Fatalf("enterPhase err = %v, want a no-baseline abort", err)
	}
	if got := getPhase(t, iso.ID).Status; got != "failed" {
		t.Fatalf("phase status = %q, want failed", got)
	}
}

// TestEnterPhase_ZeroEntriesAborts pins MANT-1: a frozen service with zero
// recorded baseline entries fails the gate (nothing to replay ⇒ all-miss).
func TestEnterPhase_ZeroEntriesAborts(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	mkCompletedBaseline(t, exp.ID, 0)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)

	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "zero entries") {
		t.Fatalf("enterPhase err = %v, want a zero-entries abort", err)
	}
	if got := getPhase(t, iso.ID).Status; got != "failed" {
		t.Fatalf("phase status = %q, want failed", got)
	}
}
