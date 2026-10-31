package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"manteion-go/internal/id"
)

// An entry acts on its phase only while its claim is the phase's current
// one: once a newer entry claimed the phase, the old entry's failure and its
// return to paused change nothing (no transition, no bump), and the newer
// claim stays in force.
func TestEntryClaim_StaleClaimLeavesTheNewerEntryAlone(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	svc := id.New("svc-stale")
	ph := mkRunningRulePhase(t, exp.ID, svc, 0)
	stale := o.claimPhase(ph.ID)
	current := o.claimPhase(ph.ID) // a pause and a resume later: a newer entry holds the phase
	g0 := generation(t, o, svc)

	cause := errors.New("orchestrator: converge gate: the stale entry gave up")
	if err := o.failEntry(ctx, ph, stale, cause); !errors.Is(err, cause) {
		t.Fatalf("stale failEntry = %v, want the cause back", err)
	}
	if err := o.leavePaused(ctx, ph, stale, cause); !errors.Is(err, cause) {
		t.Fatalf("stale leavePaused = %v, want the cause back", err)
	}
	if got := getPhase(t, ph.ID); got.Status != "running" || got.FailureReason != "" {
		t.Fatalf("phase = %s / %q after a stale entry's failure, want running and untouched", got.Status, got.FailureReason)
	}
	if g := generation(t, o, svc); g != g0 {
		t.Fatalf("generation %d → %d: a stale entry bumped", g0, g)
	}
	if !o.holdsClaim(ph.ID, current) {
		t.Fatal("the newer claim was lost")
	}

	// The current claim's failure does fail the phase.
	if err := o.failEntry(ctx, ph, current, cause); !errors.Is(err, cause) {
		t.Fatalf("failEntry = %v, want the cause back", err)
	}
	if got := getPhase(t, ph.ID); got.Status != "failed" || !strings.Contains(got.FailureReason, "the stale entry gave up") {
		t.Fatalf("phase = %s / %q, want failed with the reason", got.Status, got.FailureReason)
	}
	if o.holdsClaim(ph.ID, current) {
		t.Fatal("the claim outlived the phase's terminal transition")
	}
}
