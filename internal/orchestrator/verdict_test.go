package orchestrator

import (
	"context"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// finishIsolation registers a fake SDK for `frontend` serving `snap` as its
// fidelity, completes the running isolation phase through finishPhase, and
// returns the persisted verdict.
func finishIsolationVerdict(t *testing.T, o *Orchestrator, exp, isoID string, snap *atroposdk.FidelitySnapshot) {
	t.Helper()
	sdk := newFakeSDK(t, true)
	sdk.fidelity = snap
	registerSDK(t, "frontend", sdk.server.URL)
	if !o.finishPhase(context.Background(), isoID, "completed", "running") {
		t.Fatal("finishPhase returned false")
	}
}

func cleanSnapshot() *atroposdk.FidelitySnapshot {
	return &atroposdk.FidelitySnapshot{
		ReplayHits: 100, ReplayMisses: 0,
		ReplayAgeMs: atroposdk.FidelityReplayAge{Max: 60000, Mean: 30000},
		Preload:     atroposdk.FidelityPreloadState{Committed: true, Entries: 100},
	}
}

// TestFinishPhase_VerdictInvalidOnMiss pins MANT-6/INV-6: a single replay miss
// makes the phase verdict INVALID with the machine-readable reason.
func TestFinishPhase_VerdictInvalidOnMiss(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 0)

	snap := cleanSnapshot()
	snap.ReplayMisses = 1
	finishIsolationVerdict(t, o, exp.ID, iso.ID, snap)

	v, err := testExpRepo.GetPhaseVerdict(ctx, iso.ID)
	if err != nil || v == nil {
		t.Fatalf("get verdict: %v", err)
	}
	if v.Verdict != "INVALID" || !contains(v.Reasons, "fidelity_violation:replay_miss") {
		t.Fatalf("verdict = %q reasons %v, want INVALID + replay_miss", v.Verdict, v.Reasons)
	}
}

// TestFinishPhase_VerdictInvalidOnMissingTelemetry pins MANT-6: a failed
// fidelity pull (the instance serves no snapshot) is INVALID(telemetry_missing)
// — an unmeasured freeze is treated as a failed one.
func TestFinishPhase_VerdictInvalidOnMissingTelemetry(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 0)

	finishIsolationVerdict(t, o, exp.ID, iso.ID, nil) // nil ⇒ fidelity endpoint 503s

	v, err := testExpRepo.GetPhaseVerdict(ctx, iso.ID)
	if err != nil || v == nil {
		t.Fatalf("get verdict: %v", err)
	}
	if v.Verdict != "INVALID" || !contains(v.Reasons, "telemetry_missing") {
		t.Fatalf("verdict = %q reasons %v, want INVALID + telemetry_missing", v.Verdict, v.Reasons)
	}
}

// TestFinishPhase_WarningOnCollisions pins MANT-6: a clean replay over a
// recording with a divergent-collision rate > 1% is VALID_WITH_WARNINGS.
func TestFinishPhase_WarningOnCollisions(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	base := mkCompletedBaseline(t, exp.ID, 0)
	// Same key recorded with two different bodies ⇒ one divergent collision.
	ingestSig(t, o, exp.ID, base.ID, "i1", 1, "k", "sha-a")
	ingestSig(t, o, exp.ID, base.ID, "i1", 2, "k", "sha-b")
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)

	finishIsolationVerdict(t, o, exp.ID, iso.ID, cleanSnapshot())

	v, err := testExpRepo.GetPhaseVerdict(ctx, iso.ID)
	if err != nil || v == nil {
		t.Fatalf("get verdict: %v", err)
	}
	if v.Verdict != "VALID_WITH_WARNINGS" || v.CollisionRate <= 0.01 {
		t.Fatalf("verdict = %q rate %v, want VALID_WITH_WARNINGS + rate>1%%", v.Verdict, v.CollisionRate)
	}
}

// TestFinishPhase_VerdictValidCleanRun pins MANT-6: a clean replay over a clean
// recording is VALID with no reasons, and carries the real replay age.
func TestFinishPhase_VerdictValidCleanRun(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	mkCompletedBaseline(t, exp.ID, 0) // clean, no collisions, not degraded
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)

	finishIsolationVerdict(t, o, exp.ID, iso.ID, cleanSnapshot())

	v, err := testExpRepo.GetPhaseVerdict(ctx, iso.ID)
	if err != nil || v == nil {
		t.Fatalf("get verdict: %v", err)
	}
	if v.Verdict != "VALID" || len(v.Reasons) != 0 {
		t.Fatalf("verdict = %q reasons %v, want VALID + no reasons", v.Verdict, v.Reasons)
	}
	if v.ReplayAgeMeanMs != 30000 {
		t.Fatalf("replay_age_mean_ms = %d, want 30000 (from the snapshot, not hardcoded 0)", v.ReplayAgeMeanMs)
	}
}

// ingestSig ingests one entry with an explicit (key, body sha) so tests can
// drive collision detection deterministically.
func ingestSig(t *testing.T, o *Orchestrator, exp, phase, instance string, batchSeq int, key, sha string) {
	t.Helper()
	_, err := o.cacheStore.Ingest(exp, phase, "frontend", instance, batchSeq,
		[]atroposdk.CacheBoxWireEntry{{Key: key, StatusCode: 200, ResponseBodySHA256: sha}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
}
