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

// mkRunningBaseline creates a running persist_cache (recording) phase.
func mkRunningBaseline(t *testing.T, expID string, pos int) *model.ExperimentPhase {
	t.Helper()
	ctx := context.Background()
	p := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: expID, Name: "baseline", Position: pos, Status: "pending", PersistCache: true}
	if err := testExpRepo.CreatePhase(ctx, p); err != nil {
		t.Fatalf("create baseline: %v", err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, p.ID, "running"); err != nil {
		t.Fatalf("run baseline: %v", err)
	}
	return p
}

// ingestN makes `instance` a pusher (and thus a drain-expected instance) by
// ingesting n entries for (exp, phase, service, instance) at batchSeq.
func ingestN(t *testing.T, o *Orchestrator, exp, phase, service, instance string, batchSeq, n int) {
	t.Helper()
	entries := make([]atroposdk.CacheBoxWireEntry, n)
	for i := range entries {
		entries[i] = atroposdk.CacheBoxWireEntry{Key: fmt.Sprintf("%s-%d", instance, i), StatusCode: 200}
	}
	if _, err := o.cacheStore.Ingest(exp, phase, service, instance, batchSeq, entries); err != nil {
		t.Fatalf("ingest: %v", err)
	}
}

func report(o *Orchestrator, exp, phase, service, instance string, recorded int64) {
	o.cacheStore.RecordDrainReport(atroposdk.DrainReport{
		ExperimentID: exp, PhaseID: phase, Service: service, InstanceID: instance, EntriesRecorded: recorded,
	})
}

// TestFinishPhase_DrainGateWaitsForReports pins MANT-2/INV-3: a recording phase
// holds at 'draining' until every expected instance has flushed and reported
// with received == entries_recorded, then completes clean.
func TestFinishPhase_DrainGateWaitsForReports(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.WithDrainTimeout(5 * time.Second)
	o.WithDrainPollInterval(20 * time.Millisecond)

	exp := mkExp(t)
	base := mkRunningBaseline(t, exp.ID, 0)
	ingestN(t, o, exp.ID, base.ID, "frontend", "i1", 1, 3)
	ingestN(t, o, exp.ID, base.ID, "frontend", "i2", 1, 2)

	done := make(chan bool, 1)
	go func() { done <- o.finishPhase(context.Background(), base.ID, "completed", "running") }()

	waitFor(t, "phase draining", 2*time.Second, func() bool { return getPhase(t, base.ID).Status == "draining" })

	// No reports yet → must not complete.
	time.Sleep(100 * time.Millisecond)
	if s := getPhase(t, base.ID).Status; s != "draining" {
		t.Fatalf("status %q before any report, want draining", s)
	}

	// i1 fully reported, i2 silent → still draining.
	report(o, exp.ID, base.ID, "frontend", "i1", 3)
	time.Sleep(100 * time.Millisecond)
	if s := getPhase(t, base.ID).Status; s != "draining" {
		t.Fatalf("status %q with i2 still owing, want draining", s)
	}

	// i2 reports → clean completion.
	report(o, exp.ID, base.ID, "frontend", "i2", 2)
	waitFor(t, "phase completed", 3*time.Second, func() bool { return getPhase(t, base.ID).Status == "completed" })
	if !<-done {
		t.Fatal("finishPhase returned false")
	}
	drain, err := testExpRepo.GetPhaseDrain(ctx, base.ID)
	if err != nil || drain == nil {
		t.Fatalf("get drain result: %v", err)
	}
	if drain.Status != "clean" {
		t.Fatalf("drain status %q, want clean", drain.Status)
	}
}

// TestFinishPhase_DrainTimeoutDegraded pins MANT-2: a silent instance at the
// drain timeout yields a degraded result naming it, and the phase still
// completes (the degradation is recorded, not swallowed).
func TestFinishPhase_DrainTimeoutDegraded(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.WithDrainTimeout(300 * time.Millisecond)
	o.WithDrainPollInterval(20 * time.Millisecond)

	exp := mkExp(t)
	base := mkRunningBaseline(t, exp.ID, 0)
	ingestN(t, o, exp.ID, base.ID, "frontend", "i1", 1, 3)
	ingestN(t, o, exp.ID, base.ID, "frontend", "i2", 1, 2)
	report(o, exp.ID, base.ID, "frontend", "i1", 3) // i1 clean; i2 stays silent

	start := time.Now()
	won := o.finishPhase(ctx, base.ID, "completed", "running")
	elapsed := time.Since(start)
	if !won {
		t.Fatal("finishPhase returned false")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("drain took %v, want ~timeout (300ms)", elapsed)
	}
	if s := getPhase(t, base.ID).Status; s != "completed" {
		t.Fatalf("status %q, want completed (degraded still completes)", s)
	}
	drain, err := testExpRepo.GetPhaseDrain(ctx, base.ID)
	if err != nil || drain == nil {
		t.Fatalf("get drain result: %v", err)
	}
	if drain.Status != "degraded" {
		t.Fatalf("drain status %q, want degraded", drain.Status)
	}
	if !contains(drain.MissingInstances, "i2") {
		t.Fatalf("missing_instances %v, want to name i2", drain.MissingInstances)
	}
}

// TestScheduler_RefusesDegradedBaseline pins MANT-2(d): an isolation phase whose
// source recording is degraded refuses to start, unless the override is set.
func TestScheduler_RefusesDegradedBaseline(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	base := mkCompletedBaseline(t, exp.ID, 0)
	if err := testExpRepo.UpsertPhaseDrain(ctx, base.ID,
		&model.PhaseDrainResult{Status: "degraded", MissingInstances: []string{"i-dead"}, ShortfallEntries: 5}); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}
	if err := o.cacheStore.Append(exp.ID, base.ID, "frontend",
		[]atroposdk.CacheBoxWireEntry{{Key: "k1", StatusCode: 200, Body: []byte("v1")}}); err != nil {
		t.Fatal(err)
	}
	sdk := newFakeSDK(t, true)
	registerSDK(t, "frontend", sdk.server.URL)

	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	err := o.enterPhase(ctx, iso, true)
	if err == nil || !strings.Contains(err.Error(), "degraded") {
		t.Fatalf("enterPhase err = %v, want a degraded-baseline refusal", err)
	}
	if s := getPhase(t, iso.ID).Status; s != "failed" {
		t.Fatalf("phase status = %q, want failed", s)
	}

	// The override lets it proceed.
	o.WithAllowDegradedBaseline(true)
	iso2 := mkRunningIsolation(t, exp.ID, "frontend", 2)
	if err := o.enterPhase(ctx, iso2, true); err != nil {
		t.Fatalf("with override, enterPhase err = %v, want nil", err)
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
