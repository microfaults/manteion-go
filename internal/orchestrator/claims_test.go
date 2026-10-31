package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/store"
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

// entryHolds reads the phase's status before it checks the claim. Every
// transition out of running ends the claim, so a claim still current after
// the read means nothing took the phase before the read returned. Checked the
// other way round, a pause and a resume that land while the read is in flight
// leave the status running and pass both checks, and the superseded entry
// starts load next to the newer one. Here the read waits on a table lock
// while a newer entry claims the phase (a pause and a resume, in memory: the
// status ends where it started, running). The orchestrator reads phases over
// one connection of its own, so the test knows the read is waiting by that
// connection's backend pid, whatever else waits on the table.
func TestEntryHolds_ReadsTheStatusBeforeTheClaim(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	reader, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	reader.SetMaxOpenConns(1) // every read below runs on this one backend
	var readerPID int
	if err := reader.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&readerPID); err != nil {
		t.Fatalf("read the reader's backend pid: %v", err)
	}
	o.experiments = store.NewExperimentRepo(reader)
	_, phases := mkExperiment(t, 1)
	ph := phases[0]
	if err := testExpRepo.UpdatePhaseStatus(ctx, ph.ID, "running"); err != nil {
		t.Fatalf("run phase: %v", err)
	}
	claim := o.claimPhase(ph.ID)

	// Every read of experiment_phases waits until this transaction ends.
	tx, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `LOCK TABLE experiment_phases IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock experiment_phases: %v", err)
	}

	held := make(chan error, 1)
	go func() { held <- o.entryHolds(ctx, ph.ID, claim) }()
	waitFor(t, "the status read to wait on the lock", 5*time.Second, func() bool {
		var waiting bool
		err := testDB.QueryRowContext(ctx,
			`SELECT coalesce(wait_event_type = 'Lock', false) FROM pg_stat_activity WHERE pid = $1`, readerPID).Scan(&waiting)
		return err == nil && waiting
	})
	o.claimPhase(ph.ID) // a pause and a resume land while the read is in flight
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := receive(t, "entryHolds", held); !errors.Is(err, errLeftRunning) {
		t.Fatalf("entryHolds = %v, want errLeftRunning: a newer entry claimed the phase while its status was read", err)
	}
}
