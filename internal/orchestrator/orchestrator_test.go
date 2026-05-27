// Package orchestrator_test contains integration tests for the Orchestrator FSM.
//
// These tests require a live PostgreSQL instance. They are opt-in: set
// MANTEION_TEST_DB=1 (or supply a DSN via MANTEION_DATABASE_URL) before running.
//
// Quick start:
//
//	cd manteion-go
//	docker-compose up -d          # starts Postgres 17 on :5432
//	MANTEION_TEST_DB=1 go test ./internal/orchestrator/ -v -count=1
package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/atropos"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/db"
	"manteion-go/internal/model"
	"manteion-go/internal/store"

	"database/sql"
)

// ----------------------------------------------------------------------------
// Package-level state shared across all tests in this file.
// ----------------------------------------------------------------------------

var (
	testDB        *sql.DB
	testExperRepo *store.ExperimentRepo
	testRuleRepo  *store.RuleRepo
	testFaultRepo *store.FaultRepo
	testWorkRepo  *store.WorkloadRepo
)

// TestMain is the test harness entry point. It:
//  1. Skips the entire suite when MANTEION_TEST_DB is not set.
//  2. Opens a real DB connection via db.Open, which also runs migrations.
//  3. Provides shared repos to every sub-test.
//  4. Closes the pool on exit.
func TestMain(m *testing.M) {
	// Guard: require explicit opt-in so the suite never blocks CI environments
	// that lack Postgres.
	if os.Getenv("MANTEION_TEST_DB") == "" {
		fmt.Fprintln(os.Stderr,
			"orchestrator integration tests skipped — set MANTEION_TEST_DB=1 to run")
		os.Exit(0)
	}

	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	testDB, err = db.Open(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator test: cannot connect to DB: %v\n", err)
		os.Exit(1)
	}

	testExperRepo = store.NewExperimentRepo(testDB)
	testRuleRepo = store.NewRuleRepo(testDB)
	testFaultRepo = store.NewFaultRepo(testDB)
	testWorkRepo = store.NewWorkloadRepo(testDB)

	code := m.Run()

	_ = db.Close(testDB)
	os.Exit(code)
}

// ----------------------------------------------------------------------------
// Test helpers
// ----------------------------------------------------------------------------

// newNoOpController builds an atrocontrol.Controller backed by an empty
// resolver. When enterPhase or StopRun call PushRules, the fanout finds zero
// SDK instances and returns an error — but the Orchestrator only logs a warning
// for fanout failures; it does not propagate them. FSM transitions therefore
// complete normally even with this no-op controller.
func newNoOpController() *atrocontrol.Controller {
	resolver := &emptyResolver{}
	tx := atropos.NewClient()
	return atrocontrol.New(tx, resolver,
		atrocontrol.WithDefaultTimeout(500*time.Millisecond),
	)
}

// emptyResolver implements atrocontrol.InstanceResolver with zero instances.
// PushRules will call ForService and get back nil — the fanout will short-circuit
// with "no instances found" without attempting any network connections.
type emptyResolver struct{}

func (e *emptyResolver) ForService(_ context.Context, _ string) ([]*model.SDKInstance, error) {
	return nil, nil
}
func (e *emptyResolver) ForInstance(_ context.Context, id string) (*model.SDKInstance, error) {
	return nil, fmt.Errorf("instance %q not found", id)
}

// newOrchestrator constructs a full Orchestrator wired to the real DB repos.
// zeus and prom are nil so code paths that guard with `if o.zeusClient != nil`
// skip cleanly. cacheStore points to a temp directory that is removed on
// test cleanup.
func newOrchestrator(t *testing.T) *Orchestrator {
	t.Helper()
	cs := cachestore.New(t.TempDir())
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	orc := New(
		testExperRepo,
		testRuleRepo,
		testFaultRepo,
		testWorkRepo,
		newNoOpController(),
		nil, // zeus.Client — not needed for FSM tests
		cs,
		logger,
	)
	// FSM tests drive runs manually; without a Zeus poller a started run would
	// otherwise auto-complete and race the test's explicit Pause/Stop. Tests that
	// specifically exercise auto-complete (DAG fan-out, crash recovery) re-enable it.
	// autoComplete is unexported; in-package tests set it directly so the toggle
	// never leaks into the production API surface.
	orc.autoComplete = false
	return orc
}

// seedMinimalExperiment inserts a minimal experiment with a workflow reference.
// Returns the inserted experiment. Everything is given unique IDs derived from
// the test name so parallel sub-tests do not collide.
func seedMinimalExperiment(t *testing.T, ctx context.Context) *model.Experiment {
	t.Helper()
	tag := uniqueTag(t)

	exp := &model.Experiment{
		ID:                "exp-" + tag,
		Name:              "test-experiment-" + tag,
		PrimaryWorkflowID: "wf-" + tag,
		Status:            "planned",
		CreatedAt:         time.Now(),
	}
	if err := testExperRepo.Create(ctx, exp); err != nil {
		t.Fatalf("seed experiment: %v", err)
	}

	t.Cleanup(func() {
		_ = testExperRepo.Delete(context.Background(), exp.ID)
	})

	return exp
}

// seedRun inserts an ExperimentRun in "pending" status for the given experiment.
// The run is a baseline (no frozen services, satisfying the FK + validation
// constraints). Returns the run.
func seedRun(t *testing.T, ctx context.Context, exp *model.Experiment) *model.ExperimentRun {
	t.Helper()
	tag := uniqueTag(t)

	run := &model.ExperimentRun{
		ID:           "run-" + tag,
		ExperimentID: exp.ID,
		RunType:      "baseline",
		RunIndex:     0,
		MetaTraceID:  "trace-" + tag,
		Status:       "pending",
		// PhaseRules is empty — enterPhase with empty rule IDs pushes nil
		// (a clear), which is a no-op on the no-op controller.
		PhaseRules:   []model.PhaseRuleSet{{Phase: 0, RuleIDs: nil}},
		CurrentPhase: 0,
		CreatedAt:    time.Now(),
	}
	if err := testExperRepo.CreateRun(ctx, run); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	t.Cleanup(func() {
		_, _ = testDB.ExecContext(context.Background(),
			`DELETE FROM experiment_runs WHERE id = $1`, run.ID)
	})

	return run
}

// assertRunStatus fetches the run from the DB and checks its status field.
// It is a fatal assertion.
func assertRunStatus(t *testing.T, ctx context.Context, runID, want string) {
	t.Helper()
	got, err := testExperRepo.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("assertRunStatus: GetRun(%q): %v", runID, err)
	}
	if got.Status != want {
		t.Fatalf("run %q: expected status %q, got %q", runID, want, got.Status)
	}
}

// uniqueTag returns a string that is unique within the test run, derived from
// the test name and the current nanosecond. Safe for use as ID suffixes.
func uniqueTag(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

// ----------------------------------------------------------------------------
// FSM transition tests
// ----------------------------------------------------------------------------

// TestStartRun_PendingToRunning verifies the happy-path transition:
// a freshly-seeded "pending" run becomes "running" after StartRun.
func TestStartRun_PendingToRunning(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	assertRunStatus(t, ctx, run.ID, "running")

	// Clean up background goroutines spawned by StartRun before the test ends.
	// StopRun cancels the watch context and is idempotent if the run is already
	// in a terminal state.
	_ = orc.StopRun(ctx, run.ID, "completed")
}

// TestStartRun_NonPendingReturnsError verifies that StartRun rejects a run
// that is not in "pending" status.
func TestStartRun_NonPendingReturnsError(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	// Advance to "running" first.
	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("first StartRun: %v", err)
	}
	defer orc.StopRun(ctx, run.ID, "completed") //nolint:errcheck

	// Calling StartRun again while running must return an error.
	err := orc.StartRun(ctx, run.ID)
	if err == nil {
		t.Fatal("expected error when starting a non-pending run, got nil")
	}
}

// TestPauseRun_RunningToPaused verifies the pause transition and that the
// resulting DB status is "paused".
func TestPauseRun_RunningToPaused(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "running")

	// PauseRun must succeed and persist "paused" status.
	if err := orc.PauseRun(ctx, run.ID); err != nil {
		t.Fatalf("PauseRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "paused")
}

// TestPauseRun_NonRunningReturnsError verifies that PauseRun rejects a run
// that is not currently "running".
func TestPauseRun_NonRunningReturnsError(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)
	// The run is still "pending" — pause should fail.

	err := orc.PauseRun(ctx, run.ID)
	if err == nil {
		t.Fatal("expected error when pausing a non-running run, got nil")
	}
}

// TestResumeRun_PausedToRunning verifies the full pending→running→paused→running
// cycle. The final status in the DB must be "running".
func TestResumeRun_PausedToRunning(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := orc.PauseRun(ctx, run.ID); err != nil {
		t.Fatalf("PauseRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "paused")

	if err := orc.ResumeRun(ctx, run.ID); err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "running")

	// Clean up background goroutine.
	_ = orc.StopRun(ctx, run.ID, "completed")
}

// TestResumeRun_NonPausedReturnsError verifies that ResumeRun rejects a run
// that is not in "paused" status.
func TestResumeRun_NonPausedReturnsError(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	defer orc.StopRun(ctx, run.ID, "completed") //nolint:errcheck

	// The run is "running", not "paused" — ResumeRun should fail.
	err := orc.ResumeRun(ctx, run.ID)
	if err == nil {
		t.Fatal("expected error when resuming a non-paused run, got nil")
	}
}

// TestStopRun_RunningToCompleted verifies that StopRun persists "completed"
// status and cancels the in-memory watcher entry.
func TestStopRun_RunningToCompleted(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	if err := orc.StopRun(ctx, run.ID, "completed"); err != nil {
		t.Fatalf("StopRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "completed")

	// Verify the Orchestrator removed the run from its in-flight map so a
	// second StopRun call is safe (idempotent at the in-memory level).
	orc.mu.Lock()
	_, still := orc.running[run.ID]
	orc.mu.Unlock()
	if still {
		t.Fatal("expected run to be removed from orc.running after StopRun")
	}
}

// TestStopRun_RunningToFailed verifies StopRun with a "failed" terminal status.
func TestStopRun_RunningToFailed(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	if err := orc.StopRun(ctx, run.ID, "failed"); err != nil {
		t.Fatalf("StopRun(failed): %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "failed")
}

// seedRunWithDeps inserts an ExperimentRun with custom DependsOn.
func seedRunWithDeps(t *testing.T, ctx context.Context, exp *model.Experiment, runType string, deps []string, frozen []model.CacheBoxConfig) *model.ExperimentRun {
	t.Helper()
	tag := uniqueTag(t)

	run := &model.ExperimentRun{
		ID:             "run-" + tag,
		ExperimentID:   exp.ID,
		RunType:        runType,
		RunIndex:       0,
		MetaTraceID:    "trace-" + tag,
		Status:         "pending",
		PhaseRules:     []model.PhaseRuleSet{{Phase: 0, RuleIDs: nil}},
		CurrentPhase:   0,
		DependsOn:      deps,
		FrozenServices: frozen,
		CreatedAt:      time.Now(),
	}
	if err := testExperRepo.CreateRun(ctx, run); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	t.Cleanup(func() {
		_, _ = testDB.ExecContext(context.Background(),
			`DELETE FROM experiment_runs WHERE id = $1`, run.ID)
	})

	return run
}

// TestRecover_RunningRunRestored verifies that Recover picks up a "running" run
// from the DB and populates the in-memory running map. Since zeus is nil, the
// run has no watcher or poller and gets auto-completed.
func TestRecover_RunningRunRestored(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)
	orc.autoComplete = true // this test asserts the orphan auto-completes

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	// Manually set the run to "running" in DB, simulating a crash mid-run.
	if err := testExperRepo.UpdateRunStatus(ctx, run.ID, "running"); err != nil {
		t.Fatalf("seed running status: %v", err)
	}

	// Recover should find it and handle it.
	if err := orc.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// With no zeus and no multi-phase watcher, the run should auto-complete.
	// Give the async goroutine a moment.
	time.Sleep(200 * time.Millisecond)

	assertRunStatus(t, ctx, run.ID, "completed")
}

// TestRecover_PausedRunLeftAlone verifies that Recover does not touch paused runs.
func TestRecover_PausedRunLeftAlone(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	if err := testExperRepo.UpdateRunStatus(ctx, run.ID, "running"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := testExperRepo.UpdateRunStatus(ctx, run.ID, "paused"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := orc.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	assertRunStatus(t, ctx, run.ID, "paused")
}

// TestAdvanceExperiment_DAGSequencing verifies that completing a baseline run
// triggers the dependent isolation run to start.
func TestAdvanceExperiment_DAGSequencing(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)
	orc.autoComplete = true // DAG fan-out relies on driver-less runs auto-completing

	exp := seedMinimalExperiment(t, ctx)

	// Create a two-run DAG: baseline (no deps) → isolation (depends on baseline)
	baseline := seedRunWithDeps(t, ctx, exp, "baseline", nil, nil)
	frozen := []model.CacheBoxConfig{{
		Service: "svc", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny",
	}}
	isolation := seedRunWithDeps(t, ctx, exp, "isolation", []string{baseline.ID}, frozen)

	// Start the experiment — should only start the baseline.
	if err := orc.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("StartExperiment: %v", err)
	}

	// Give auto-complete goroutine time to fire (baseline has no watcher/poller).
	time.Sleep(300 * time.Millisecond)

	// Baseline should have auto-completed; isolation should have been started
	// by advanceExperiment and also auto-completed.
	assertRunStatus(t, ctx, baseline.ID, "completed")

	time.Sleep(300 * time.Millisecond)
	assertRunStatus(t, ctx, isolation.ID, "completed")
}

// TestAdvanceExperiment_FailureCascade verifies that a failed run cascades
// failure to its dependents.
func TestAdvanceExperiment_FailureCascade(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)

	baseline := seedRunWithDeps(t, ctx, exp, "baseline", nil, nil)
	frozen := []model.CacheBoxConfig{{
		Service: "svc", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny",
	}}
	isolation := seedRunWithDeps(t, ctx, exp, "isolation", []string{baseline.ID}, frozen)

	// The cascade runs inside advanceExperiment, which only acts on a running
	// experiment — mark it running without StartExperiment (which would auto-walk
	// the DAG) so we can drive the baseline failure explicitly.
	if err := testExperRepo.UpdateStatus(ctx, exp.ID, "running"); err != nil {
		t.Fatalf("set experiment running: %v", err)
	}

	// Start baseline, then fail it.
	if err := orc.StartRun(ctx, baseline.ID); err != nil {
		t.Fatalf("StartRun(baseline): %v", err)
	}
	if err := orc.StopRun(ctx, baseline.ID, "failed"); err != nil {
		t.Fatalf("StopRun(failed): %v", err)
	}

	// Give advanceExperiment goroutine time.
	time.Sleep(300 * time.Millisecond)

	assertRunStatus(t, ctx, baseline.ID, "failed")
	assertRunStatus(t, ctx, isolation.ID, "failed")
}

// TestFullCycle_PauseResumeThenStop exercises the entire run lifecycle in one
// test: pending → running → paused → running → completed.
func TestFullCycle_PauseResumeThenStop(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	// pending → running
	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "running")

	// running → paused
	if err := orc.PauseRun(ctx, run.ID); err != nil {
		t.Fatalf("PauseRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "paused")

	// paused → running
	if err := orc.ResumeRun(ctx, run.ID); err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "running")

	// running → completed
	if err := orc.StopRun(ctx, run.ID, "completed"); err != nil {
		t.Fatalf("StopRun: %v", err)
	}
	assertRunStatus(t, ctx, run.ID, "completed")
}

// TestStartRun_ConcurrentNoDoubleStart verifies the atomic pending→running
// claim: many concurrent StartRun calls on the same pending run yield exactly
// one success (the rest get "not pending"), so a run is never double-started
// (which previously spawned duplicate goroutines + duplicate Zeus attacks when
// two advanceExperiment goroutines raced).
func TestStartRun_ConcurrentNoDoubleStart(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)
	t.Cleanup(func() { _ = orc.StopRun(context.Background(), run.ID, "completed") })

	const n = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := orc.StartRun(ctx, run.ID); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("expected exactly 1 successful StartRun, got %d", successes)
	}
	assertRunStatus(t, ctx, run.ID, "running")
}

// TestFinishRun_RespectsFromStates verifies the from-aware terminal guard: the
// auto-complete path (finishRun from "running") must not complete a paused run,
// so a pause is never clobbered by a racing auto-complete.
func TestFinishRun_RespectsFromStates(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t)

	exp := seedMinimalExperiment(t, ctx)
	run := seedRun(t, ctx, exp)

	if err := orc.StartRun(ctx, run.ID); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := orc.PauseRun(ctx, run.ID); err != nil {
		t.Fatalf("PauseRun: %v", err)
	}

	// Auto-complete semantics: from {running} only — must not touch a paused run.
	if orc.finishRun(ctx, run.ID, "completed", "running") {
		t.Fatal("finishRun(from=running) should not have transitioned a paused run")
	}
	assertRunStatus(t, ctx, run.ID, "paused")

	// Operator stop allows {running,paused} → terminal.
	if !orc.finishRun(ctx, run.ID, "completed", "running", "paused") {
		t.Fatal("finishRun(from=running,paused) should have completed the paused run")
	}
	assertRunStatus(t, ctx, run.ID, "completed")
}

// assertExperimentStatus fetches the experiment and checks its status.
func assertExperimentStatus(t *testing.T, ctx context.Context, expID, want string) {
	t.Helper()
	got, err := testExperRepo.Get(ctx, expID)
	if err != nil {
		t.Fatalf("assertExperimentStatus: Get(%q): %v", expID, err)
	}
	if got.Status != want {
		t.Fatalf("experiment %q: expected status %q, got %q", expID, want, got.Status)
	}
}

// TestExperimentLifecycle_PauseResumeCancel verifies experiment-level pause/
// resume/cancel and their propagation to child runs: pause pauses running runs,
// resume resumes them, and cancel terminates all non-terminal runs as
// "cancelled" (running and pending alike).
func TestExperimentLifecycle_PauseResumeCancel(t *testing.T) {
	ctx := context.Background()
	orc := newOrchestrator(t) // auto-complete off → baseline stays running without a poller

	exp := seedMinimalExperiment(t, ctx)
	baseline := seedRunWithDeps(t, ctx, exp, "baseline", nil, nil)
	frozen := []model.CacheBoxConfig{{
		Service: "svc", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny",
	}}
	dependent := seedRunWithDeps(t, ctx, exp, "isolation", []string{baseline.ID}, frozen)

	if err := testExperRepo.UpdateStatus(ctx, exp.ID, "running"); err != nil {
		t.Fatalf("set experiment running: %v", err)
	}
	if err := orc.StartRun(ctx, baseline.ID); err != nil {
		t.Fatalf("StartRun(baseline): %v", err)
	}
	assertRunStatus(t, ctx, baseline.ID, "running")

	// Pause → experiment + running baseline paused.
	if err := orc.PauseExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("PauseExperiment: %v", err)
	}
	assertExperimentStatus(t, ctx, exp.ID, "paused")
	assertRunStatus(t, ctx, baseline.ID, "paused")

	// Resume → experiment + baseline running again.
	if err := orc.ResumeExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("ResumeExperiment: %v", err)
	}
	assertExperimentStatus(t, ctx, exp.ID, "running")
	assertRunStatus(t, ctx, baseline.ID, "running")

	// Cancel → experiment cancelled; running baseline + pending dependent cancelled.
	if err := orc.CancelExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("CancelExperiment: %v", err)
	}
	assertExperimentStatus(t, ctx, exp.ID, "cancelled")
	assertRunStatus(t, ctx, baseline.ID, "cancelled")
	assertRunStatus(t, ctx, dependent.ID, "cancelled")
}
