package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// How long Recover may hold start-up: its zeus checks of workflow runs share
// one deadline, a phase whose drivers are all lost is failed in the
// background, and a shutdown during the checks is not reported as zeus
// missing the deadline. Recover also skips the pending phases of an ended
// experiment.

// endOnCleanup cancels the experiment when the test ends, so nothing the test
// left running, paused or pending reaches later tests through the shared
// database, where every later Recover would pick it up. An experiment that has
// already ended is left as it is (the cancel is refused).
func endOnCleanup(t *testing.T, o *Orchestrator, expID string) {
	t.Helper()
	t.Cleanup(func() { _ = o.CancelExperiment(context.Background(), expID) })
}

// droppingZeus fronts fz with a server that drops every connection (a
// transport error, not an answer) until the returned func is called, and then
// passes every request to fz.
func droppingZeus(t *testing.T, fz *fakeZeus) (url string, up func()) {
	t.Helper()
	var down atomic.Bool
	down.Store(true)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			panic(http.ErrAbortHandler)
		}
		fz.srv.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)
	return front.URL, func() { down.Store(false) }
}

// Several running phases, each with a run, while zeus drops every connection:
// Recover's checks share one deadline, so together they take about one budget
// instead of one budget each. Every phase stays running with its poller, which
// decides once zeus answers: a run zeus still has carries its phase to
// completion, a run zeus lost fails its phase.
func TestRecover_RunChecksShareOneDeadline(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	zeusURL, zeusUp := droppingZeus(t, fz)
	o := newOrch(t, zeusURL)
	// With a budget per run, each run here costs about 1 s (calls at 0 and 1 s;
	// the 2 s backoff would pass the budget): 5 s for five.
	const budget = 1500 * time.Millisecond
	o.WithReconcileBudget(budget)
	o.WithMaxPollDuration(time.Minute) // the safety net must not be what ends them

	type recovered struct {
		expID, phaseID, runID string
		known                 bool // zeus still has the run once it answers
	}
	var all []recovered
	for i := 0; i < 5; i++ {
		exp, phases := mkExperiment(t, 1)
		r := recovered{exp.ID, phases[0].ID, fmt.Sprintf("run-deadline-%d-%s", i, exp.ID), i%2 == 0}
		attachAttackWorkflow(t, r.phaseID, model.PhaseWorkflow{VUs: 1, DurationSec: 1, ZeusRunID: r.runID})
		setRunning(t, r.expID, r.phaseID)
		endOnCleanup(t, o, r.expID)
		if r.known {
			fz.mu.Lock()
			fz.runs[r.runID] = "running"
			fz.mu.Unlock()
		}
		all = append(all, r)
	}

	start := time.Now()
	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if took := time.Since(start); took > 2*budget {
		t.Errorf("Recover took %s for 5 runs zeus did not answer, want at most about one %s budget (bound %s)", took, budget, 2*budget)
	}
	for _, r := range all {
		if p := getPhase(t, r.phaseID); p.Status != "running" {
			t.Fatalf("phase %s = %s/%q after recovery, want running", r.phaseID, p.Status, p.FailureReason)
		}
	}

	zeusUp()
	for _, r := range all {
		if r.known {
			continue
		}
		p := waitPhaseStatus(t, r.phaseID, "failed")
		wantContains(t, "phase failure_reason", p.FailureReason, "zeus no longer knows run "+r.runID)
	}
	for _, r := range all {
		if p := getPhase(t, r.phaseID); r.known && p.Status != "running" {
			t.Errorf("phase %s = %s/%q while zeus runs its load, want running", r.phaseID, p.Status, p.FailureReason)
		}
	}
	fz.endAllRuns("completed", "")
	for _, r := range all {
		if r.known {
			waitPhaseStatus(t, r.phaseID, "completed")
			waitExpStatus(t, r.expID, "completed")
		}
	}
}

// A phase whose only run zeus has lost is failed, but in the background: its
// exit bump waits for the fleet, which cannot poll before the API listens, so
// Recover must not wait with it. The phase fails at once with its recovery
// reason, and the experiment's next phase is skipped, never started.
func TestRecover_AllLostPhaseFailsInBackground(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t) // knows no runs: GET /runs/{id} answers 404
	o := newOrch(t, fz.srv.URL)
	const converge = 2 * time.Second
	o.WithConvergeTimeout(converge)

	exp := mkExp(t)
	svc := "lost-" + id.New("svc")
	registerSDK(t, svc, "http://lost.invalid") // alive, never observes: the exit bump's wait runs its full timeout
	lost := mkRunningIsolation(t, exp.ID, svc, 0)
	next := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "next", Position: 1, Status: "pending"}
	if err := testExpRepo.CreatePhase(ctx, next); err != nil {
		t.Fatal(err)
	}
	runID := "run-lost-" + exp.ID
	attachAttackWorkflow(t, lost.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1, ZeusRunID: runID})
	if err := testExpRepo.UpdateStatus(ctx, exp.ID, "running"); err != nil {
		t.Fatal(err)
	}
	endOnCleanup(t, o, exp.ID)

	start := time.Now()
	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if took := time.Since(start); took >= converge/2 {
		t.Errorf("Recover took %s, want it not to wait out the failed phase's %s exit convergence", took, converge)
	}
	var p *model.ExperimentPhase
	waitFor(t, "lost phase failed", converge/2, func() bool {
		p = getPhase(t, lost.ID)
		return p.Status == "failed"
	})
	wantContains(t, "phase failure_reason", p.FailureReason, "recovery:", "run "+runID)

	waitExpStatus(t, exp.ID, "failed")
	if got := getPhase(t, next.ID); got.Status != "skipped" {
		t.Errorf("next phase = %s, want skipped (the cascade, never a start)", got.Status)
	}
}

// A terminate that stopped part-way can also leave pending phases under the
// ended experiment. Recover skips them as the cancel would have; a pending
// phase of a running experiment is the scheduler's, and stays pending.
func TestRecover_SkipsPendingPhaseOfEndedExperiment(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")

	expC, phasesC := mkExperiment(t, 2)
	if err := testExpRepo.UpdatePhaseStatus(ctx, phasesC[0].ID, "skipped"); err != nil {
		t.Fatal(err)
	}
	if err := testExpRepo.UpdateStatus(ctx, expC.ID, "cancelled"); err != nil {
		t.Fatal(err)
	}

	// Running experiment: a paused phase holds the scheduler, so nothing
	// starts its pending phase.
	expR, phasesR := mkExperiment(t, 2)
	setRunning(t, expR.ID, phasesR[0].ID)
	if err := testExpRepo.UpdatePhaseStatus(ctx, phasesR[0].ID, "paused"); err != nil {
		t.Fatal(err)
	}
	endOnCleanup(t, o, expR.ID)

	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	waitFor(t, "pending phase of the cancelled experiment skipped", 5*time.Second, func() bool {
		return getPhase(t, phasesC[1].ID).Status == "skipped"
	})
	if got := getExp(t, expC.ID).Status; got != "cancelled" {
		t.Errorf("cancelled experiment status %q after recovery, want cancelled", got)
	}
	if got := getPhase(t, phasesR[1].ID).Status; got != "pending" {
		t.Errorf("pending phase of the running experiment = %s, want pending", got)
	}
	if got := getPhase(t, phasesR[0].ID).Status; got != "paused" {
		t.Errorf("paused phase of the running experiment = %s, want paused", got)
	}
}

// A shutdown while Recover waits between zeus checks ends the checks at once,
// and the log says recovery was interrupted, not that zeus missed the
// deadline.
func TestRecover_InterruptedRunCheckIsNotABudgetMiss(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // every call is refused
	o := newOrch(t, deadURL)
	o.WithReconcileBudget(time.Minute)
	logs := &logBuffer{}
	o.logger = slog.New(slog.NewTextHandler(logs, nil))

	exp, phases := mkExperiment(t, 1)
	runID := "run-interrupted-" + exp.ID
	attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1, ZeusRunID: runID})
	setRunning(t, exp.ID, phases[0].ID)
	endOnCleanup(t, o, exp.ID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Recover(ctx) }()
	time.Sleep(300 * time.Millisecond) // inside the run check's first 1 s backoff
	cancelled := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Recover did not return within 2s of its context ending")
	}
	if took := time.Since(cancelled); took > time.Second {
		t.Errorf("Recover returned %s after its context ended, want promptly", took)
	}

	var runLines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, runID) {
			runLines = append(runLines, line)
		}
	}
	interrupted := false
	for _, line := range runLines {
		if strings.Contains(line, "budget") || strings.Contains(line, "deadline") {
			t.Errorf("interrupted check logged as a missed deadline: %s", line)
		}
		if strings.Contains(line, "interrupted") {
			interrupted = true
		}
	}
	if !interrupted {
		t.Errorf("no log line says the check of %s was interrupted; lines: %q", runID, runLines)
	}

	if err := o.CancelExperiment(context.Background(), exp.ID); err != nil {
		t.Fatalf("cancel experiment: %v", err)
	}
	waitPhaseStatus(t, phases[0].ID, "skipped")
}

// manteion restarted after one phase ended and before the next started.
// Recover's scheduler re-walk starts that next phase in the background: its
// entry waits for the fleet to converge, which cannot happen before the API
// server listens, so Recover must not wait with it (an entry started before
// the listener only times out and fails a healthy experiment).
func TestRecover_AdvancesStalledExperimentInBackground(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "") // no zeus: the next phase has rules and no load
	const converge = 2 * time.Second
	o.WithConvergeTimeout(converge)

	exp := mkExp(t)
	first := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "first", Position: 0, Status: "pending"}
	if err := testExpRepo.CreatePhase(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, first.ID, "completed"); err != nil {
		t.Fatal(err)
	}
	svc := "stalled-" + id.New("svc")
	registerSDK(t, svc, "http://stalled.invalid") // alive, never observes: an entry waits out its convergence timeout
	next := mkPendingRulePhase(t, exp.ID, svc, 1)
	if err := testExpRepo.UpdateStatus(ctx, exp.ID, "running"); err != nil {
		t.Fatal(err)
	}
	endOnCleanup(t, o, exp.ID)

	start := time.Now()
	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if took := time.Since(start); took >= converge/2 {
		t.Errorf("Recover took %s, want it not to wait out the next phase's %s entry convergence", took, converge)
	}
	var p *model.ExperimentPhase
	waitFor(t, "next phase entered", 5*time.Second, func() bool {
		p = getPhase(t, next.ID)
		return p.Status != "pending"
	})
	if p.StartedAt == nil {
		t.Errorf("next phase = %s with no started_at, want it entered", p.Status)
	}
}

// A shutdown while Recover checks an attack-only phase's drivers cuts the
// checks short: every call fails, which says nothing about zeus. Nothing is
// failed; the phase stays as it was for the next start to check.
func TestRecover_CancelDuringDriverChecksFailsNothing(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // every call is refused
	o := newOrch(t, deadURL)

	exp, phases := mkExperiment(t, 1)
	attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{
		VUs: 1, DurationSec: 1, TargetURL: "http://frontend:8080/", ZeusAttackID: "atk-shutdown-" + exp.ID,
	})
	setRunning(t, exp.ID, phases[0].ID)
	endOnCleanup(t, o, exp.ID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Recover(ctx) }()
	time.Sleep(150 * time.Millisecond) // inside the attack check's retries (about 0.6 s)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Recover did not return within 3s of its context ending")
	}
	time.Sleep(300 * time.Millisecond) // a background fail would have landed by now
	if p := getPhase(t, phases[0].ID); p.Status != "running" || p.FailureReason != "" {
		t.Errorf("phase = %s/%q after a start-up cancelled during its driver checks, want running", p.Status, p.FailureReason)
	}
	if got := getExp(t, exp.ID).Status; got != "running" {
		t.Errorf("experiment status %q, want running", got)
	}
}
