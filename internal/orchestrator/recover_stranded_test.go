package orchestrator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// What Recover does with the phases it finds at start-up: a phase still in
// flight under an experiment that has ended is ended, and a run is failed only
// when zeus answers that it does not know it.

// A cancel or stop that failed part-way leaves a phase running, paused or
// draining under an experiment that has ended. Recover ends each one as
// skipped through the normal terminal path: its load is stopped in zeus, its
// open fault events are closed, its services are bumped off it, and the
// experiment keeps its terminal status.
func TestRecover_SkipsPhaseStrandedUnderTerminalExperiment(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	events := store.NewPhaseFaultEventRepo(testDB)

	// Cancelled experiment, phase running: it freezes a service of its own,
	// zeus still runs its load, and its fault event is open.
	expC := mkExp(t)
	svc := "stranded-" + id.New("svc")
	running := mkRunningIsolation(t, expC.ID, svc, 0)
	runID := "run-stranded-" + expC.ID
	fz.mu.Lock()
	fz.runs[runID] = "running"
	fz.mu.Unlock()
	attachAttackWorkflow(t, running.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1, ZeusRunID: runID})
	if err := events.Create(ctx, &model.PhaseFaultEvent{
		ID: id.New("fevt"), PhaseID: running.ID, Source: "cachebox", Service: svc,
		Kind: "cachebox:replay", StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("open fault event: %v", err)
	}
	if err := testExpRepo.UpdateStatus(ctx, expC.ID, "cancelled"); err != nil {
		t.Fatal(err)
	}

	// Completed experiment, phase paused.
	expP, phasesP := mkExperiment(t, 1)
	for _, st := range []string{"running", "paused"} {
		if err := testExpRepo.UpdatePhaseStatus(ctx, phasesP[0].ID, st); err != nil {
			t.Fatal(err)
		}
	}
	if err := testExpRepo.UpdateStatus(ctx, expP.ID, "completed"); err != nil {
		t.Fatal(err)
	}

	// Failed experiment, phase draining.
	expD, phasesD := mkExperiment(t, 1)
	if err := testExpRepo.UpdatePhaseStatus(ctx, phasesD[0].ID, "draining"); err != nil {
		t.Fatal(err)
	}
	if err := testExpRepo.UpdateStatus(ctx, expD.ID, "failed"); err != nil {
		t.Fatal(err)
	}

	g0, err := o.generations.Get(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}

	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	for _, ph := range []string{running.ID, phasesP[0].ID, phasesD[0].ID} {
		waitFor(t, "stranded phase "+ph+" skipped", 5*time.Second, func() bool {
			return getPhase(t, ph).Status == "skipped"
		})
	}
	// The rest of the terminal path runs after the skipped CAS.
	waitFor(t, "stranded phase's fault event closed", 5*time.Second, func() bool {
		evs, err := events.ListForPhase(ctx, running.ID)
		return err == nil && len(evs) == 1 && evs[0].EndedAt != nil
	})
	if runs, _ := fz.driverStatuses(); runs[runID] != "stopped" {
		t.Errorf("stranded phase's zeus run is %q, want stopped", runs[runID])
	}
	if g, err := o.generations.Get(ctx, svc); err != nil || g <= g0 {
		t.Errorf("generation of %s = %d (err %v), want bumped past %d", svc, g, err, g0)
	}

	time.Sleep(200 * time.Millisecond) // let the scheduler re-walk that follows each end run
	for expID, want := range map[string]string{expC.ID: "cancelled", expP.ID: "completed", expD.ID: "failed"} {
		if got := getExp(t, expID).Status; got != want {
			t.Errorf("experiment %s status %q after recovery, want %q (kept)", expID, got, want)
		}
	}
}

// zeus refuses the connection when manteion asks about a run (it is itself
// restarting): that says nothing about the run, so recovery does not fail the
// phase; once the reconcile budget is spent the phase's poller takes it over.
func TestRecover_RunTransportErrorKeepsPhase(t *testing.T) {
	ctx := context.Background()
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // every call is refused
	o := newOrch(t, deadURL)
	o.WithReconcileBudget(300 * time.Millisecond)

	exp, phases := mkExperiment(t, 1)
	attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1, ZeusRunID: "run-unreachable-" + exp.ID})
	setRunning(t, exp.ID, phases[0].ID)
	endOnCleanup(t, o, exp.ID) // the cancel below ends it too, unless the test stops before it

	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	time.Sleep(time.Second)
	if p := getPhase(t, phases[0].ID); p.Status != "running" || p.FailureReason != "" {
		t.Fatalf("phase = %s/%q after zeus refused the reconcile, want running", p.Status, p.FailureReason)
	}

	if err := o.CancelExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("cancel experiment: %v", err)
	}
	waitPhaseStatus(t, phases[0].ID, "skipped")
}

// zeus is up and does not know the run (it restarted and lost it): recovery
// fails the phase at once, without waiting out the reconcile budget.
func TestRecover_RunNotFoundFailsPhase(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t) // knows no runs: GET /runs/{id} answers 404
	o := newOrch(t, fz.srv.URL)

	exp, phases := mkExperiment(t, 1)
	runID := "run-lost-" + exp.ID
	wfID := attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1, ZeusRunID: runID})
	setRunning(t, exp.ID, phases[0].ID)
	endOnCleanup(t, o, exp.ID)

	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	var p *model.ExperimentPhase
	waitFor(t, "phase failed", 10*time.Second, func() bool {
		p = getPhase(t, phases[0].ID)
		return p.Status == "failed"
	})
	wantContains(t, "phase failure_reason", p.FailureReason, "recovery:", "run "+runID, "for workflow "+wfID)
}

// The run handed to the poller after zeus did not answer within the budget:
// the poller decides once zeus answers. If zeus still has the run, the phase
// carries on and completes with the run; if zeus restarted and lost it, the
// phase fails on its 404 through the poller's not-found path.
func TestRecover_RunTransportErrorThenZeusAnswers(t *testing.T) {
	for _, knows := range []bool{true, false} {
		name := "zeus lost the run"
		if knows {
			name = "zeus knows the run"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fz := newFakeZeus(t)
			var down atomic.Bool
			down.Store(true)
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if down.Load() {
					panic(http.ErrAbortHandler) // drop the connection: a transport error, not an answer
				}
				fz.srv.Config.Handler.ServeHTTP(w, r)
			}))
			t.Cleanup(front.Close)
			o := newOrch(t, front.URL)
			o.WithReconcileBudget(300 * time.Millisecond)
			o.WithMaxPollDuration(time.Minute) // the safety net must not be what ends it

			exp, phases := mkExperiment(t, 1)
			runID := "run-recover-" + exp.ID
			attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1, ZeusRunID: runID})
			setRunning(t, exp.ID, phases[0].ID)
			endOnCleanup(t, o, exp.ID)

			if err := o.Recover(ctx); err != nil {
				t.Fatalf("recover: %v", err)
			}
			time.Sleep(300 * time.Millisecond) // the poller ticks against the dropped connections
			if p := getPhase(t, phases[0].ID); p.Status != "running" {
				t.Fatalf("phase = %s/%q while zeus drops connections, want running", p.Status, p.FailureReason)
			}

			if knows {
				fz.mu.Lock()
				fz.runs[runID] = "running"
				fz.mu.Unlock()
			}
			down.Store(false)

			if !knows {
				p := waitPhaseStatus(t, phases[0].ID, "failed")
				wantContains(t, "phase failure_reason", p.FailureReason, "zeus no longer knows run "+runID)
				if strings.Contains(p.FailureReason, "safety-net") {
					t.Errorf("phase failure_reason = %q, want the not-found reason", p.FailureReason)
				}
				return
			}
			time.Sleep(300 * time.Millisecond) // several poll ticks with zeus answering "running"
			if p := getPhase(t, phases[0].ID); p.Status != "running" {
				t.Fatalf("phase = %s/%q while zeus runs its load, want running", p.Status, p.FailureReason)
			}
			fz.endAllRuns("completed", "")
			if p := waitPhaseStatus(t, phases[0].ID, "completed"); p.FailureReason != "" {
				t.Errorf("completed phase carries failure_reason %q", p.FailureReason)
			}
			waitExpStatus(t, exp.ID, "completed")
		})
	}
}

// setRunning marks the experiment and its phase running, as a restart finds
// them.
func setRunning(t *testing.T, expID, phaseID string) {
	t.Helper()
	ctx := context.Background()
	if err := testExpRepo.UpdateStatus(ctx, expID, "running"); err != nil {
		t.Fatal(err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, phaseID, "running"); err != nil {
		t.Fatal(err)
	}
}
