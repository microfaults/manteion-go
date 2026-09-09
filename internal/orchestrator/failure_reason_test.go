package orchestrator

import (
	"context"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// Failure reasons (migration 11): every path that fails a phase records why,
// and an experiment failed by the cascade names the phase. These tests drive
// each path through the real orchestrator against the fake zeus and read the
// reason back through the store, the way the API does.

// ---- fake zeus failure-injection helpers ----

// rejectRuns makes every POST .../runs answer status + body.
func (f *fakeZeus) rejectRuns(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runsReject = &fakeReject{status: status, body: body}
}

// dropRunStarts makes every POST .../runs abort the connection: the client
// sees a transport error (EOF), not a zeus answer.
func (f *fakeZeus) dropRunStarts() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runsDrop = true
}

// endAllRuns moves every known run to status with the given reason, as zeus
// reports a run that failed or was rejected after being accepted.
func (f *fakeZeus) endAllRuns(status, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id := range f.runs {
		f.runs[id] = status
		f.runReasons[id] = reason
	}
}

// endAllAttacks moves every known attack to status.
func (f *fakeZeus) endAllAttacks(status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.attacks {
		a.status = status
	}
}

// waitPhaseStatus polls until the phase reaches want and returns it.
func waitPhaseStatus(t *testing.T, phaseID, want string) *model.ExperimentPhase {
	t.Helper()
	var p *model.ExperimentPhase
	waitFor(t, "phase "+phaseID+" "+want, 15*time.Second, func() bool {
		p = getPhase(t, phaseID)
		return p.Status == want
	})
	return p
}

// waitExpStatus polls until the experiment reaches want and returns it.
func waitExpStatus(t *testing.T, expID, want string) *model.Experiment {
	t.Helper()
	var e *model.Experiment
	waitFor(t, "experiment "+expID+" "+want, 15*time.Second, func() bool {
		e = getExp(t, expID)
		return e.Status == want
	})
	return e
}

// wantContains fails unless s contains every needle.
func wantContains(t *testing.T, what, s string, needles ...string) {
	t.Helper()
	for _, n := range needles {
		if !strings.Contains(s, n) {
			t.Errorf("%s = %q, want it to contain %q", what, s, n)
		}
	}
}

// startedRun waits for the phase's single workflow row to carry a run id.
func startedRun(t *testing.T, phaseID string) model.PhaseWorkflow {
	t.Helper()
	var pw model.PhaseWorkflow
	waitFor(t, "zeus_run_id persisted", 5*time.Second, func() bool {
		pws, err := testExpRepo.ListPhaseWorkflows(context.Background(), phaseID)
		if err != nil || len(pws) != 1 || pws[0].ZeusRunID == "" {
			return false
		}
		pw = pws[0]
		return true
	})
	return pw
}

// ---- enter sequence ----

// zeus answers the run start with a 422: the only configured driver never
// starts, the phase fails naming the run, the workflow and zeus's answer, and
// the experiment's reason names the phase.
func TestFailureReason_EnterSequence_ZeusRejectsRun(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	fz.rejectRuns(422, `{"error":"dataset schema mismatch"}`)
	o := newOrch(t, fz.srv.URL)
	exp, phases := mkExperiment(t, 2)
	wfID := attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1}) // run-only

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	p := waitPhaseStatus(t, phases[0].ID, "failed")
	wantContains(t, "phase failure_reason", p.FailureReason,
		"no load driver could be started (1 configured)",
		"zeus rejected run run-", "for workflow "+wfID, "status 422", "dataset schema mismatch")

	e := waitExpStatus(t, exp.ID, "failed")
	if want := "phase " + phases[0].Name + " failed: "; !strings.HasPrefix(e.FailureReason, want) {
		t.Errorf("experiment failure_reason = %q, want prefix %q", e.FailureReason, want)
	}
	wantContains(t, "experiment failure_reason", e.FailureReason, wfID, "dataset schema mismatch")
	if got := getPhase(t, phases[1].ID); got.Status != "skipped" || got.FailureReason != "" {
		t.Errorf("cascade-skipped phase = %s/%q, want skipped with no reason", got.Status, got.FailureReason)
	}
}

// The run start fails at the transport level (connection dropped after the
// workflow was materialized): the reason says zeus was unreachable for that
// run rather than that zeus refused it.
func TestFailureReason_EnterSequence_StartRunTransportError(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	fz.dropRunStarts()
	o := newOrch(t, fz.srv.URL)
	exp, phases := mkExperiment(t, 1)
	wfID := attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1})

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	p := waitPhaseStatus(t, phases[0].ID, "failed")
	wantContains(t, "phase failure_reason", p.FailureReason,
		"no load driver could be started (1 configured)",
		"start run run-", "for workflow "+wfID, "zeus unreachable")
	if strings.Contains(p.FailureReason, "rejected") {
		t.Errorf("transport failure reported as a refusal: %q", p.FailureReason)
	}
	e := waitExpStatus(t, exp.ID, "failed")
	wantContains(t, "experiment failure_reason", e.FailureReason, "phase "+phases[0].Name+" failed: ", "zeus unreachable")
}

// The preload gate (INV-4) refuses a frozen phase with no baseline: the
// reason is the gate's own message, without the internal "orchestrator: "
// log prefix.
func TestFailureReason_PreloadGate(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 0)

	if err := o.enterPhase(ctx, iso, true); err == nil {
		t.Fatal("enterPhase succeeded, want the preload-gate abort")
	}
	p := getPhase(t, iso.ID)
	if p.Status != "failed" {
		t.Fatalf("phase status = %q, want failed", p.Status)
	}
	wantContains(t, "phase failure_reason", p.FailureReason, "preload gate", "baseline")
	if strings.HasPrefix(p.FailureReason, "orchestrator: ") {
		t.Errorf("failure_reason carries the log prefix: %q", p.FailureReason)
	}
}

// ---- poller ----

// zeus accepts the run, then reports it rejected / failed: the poller fails
// the phase with the run, workflow, terminal state and zeus's reason.
func TestFailureReason_Poller_RunEndsRejectedOrFailed(t *testing.T) {
	cases := []struct {
		status, reason string
		want           []string
	}{
		{"rejected", "dataset schema mismatch", []string{"zeus rejected run ", "dataset schema mismatch"}},
		{"failed", "k6 exited 1", []string{"ended failed", "k6 exited 1"}},
	}
	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			ctx := context.Background()
			fz := newFakeZeus(t)
			o := newOrch(t, fz.srv.URL)
			exp, phases := mkExperiment(t, 1)
			wfID := attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1})

			if err := o.StartExperiment(ctx, exp.ID); err != nil {
				t.Fatalf("start experiment: %v", err)
			}
			pw := startedRun(t, phases[0].ID)
			fz.endAllRuns(tc.status, tc.reason)

			p := waitPhaseStatus(t, phases[0].ID, "failed")
			wantContains(t, "phase failure_reason", p.FailureReason,
				append([]string{pw.ZeusRunID, "for workflow " + wfID}, tc.want...)...)
			e := waitExpStatus(t, exp.ID, "failed")
			wantContains(t, "experiment failure_reason", e.FailureReason,
				"phase "+phases[0].Name+" failed: ", pw.ZeusRunID)
		})
	}
}

// An additive attack ends in a state the poller does not recognize: the
// reason names the attack and its state.
func TestFailureReason_Poller_AttackEndsUnexpectedly(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	exp, phases := mkExperiment(t, 1)
	wfID := attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{
		VUs: 1, RateRPS: 1, DurationSec: 1, TargetURL: "http://frontend:8080/", TargetMethod: "GET",
	})
	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	var attackID string
	waitFor(t, "run + attack started", 5*time.Second, func() bool {
		pws, err := testExpRepo.ListPhaseWorkflows(ctx, phases[0].ID)
		if err != nil || len(pws) != 1 || pws[0].ZeusRunID == "" || pws[0].ZeusAttackID == "" {
			return false
		}
		attackID = pws[0].ZeusAttackID
		return true
	})
	fz.endAllAttacks("failed")

	p := waitPhaseStatus(t, phases[0].ID, "failed")
	wantContains(t, "phase failure_reason", p.FailureReason,
		"zeus attack "+attackID, "for workflow "+wfID, "ended failed")
}

// The safety-net deadline fires while the run is still in flight: the reason
// says so and carries the deadline that was exceeded.
func TestFailureReason_Poller_SafetyNetDeadline(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	o.WithMaxPollDuration(100 * time.Millisecond)
	o.WithPollGrace(0) // deadline = the 1 s phase duration
	exp, phases := mkExperiment(t, 1)
	attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1})

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	startedRun(t, phases[0].ID) // the run stays "running" forever in the fake

	p := waitPhaseStatus(t, phases[0].ID, "failed")
	wantContains(t, "phase failure_reason", p.FailureReason, "safety-net deadline", "(1s)")
	e := waitExpStatus(t, exp.ID, "failed")
	wantContains(t, "experiment failure_reason", e.FailureReason,
		"phase "+phases[0].Name+" failed: ", "safety-net deadline")
}

// ---- recovery ----

// After a restart every persisted zeus handle is gone: recovery fails the
// phase naming each lost driver.
func TestFailureReason_Recover_AllDriversLost(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t) // knows neither handle → both reconcile as lost
	o := newOrch(t, fz.srv.URL)
	exp, phases := mkExperiment(t, 2)
	wfID := attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{
		VUs: 1, DurationSec: 1, TargetURL: "http://frontend:8080/",
		ZeusAttackID: "atk-lost-" + exp.ID, ZeusRunID: "run-lost-" + exp.ID,
	})
	if err := testExpRepo.UpdateStatus(ctx, exp.ID, "running"); err != nil {
		t.Fatal(err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, phases[0].ID, "running"); err != nil {
		t.Fatal(err)
	}

	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	p := waitPhaseStatus(t, phases[0].ID, "failed")
	wantContains(t, "phase failure_reason", p.FailureReason,
		"recovery:", "2 zeus load drivers lost", "atk-lost-"+exp.ID, "run-lost-"+exp.ID, wfID)
	e := waitExpStatus(t, exp.ID, "failed")
	wantContains(t, "experiment failure_reason", e.FailureReason,
		"phase "+phases[0].Name+" failed: ", "recovery:")
}

// ---- operator stops ----

// An operator stop with status=failed is a fail path too: the phase says so,
// the experiment names the phase; stopping the experiment as failed records
// the operator's action on the experiment and skips (not fails) its phases.
func TestFailureReason_OperatorStop(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.autoComplete = false // keep driver-less phases running so we can stop them

	exp, phases := mkExperiment(t, 2)
	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	if err := o.StopPhase(ctx, phases[0].ID, "failed"); err != nil {
		t.Fatalf("stop phase failed: %v", err)
	}
	if p := getPhase(t, phases[0].ID); p.Status != "failed" || p.FailureReason != "stopped by operator as failed" {
		t.Errorf("stopped phase = %s/%q, want failed/%q", p.Status, p.FailureReason, "stopped by operator as failed")
	}
	e := waitExpStatus(t, exp.ID, "failed")
	if want := "phase " + phases[0].Name + " failed: stopped by operator as failed"; e.FailureReason != want {
		t.Errorf("experiment failure_reason = %q, want %q", e.FailureReason, want)
	}

	exp2, phases2 := mkExperiment(t, 1)
	if err := o.StartExperiment(ctx, exp2.ID); err != nil {
		t.Fatalf("start experiment 2: %v", err)
	}
	if err := o.StopExperiment(ctx, exp2.ID, "failed"); err != nil {
		t.Fatalf("stop experiment failed: %v", err)
	}
	if e := getExp(t, exp2.ID); e.Status != "failed" || e.FailureReason != "stopped by operator as failed" {
		t.Errorf("stopped experiment = %s/%q, want failed/%q", e.Status, e.FailureReason, "stopped by operator as failed")
	}
	if p := getPhase(t, phases2[0].ID); p.Status != "skipped" || p.FailureReason != "" {
		t.Errorf("phase of stopped experiment = %s/%q, want skipped with no reason", p.Status, p.FailureReason)
	}
}

// A healthy run leaves no reason anywhere.
func TestFailureReason_AbsentOnHealthyRun(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp, phases := mkExperiment(t, 1)
	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	e := waitExpStatus(t, exp.ID, "completed")
	if e.FailureReason != "" {
		t.Errorf("completed experiment carries failure_reason %q", e.FailureReason)
	}
	if p := getPhase(t, phases[0].ID); p.Status != "completed" || p.FailureReason != "" {
		t.Errorf("completed phase = %s/%q, want completed with no reason", p.Status, p.FailureReason)
	}
}
