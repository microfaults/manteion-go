package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// rowHandles returns the run and attack handles on a phase's single workflow
// row.
func rowHandles(t *testing.T, phaseID string) (runID, attackID string) {
	t.Helper()
	pws, err := testExpRepo.ListPhaseWorkflows(context.Background(), phaseID)
	if err != nil || len(pws) != 1 {
		t.Fatalf("phase workflows = %+v, %v; want one row", pws, err)
	}
	return pws[0].ZeusRunID, pws[0].ZeusAttackID
}

// A pause takes its snapshot of the phase's drivers inside its transition and
// stops only those. A resume that lands while the pause is still stopping
// them (its teardown stalled on a zeus call) starts a second entry, which
// records its own run, attack and poller; the pause's teardown, resuming,
// must leave all three alone. (A teardown that re-reads the rows after its
// CAS would stop the resume's freshly recorded run, and one that cancels the
// poller after its stops would cancel the resume's poller.)
func TestPausePhase_ResumeDuringItsTeardown_KeepsTheResumesLoad(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	_, phases := mkExperiment(t, 1)
	ph := phases[0]
	attachAttackWorkflow(t, ph.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 30, TargetURL: "http://target.invalid/"}) // a run and an attack
	t.Cleanup(func() { o.cancelPoller(ph.ID) })
	if err := o.StartPhase(ctx, ph.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	firstRun, firstAttack := rowHandles(t, ph.ID)

	resumed := make(chan error, 1)
	var fired atomic.Bool
	fz.onStop(func(kind, id string) {
		if fired.CompareAndSwap(false, true) {
			resumed <- o.StartPhase(ctx, ph.ID) // the pause's first stop stalls on a whole resume
		}
	})
	if err := o.PausePhase(ctx, ph.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if err := receive(t, "the resume", resumed); err != nil {
		t.Fatalf("resume during the pause's teardown: %v", err)
	}

	run, attack := rowHandles(t, ph.ID)
	if run == firstRun || attack == firstAttack {
		t.Fatalf("row names run %q and attack %q, want the resume's (the first entry's were %q and %q)", run, attack, firstRun, firstAttack)
	}
	runs, attacks := fz.driverStatuses()
	for id, want := range map[string]string{firstRun: "stopped", run: "running"} {
		if runs[id] != want {
			t.Errorf("run %s is %q, want %s", id, runs[id], want)
		}
	}
	for id, want := range map[string]string{firstAttack: "stopped", attack: "running"} {
		if attacks[id] != want {
			t.Errorf("attack %s is %q, want %s", id, attacks[id], want)
		}
	}
	if !pollerRunning(o, ph.ID) {
		t.Error("the pause cancelled the resume's poller")
	}
	if got := getPhase(t, ph.ID); got.Status != "running" || got.FailureReason != "" {
		t.Errorf("phase = %s / %q, want running with no failure (the resume's)", got.Status, got.FailureReason)
	}
}

// A stop that lands while an entry starts its attack takes its snapshot of the
// phase's drivers before the entry is done: the attack's handle is recorded
// before zeus accepts it, so the stop's pre-transition teardown stops an
// attack zeus does not know yet, and the entry, still holding its claim,
// records its poller and returns. After its terminal transition the stop
// sweeps again — it cancels the poller and stops every driver the rows name —
// so no load and no poller outlive the stopped phase.
func TestStopPhase_DuringAnEntrysAttackStart_LeavesNoLoadRunning(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	_, phases := mkExperiment(t, 1)
	ph := phases[0]
	attachAttackWorkflow(t, ph.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 30, TargetURL: "http://target.invalid/"}) // a run and an attack
	t.Cleanup(func() { o.cancelPoller(ph.ID) })

	stopping := make(chan struct{})
	release := make(chan struct{})
	var stopFired atomic.Bool
	fz.onStop(func(kind, id string) {
		if stopFired.CompareAndSwap(false, true) {
			close(stopping) // the stop's snapshot is taken; its first stop stalls
			<-release
		}
	})
	stopped := make(chan error, 1)
	var atkFired atomic.Bool
	fz.onAttackStart(func() {
		if !atkFired.CompareAndSwap(false, true) {
			return
		}
		go func() { stopped <- o.StopPhase(ctx, ph.ID, "skipped") }()
		select {
		case <-stopping:
		case <-time.After(10 * time.Second):
			t.Error("the stop never reached its teardown")
		}
	})

	if err := o.StartPhase(ctx, ph.ID); err != nil {
		t.Fatalf("start = %v, want nil: the entry held its claim until it recorded its poller", err)
	}
	close(release)
	if err := receive(t, "the stop", stopped); err != nil {
		t.Fatalf("stop: %v", err)
	}

	runs, attacks := fz.driverStatuses()
	if len(runs) != 1 || len(attacks) != 1 {
		t.Fatalf("zeus knows runs %v and attacks %v, want one of each", runs, attacks)
	}
	for kind, drivers := range map[string]map[string]string{"run": runs, "attack": attacks} {
		for id, st := range drivers {
			if st != "stopped" {
				t.Errorf("%s %s is %s, want stopped: load outlives the stopped phase", kind, id, st)
			}
		}
	}
	if pollerRunning(o, ph.ID) {
		t.Error("a poller survives on the stopped phase")
	}
	if got := getPhase(t, ph.ID); got.Status != "skipped" {
		t.Errorf("phase = %s, want skipped (the stop's)", got.Status)
	}
}
