package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"manteion-go/internal/id"
	"manteion-go/internal/store"
)

// TestTypedErrors_MessagesUnchanged pins that the typed errors carry the
// exact text the log lines and expctl have always shown, and that they
// survive %w wrapping for errors.As.
func TestTypedErrors_MessagesUnchanged(t *testing.T) {
	overlap := &ErrServiceOverlap{ExperimentID: "exp-2", RunningExperimentID: "exp-1", Services: []string{"cartservice", "frontend"}}
	want := `experiment "exp-2" cannot start: services [cartservice frontend] overlap running experiment "exp-1"; wait for it to finish or stop it`
	if got := overlap.Error(); got != want {
		t.Errorf("ErrServiceOverlap.Error() =\n  %s\nwant\n  %s", got, want)
	}
	var gotOverlap *ErrServiceOverlap
	if !errors.As(fmt.Errorf("start: %w", overlap), &gotOverlap) || gotOverlap.RunningExperimentID != "exp-1" {
		t.Errorf("errors.As through %%w lost ErrServiceOverlap: %+v", gotOverlap)
	}

	st := &ErrInvalidState{Kind: "phase", ID: "phase-1", Current: "completed", Wanted: "pending|paused",
		Msg: `orchestrator: phase "phase-1" not startable from status "completed"`}
	if st.Error() != st.Msg {
		t.Errorf("ErrInvalidState.Error() = %q, want Msg verbatim", st.Error())
	}
	var gotState *ErrInvalidState
	if !errors.As(fmt.Errorf("x: %w", st), &gotState) || gotState.Current != "completed" {
		t.Errorf("errors.As through %%w lost ErrInvalidState: %+v", gotState)
	}
}

// TestZeusError pins the transport-vs-response split: only a *url.Error
// (connection refused, DNS, timeout) from the zeus client is "unreachable";
// a zeus 4xx/5xx is a rejection and stays a plain wrapped error. Both keep
// the "<op>: <cause>" text.
func TestZeusError(t *testing.T) {
	transport := fmt.Errorf("zeus: request failed: %w", &url.Error{
		Op: "Post", URL: "http://127.0.0.1:1/api/v1/workflows",
		Err: errors.New("dial tcp 127.0.0.1:1: connect: connection refused"),
	})
	err := zeusError(`register workflow "wf-1" in zeus`, transport)
	var zu *ErrZeusUnreachable
	if !errors.As(err, &zu) {
		t.Fatalf("zeusError(transport) = %T, want *ErrZeusUnreachable", err)
	}
	var ue *url.Error
	if !errors.As(err, &ue) {
		t.Errorf("ErrZeusUnreachable must unwrap to the transport error")
	}
	want := `register workflow "wf-1" in zeus: zeus: request failed: Post "http://127.0.0.1:1/api/v1/workflows": dial tcp 127.0.0.1:1: connect: connection refused`
	if err.Error() != want {
		t.Errorf("message =\n  %s\nwant\n  %s", err.Error(), want)
	}

	rejected := zeusError(`register workflow "wf-1" in zeus`, errors.New("zeus: register workflow: status 400: bad dsl"))
	if errors.As(rejected, &zu) {
		t.Errorf("a zeus response is not \"unreachable\": %v", rejected)
	}
	if got := rejected.Error(); got != `register workflow "wf-1" in zeus: zeus: register workflow: status 400: bad dsl` {
		t.Errorf("rejected message = %q", got)
	}
}

func TestStopFinalStatus_IsValidation(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	err := o.StopExperiment(ctx, "exp-nope", "bogus")
	if !errors.Is(err, ErrValidation) || err.Error() != `orchestrator: invalid final status "bogus"` {
		t.Errorf("StopExperiment(bogus) = %v, want ErrValidation with the unchanged message", err)
	}
	err = o.StopPhase(ctx, "phase-nope", "bogus")
	if !errors.Is(err, ErrValidation) || err.Error() != `orchestrator: invalid phase final status "bogus"` {
		t.Errorf("StopPhase(bogus) = %v, want ErrValidation with the unchanged message", err)
	}
}

// TestLifecycleRefusals_AreTyped walks every refusal the API maps: unknown ids
// are store.ErrNotFound, wrong statuses are *ErrInvalidState carrying the
// observed status, a plan without phases is ErrValidation, and an admission
// refusal is *ErrServiceOverlap naming the holder and the shared services.
func TestLifecycleRefusals_AreTyped(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")

	wantState := func(t *testing.T, err error, current, wanted, msg string) {
		t.Helper()
		var st *ErrInvalidState
		if !errors.As(err, &st) {
			t.Fatalf("err = %v (%T), want *ErrInvalidState", err, err)
		}
		if st.Current != current || st.Wanted != wanted {
			t.Errorf("ErrInvalidState = current %q wanted %q, want %q / %q", st.Current, st.Wanted, current, wanted)
		}
		if err.Error() != msg {
			t.Errorf("message = %q, want unchanged %q", err.Error(), msg)
		}
	}
	wantNotFound := func(t *testing.T, what string, err error) {
		t.Helper()
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s = %v, want store.ErrNotFound", what, err)
		}
	}

	// --- experiments ---
	wantNotFound(t, "StartExperiment(unknown)", o.StartExperiment(ctx, "exp-nope"))
	wantNotFound(t, "PauseExperiment(unknown)", o.PauseExperiment(ctx, "exp-nope"))
	wantNotFound(t, "ResumeExperiment(unknown)", o.ResumeExperiment(ctx, "exp-nope"))
	wantNotFound(t, "CancelExperiment(unknown)", o.CancelExperiment(ctx, "exp-nope"))
	wantNotFound(t, "StopExperiment(unknown)", o.StopExperiment(ctx, "exp-nope", ""))

	empty, _ := mkExperiment(t, 0)
	if err := o.StartExperiment(ctx, empty.ID); !errors.Is(err, ErrValidation) || err.Error() != "orchestrator: experiment has no phases" {
		t.Errorf("StartExperiment(no phases) = %v, want ErrValidation with the unchanged message", err)
	}

	done, donePhases := mkExperiment(t, 1)
	for _, to := range []string{"running", "completed"} {
		if _, err := testExpRepo.TransitionExperiment(ctx, done.ID, to, "planned", "running"); err != nil {
			t.Fatalf("transition %s: %v", to, err)
		}
	}
	wantState(t, o.StartExperiment(ctx, done.ID), "completed", "planned",
		fmt.Sprintf("orchestrator: experiment %q is not planned", done.ID))
	wantState(t, o.CancelExperiment(ctx, done.ID), "completed", "planned|running",
		fmt.Sprintf("orchestrator: experiment %q is already terminal", done.ID))
	wantState(t, o.StopExperiment(ctx, done.ID, "failed"), "completed", "planned|running",
		fmt.Sprintf("orchestrator: experiment %q is already terminal", done.ID))

	planned, plannedPhases := mkExperiment(t, 1)
	wantState(t, o.PauseExperiment(ctx, planned.ID), "planned", "running",
		fmt.Sprintf("orchestrator: experiment %q is not running (status=planned)", planned.ID))
	wantState(t, o.ResumeExperiment(ctx, planned.ID), "planned", "running",
		fmt.Sprintf("orchestrator: experiment %q is not running (status=planned)", planned.ID))

	// A running experiment with nothing to pause / resume.
	idle, _ := mkExperiment(t, 1)
	if _, err := testExpRepo.TransitionExperiment(ctx, idle.ID, "running", "planned"); err != nil {
		t.Fatalf("run idle: %v", err)
	}
	wantState(t, o.PauseExperiment(ctx, idle.ID), "running", "running phase",
		fmt.Sprintf("orchestrator: experiment %q has no running phase to pause", idle.ID))
	wantState(t, o.ResumeExperiment(ctx, idle.ID), "running", "paused phase",
		fmt.Sprintf("orchestrator: experiment %q has no paused phase to resume", idle.ID))

	// --- phases ---
	wantNotFound(t, "StartPhase(unknown)", o.StartPhase(ctx, "phase-nope"))
	wantNotFound(t, "PausePhase(unknown)", o.PausePhase(ctx, "phase-nope"))
	wantNotFound(t, "StopPhase(unknown)", o.StopPhase(ctx, "phase-nope", ""))

	completed := donePhases[0]
	if err := testExpRepo.UpdatePhaseStatus(ctx, completed.ID, "completed"); err != nil {
		t.Fatalf("complete phase: %v", err)
	}
	wantState(t, o.StartPhase(ctx, completed.ID), "completed", "pending|paused",
		fmt.Sprintf("orchestrator: phase %q not startable from status %q", completed.ID, "completed"))
	pending := plannedPhases[0]
	wantState(t, o.PausePhase(ctx, pending.ID), "pending", "running",
		fmt.Sprintf("orchestrator: phase %q is not running", pending.ID))

	// --- admission ---
	shared := "shared-" + id.New("s")
	holder := mkExp(t)
	frozenPhasePending(t, holder.ID, shared, "exact", 0)
	if _, err := testExpRepo.TransitionExperiment(ctx, holder.ID, "running", "planned"); err != nil {
		t.Fatalf("run holder: %v", err)
	}
	candidate := mkExp(t)
	frozenPhasePending(t, candidate.ID, shared, "exact", 0)
	err := o.StartExperiment(ctx, candidate.ID)
	var overlap *ErrServiceOverlap
	if !errors.As(err, &overlap) {
		t.Fatalf("StartExperiment(overlap) = %v (%T), want *ErrServiceOverlap", err, err)
	}
	if overlap.ExperimentID != candidate.ID || overlap.RunningExperimentID != holder.ID ||
		len(overlap.Services) != 1 || overlap.Services[0] != shared {
		t.Errorf("ErrServiceOverlap = %+v, want candidate %s / holder %s / [%s]", overlap, candidate.ID, holder.ID, shared)
	}
}
