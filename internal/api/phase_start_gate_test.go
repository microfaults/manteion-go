package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// The two routes that start or resume a single phase, POST
// /experiments/{id}/phases/{phaseId}/start and POST /phases/{phaseId}/resume,
// refuse unless the phase's experiment is running. A phase's rules, freeze and
// cache-box role are served only while its experiment runs too
// (store.RuleRepo.ForService, ExperimentRepo.ListRunningPhases), so a phase
// entered under a planned or ended experiment would drive its load against a
// bed serving none of it and be harvested as a fault phase that measured no
// fault.

// notRunningStatuses are the experiment statuses a phase entry is refused
// under.
var notRunningStatuses = []string{"planned", "completed", "failed", "cancelled"}

// zeusRecorder is a fake zeus that records every request and answers 500. An
// entry that got past the gate would call it: a phase with a workflow
// registers it in zeus before it starts the run.
type zeusRecorder struct {
	mu    sync.Mutex
	calls []string
}

func newGateEnv(t *testing.T) (*editTestEnv, *zeusRecorder) {
	t.Helper()
	e := newEditTestEnv(t)
	z := &zeusRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		z.mu.Lock()
		z.calls = append(z.calls, r.Method+" "+r.URL.Path)
		z.mu.Unlock()
		http.Error(w, "recorder", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	e.s.orch = newTestOrch(t, e.db, srv.URL)
	return e, z
}

func (z *zeusRecorder) seen() []string {
	z.mu.Lock()
	defer z.mu.Unlock()
	return slices.Clone(z.calls)
}

// loadPhase seeds, under exp, a phase in status ("pending" or "paused") with
// one workflow attached, so that entering it calls zeus.
func loadPhase(t *testing.T, e *editTestEnv, exp *model.Experiment, status string) *model.ExperimentPhase {
	t.Helper()
	ph := e.phase(exp, "load-"+id.New("p"), 0)
	if err := e.exp.AttachPhaseWorkflows(e.ctx, ph.ID, []model.PhaseWorkflow{{WorkflowID: e.workflow(), VUs: 1, DurationSec: 1}}); err != nil {
		t.Fatalf("attach workflow: %v", err)
	}
	if status == "paused" {
		for _, s := range []string{"running", "paused"} {
			if err := e.exp.UpdatePhaseStatus(e.ctx, ph.ID, s); err != nil {
				t.Fatalf("phase -> %s: %v", s, err)
			}
		}
	}
	return ph
}

// wantNotRunning asserts that w is the invalid-state refusal the experiment
// routes already answer for an experiment that is not running: byte for byte
// the body of POST /experiments/{id}/pause for the same experiment.
func wantNotRunning(t *testing.T, e *editTestEnv, w *httptest.ResponseRecorder, exp *model.Experiment) {
	t.Helper()
	body := decodeErrBody(t, w, http.StatusConflict, "invalid_state")
	if body["status"] != exp.Status {
		t.Errorf("status = %v, want the experiment's %s", body["status"], exp.Status)
	}
	if want := fmt.Sprintf("orchestrator: experiment %q is not running (status=%s)", exp.ID, exp.Status); body["error"] != want {
		t.Errorf("error = %v, want %q", body["error"], want)
	}
	url, pv := expAction(exp.ID, "pause")
	ref := e.do(e.s.handlePauseExperiment, http.MethodPost, url, "", pv)
	if got, want := strings.TrimSpace(w.Body.String()), strings.TrimSpace(ref.Body.String()); w.Code != ref.Code || got != want {
		t.Errorf("refusal = %d %s, want what the experiment pause answers: %d %s", w.Code, got, ref.Code, want)
	}
}

// wantUntouched asserts that the refused entry changed nothing: the phase is
// still in status and zeus was never called (no workflow registered, no run
// started).
func wantUntouched(t *testing.T, e *editTestEnv, z *zeusRecorder, ph *model.ExperimentPhase, status string) {
	t.Helper()
	if got, err := e.exp.GetPhase(e.ctx, ph.ID); err != nil || got.Status != status {
		t.Errorf("phase = %+v, %v; want still %s", got, err, status)
	}
	if calls := z.seen(); len(calls) != 0 {
		t.Errorf("zeus calls = %q, want none", calls)
	}
}

// TestStartPhase_RefusedUnlessExperimentRunning: the nested start answers 409
// invalid_state carrying the experiment's status, for a fresh start and a
// resume alike, and nothing is claimed or launched.
func TestStartPhase_RefusedUnlessExperimentRunning(t *testing.T) {
	for _, expStatus := range notRunningStatuses {
		for _, phaseStatus := range []string{"pending", "paused"} {
			t.Run(expStatus+"/"+phaseStatus, func(t *testing.T) {
				e, z := newGateEnv(t)
				exp := e.experiment(expStatus)
				ph := loadPhase(t, e, exp, phaseStatus)

				url, pv := phaseAction(exp.ID, ph.ID, "start")
				w := e.do(e.s.handleStartPhase, http.MethodPost, url, "", pv)
				wantNotRunning(t, e, w, exp)
				wantUntouched(t, e, z, ph, phaseStatus)
			})
		}
	}
}

// TestResumePhase_RefusedUnlessExperimentRunning: the same for the flat
// resume, which starts a pending phase as well as resuming a paused one.
func TestResumePhase_RefusedUnlessExperimentRunning(t *testing.T) {
	for _, expStatus := range notRunningStatuses {
		for _, phaseStatus := range []string{"paused", "pending"} {
			t.Run(expStatus+"/"+phaseStatus, func(t *testing.T) {
				e, z := newGateEnv(t)
				exp := e.experiment(expStatus)
				ph := loadPhase(t, e, exp, phaseStatus)

				url, pv := flatPhaseAction(ph.ID, "resume")
				w := e.do(e.s.handleResumePhaseFlat, http.MethodPost, url, "", pv)
				wantNotRunning(t, e, w, exp)
				wantUntouched(t, e, z, ph, phaseStatus)
			})
		}
	}
}

// TestStartPhase_AllowedWhenExperimentRunning: under a running experiment
// both routes answer what they did before the gate (202 for the start, 200
// {"status":"running"} for the resume) and the phase is entered. The phases
// have no drivers, so they auto-complete once entered.
func TestStartPhase_AllowedWhenExperimentRunning(t *testing.T) {
	entered := func(t *testing.T, e *editTestEnv, ph *model.ExperimentPhase) {
		t.Helper()
		waitForStatus(t, "the phase is entered and auto-completes", 10*time.Second, func() string {
			got, err := e.exp.GetPhase(e.ctx, ph.ID)
			if err != nil {
				return ""
			}
			if got.FailureReason != "" {
				t.Fatalf("phase failed: %s", got.FailureReason)
			}
			return got.Status
		}, "completed")
	}

	t.Run("nested start of a pending phase", func(t *testing.T) {
		e := newEditTestEnv(t)
		exp := e.experiment("running")
		ph := e.phase(exp, "go", 0)
		url, pv := phaseAction(exp.ID, ph.ID, "start")
		w := e.do(e.s.handleStartPhase, http.MethodPost, url, "", pv)
		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d body = %s, want 202", w.Code, w.Body.String())
		}
		entered(t, e, ph)
	})

	t.Run("flat resume of a paused phase", func(t *testing.T) {
		e := newEditTestEnv(t)
		exp := e.experiment("running")
		ph := e.phase(exp, "go", 0)
		for _, s := range []string{"running", "paused"} {
			if err := e.exp.UpdatePhaseStatus(e.ctx, ph.ID, s); err != nil {
				t.Fatalf("phase -> %s: %v", s, err)
			}
		}
		url, pv := flatPhaseAction(ph.ID, "resume")
		w := e.do(e.s.handleResumePhaseFlat, http.MethodPost, url, "", pv)
		if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"status":"running"}` {
			t.Fatalf("status = %d body = %s, want 200 {\"status\":\"running\"}", w.Code, w.Body.String())
		}
		entered(t, e, ph)
	})
}

// TestStartPhase_PreflightStillWinsUnderPlanned: the start's 422 refusals keep
// their precedence over the not-running 409. A plan whose cache-box rule no
// longer keys like its service's freeze is 422 validation naming the rule, and
// a workflow whose dataset zeus no longer has is 422 dataset_missing, both
// under a planned experiment; the phase stays pending.
func TestStartPhase_PreflightStillWinsUnderPlanned(t *testing.T) {
	t.Run("cache-box rule agreement", func(t *testing.T) {
		e, z := newGateEnv(t)
		exp := e.experiment("planned")
		base := e.phase(exp, "base", 0)
		e.phase(exp, "iso", 1, model.CacheBoxConfig{Service: "cart", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"})
		ruleID := e.cacheBoxRule("cart", "exact")
		if err := e.exp.AttachPhaseRules(e.ctx, base.ID, []string{ruleID}); err != nil {
			t.Fatalf("attach rules: %v", err)
		}
		rules := store.NewRuleRepo(e.db, nil)
		r, err := rules.Get(e.ctx, ruleID)
		if err != nil {
			t.Fatal(err)
		}
		r.Action.CacheBox.KeyStrategy = "canonical_v2"
		if err := rules.Update(e.ctx, r); err != nil {
			t.Fatalf("edit rule: %v", err)
		}

		url, pv := phaseAction(exp.ID, base.ID, "start")
		w := e.do(e.s.handleStartPhase, http.MethodPost, url, "", pv)
		msg, _ := decodeErrBody(t, w, http.StatusUnprocessableEntity, "validation")["error"].(string)
		for _, want := range []string{ruleID, `"edit-cb-rule"`, `keys with "canonical_v2"`, `phase "iso" freezes "cart" keying with "exact"`} {
			if !strings.Contains(msg, want) {
				t.Errorf("error = %q, want it to contain %q", msg, want)
			}
		}
		wantUntouched(t, e, z, base, "pending")
	})

	t.Run("dataset preflight", func(t *testing.T) {
		e, z := newGateEnv(t)
		fz := newFakeDatasetZeus(t) // knows no dataset
		e.s.zeus = zeus.NewClient(fz.srv.URL)
		exp := e.experiment("planned")
		ph := e.phase(exp, "load", 0)
		dsID := "ds-gone-" + id.New("n")
		if err := e.exp.AttachPhaseWorkflows(e.ctx, ph.ID, []model.PhaseWorkflow{{WorkflowID: e.workflow(), VUs: 1, DurationSec: 1, DatasetID: dsID}}); err != nil {
			t.Fatalf("attach workflow: %v", err)
		}

		url, pv := phaseAction(exp.ID, ph.ID, "start")
		w := e.do(e.s.handleStartPhase, http.MethodPost, url, "", pv)
		want := fmt.Sprintf(`{"error":"dataset(s) not found in zeus: %s","code":"dataset_missing","dataset_ids":[%q]}`, dsID, dsID)
		if w.Code != http.StatusUnprocessableEntity || strings.TrimSpace(w.Body.String()) != want {
			t.Errorf("start = %d %s, want 422 %s", w.Code, w.Body.String(), want)
		}
		wantUntouched(t, e, z, ph, "pending")
	})
}

// TestStartPhase_PhaseOfAnotherExperimentNotFound: the nested start answers
// 404 not_found for a phase that is not one of the experiment its path names,
// and starts nothing.
func TestStartPhase_PhaseOfAnotherExperimentNotFound(t *testing.T) {
	e, z := newGateEnv(t)
	owner := e.experiment("running")
	ph := loadPhase(t, e, owner, "pending")
	other := e.experiment("running")

	url, pv := phaseAction(other.ID, ph.ID, "start")
	w := e.do(e.s.handleStartPhase, http.MethodPost, url, "", pv)
	if body := decodeErrBody(t, w, http.StatusNotFound, "not_found"); body["error"] != "phase not found" {
		t.Errorf("error = %v, want \"phase not found\"", body["error"])
	}
	wantUntouched(t, e, z, ph, "pending")
}
