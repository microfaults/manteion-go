package api

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/atropos"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// newTestOrch builds an orchestrator over the test database with an empty
// SDK fleet and, when zeusURL is non-empty, a zeus client pointed at it.
func newTestOrch(t *testing.T, database *sql.DB, zeusURL string) *orchestrator.Orchestrator {
	t.Helper()
	logger := discardLogger()
	controller := atrocontrol.New(
		atropos.NewClient(atropos.WithHTTPClient(&http.Client{Timeout: time.Second})),
		&atrocontrol.RepoResolver{Repo: store.NewSDKRepo(database)},
		atrocontrol.WithDefaultTimeout(time.Second), atrocontrol.WithLogger(logger),
	)
	var zc *zeus.Client
	if zeusURL != "" {
		zc = zeus.NewClient(zeusURL)
	}
	orch := orchestrator.New(
		store.NewExperimentRepo(database), store.NewRuleRepo(database), store.NewFaultRepo(database),
		store.NewWorkloadRepo(database), store.NewWorkflowRepo(database),
		controller, nil, zc, cachestore.New(t.TempDir()),
		store.NewPhaseFaultEventRepo(database), logger,
	)
	orch.WithDrainTimeout(500 * time.Millisecond)
	orch.WithDrainPollInterval(20 * time.Millisecond)
	return orch
}

// frozenPhase seeds a pending isolation phase freezing service under exp.
func (e *editTestEnv) frozenPhase(exp *model.Experiment, service string, position int) *model.ExperimentPhase {
	e.t.Helper()
	return e.phase(exp, fmt.Sprintf("iso-%d", position), position,
		model.CacheBoxConfig{Service: service, Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"})
}

func expAction(expID, action string) (string, map[string]string) {
	return "/api/v1/experiments/" + expID + "/" + action, map[string]string{"id": expID}
}

func phaseAction(expID, phaseID, action string) (string, map[string]string) {
	return "/api/v1/experiments/" + expID + "/phases/" + phaseID + "/" + action,
		map[string]string{"id": expID, "phaseId": phaseID}
}

func flatPhaseAction(phaseID, action string) (string, map[string]string) {
	return "/api/v1/phases/" + phaseID + "/" + action, map[string]string{"phaseId": phaseID}
}

// =========================================================================
// Experiment lifecycle: start / pause / resume / cancel / stop
// =========================================================================

func TestExperimentLifecycleHandlers_ErrorEnvelope(t *testing.T) {
	e := newEditTestEnv(t)

	t.Run("start: unknown experiment is 404 not_found", func(t *testing.T) {
		url, pv := expAction("exp-nope", "start")
		w := e.do(e.s.handleStartExperiment, http.MethodPost, url, "", pv)
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})

	t.Run("start: running experiment is 409 invalid_state carrying its status", func(t *testing.T) {
		exp := e.experiment("running")
		e.phase(exp, "baseline", 0)
		url, pv := expAction(exp.ID, "start")
		w := e.do(e.s.handleStartExperiment, http.MethodPost, url, "", pv)
		body := decodeErrBody(t, w, http.StatusConflict, "invalid_state")
		if body["status"] != "running" {
			t.Errorf("status=%v, want running", body["status"])
		}
		if msg, _ := body["error"].(string); !strings.Contains(msg, "not planned") {
			t.Errorf("error=%q, want the not-planned message", msg)
		}
	})

	t.Run("start: experiment without phases is 422 validation", func(t *testing.T) {
		exp := e.experiment("planned")
		url, pv := expAction(exp.ID, "start")
		w := e.do(e.s.handleStartExperiment, http.MethodPost, url, "", pv)
		body := decodeErrBody(t, w, http.StatusUnprocessableEntity, "validation")
		if msg, _ := body["error"].(string); !strings.Contains(msg, "no phases") {
			t.Errorf("error=%q, want the no-phases message", msg)
		}
		if got, err := e.exp.Get(e.ctx, exp.ID); err != nil || got.Status != "planned" {
			t.Errorf("experiment status=%v err=%v, want still planned", got, err)
		}
	})

	t.Run("start: service overlap is 409 service_overlap naming the holder", func(t *testing.T) {
		shared := "shared-" + id.New("s")
		holder := e.experiment("running")
		e.frozenPhase(holder, shared, 0)
		candidate := e.experiment("planned")
		e.frozenPhase(candidate, shared, 0)

		url, pv := expAction(candidate.ID, "start")
		w := e.do(e.s.handleStartExperiment, http.MethodPost, url, "", pv)
		body := decodeErrBody(t, w, http.StatusConflict, "service_overlap")
		if body["running_experiment_id"] != holder.ID {
			t.Errorf("running_experiment_id=%v, want %s", body["running_experiment_id"], holder.ID)
		}
		if !reflect.DeepEqual(body["services"], []any{shared}) {
			t.Errorf("services=%v, want [%s]", body["services"], shared)
		}
		if msg, _ := body["error"].(string); !strings.Contains(msg, holder.ID) || !strings.Contains(msg, shared) {
			t.Errorf("error=%q, want it to name %s and %s", msg, holder.ID, shared)
		}
		if got, err := e.exp.Get(e.ctx, candidate.ID); err != nil || got.Status != "planned" {
			t.Errorf("candidate status=%v err=%v, want still planned (start refused)", got, err)
		}
	})

	t.Run("pause/resume/cancel/stop: wrong status is 409 invalid_state, unknown is 404", func(t *testing.T) {
		cases := []struct {
			action string
			h      http.HandlerFunc
			seed   string
		}{
			{"pause", e.s.handlePauseExperiment, "planned"},
			{"resume", e.s.handleResumeExperiment, "planned"},
			{"cancel", e.s.handleCancelExperiment, "completed"},
			{"stop", e.s.handleStopExperiment, "completed"},
		}
		for _, c := range cases {
			exp := e.experiment(c.seed)
			url, pv := expAction(exp.ID, c.action)
			w := e.do(c.h, http.MethodPost, url, "", pv)
			body := decodeErrBody(t, w, http.StatusConflict, "invalid_state")
			if body["status"] != c.seed {
				t.Errorf("%s: status=%v, want %s", c.action, body["status"], c.seed)
			}
			if got, err := e.exp.Get(e.ctx, exp.ID); err != nil || got.Status != c.seed {
				t.Errorf("%s: experiment status=%v err=%v, want unchanged %s", c.action, got, err, c.seed)
			}

			url, pv = expAction("exp-nope", c.action)
			w = e.do(c.h, http.MethodPost, url, "", pv)
			decodeErrBody(t, w, http.StatusNotFound, "not_found")
		}
	})

	t.Run("stop: unknown final status is 422 validation", func(t *testing.T) {
		exp := e.experiment("running")
		url, pv := expAction(exp.ID, "stop?status=bogus")
		w := e.do(e.s.handleStopExperiment, http.MethodPost, url, "", pv)
		decodeErrBody(t, w, http.StatusUnprocessableEntity, "validation")
		if got, err := e.exp.Get(e.ctx, exp.ID); err != nil || got.Status != "running" {
			t.Errorf("experiment status=%v err=%v, want still running", got, err)
		}
	})
}

// =========================================================================
// Phase lifecycle: nested start/stop, flat pause/resume/stop
// =========================================================================

func TestPhaseLifecycleHandlers_ErrorEnvelope(t *testing.T) {
	e := newEditTestEnv(t)

	completedPhase := func(t *testing.T) (*model.Experiment, *model.ExperimentPhase) {
		t.Helper()
		exp := e.experiment("running")
		ph := e.phase(exp, "done", 0)
		if err := e.exp.UpdatePhaseStatus(e.ctx, ph.ID, "completed"); err != nil {
			t.Fatalf("complete phase: %v", err)
		}
		return exp, ph
	}

	t.Run("nested start: completed phase is 409 invalid_state, unknown is 404", func(t *testing.T) {
		exp, ph := completedPhase(t)
		url, pv := phaseAction(exp.ID, ph.ID, "start")
		w := e.do(e.s.handleStartPhase, http.MethodPost, url, "", pv)
		body := decodeErrBody(t, w, http.StatusConflict, "invalid_state")
		if body["status"] != "completed" {
			t.Errorf("status=%v, want completed", body["status"])
		}
		url, pv = phaseAction(exp.ID, "phase-nope", "start")
		w = e.do(e.s.handleStartPhase, http.MethodPost, url, "", pv)
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})

	t.Run("nested stop: unknown phase is 404", func(t *testing.T) {
		url, pv := phaseAction("exp-1", "phase-nope", "stop")
		w := e.do(e.s.handleStopPhase, http.MethodPost, url, "", pv)
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})

	t.Run("flat pause: pending phase is 409 invalid_state, unknown is 404", func(t *testing.T) {
		exp := e.experiment("running")
		ph := e.phase(exp, "pending", 0)
		url, pv := flatPhaseAction(ph.ID, "pause")
		w := e.do(e.s.handlePausePhaseFlat, http.MethodPost, url, "", pv)
		body := decodeErrBody(t, w, http.StatusConflict, "invalid_state")
		if body["status"] != "pending" {
			t.Errorf("status=%v, want pending", body["status"])
		}
		url, pv = flatPhaseAction("phase-nope", "pause")
		w = e.do(e.s.handlePausePhaseFlat, http.MethodPost, url, "", pv)
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})

	t.Run("flat resume: completed phase is 409 invalid_state, unknown is 404", func(t *testing.T) {
		_, ph := completedPhase(t)
		url, pv := flatPhaseAction(ph.ID, "resume")
		w := e.do(e.s.handleResumePhaseFlat, http.MethodPost, url, "", pv)
		body := decodeErrBody(t, w, http.StatusConflict, "invalid_state")
		if body["status"] != "completed" {
			t.Errorf("status=%v, want completed", body["status"])
		}
		url, pv = flatPhaseAction("phase-nope", "resume")
		w = e.do(e.s.handleResumePhaseFlat, http.MethodPost, url, "", pv)
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})

	t.Run("flat stop: unknown phase is 404", func(t *testing.T) {
		url, pv := flatPhaseAction("phase-nope", "stop")
		w := e.do(e.s.handleStopPhaseFlat, http.MethodPost, url, "", pv)
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})
}

// =========================================================================
// Create / update: conflicts, unknown references, plan-only edits
// =========================================================================

func TestCreateHandlers_ErrorEnvelope(t *testing.T) {
	e := newEditTestEnv(t)
	create := func(t *testing.T, body string) *httptest.ResponseRecorder {
		t.Helper()
		return e.do(e.s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", body, nil)
	}
	// A client-supplied id lets the test clean up whatever the handler wrote
	// before it refused.
	freshID := func(t *testing.T) string {
		t.Helper()
		expID := id.New("exp")
		t.Cleanup(func() { _, _ = e.db.Exec("DELETE FROM experiments WHERE id = $1", expID) })
		return expID
	}

	t.Run("create experiment: duplicate id is 409 conflict", func(t *testing.T) {
		exp := e.experiment("planned")
		w := create(t, fmt.Sprintf(`{"id":%q,"name":"dup"}`, exp.ID))
		decodeErrBody(t, w, http.StatusConflict, "conflict")
		if got, err := e.exp.Get(e.ctx, exp.ID); err != nil || got.Name != exp.Name {
			t.Errorf("existing experiment changed: %+v err=%v", got, err)
		}
	})

	t.Run("create experiment: unknown workflow id is 422 validation", func(t *testing.T) {
		body := fmt.Sprintf(`{"id":%q,"name":"x","phases":[{"name":"p","position":0,
			"workflows":[{"workflow_id":"wf-nope","vus":1,"duration_sec":1}]}]}`, freshID(t))
		w := create(t, body)
		got := decodeErrBody(t, w, http.StatusUnprocessableEntity, "validation")
		if msg, _ := got["error"].(string); !strings.Contains(msg, "wf-nope") {
			t.Errorf("error=%q, want it to name the unknown workflow id", msg)
		}
	})

	t.Run("create experiment: unknown rule id is 422 validation", func(t *testing.T) {
		body := fmt.Sprintf(`{"id":%q,"name":"x","phases":[{"name":"p","position":0,"rule_ids":["rule-nope"]}]}`, freshID(t))
		w := create(t, body)
		got := decodeErrBody(t, w, http.StatusUnprocessableEntity, "validation")
		if msg, _ := got["error"].(string); !strings.Contains(msg, "rule-nope") {
			t.Errorf("error=%q, want it to name the unknown rule id", msg)
		}
	})

	t.Run("create phase: unknown experiment is 404 not_found", func(t *testing.T) {
		w := e.do(e.s.handleCreatePhase, http.MethodPost, "/api/v1/experiments/exp-nope/phases",
			`{"name":"p"}`, map[string]string{"id": "exp-nope"})
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})

	t.Run("create phase: duplicate name is 409 conflict", func(t *testing.T) {
		exp := e.experiment("planned")
		e.phase(exp, "taken", 0)
		w := e.do(e.s.handleCreatePhase, http.MethodPost, "/api/v1/experiments/"+exp.ID+"/phases",
			`{"name":"taken"}`, map[string]string{"id": exp.ID})
		decodeErrBody(t, w, http.StatusConflict, "conflict")
	})

	t.Run("create phase: unknown workflow id is 422 validation", func(t *testing.T) {
		exp := e.experiment("planned")
		w := e.do(e.s.handleCreatePhase, http.MethodPost, "/api/v1/experiments/"+exp.ID+"/phases",
			`{"name":"p","workflows":[{"workflow_id":"wf-nope","vus":1,"duration_sec":1}]}`, map[string]string{"id": exp.ID})
		decodeErrBody(t, w, http.StatusUnprocessableEntity, "validation")
	})

	t.Run("update experiment: running experiment is 409 invalid_state with status", func(t *testing.T) {
		exp := e.experiment("running")
		w := e.do(e.s.handleUpdateExperiment, http.MethodPut, "/api/v1/experiments/"+exp.ID,
			`{"name":"nope"}`, map[string]string{"id": exp.ID})
		body := decodeErrBody(t, w, http.StatusConflict, "invalid_state")
		if body["status"] != "running" {
			t.Errorf("status=%v, want running", body["status"])
		}
	})

	t.Run("update phase: completed phase is 409 invalid_state with the phase status", func(t *testing.T) {
		exp := e.experiment("planned")
		ph := e.phase(exp, "done", 0)
		if err := e.exp.UpdatePhaseStatus(e.ctx, ph.ID, "completed"); err != nil {
			t.Fatalf("complete phase: %v", err)
		}
		w := e.do(e.s.handleUpdatePhase, http.MethodPut, "/api/v1/experiments/"+exp.ID+"/phases/"+ph.ID,
			`{"name":"nope"}`, map[string]string{"id": exp.ID, "phaseId": ph.ID})
		body := decodeErrBody(t, w, http.StatusConflict, "invalid_state")
		if body["status"] != "completed" {
			t.Errorf("status=%v, want completed", body["status"])
		}
	})

	t.Run("delete: unknown experiment and phase are 404 not_found", func(t *testing.T) {
		w := e.do(e.s.handleDeleteExperiment, http.MethodDelete, "/api/v1/experiments/exp-nope", "", map[string]string{"id": "exp-nope"})
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
		w = e.do(e.s.handleDeletePhase, http.MethodDelete, "/api/v1/experiments/exp-1/phases/phase-nope", "",
			map[string]string{"id": "exp-1", "phaseId": "phase-nope"})
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})
}

// TestHandleStartPhase_ZeusUnreachable pins the 502: a phase whose start has
// to materialize a workflow into zeus, with zeus down, is refused as
// zeus_unreachable — and the enter sequence has already failed the phase.
func TestHandleStartPhase_ZeusUnreachable(t *testing.T) {
	e := newEditTestEnv(t)
	// 127.0.0.1:1 refuses connections immediately: a transport failure, not a
	// zeus response.
	s := &Server{experiments: e.exp, logger: discardLogger(), orch: newTestOrch(t, e.db, "http://127.0.0.1:1")}
	exp := e.experiment("planned")
	ph := e.phase(exp, "load", 0)
	wf := e.workflow()
	if err := e.exp.AttachPhaseWorkflows(e.ctx, ph.ID, []model.PhaseWorkflow{{WorkflowID: wf, VUs: 1, DurationSec: 1}}); err != nil {
		t.Fatalf("attach workflow: %v", err)
	}

	url, pv := phaseAction(exp.ID, ph.ID, "start")
	w := e.do(s.handleStartPhase, http.MethodPost, url, "", pv)
	body := decodeErrBody(t, w, http.StatusBadGateway, "zeus_unreachable")
	if msg, _ := body["error"].(string); !strings.Contains(msg, wf) {
		t.Errorf("error=%q, want it to name the workflow being registered", msg)
	}
	if got, err := e.exp.GetPhase(e.ctx, ph.ID); err != nil || got.Status != "failed" {
		t.Errorf("phase status=%v err=%v, want failed (the enter sequence finalizes it)", got, err)
	}
}
