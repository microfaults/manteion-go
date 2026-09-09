package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/db"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// editTestEnv is the DB-backed fixture for the experiment / phase edit
// handlers (MANTEION_TEST_DB=1). Every row it seeds is deleted at cleanup;
// ids are minted per test so concurrent packages sharing the database do
// not collide.
type editTestEnv struct {
	t   *testing.T
	ctx context.Context
	db  *sql.DB
	s   *Server
	exp *store.ExperimentRepo
}

func newEditTestEnv(t *testing.T) *editTestEnv {
	t.Helper()
	if os.Getenv("MANTEION_TEST_DB") == "" {
		t.Skip("set MANTEION_TEST_DB=1 to run")
	}
	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	database, err := db.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(database) })
	expRepo := store.NewExperimentRepo(database)
	return &editTestEnv{
		t: t, ctx: ctx, db: database, exp: expRepo,
		s: &Server{experiments: expRepo, logger: discardLogger(), orch: newTestOrch(t, database, "")},
	}
}

// experiment seeds an experiment row in the given status; its phases and
// join rows cascade on delete.
func (e *editTestEnv) experiment(status string) *model.Experiment {
	e.t.Helper()
	exp := &model.Experiment{
		ID: id.New("exp"), Name: "edit-" + id.New("n"), Description: "before",
		Hypothesis: "h0", Status: status, CreatedAt: time.Now(),
	}
	if err := e.exp.Create(e.ctx, exp); err != nil {
		e.t.Fatalf("create experiment: %v", err)
	}
	e.t.Cleanup(func() { _, _ = e.db.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
	return exp
}

// phase seeds a pending phase under exp.
func (e *editTestEnv) phase(exp *model.Experiment, name string, position int, frozen ...model.CacheBoxConfig) *model.ExperimentPhase {
	e.t.Helper()
	ph := &model.ExperimentPhase{
		ID: id.New("phase"), ExperimentID: exp.ID, Name: name, Position: position,
		Status: "pending", FrozenServices: frozen,
	}
	if err := e.exp.CreatePhase(e.ctx, ph); err != nil {
		e.t.Fatalf("create phase %q: %v", name, err)
	}
	return ph
}

// rule seeds a rules row (phase_rules.rule_id is FK ON DELETE RESTRICT, so
// cleanup clears the join rows first regardless of cleanup ordering).
func (e *editTestEnv) rule() string {
	e.t.Helper()
	r := &model.Rule{
		ID: id.New("rule"), Name: "edit-rule", Service: "frontend", Mode: "inline",
		Action:    model.RuleAction{Type: "cachebox", CacheBox: &model.CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: "exact"}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.NewRuleRepo(e.db).Create(e.ctx, r); err != nil {
		e.t.Fatalf("create rule: %v", err)
	}
	e.t.Cleanup(func() {
		_, _ = e.db.Exec("DELETE FROM phase_rules WHERE rule_id = $1", r.ID)
		_, _ = e.db.Exec("DELETE FROM rules WHERE id = $1", r.ID)
	})
	return r.ID
}

// workflow seeds a workflows row (phase_workflows.workflow_id FK).
func (e *editTestEnv) workflow() string {
	e.t.Helper()
	wf := &model.Workflow{
		ID: id.New("wf"), Name: "edit-wf-" + id.New("n"),
		DSL:       json.RawMessage(`{"type":"request","method":"GET","url":"http://target:8080/"}`),
		CreatedAt: time.Now(),
	}
	if err := store.NewWorkflowRepo(e.db).Create(e.ctx, wf); err != nil {
		e.t.Fatalf("create workflow: %v", err)
	}
	e.t.Cleanup(func() {
		_, _ = e.db.Exec("DELETE FROM phase_workflows WHERE workflow_id = $1", wf.ID)
		_, _ = e.db.Exec("DELETE FROM workflows WHERE id = $1", wf.ID)
	})
	return wf.ID
}

// do invokes handler directly with a raw JSON body and the mux path values
// set by hand (the handlers are exercised without the router, like the
// other DB-gated handler tests in this package).
func (e *editTestEnv) do(handler http.HandlerFunc, method, target, body string, pathValues map[string]string) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for k, v := range pathValues {
		req.SetPathValue(k, v)
	}
	w := httptest.NewRecorder()
	handler(w, req)
	return w
}

func decodePhase(t *testing.T, w *httptest.ResponseRecorder) phaseDetail {
	t.Helper()
	var got phaseDetail
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode phase: %v (body=%s)", err, w.Body.String())
	}
	return got
}

func decodeExperiment(t *testing.T, w *httptest.ResponseRecorder) experimentDetailResponse {
	t.Helper()
	var got experimentDetailResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode experiment: %v (body=%s)", err, w.Body.String())
	}
	return got
}

// =========================================================================
// POST /experiments/{id}/phases — position defaulting
// =========================================================================

// TestHandleCreatePhase_Position pins the omitted-vs-explicit position
// semantics: an omitted position appends (max+1) while an explicit
// "position": 0 is honoured rather than being read as "unset".
func TestHandleCreatePhase_Position(t *testing.T) {
	e := newEditTestEnv(t)
	exp := e.experiment("planned")
	e.phase(exp, "seed", 5)
	target := "/api/v1/experiments/" + exp.ID + "/phases"
	pv := map[string]string{"id": exp.ID}

	w := e.do(e.s.handleCreatePhase, http.MethodPost, target, `{"name":"appended"}`, pv)
	if w.Code != http.StatusCreated {
		t.Fatalf("omitted position: status=%d body=%s", w.Code, w.Body.String())
	}
	if got := decodePhase(t, w).Position; got != 6 {
		t.Errorf("omitted position = %d, want 6 (append after max 5)", got)
	}

	w = e.do(e.s.handleCreatePhase, http.MethodPost, target, `{"name":"explicit-zero","position":0}`, pv)
	if w.Code != http.StatusCreated {
		t.Fatalf("explicit position 0: status=%d body=%s", w.Code, w.Body.String())
	}
	if got := decodePhase(t, w).Position; got != 0 {
		t.Errorf("explicit position 0 = %d, want 0 (must not be treated as omitted)", got)
	}
}

// =========================================================================
// PUT /experiments/{id}
// =========================================================================

func TestHandleUpdateExperiment(t *testing.T) {
	e := newEditTestEnv(t)
	target := func(expID string) (string, map[string]string) {
		return "/api/v1/experiments/" + expID, map[string]string{"id": expID}
	}

	t.Run("planned: only the fields present change", func(t *testing.T) {
		exp := e.experiment("planned")
		e.phase(exp, "baseline", 0)
		url, pv := target(exp.ID)
		w := e.do(e.s.handleUpdateExperiment, http.MethodPut, url, `{"name":"renamed","hypothesis":""}`, pv)
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		got := decodeExperiment(t, w)
		if got.Name != "renamed" {
			t.Errorf("name=%q, want renamed", got.Name)
		}
		if got.Description != "before" {
			t.Errorf("description=%q, want \"before\" (omitted field keeps its value)", got.Description)
		}
		if got.Hypothesis != "" {
			t.Errorf("hypothesis=%q, want cleared by an explicit empty string", got.Hypothesis)
		}
		if got.Status != "planned" || len(got.Phases) != 1 {
			t.Errorf("detail = status %q / %d phases, want planned / 1 (same shape as GET)", got.Status, len(got.Phases))
		}
		stored, err := e.exp.Get(e.ctx, exp.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if stored.Name != "renamed" || stored.Description != "before" || stored.Hypothesis != "" {
			t.Errorf("stored row = %+v, want the edit persisted", stored)
		}
	})

	t.Run("empty name is rejected", func(t *testing.T) {
		exp := e.experiment("planned")
		url, pv := target(exp.ID)
		w := e.do(e.s.handleUpdateExperiment, http.MethodPut, url, `{"name":""}`, pv)
		decodeErrBody(t, w, http.StatusUnprocessableEntity, "validation")
	})

	t.Run("running experiment is not editable", func(t *testing.T) {
		exp := e.experiment("running")
		url, pv := target(exp.ID)
		w := e.do(e.s.handleUpdateExperiment, http.MethodPut, url, `{"name":"nope"}`, pv)
		if body := decodeErrBody(t, w, http.StatusConflict, "invalid_state"); body["status"] != "running" {
			t.Errorf("status=%v, want running", body["status"])
		}
		if !strings.Contains(w.Body.String(), "experiment must be planned to edit") {
			t.Errorf("body=%s, want the planned-only message", w.Body.String())
		}
		stored, err := e.exp.Get(e.ctx, exp.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if stored.Name != exp.Name {
			t.Errorf("running experiment was modified: name=%q", stored.Name)
		}
	})

	t.Run("unknown experiment", func(t *testing.T) {
		url, pv := target("exp-nope")
		w := e.do(e.s.handleUpdateExperiment, http.MethodPut, url, `{"name":"x"}`, pv)
		decodeErrBody(t, w, http.StatusNotFound, "not_found")
	})
}

// =========================================================================
// PUT /experiments/{id}/phases/{phaseId}
// =========================================================================

func TestHandleUpdatePhase(t *testing.T) {
	e := newEditTestEnv(t)
	frontendExact := model.CacheBoxConfig{Service: "frontend", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}
	target := func(expID, phaseID string) (string, map[string]string) {
		return "/api/v1/experiments/" + expID + "/phases/" + phaseID,
			map[string]string{"id": expID, "phaseId": phaseID}
	}

	t.Run("happy path replaces workflows and rules", func(t *testing.T) {
		exp := e.experiment("planned")
		ph := e.phase(exp, "iso", 0)
		wfA, wfB := e.workflow(), e.workflow()
		ruleA, ruleB := e.rule(), e.rule()
		// Pre-existing attachments the edit must replace, not merge into.
		if err := e.exp.AttachPhaseWorkflows(e.ctx, ph.ID, []model.PhaseWorkflow{{WorkflowID: wfA, VUs: 1, DurationSec: 1}}); err != nil {
			t.Fatalf("attach workflows: %v", err)
		}
		if err := e.exp.AttachPhaseRules(e.ctx, ph.ID, []string{ruleA}); err != nil {
			t.Fatalf("attach rules: %v", err)
		}

		url, pv := target(exp.ID, ph.ID)
		body := fmt.Sprintf(`{"name":"iso-cart","position":3,"persist_cache":true,
			"frozen_services":[{"service":"cart","mode":"replay","key_strategy":"exact","mutation_policy":"deny"}],
			"workflows":[{"workflow_id":%q,"vus":7,"duration_sec":30}],
			"rule_ids":[%q]}`, wfB, ruleB)
		w := e.do(e.s.handleUpdatePhase, http.MethodPut, url, body, pv)
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		got := decodePhase(t, w)
		if got.ID != ph.ID || got.Name != "iso-cart" || got.Position != 3 || !got.PersistCache || got.Status != "pending" {
			t.Errorf("phase row = %+v, want iso-cart/3/persist_cache/pending", got.ExperimentPhase)
		}
		if len(got.FrozenServices) != 1 || got.FrozenServices[0].Service != "cart" {
			t.Errorf("frozen_services = %+v, want [cart]", got.FrozenServices)
		}
		if len(got.Workflows) != 1 || got.Workflows[0].WorkflowID != wfB || got.Workflows[0].VUs != 7 {
			t.Errorf("workflows = %+v, want only %s with 7 vus", got.Workflows, wfB)
		}
		if !reflect.DeepEqual(got.RuleIDs, []string{ruleB}) {
			t.Errorf("rule_ids = %v, want [%s]", got.RuleIDs, ruleB)
		}

		// Omitted lists and scalars are left alone; empty lists clear.
		w = e.do(e.s.handleUpdatePhase, http.MethodPut, url, `{"name":"iso-cart-2"}`, pv)
		if w.Code != http.StatusOK {
			t.Fatalf("rename-only: status=%d body=%s", w.Code, w.Body.String())
		}
		got = decodePhase(t, w)
		if got.Name != "iso-cart-2" || got.Position != 3 || !got.PersistCache || len(got.Workflows) != 1 || len(got.RuleIDs) != 1 {
			t.Errorf("rename-only edit must leave everything else untouched: %+v workflows=%d rules=%d",
				got.ExperimentPhase, len(got.Workflows), len(got.RuleIDs))
		}
		w = e.do(e.s.handleUpdatePhase, http.MethodPut, url, `{"workflows":[],"rule_ids":[]}`, pv)
		if w.Code != http.StatusOK {
			t.Fatalf("clear lists: status=%d body=%s", w.Code, w.Body.String())
		}
		got = decodePhase(t, w)
		if len(got.Workflows) != 0 || len(got.RuleIDs) != 0 {
			t.Errorf("empty lists must clear the attachments: workflows=%+v rules=%v", got.Workflows, got.RuleIDs)
		}
	})

	t.Run("phase not pending", func(t *testing.T) {
		exp := e.experiment("planned")
		ph := e.phase(exp, "done", 0)
		if err := e.exp.UpdatePhaseStatus(e.ctx, ph.ID, "completed"); err != nil {
			t.Fatalf("complete phase: %v", err)
		}
		url, pv := target(exp.ID, ph.ID)
		w := e.do(e.s.handleUpdatePhase, http.MethodPut, url, `{"name":"nope"}`, pv)
		if body := decodeErrBody(t, w, http.StatusConflict, "invalid_state"); body["status"] != "completed" {
			t.Errorf("status=%v, want completed", body["status"])
		}
		if !strings.Contains(w.Body.String(), "phase must be pending to edit") {
			t.Errorf("body=%s, want the pending-only message", w.Body.String())
		}
		stored, err := e.exp.GetPhase(e.ctx, ph.ID)
		if err != nil {
			t.Fatalf("get phase: %v", err)
		}
		if stored.Name != "done" {
			t.Errorf("completed phase was modified: name=%q", stored.Name)
		}
	})

	t.Run("experiment not planned", func(t *testing.T) {
		exp := e.experiment("running")
		ph := e.phase(exp, "pending-in-running", 0)
		url, pv := target(exp.ID, ph.ID)
		w := e.do(e.s.handleUpdatePhase, http.MethodPut, url, `{"name":"nope"}`, pv)
		if body := decodeErrBody(t, w, http.StatusConflict, "invalid_state"); body["status"] != "running" {
			t.Errorf("status=%v, want running", body["status"])
		}
		if !strings.Contains(w.Body.String(), "experiment must be planned to edit") {
			t.Errorf("body=%s, want the planned-only message", w.Body.String())
		}
	})

	t.Run("phase of another experiment", func(t *testing.T) {
		owner := e.experiment("planned")
		ph := e.phase(owner, "owned", 0)
		other := e.experiment("planned")
		url, pv := target(other.ID, ph.ID)
		w := e.do(e.s.handleUpdatePhase, http.MethodPut, url, `{"name":"stolen"}`, pv)
		if w.Code != http.StatusNotFound {
			t.Fatalf("foreign phase: status=%d body=%s, want 404", w.Code, w.Body.String())
		}
		stored, err := e.exp.GetPhase(e.ctx, ph.ID)
		if err != nil {
			t.Fatalf("get phase: %v", err)
		}
		if stored.Name != "owned" {
			t.Errorf("phase edited through the wrong experiment: name=%q", stored.Name)
		}
		url, pv = target(owner.ID, "phase-nope")
		w = e.do(e.s.handleUpdatePhase, http.MethodPut, url, `{"name":"x"}`, pv)
		if w.Code != http.StatusNotFound {
			t.Fatalf("unknown phase: status=%d body=%s, want 404", w.Code, w.Body.String())
		}
	})

	t.Run("cache-box strategy disagreement across phases", func(t *testing.T) {
		exp := e.experiment("planned")
		e.phase(exp, "iso-a", 0, frontendExact)
		ph := e.phase(exp, "iso-b", 1, frontendExact)
		url, pv := target(exp.ID, ph.ID)
		body := `{"frozen_services":[{"service":"frontend","mode":"replay","key_strategy":"exact_with_host","mutation_policy":"deny"}]}`
		w := e.do(e.s.handleUpdatePhase, http.MethodPut, url, body, pv)
		decodeErrBody(t, w, http.StatusUnprocessableEntity, "validation")
		if !strings.Contains(w.Body.String(), "disagreement") {
			t.Errorf("body=%s, want the INV-2 disagreement message", w.Body.String())
		}
		stored, err := e.exp.GetPhase(e.ctx, ph.ID)
		if err != nil {
			t.Fatalf("get phase: %v", err)
		}
		if len(stored.FrozenServices) != 1 || stored.FrozenServices[0].KeyStrategy != "exact" {
			t.Errorf("rejected edit was persisted: %+v", stored.FrozenServices)
		}
	})

	t.Run("duplicate name and position are conflicts", func(t *testing.T) {
		exp := e.experiment("planned")
		e.phase(exp, "taken", 0)
		ph := e.phase(exp, "free", 1)
		url, pv := target(exp.ID, ph.ID)
		w := e.do(e.s.handleUpdatePhase, http.MethodPut, url, `{"name":"taken"}`, pv)
		decodeErrBody(t, w, http.StatusConflict, "conflict")
		// (experiment_id, position) is DEFERRABLE INITIALLY DEFERRED, so the
		// violation only surfaces at commit — it must still map to 409.
		w = e.do(e.s.handleUpdatePhase, http.MethodPut, url, `{"position":0}`, pv)
		decodeErrBody(t, w, http.StatusConflict, "conflict")
		stored, err := e.exp.GetPhase(e.ctx, ph.ID)
		if err != nil {
			t.Fatalf("get phase: %v", err)
		}
		if stored.Name != "free" || stored.Position != 1 {
			t.Errorf("conflicting edit was persisted: %+v", stored)
		}
	})
}
