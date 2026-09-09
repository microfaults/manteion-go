package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"manteion-go/internal/model"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
)

// noDBServer is a Server whose repos are nil: every handler path exercised
// through it must refuse the request before touching the store.
func noDBServer() *Server {
	logger := discardLogger()
	return &Server{
		logger: logger,
		orch:   orchestrator.New(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, logger),
	}
}

// decodeErrBody decodes an error envelope and asserts status + code; the body
// is returned for field assertions.
func decodeErrBody(t *testing.T, w *httptest.ResponseRecorder, wantStatus int, wantCode string) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v (status=%d body=%s)", err, w.Code, w.Body.String())
	}
	if w.Code != wantStatus || body["code"] != wantCode {
		t.Fatalf("status=%d code=%v body=%s, want %d %s", w.Code, body["code"], w.Body.String(), wantStatus, wantCode)
	}
	if msg, _ := body["error"].(string); msg == "" {
		t.Errorf("body=%s, want a non-empty \"error\" message", w.Body.String())
	}
	return body
}

func TestWriteErrorCode(t *testing.T) {
	w := httptest.NewRecorder()
	writeErrorCode(w, http.StatusConflict, codeServiceOverlap, `experiment "b" cannot start`, map[string]any{
		"running_experiment_id": "exp-a",
		"services":              []string{"cartservice"},
		// The envelope's own keys win over extras.
		"error": "must not override",
		"code":  "must not override",
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("status=%d, want 409", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type=%q, want application/json", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, w.Body.String())
	}
	if body["error"] != `experiment "b" cannot start` {
		t.Errorf("error=%v, want the message (extras must not override it)", body["error"])
	}
	if body["code"] != "service_overlap" {
		t.Errorf("code=%v, want service_overlap (extras must not override it)", body["code"])
	}
	if body["running_experiment_id"] != "exp-a" {
		t.Errorf("running_experiment_id=%v, want exp-a", body["running_experiment_id"])
	}
	if !reflect.DeepEqual(body["services"], []any{"cartservice"}) {
		t.Errorf("services=%v, want [cartservice]", body["services"])
	}

	// No extras: exactly the two envelope keys.
	w = httptest.NewRecorder()
	writeErrorCode(w, http.StatusNotFound, codeNotFound, "experiment not found", nil)
	body = nil
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if want := map[string]any{"error": "experiment not found", "code": "not_found"}; !reflect.DeepEqual(body, want) {
		t.Errorf("body=%v, want %v", body, want)
	}
}

// TestErrorEnvelope pins the error → (status, code, extra) mapping, through
// %w wrapping the way the orchestrator and store hand errors up.
func TestErrorEnvelope(t *testing.T) {
	ctx := context.Background()
	overlap := &orchestrator.ErrServiceOverlap{
		ExperimentID: "exp-2", RunningExperimentID: "exp-1", Services: []string{"cartservice", "frontend"},
	}
	invalid := &orchestrator.ErrInvalidState{
		Kind: "experiment", ID: "exp-1", Current: "running", Wanted: "planned",
		Msg: `orchestrator: experiment "exp-1" is not planned`,
	}
	zeusDown := &orchestrator.ErrZeusUnreachable{
		Op: `register workflow "wf-1" in zeus`,
		Err: fmt.Errorf("zeus: request failed: %w", &url.Error{
			Op: "Post", URL: "http://127.0.0.1:1/api/v1/workflows", Err: errors.New("connection refused"),
		}),
	}
	// Real producers of the validation-tagged errors, reached without a DB:
	// both refuse before touching a repository.
	badFinal := noDBServer().orch.StopExperiment(ctx, "exp-1", "bogus")
	storeInvalid := store.NewExperimentRepo(nil).Create(ctx, &model.Experiment{ID: "exp-1", Status: "planned"})

	cases := []struct {
		name   string
		err    error
		status int
		code   string
		extra  map[string]any
	}{
		{"service overlap", fmt.Errorf("start: %w", overlap), http.StatusConflict, "service_overlap",
			map[string]any{"running_experiment_id": "exp-1", "services": []string{"cartservice", "frontend"}}},
		{"invalid state", fmt.Errorf("start: %w", invalid), http.StatusConflict, "invalid_state",
			map[string]any{"status": "running"}},
		{"invalid state, current unknown", &orchestrator.ErrInvalidState{Msg: "x"}, http.StatusConflict, "invalid_state",
			map[string]any{"status": ""}},
		{"not found", fmt.Errorf("orchestrator: load phase: %w", store.ErrNotFound), http.StatusNotFound, "not_found", nil},
		{"conflict", fmt.Errorf("phase name %q already used in this experiment: %w", "x", store.ErrConflict), http.StatusConflict, "conflict", nil},
		{"store validation", storeInvalid, http.StatusUnprocessableEntity, "validation", nil},
		{"orchestrator validation", badFinal, http.StatusUnprocessableEntity, "validation", nil},
		{"zeus unreachable", fmt.Errorf("orchestrator: materialize workflows: %w", zeusDown), http.StatusBadGateway, "zeus_unreachable", nil},
		{"unclassified", errors.New("boom"), http.StatusInternalServerError, "internal", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.err == nil {
				t.Fatal("test setup: producer returned nil error")
			}
			status, code, extra := errorEnvelope(c.err)
			if status != c.status || code != c.code {
				t.Errorf("errorEnvelope(%v) = %d %q, want %d %q", c.err, status, code, c.status, c.code)
			}
			if !reflect.DeepEqual(extra, c.extra) {
				t.Errorf("extra = %#v, want %#v", extra, c.extra)
			}
		})
	}
	// Validation producers keep their messages verbatim (logs and the UI
	// both read them).
	if got := storeInvalid.Error(); got != "experiment: name required" {
		t.Errorf("store validation message = %q, want unchanged", got)
	}
	if got := badFinal.Error(); got != `orchestrator: invalid final status "bogus"` {
		t.Errorf("orchestrator validation message = %q, want unchanged", got)
	}
}

func TestWriteMappedError(t *testing.T) {
	w := httptest.NewRecorder()
	writeMappedError(w, fmt.Errorf("orchestrator: load phase: %w", store.ErrNotFound))
	body := decodeErrBody(t, w, http.StatusNotFound, "not_found")
	if body["error"] != "orchestrator: load phase: store: not found" {
		t.Errorf("error=%v, want the full error text", body["error"])
	}
}

// TestExperimentHandlers_RequestErrors pins the request-shaped refusals that
// need no database: malformed JSON is 400 bad_json, and a well-formed body
// the plan rejects is 422 validation — before anything is written.
func TestExperimentHandlers_RequestErrors(t *testing.T) {
	s := noDBServer()
	do := func(t *testing.T, h http.HandlerFunc, method, target, body string, pv map[string]string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		for k, v := range pv {
			req.SetPathValue(k, v)
		}
		w := httptest.NewRecorder()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("handler panicked: %v — it reached the store before validating the request", r)
				}
			}()
			h(w, req)
		}()
		return w
	}
	expPV := map[string]string{"id": "exp-1"}
	phasePV := map[string]string{"id": "exp-1", "phaseId": "phase-1"}
	disagree := `{"name":"x","phases":[
		{"name":"a","position":0,"frozen_services":[{"service":"frontend","mode":"replay","key_strategy":"exact","mutation_policy":"deny"}]},
		{"name":"b","position":1,"frozen_services":[{"service":"frontend","mode":"replay","key_strategy":"exact_with_host","mutation_policy":"deny"}]}]}`

	cases := []struct {
		name     string
		h        http.HandlerFunc
		method   string
		target   string
		body     string
		pv       map[string]string
		status   int
		code     string
		contains string
	}{
		{"create experiment: malformed JSON", s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", `{"name":`, nil, 400, "bad_json", ""},
		{"create experiment: empty name", s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", `{"name":""}`, nil, 422, "validation", "name required"},
		{"create experiment: strategy disagreement", s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", disagree, nil, 422, "validation", "disagreement"},
		{"create experiment: empty phase name", s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments",
			`{"name":"x","phases":[{"name":""}]}`, nil, 422, "validation", "phase name required"},
		{"create experiment: vus <= 0", s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments",
			`{"name":"x","phases":[{"name":"p","workflows":[{"workflow_id":"wf","vus":0,"duration_sec":10}]}]}`, nil, 422, "validation", "vus must be > 0"},
		{"create experiment: duration <= 0", s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments",
			`{"name":"x","phases":[{"name":"p","workflows":[{"workflow_id":"wf","vus":1,"duration_sec":0}]}]}`, nil, 422, "validation", "duration_sec must be > 0"},
		{"create experiment: bad mutation_policy", s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments",
			`{"name":"x","phases":[{"name":"p","frozen_services":[{"service":"cart","mode":"replay","key_strategy":"exact","mutation_policy":"maybe"}]}]}`, nil, 422, "validation", "mutation_policy"},
		{"update experiment: malformed JSON", s.handleUpdateExperiment, http.MethodPut, "/api/v1/experiments/exp-1", `{`, expPV, 400, "bad_json", ""},
		{"update experiment: empty name", s.handleUpdateExperiment, http.MethodPut, "/api/v1/experiments/exp-1", `{"name":""}`, expPV, 422, "validation", "name may not be empty"},
		{"create phase: malformed JSON", s.handleCreatePhase, http.MethodPost, "/api/v1/experiments/exp-1/phases", `[`, expPV, 400, "bad_json", ""},
		{"create phase: empty name", s.handleCreatePhase, http.MethodPost, "/api/v1/experiments/exp-1/phases", `{"name":""}`, expPV, 422, "validation", "name required"},
		{"update phase: malformed JSON", s.handleUpdatePhase, http.MethodPut, "/api/v1/experiments/exp-1/phases/phase-1", `{`, phasePV, 400, "bad_json", ""},
		{"update phase: empty name", s.handleUpdatePhase, http.MethodPut, "/api/v1/experiments/exp-1/phases/phase-1", `{"name":""}`, phasePV, 422, "validation", "name may not be empty"},
		{"stop experiment: unknown final status", s.handleStopExperiment, http.MethodPost, "/api/v1/experiments/exp-1/stop?status=bogus", "", expPV, 422, "validation", "invalid final status"},
		{"stop phase: unknown final status", s.handleStopPhase, http.MethodPost, "/api/v1/experiments/exp-1/phases/phase-1/stop?status=bogus", "", phasePV, 422, "validation", "invalid phase final status"},
		{"stop phase (flat): unknown final status", s.handleStopPhaseFlat, http.MethodPost, "/api/v1/phases/phase-1/stop?status=bogus", "", phasePV, 422, "validation", "invalid phase final status"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := do(t, c.h, c.method, c.target, c.body, c.pv)
			body := decodeErrBody(t, w, c.status, c.code)
			if msg, _ := body["error"].(string); c.contains != "" && !strings.Contains(msg, c.contains) {
				t.Errorf("error=%q, want it to mention %q", msg, c.contains)
			}
		})
	}
}
