package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// fakeFaultRepo is a minimal in-memory FaultStore implementation for handler tests.
type fakeFaultRepo struct {
	specs map[string]*model.FaultSpec
	comps map[string]*model.FaultComposition
	err   error
}

func newFakeFaultRepo() *fakeFaultRepo {
	return &fakeFaultRepo{
		specs: make(map[string]*model.FaultSpec),
		comps: make(map[string]*model.FaultComposition),
	}
}

func (f *fakeFaultRepo) CreateSpec(ctx context.Context, spec *model.FaultSpec) error {
	if f.err != nil {
		return f.err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	f.specs[spec.ID] = spec
	return nil
}

func (f *fakeFaultRepo) GetSpec(ctx context.Context, id string) (*model.FaultSpec, error) {
	if s, ok := f.specs[id]; ok {
		return s, nil
	}
	return nil, errFakeNotFound
}

func (f *fakeFaultRepo) ListSpecs(ctx context.Context) ([]*model.FaultSpec, error) {
	out := make([]*model.FaultSpec, 0, len(f.specs))
	for _, s := range f.specs {
		out = append(out, s)
	}
	return out, nil
}

func (f *fakeFaultRepo) DeleteSpec(ctx context.Context, id string) error {
	if _, ok := f.specs[id]; !ok {
		return errFakeNotFound
	}
	delete(f.specs, id)
	return nil
}

func (f *fakeFaultRepo) CreateComposition(ctx context.Context, c *model.FaultComposition) error {
	if f.err != nil {
		return f.err
	}
	f.comps[c.ID] = c
	return nil
}

func (f *fakeFaultRepo) GetComposition(ctx context.Context, id string) (*model.FaultComposition, error) {
	if c, ok := f.comps[id]; ok {
		return c, nil
	}
	return nil, errFakeNotFound
}

func (f *fakeFaultRepo) ListCompositions(ctx context.Context) ([]*model.FaultComposition, error) {
	out := make([]*model.FaultComposition, 0, len(f.comps))
	for _, c := range f.comps {
		out = append(out, c)
	}
	return out, nil
}

func (f *fakeFaultRepo) DeleteComposition(ctx context.Context, id string) error {
	if _, ok := f.comps[id]; !ok {
		return errFakeNotFound
	}
	delete(f.comps, id)
	return nil
}

func (f *fakeFaultRepo) SpecResolver(ctx context.Context) model.FaultSpecResolver {
	return func(id string) *model.FaultSpec { return f.specs[id] }
}

func (f *fakeFaultRepo) CompositionResolver(ctx context.Context) model.CompositionResolver {
	return func(id string) *model.FaultComposition { return f.comps[id] }
}

// Alias the real store sentinel so handler code's
// errors.Is(err, store.ErrNotFound) correctly returns 404 against the fake.
var errFakeNotFound = store.ErrNotFound

// errSimulatedDB stands in for a post-validation storage failure when a test
// sets fakeFaultRepo.err — used to assert the handler reports 500, not 400,
// once ValidateComposition has already succeeded.
var errSimulatedDB = errors.New("simulated db failure")

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandleCreateFaultSpec(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/specs", s.handleCreateFaultSpec)

	body := `{
		"id":"spec-1",
		"name":"200ms latency",
		"category":"inline",
		"fault_type":"latency",
		"params":{"delay":"200ms"}
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/specs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	if repo.specs["spec-1"] == nil {
		t.Fatal("spec not stored")
	}
}

func TestHandleCreateFaultSpec_ValidationError(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/specs", s.handleCreateFaultSpec)

	body := `{"id":"spec-2","name":"x","category":"inline","fault_type":"nonsense","params":{}}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/specs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestHandleCreateFaultComposition_ValidationCalled(t *testing.T) {
	repo := newFakeFaultRepo()
	repo.specs["spec-a"] = &model.FaultSpec{
		ID: "spec-a", Name: "latency", Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{"delay":"100ms"}`),
	}
	repo.specs["spec-b"] = &model.FaultSpec{
		ID: "spec-b", Name: "error", Category: "inline", FaultType: "error",
		Params: json.RawMessage(`{"status_code":500}`),
	}

	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/compositions", s.handleCreateFaultComposition)

	body := `{
		"id":"comp-1","name":"lat+err","execution_mode":"parallel",
		"members":[
			{"fault_spec_id":"spec-a"},
			{"fault_spec_id":"spec-b"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/compositions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	if repo.comps["comp-1"] == nil {
		t.Fatal("composition not stored")
	}
}

func TestHandleCreateFaultComposition_DanglingSpec(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/compositions", s.handleCreateFaultComposition)

	body := `{
		"id":"comp-2","name":"bad","execution_mode":"parallel",
		"members":[
			{"fault_spec_id":"does-not-exist"},
			{"fault_spec_id":"also-missing"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/compositions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", w.Code, w.Body.String())
	}
	if repo.comps["comp-2"] != nil {
		t.Fatal("dangling composition should not have been stored")
	}
}

func TestHandleCreateFaultSpec_MalformedJSON(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/specs", s.handleCreateFaultSpec)

	// Truncated object (missing closing brace) — readJSON should reject.
	body := `{"id":"spec-1","name":"x",`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/specs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", w.Code, w.Body.String())
	}
	if len(repo.specs) != 0 {
		t.Errorf("malformed request should not have stored anything, got %d specs", len(repo.specs))
	}
}

func TestHandleListFaultSpecs(t *testing.T) {
	repo := newFakeFaultRepo()
	repo.specs["s1"] = &model.FaultSpec{
		ID: "s1", Name: "a", Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{"delay":"50ms"}`),
	}
	repo.specs["s2"] = &model.FaultSpec{
		ID: "s2", Name: "b", Category: "inline", FaultType: "error",
		Params: json.RawMessage(`{"status_code":500}`),
	}
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/faults/specs", s.handleListFaultSpecs)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/faults/specs", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var got []*model.FaultSpec
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d specs, want 2", len(got))
	}
}

func TestHandleGetFaultSpec_NotFound(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/faults/specs/{id}", s.handleGetFaultSpec)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/faults/specs/ghost", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestHandleDeleteFaultSpec_Success(t *testing.T) {
	repo := newFakeFaultRepo()
	repo.specs["spec-1"] = &model.FaultSpec{
		ID: "spec-1", Name: "a", Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{"delay":"50ms"}`),
	}
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/faults/specs/{id}", s.handleDeleteFaultSpec)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/faults/specs/spec-1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if _, exists := repo.specs["spec-1"]; exists {
		t.Error("spec not removed from store after successful delete")
	}
}

func TestHandleDeleteFaultSpec_NotFound(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v1/faults/specs/{id}", s.handleDeleteFaultSpec)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/faults/specs/ghost", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestHandleListFaultCompositions(t *testing.T) {
	repo := newFakeFaultRepo()
	repo.comps["c1"] = &model.FaultComposition{ID: "c1", Name: "comp"}
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/faults/compositions", s.handleListFaultCompositions)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/faults/compositions", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestHandleGetFaultComposition_NotFound(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/faults/compositions/{id}", s.handleGetFaultComposition)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/faults/compositions/ghost", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// TestHandleCreateFaultComposition_DBError exercises the post-validation
// failure path. The fake's err field is injected so CreateComposition returns
// after ValidateComposition has already succeeded — confirming C1 (500, not
// 400) when the store layer itself fails.
func TestHandleCreateFaultComposition_DBError(t *testing.T) {
	repo := newFakeFaultRepo()
	repo.specs["spec-a"] = &model.FaultSpec{
		ID: "spec-a", Name: "latency", Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{"delay":"100ms"}`),
	}
	repo.specs["spec-b"] = &model.FaultSpec{
		ID: "spec-b", Name: "error", Category: "inline", FaultType: "error",
		Params: json.RawMessage(`{"status_code":500}`),
	}
	repo.err = errSimulatedDB

	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/compositions", s.handleCreateFaultComposition)

	body := `{
		"id":"comp-3","name":"ok","execution_mode":"parallel",
		"members":[
			{"fault_spec_id":"spec-a"},
			{"fault_spec_id":"spec-b"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/compositions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500, body=%s", w.Code, w.Body.String())
	}
	if repo.comps["comp-3"] != nil {
		t.Error("composition should not be stored when store returns error")
	}
}
