package api

import (
	"bytes"
	"context"
	"encoding/json"
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
		"config":{"delay":"200ms"}
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

	body := `{"id":"spec-2","name":"x","category":"inline","fault_type":"nonsense","config":{}}`

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
		Config: json.RawMessage(`{"delay":"100ms"}`),
	}
	repo.specs["spec-b"] = &model.FaultSpec{
		ID: "spec-b", Name: "error", Category: "inline", FaultType: "error",
		Config: json.RawMessage(`{"status_code":500}`),
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
