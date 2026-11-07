package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
	"manteion-go/internal/opaharness"
	"manteion-go/internal/store"
)

// ---------------------------------------------------------------------------
// Fakes — DB-free views of the OPA repos and the harness.
// ---------------------------------------------------------------------------

type fakeOPAPolicies struct {
	byID map[string]*model.OPAPolicy
	err  error
}

func newFakeOPAPolicies(ps ...*model.OPAPolicy) *fakeOPAPolicies {
	f := &fakeOPAPolicies{byID: map[string]*model.OPAPolicy{}}
	for _, p := range ps {
		f.byID[p.ID] = p
	}
	return f
}

func (f *fakeOPAPolicies) nameTaken(name, exceptID string) bool {
	for _, p := range f.byID {
		if p.Name == name && p.ID != exceptID {
			return true
		}
	}
	return false
}

func (f *fakeOPAPolicies) Create(_ context.Context, p *model.OPAPolicy) error {
	if f.err != nil {
		return f.err
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if f.nameTaken(p.Name, p.ID) {
		return store.ErrConflict
	}
	cp := *p
	f.byID[p.ID] = &cp
	return nil
}

func (f *fakeOPAPolicies) Get(_ context.Context, id string) (*model.OPAPolicy, error) {
	if f.err != nil {
		return nil, f.err
	}
	if p, ok := f.byID[id]; ok {
		cp := *p
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeOPAPolicies) Update(_ context.Context, p *model.OPAPolicy) error {
	if f.err != nil {
		return f.err
	}
	if err := p.Validate(); err != nil {
		return err
	}
	existing, ok := f.byID[p.ID]
	if !ok {
		return store.ErrNotFound
	}
	if f.nameTaken(p.Name, p.ID) {
		return store.ErrConflict
	}
	cp := *p
	cp.CreatedAt = existing.CreatedAt
	f.byID[p.ID] = &cp
	return nil
}

func (f *fakeOPAPolicies) Delete(_ context.Context, id string) error {
	if _, ok := f.byID[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.byID, id)
	return nil
}

func (f *fakeOPAPolicies) List(_ context.Context) ([]*model.OPAPolicy, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := []*model.OPAPolicy{}
	for _, p := range f.byID {
		cp := *p
		out = append(out, &cp)
	}
	return out, nil
}

type fakeDecisionLister struct {
	byPolicy map[string][]*model.PolicyDecision
	gotPage  store.Page
	err      error
}

func (f *fakeDecisionLister) List(_ context.Context, policyID string, p store.Page) ([]*model.PolicyDecision, int, error) {
	f.gotPage = p
	if f.err != nil {
		return nil, 0, f.err
	}
	all := f.byPolicy[policyID]
	end := min(p.Offset+p.Limit, len(all))
	if p.Offset > len(all) {
		return []*model.PolicyDecision{}, len(all), nil
	}
	return all[p.Offset:end], len(all), nil
}

type fakeEvaluator struct {
	ev       *opaharness.Evaluation
	calls    int
	gotID    string
	gotApply bool
}

func (f *fakeEvaluator) EvaluateOnce(_ context.Context, p *model.OPAPolicy, apply bool) *opaharness.Evaluation {
	f.calls++
	f.gotID, f.gotApply = p.ID, apply
	return f.ev
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const validRego = "package atropos\n\nmutations contains m if { false; m := {} }\n"

var opaTestTime = time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC)

func opaTestPolicy() *model.OPAPolicy {
	return &model.OPAPolicy{
		ID: "opa-1", Name: "checkout-hot", Enabled: true, Rego: validRego, IntervalS: 10, CooldownS: 60,
		Inputs:    []model.PolicyInput{{Name: "p99", Source: "prometheus", Query: "up", Kind: "scalar"}},
		Scope:     model.PolicyScope{Services: []string{"checkout"}},
		CreatedAt: opaTestTime, UpdatedAt: opaTestTime,
	}
}

// opaBody is a create/update body; rego is injected so tests can vary it.
func opaBody(rego string) string {
	b, _ := json.Marshal(map[string]any{
		"name": "checkout-hot", "enabled": true, "rego": rego, "cooldown_s": 60,
		"inputs": []map[string]string{{"name": "p99", "source": "prometheus", "query": "up"}},
		"scope":  map[string]any{"services": []string{"checkout"}},
	})
	return string(b)
}

func newOPAServer(pol *fakeOPAPolicies, dec *fakeDecisionLister, ev *fakeEvaluator) *Server {
	return &Server{opaPolicies: pol, policyDecisions: dec, opa: ev, logger: discardLogger()}
}

// serveOPA drives a request through the real route table.
func serveOPA(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	s.routes(mux)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func errorOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var er ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &er); err != nil {
		t.Fatalf("decode error envelope: %v (body %s)", err, w.Body.String())
	}
	return er.Error
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHandleCreateOPAPolicy(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		existing   []*model.OPAPolicy
		storeErr   error
		wantStatus int
		wantError  string
	}{
		{"valid → 201", opaBody(validRego), nil, nil, http.StatusCreated, ""},
		{"bad json → 400", `{"name":`, nil, nil, http.StatusBadRequest, "invalid JSON"},
		{"rego parse error → 422", opaBody("package atropos\nmutations contains x if { x := }"), nil, nil, http.StatusUnprocessableEntity, "rego:"},
		{"unsafe builtin → 422", opaBody("package atropos\nmutations contains r if { r := http.send({\"method\": \"get\", \"url\": \"http://x\"}) }"), nil, nil, http.StatusUnprocessableEntity, "unsafe built-in"},
		{"missing scope → 422", strings.Replace(opaBody(validRego), `"services":["checkout"]`, `"services":[]`, 1), nil, nil, http.StatusUnprocessableEntity, "scope"},
		{"duplicate name → 409", opaBody(validRego), []*model.OPAPolicy{opaTestPolicy()}, nil, http.StatusConflict, "already exists"},
		{"store failure → 500", opaBody(validRego), nil, errors.New("db down"), http.StatusInternalServerError, "failed to create"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pol := newFakeOPAPolicies(tc.existing...)
			pol.err = tc.storeErr
			w := serveOPA(t, newOPAServer(pol, &fakeDecisionLister{}, &fakeEvaluator{}), http.MethodPost, "/api/v1/opa/policies", tc.body)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantStatus != http.StatusCreated {
				if msg := errorOf(t, w); !strings.Contains(msg, tc.wantError) {
					t.Errorf("error = %q, want it to contain %q", msg, tc.wantError)
				}
				if len(pol.byID) != len(tc.existing) {
					t.Errorf("store changed on a rejected create: %d policies", len(pol.byID))
				}
				return
			}
			var got model.OPAPolicy
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !strings.HasPrefix(got.ID, "opa-") || got.IntervalS != model.DefaultOPAIntervalS || got.CreatedAt.IsZero() || got.Inputs[0].Kind != "scalar" {
				t.Errorf("created = %+v, want minted id, defaulted interval/kind, timestamps", got)
			}
			if pol.byID[got.ID] == nil {
				t.Errorf("policy %s not stored", got.ID)
			}
		})
	}
}

func TestHandleUpdateOPAPolicy(t *testing.T) {
	changed := "package atropos\n\nmutations contains {\"op\": \"delete_rule\", \"service\": \"checkout\", \"name\": \"x\"}\n"
	tests := []struct {
		name       string
		id         string
		body       string
		wantStatus int
		wantRego   string // stored rego afterwards
	}{
		{"existing → 200, stored", "opa-1", opaBody(changed), http.StatusOK, changed},
		{"unknown → 404", "opa-nope", opaBody(changed), http.StatusNotFound, validRego},
		{"compile error → 422, untouched", "opa-1", opaBody("package atropos\nmutations contains x if { x := }"), http.StatusUnprocessableEntity, validRego},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pol := newFakeOPAPolicies(opaTestPolicy())
			w := serveOPA(t, newOPAServer(pol, &fakeDecisionLister{}, &fakeEvaluator{}), http.MethodPut, "/api/v1/opa/policies/"+tc.id, tc.body)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if pol.byID["opa-1"].Rego != tc.wantRego {
				t.Errorf("stored rego = %q, want %q", pol.byID["opa-1"].Rego, tc.wantRego)
			}
			if tc.wantStatus == http.StatusOK {
				var got model.OPAPolicy
				_ = json.Unmarshal(w.Body.Bytes(), &got)
				if got.ID != "opa-1" || !got.CreatedAt.Equal(opaTestTime) || got.UpdatedAt.Equal(opaTestTime) {
					t.Errorf("response = %+v, want the stored row with path id, original created_at, new updated_at", got)
				}
			}
		})
	}
}

func TestHandleGetAndListAndDeleteOPAPolicy(t *testing.T) {
	pol := newFakeOPAPolicies(opaTestPolicy())
	s := newOPAServer(pol, &fakeDecisionLister{}, &fakeEvaluator{})

	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/opa-1", ""); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"name":"checkout-hot"`) {
		t.Errorf("get: %d %s", w.Code, w.Body.String())
	}
	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/nope", ""); w.Code != http.StatusNotFound || errorOf(t, w) != "policy not found" {
		t.Errorf("get unknown: %d %s", w.Code, w.Body.String())
	}
	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies", ""); w.Code != http.StatusOK || !strings.HasPrefix(w.Body.String(), "[{") {
		t.Errorf("list: %d %s", w.Code, w.Body.String())
	}
	if w := serveOPA(t, s, http.MethodDelete, "/api/v1/opa/policies/opa-1", ""); w.Code != http.StatusNoContent || pol.byID["opa-1"] != nil {
		t.Errorf("delete: %d, still stored %v", w.Code, pol.byID["opa-1"] != nil)
	}
	if w := serveOPA(t, s, http.MethodDelete, "/api/v1/opa/policies/opa-1", ""); w.Code != http.StatusNotFound {
		t.Errorf("delete again: %d", w.Code)
	}
	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies", ""); w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != "[]" {
		t.Errorf("empty list = %q, want [] never null", w.Body.String())
	}
}

func TestHandleListOPADecisions(t *testing.T) {
	decs := make([]*model.PolicyDecision, 0, 7)
	for i := 0; i < 7; i++ {
		decs = append(decs, &model.PolicyDecision{ID: "pdec-" + string(rune('a'+i)), PolicyID: "opa-1", EvaluatedAt: opaTestTime.Add(-time.Duration(i) * time.Minute), Mutations: json.RawMessage(`[]`), Applied: true})
	}
	dec := &fakeDecisionLister{byPolicy: map[string][]*model.PolicyDecision{"opa-1": decs}}
	s := newOPAServer(newFakeOPAPolicies(opaTestPolicy()), dec, &fakeEvaluator{})

	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/nope/decisions", ""); w.Code != http.StatusNotFound {
		t.Errorf("unknown policy: %d", w.Code)
	}
	w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/opa-1/decisions?limit=5&offset=1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	var page PageEnvelope[model.PolicyDecision]
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if dec.gotPage != (store.Page{Limit: 5, Offset: 1}) || page.Page.Total != 7 || len(page.Data) != 5 || page.Data[0].ID != "pdec-b" || page.Page.Limit != 5 || page.Page.Offset != 1 {
		t.Errorf("page = %+v (repo got %+v)", page.Page, dec.gotPage)
	}
	w = serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/opa-1/decisions?offset=50", "")
	if !strings.Contains(w.Body.String(), `"data":[]`) {
		t.Errorf("past the end: data must be [] not null: %s", w.Body.String())
	}
}

func TestHandleEvaluateOPAPolicy(t *testing.T) {
	off := false
	ev := &opaharness.Evaluation{
		PolicyID: "opa-1", EvaluatedAt: opaTestTime, Inputs: map[string]any{"p99": 1300.0}, InputDigest: "abc",
		Mutations: []opaharness.Mutation{{Op: opaharness.OpSetEnabled, Service: "checkout", Name: "r", Enabled: &off}},
		Results:   []opaharness.MutationResult{},
	}
	tests := []struct {
		name       string
		id, query  string
		eval       *fakeEvaluator
		wantStatus int
		wantApply  bool
	}{
		{"default is a dry run", "opa-1", "", &fakeEvaluator{ev: ev}, http.StatusOK, false},
		{"apply=false", "opa-1", "?apply=false", &fakeEvaluator{ev: ev}, http.StatusOK, false},
		{"apply=true", "opa-1", "?apply=true", &fakeEvaluator{ev: ev}, http.StatusOK, true},
		{"unknown policy → 404", "nope", "", &fakeEvaluator{ev: ev}, http.StatusNotFound, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newOPAServer(newFakeOPAPolicies(opaTestPolicy()), &fakeDecisionLister{}, tc.eval)
			w := serveOPA(t, s, http.MethodPost, "/api/v1/opa/policies/"+tc.id+"/evaluate"+tc.query, "")
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantStatus == http.StatusNotFound {
				if tc.eval.calls != 0 {
					t.Errorf("evaluator called for an unknown policy")
				}
				return
			}
			if tc.eval.calls != 1 || tc.eval.gotID != "opa-1" || tc.eval.gotApply != tc.wantApply {
				t.Errorf("evaluator calls %d id %q apply %v, want 1 opa-1 %v", tc.eval.calls, tc.eval.gotID, tc.eval.gotApply, tc.wantApply)
			}
			if tc.wantStatus != http.StatusOK {
				return
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
				t.Fatalf("decode: %v", err)
			}
			for _, key := range []string{"policy_id", "evaluated_at", "inputs", "input_digest", "mutations", "results", "applied", "changed"} {
				if _, ok := raw[key]; !ok {
					t.Errorf("%s missing from %s", key, w.Body.String())
				}
			}
			if string(raw["results"]) == "null" || string(raw["mutations"]) == "null" {
				t.Errorf("mutations/results must be arrays, never null: %s", w.Body.String())
			}
		})
	}
}

// TestHandleOPAWithoutHarness: with the policy engine off (no harness) the
// evaluate endpoint answers 503 and CRUD and the decision log still work.
func TestHandleOPAWithoutHarness(t *testing.T) {
	dec := &fakeDecisionLister{byPolicy: map[string][]*model.PolicyDecision{}}
	s := &Server{opaPolicies: newFakeOPAPolicies(opaTestPolicy()), policyDecisions: dec, logger: discardLogger()}

	w := serveOPA(t, s, http.MethodPost, "/api/v1/opa/policies/opa-1/evaluate", "")
	if w.Code != http.StatusServiceUnavailable || errorOf(t, w) != "opa harness disabled (MANTEION_POLICY_ENGINE)" {
		t.Errorf("evaluate without a harness: %d %s", w.Code, w.Body.String())
	}
	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/opa-1", ""); w.Code != http.StatusOK {
		t.Errorf("get without a harness: %d", w.Code)
	}
	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/opa-1/decisions", ""); w.Code != http.StatusOK {
		t.Errorf("decisions without a harness: %d", w.Code)
	}
	// NewServer given a nil *opaharness.Harness must leave the interface nil,
	// not a non-nil interface holding a nil pointer.
	if ns := NewServer(discardLogger(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, desired.NewGenerations(desired.NewMemoryStore(), desired.NewBroker()), nil, nil, nil, nil, nil, nil, nil, nil); ns.opa != nil {
		t.Errorf("NewServer(nil harness).opa = %#v, want nil", ns.opa)
	}
}

// failingDecisionLog is an opaharness.DecisionLog whose Record always fails.
type failingDecisionLog struct{}

func (failingDecisionLog) Record(context.Context, *model.PolicyDecision) error {
	return errors.New("db down")
}

func (failingDecisionLog) LastChangeAt(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

type emptyRuleLister struct{}

func (emptyRuleLister) List(context.Context) ([]*model.Rule, error) { return nil, nil }

type emptyInstanceLister struct{}

func (emptyInstanceLister) ForService(context.Context, string) ([]*model.SDKInstance, error) {
	return nil, nil
}

type fixedProvider float64

func (f fixedProvider) Fetch(context.Context, model.PolicyInput) (any, error) { return float64(f), nil }

// TestHandleEvaluateOPAPolicy_DecisionLogFailure: a real harness whose
// decision log cannot record answers 200 with the evaluation's results and
// decision_error set, not 500.
func TestHandleEvaluateOPAPolicy_DecisionLogFailure(t *testing.T) {
	p := opaTestPolicy()
	p.Rego = "package atropos\n\nmutations contains {\"op\": \"delete_rule\", \"service\": \"frontend\", \"name\": \"x\"} if input.inputs.p99 > 1\n"
	h := opaharness.New(opaharness.Deps{
		Decisions: failingDecisionLog{},
		Rules:     emptyRuleLister{},
		Instances: emptyInstanceLister{},
		Providers: opaharness.Providers{"prometheus": fixedProvider(1300)},
		Logger:    discardLogger(),
	})
	s := &Server{opaPolicies: newFakeOPAPolicies(p), policyDecisions: &fakeDecisionLister{}, opa: h, logger: discardLogger()}
	w := serveOPA(t, s, http.MethodPost, "/api/v1/opa/policies/opa-1/evaluate", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", w.Code, w.Body.String())
	}
	var ev opaharness.Evaluation
	if err := json.Unmarshal(w.Body.Bytes(), &ev); err != nil {
		t.Fatalf("decode: %v (body %s)", err, w.Body.String())
	}
	if ev.DecisionError != "record decision: db down" || ev.Error != "" {
		t.Errorf("decision_error %q error %q, want decision_error %q", ev.DecisionError, ev.Error, "record decision: db down")
	}
	if len(ev.Mutations) != 1 || len(ev.Results) != 1 || ev.Results[0].Outcome != opaharness.OutcomeRefused {
		t.Errorf("mutations %+v results %+v, want the out-of-scope delete and its refusal", ev.Mutations, ev.Results)
	}
}
