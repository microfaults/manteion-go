package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// fakeRegistry is the DB-free sdk_instances table for the desired-state
// handlers: Register upserts, RecordPoll/RecordStatus mutate the row in place.
// It mirrors store.SDKRepo's contract: Register validates, stores an empty
// capability list as absent (NULL) and clears the observed state; a status
// report is a full snapshot. Timestamps are not modelled.
type fakeRegistry struct {
	mu    sync.Mutex
	byID  map[string]*model.SDKInstance
	polls []polled
	err   error
}

type polled struct {
	id  string
	gen uint64
}

func newFakeRegistry(insts ...*model.SDKInstance) *fakeRegistry {
	f := &fakeRegistry{byID: map[string]*model.SDKInstance{}}
	for _, i := range insts {
		f.byID[i.ID] = i
	}
	return f
}

func (f *fakeRegistry) Get(_ context.Context, id string) (*model.SDKInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if i, ok := f.byID[id]; ok {
		cp := *i
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeRegistry) Register(_ context.Context, inst *model.SDKInstance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if err := inst.Validate(); err != nil {
		return err
	}
	cp := *inst
	if len(cp.Capabilities) == 0 {
		cp.Capabilities = nil
	}
	cp.ObservedGeneration, cp.ObservedEtag, cp.ReplaySetID, cp.ReplaySetState = 0, "", "", ""
	cp.ObservedUnsupported, cp.ObservedErrors, cp.ObservedAt = nil, nil, nil
	f.byID[inst.ID] = &cp
	return nil
}

func (f *fakeRegistry) RecordPoll(_ context.Context, id string, gen uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls = append(f.polls, polled{id, gen})
	i, ok := f.byID[id]
	if !ok {
		return store.ErrNotFound
	}
	i.ObservedGeneration = gen
	return nil
}

func (f *fakeRegistry) RecordStatus(_ context.Context, id string, st model.ObservedStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.byID[id]
	if !ok {
		return store.ErrNotFound
	}
	i.ObservedGeneration, i.ObservedEtag = st.AppliedGeneration, st.Etag
	i.ObservedUnsupported, i.ObservedErrors = st.Unsupported, st.Errors
	i.ReplaySetID, i.ReplaySetState = "", "" // a report without replay_set holds none
	if st.ReplaySet != nil {
		i.ReplaySetID, i.ReplaySetState = st.ReplaySet.ID, st.ReplaySet.State
	}
	return nil
}

// Desired-state fixtures for "cartservice": an inline rule, a network rule, an
// active inline fault config, an active resource fault config, and a running
// replay phase (synthesized cache-box rule).
type fakeDesiredRules struct{ rules []*model.Rule }

func (f *fakeDesiredRules) ForService(_ context.Context, svc string) ([]*model.Rule, error) {
	var out []*model.Rule
	for _, r := range f.rules {
		if r.Service == svc {
			out = append(out, r)
		}
	}
	return out, nil
}

type fakeDesiredConfigs struct{ configs []*model.FaultConfig }

func (f *fakeDesiredConfigs) ListActiveForService(_ context.Context, svc string) ([]*model.FaultConfig, error) {
	var out []*model.FaultConfig
	for _, c := range f.configs {
		if c.Service == svc {
			out = append(out, c)
		}
	}
	return out, nil
}

type fakeDesiredCacheBox struct {
	byService map[string]*model.CacheBoxRuleContext
}

func (f *fakeDesiredCacheBox) ActiveCacheBoxPhaseForService(_ context.Context, svc string) (*model.CacheBoxRuleContext, error) {
	return f.byService[svc], nil
}

type desiredFixture struct {
	gens  *desired.Generations
	mat   *desired.Materializer
	reg   *fakeRegistry
	rules *fakeDesiredRules
}

func newDesiredFixture(insts ...*model.SDKInstance) *desiredFixture {
	specs := newFakeFaultRepo() // fault_handler_test.go
	specs.specs["spec-inline"] = &model.FaultSpec{ID: "spec-inline", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"100ms"}`)}
	specs.specs["spec-net"] = &model.FaultSpec{ID: "spec-net", Category: "network", FaultType: "blackhole", Network: &model.NetworkEnvelope{Target: "redis", Direction: "upstream"}}
	mk := func(id, spec string) *model.Rule {
		return &model.Rule{ID: id, Name: id, Service: "cartservice", Enabled: true, Mode: "background", Priority: 10,
			Action: model.RuleAction{Type: "fault_spec", FaultSpecID: spec}}
	}
	gens := desired.NewGenerations(desired.NewMemoryStore(), desired.NewBroker())
	rules := &fakeDesiredRules{rules: []*model.Rule{mk("r-inline", "spec-inline"), mk("r-net", "spec-net")}}
	mat := &desired.Materializer{
		Generations: gens, Rules: rules, Specs: specs,
		FaultConfigs: &fakeDesiredConfigs{configs: []*model.FaultConfig{
			{ID: "fc-inline", Service: "cartservice", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1s"}`)},
			{ID: "fc-cpu", Service: "cartservice", Category: "resource", FaultType: "cpu", Params: json.RawMessage(`{"target_load":0.5}`)},
		}},
		CacheBox: &fakeDesiredCacheBox{byService: map[string]*model.CacheBoxRuleContext{
			"cartservice": {ExperimentID: "exp-1", PhaseID: "phase-iso", Mode: "replay", KeyStrategy: "canonical_v2", StrategyVersion: 2},
		}},
	}
	return &desiredFixture{gens: gens, mat: mat, reg: newFakeRegistry(insts...), rules: rules}
}

func (fx *desiredFixture) server() *Server {
	return &Server{registry: fx.reg, sdkInstances: fx.reg, generations: fx.gens, materializer: fx.mat,
		broker: fx.gens.Broker(), logger: discardLogger(),
		// bumpGeneration still moves rule_version for the poll keyed on it
		// (TODO(desired-state Task 6)); the spy keeps that off a database.
		rulever: &spyRuleVersion{log: &bumpLog{}}}
}

// serveJSON drives a request through the real route table. body is sent as
// is when it is an io.Reader (e.g. malformed JSON), else JSON-encoded.
func serveJSON(t *testing.T, s *Server, method, path string, body any, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader = bytes.NewReader(nil)
	switch b := body.(type) {
	case nil:
	case io.Reader:
		rd = b
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rd)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	mux := http.NewServeMux()
	s.routes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func ruleNamesOf(t *testing.T, body []byte) []string {
	t.Helper()
	var sync RuleSyncWire
	if err := json.Unmarshal(body, &sync); err != nil {
		t.Fatalf("decode RuleSyncWire: %v (%s)", err, body)
	}
	out := []string{}
	for _, r := range sync.Rules {
		out = append(out, r.Name)
	}
	return out
}

func TestHandleRegister_RespondsWithMaterializedDocument(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	body := map[string]any{"id": "cart-1", "service": "cartservice", "address": "10.0.0.5:9090",
		"poll_interval_ms": 10000, "host": "sidecar", "capabilities": []string{"inline", "cachebox"}}
	w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register", body, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	var resp RegisterResponseWire
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "registered" || resp.Version != 1 || !strings.HasPrefix(resp.Etag, "1-") {
		t.Errorf("status/version/etag = %s/%d/%s", resp.Status, resp.Version, resp.Etag)
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline,cachebox:replay:phase-iso" {
		t.Errorf("rules = %v, want the inline rule and the cache-box rule only (network filtered)", got)
	}
	if len(resp.ActiveFaults) != 1 || resp.ActiveFaults[0].ID != "fc-inline" {
		t.Errorf("active_faults = %+v, want [fc-inline] (resource filtered)", resp.ActiveFaults)
	}
	stored, _ := fx.reg.Get(context.Background(), "cart-1")
	if stored.Host != "sidecar" || len(stored.Capabilities) != 2 {
		t.Errorf("persisted host/capabilities = %q/%v", stored.Host, stored.Capabilities)
	}
	if gen, _ := fx.gens.Get(context.Background(), "cartservice"); gen != 1 {
		t.Errorf("a fresh registration bumped the generation to %d", gen)
	}
}

func TestHandleRegister_CapabilityChangeBumps(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Capabilities: []string{"inline", "cachebox"}})
	s := fx.server()
	ctx := context.Background()
	register := func(caps []string) {
		t.Helper()
		w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register",
			map[string]any{"id": "cart-1", "service": "cartservice", "address": "a", "capabilities": caps}, nil)
		if w.Code != http.StatusCreated {
			t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
		}
	}
	register([]string{"cachebox", "inline"}) // same set, other order
	if gen, _ := fx.gens.Get(ctx, "cartservice"); gen != 1 {
		t.Fatalf("re-ordered capabilities bumped to %d", gen)
	}
	register([]string{"inline", "cachebox", "resource"})
	if gen, _ := fx.gens.Get(ctx, "cartservice"); gen != 2 {
		t.Fatalf("changed capabilities: generation = %d, want 2", gen)
	}
	register(nil) // absent = all: a change again
	if gen, _ := fx.gens.Get(ctx, "cartservice"); gen != 3 {
		t.Fatalf("absent capabilities: generation = %d, want 3", gen)
	}
}

// Review Focus 4: capabilities [] is the wire's "absent" (every category), so
// moving between [] and absent is no change and must not bump, and [] is
// served the unfiltered document.
func TestHandleRegister_EmptyCapabilitiesEqualAbsent(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"}) // registered without capabilities
	s := fx.server()
	for _, tc := range []struct {
		name string
		caps []string // nil: the key is left out of the body
	}{
		{"empty list", []string{}},
		{"absent", nil},
		{"empty list again", []string{}},
	} {
		body := map[string]any{"id": "cart-1", "service": "cartservice", "address": "a"}
		if tc.caps != nil {
			body["capabilities"] = tc.caps
		}
		w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register", body, nil)
		if w.Code != http.StatusCreated {
			t.Fatalf("%s: status = %d body = %s", tc.name, w.Code, w.Body.String())
		}
		if gen, _ := fx.gens.Get(context.Background(), "cartservice"); gen != 1 {
			t.Fatalf("%s: generation = %d, want 1 ([] and absent are the same set)", tc.name, gen)
		}
		if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline,r-net,cachebox:replay:phase-iso" {
			t.Errorf("%s: rules = %v, want all three (no capabilities = all)", tc.name, got)
		}
	}
}

// An unknown capability string (the sidecar may advertise "netem") is stored
// verbatim and counts as a change, but unlocks no category. The response
// already carries the bumped generation.
func TestHandleRegister_UnknownCapabilityCountsAsAChange(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Capabilities: []string{"inline"}})
	s := fx.server()
	ctx := context.Background()
	w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register",
		map[string]any{"id": "cart-1", "service": "cartservice", "address": "a", "capabilities": []string{"netem", "inline"}}, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if gen, _ := fx.gens.Get(ctx, "cartservice"); gen != 2 {
		t.Fatalf("generation = %d, want 2 (an added unknown string is a change)", gen)
	}
	var resp RegisterResponseWire
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Version != 2 || !strings.HasPrefix(resp.Etag, "2-") {
		t.Errorf("version/etag = %d/%s, want the bumped generation 2", resp.Version, resp.Etag)
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline" {
		t.Errorf("rules = %v, want [r-inline] (netem unlocks no category)", got)
	}
	stored, _ := fx.reg.Get(ctx, "cart-1")
	if !slices.Equal(stored.Capabilities, []string{"netem", "inline"}) {
		t.Errorf("stored capabilities = %v, want them verbatim", stored.Capabilities)
	}
}

type failingMaterializer struct{}

func (failingMaterializer) Materialize(context.Context, string, *model.SDKInstance) (desired.Document, error) {
	return desired.Document{}, errors.New("compile rules: spec missing")
}

func TestHandleRegister_MaterializeFailureIsDegraded201(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	s.materializer = failingMaterializer{}
	w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register",
		map[string]any{"id": "cart-1", "service": "cartservice", "address": "a"}, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (a broken rule must not crash-loop the pod); body = %s", w.Code, w.Body.String())
	}
	// No document: [] would be an authoritative clear, and a live instance
	// re-registering while polls answer 500 must keep the faults it has armed.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for k, v := range raw {
		got[k] = string(v)
	}
	want := map[string]string{"status": `"registered"`, "version": `0`, "etag": `""`, "rules": `null`, "active_faults": `null`}
	if !maps.Equal(got, want) {
		t.Errorf("degraded body = %s, want exactly %v (no freeze_cfg or replay_set)", w.Body.String(), want)
	}
	if _, err := fx.reg.Get(context.Background(), "cart-1"); err != nil {
		t.Error("registration must be persisted even when materialization fails")
	}
}

func TestHandleRegister_ValidationAndDBErrors(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	if w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register", map[string]any{"service": "cartservice"}, nil); w.Code != http.StatusBadRequest {
		t.Errorf("missing id: status = %d, want 400", w.Code)
	}
	if w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register", strings.NewReader("{"), nil); w.Code != http.StatusBadRequest ||
		!strings.Contains(w.Body.String(), "invalid JSON") {
		t.Errorf("bad json: status = %d body = %s, want 400 invalid JSON", w.Code, w.Body.String())
	}
	fx.reg.err = errors.New("db down")
	if w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register",
		map[string]any{"id": "cart-1", "service": "cartservice", "address": "a"}, nil); w.Code != http.StatusBadRequest ||
		!strings.Contains(w.Body.String(), "db down") {
		t.Errorf("registry failure: status = %d body = %s, want 400 with the registration error", w.Code, w.Body.String())
	}
}

func TestSameCapabilities(t *testing.T) {
	for _, tc := range []struct {
		a, b []string
		want bool
	}{
		{nil, nil, true},
		{nil, []string{}, true},
		{[]string{}, nil, true},
		{[]string{"inline", "cachebox"}, []string{"cachebox", "inline"}, true},
		{[]string{"inline", "inline"}, []string{"inline"}, true},
		{[]string{"inline"}, nil, false},
		{nil, []string{"inline"}, false},
		{[]string{"inline", "inline"}, []string{"inline", "cachebox"}, false},
		{[]string{"inline"}, []string{"inline", "netem"}, false},
	} {
		if got := sameCapabilities(tc.a, tc.b); got != tc.want {
			t.Errorf("sameCapabilities(%#v, %#v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// The register body is the contract's RuleSync plus status: every Document
// field reaches the wire under its contract name, replay_set carries the
// (experiment_id, phase_id) pair it is installed under, and freeze_cfg and
// replay_set are left out when none is desired.
func TestRegisterResponseWire_CarriesTheWholeDocument(t *testing.T) {
	doc := desired.Document{
		Generation:   7,
		ETag:         "7-0123456789abcdef",
		Rules:        []atroposdk.CompiledRule{{Name: "r-inline"}},
		ActiveFaults: []atroposdk.FaultRequest{{ID: "fc-inline"}},
		FreezeCfg:    &atroposdk.DelayRequest{Mu: 1.5, Sigma: 0.25},
		ReplaySet: &model.ReplaySetRef{ID: "sha256:aa", ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "canonical_v2",
			Count: 3, SHA256: "bb", SizeBytes: 42, URL: "/api/v1/cache/artifacts/sha256:aa"},
	}
	raw := marshalRawJSON(t, RegisterResponseWire{Status: "registered", RuleSyncWire: ruleSyncFrom(doc)})
	if got := slices.Sorted(maps.Keys(raw)); !slices.Equal(got, []string{"active_faults", "etag", "freeze_cfg", "replay_set", "rules", "status", "version"}) {
		t.Errorf("keys = %v", got)
	}
	for key, want := range map[string]string{
		"status":     `"registered"`,
		"version":    `7`,
		"etag":       `"7-0123456789abcdef"`,
		"replay_set": `{"id":"sha256:aa","experiment_id":"exp-1","phase_id":"phase-iso","key_strategy":"canonical_v2","count":3,"sha256":"bb","size_bytes":42,"url":"/api/v1/cache/artifacts/sha256:aa"}`,
	} {
		if string(raw[key]) != want {
			t.Errorf("%s = %s, want %s", key, raw[key], want)
		}
	}
	var rules []atroposdk.CompiledRule
	var faults []atroposdk.FaultRequest
	var freeze atroposdk.DelayRequest
	for key, v := range map[string]any{"rules": &rules, "active_faults": &faults, "freeze_cfg": &freeze} {
		if err := json.Unmarshal(raw[key], v); err != nil {
			t.Fatalf("decode %s: %v", key, err)
		}
	}
	if len(rules) != 1 || rules[0].Name != "r-inline" || len(faults) != 1 || faults[0].ID != "fc-inline" || freeze.Mu != 1.5 || freeze.Sigma != 0.25 {
		t.Errorf("rules/active_faults/freeze_cfg = %+v / %+v / %+v", rules, faults, freeze)
	}

	none := marshalRawJSON(t, ruleSyncFrom(desired.Document{Generation: 1, ETag: "1-0123456789abcdef",
		Rules: []atroposdk.CompiledRule{}, ActiveFaults: []atroposdk.FaultRequest{}}))
	if got := slices.Sorted(maps.Keys(none)); !slices.Equal(got, []string{"active_faults", "etag", "rules", "version"}) {
		t.Errorf("keys with no freeze config or replay set = %v, want both left out", got)
	}
}

func marshalRawJSON(t *testing.T, v any) map[string]json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
