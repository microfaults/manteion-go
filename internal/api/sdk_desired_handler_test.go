package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/atropos"
	"manteion-go/internal/atropostest"
	"manteion-go/internal/desired"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// fakeRegistry is the DB-free sdk_instances table for the desired-state
// handlers: Register upserts, RecordPoll/RecordStatus mutate the row in place.
// It mirrors store.SDKRepo's contract: Register validates, stores an empty
// capability list as absent (NULL) and clears the observed state; a poll sets
// the observed generation; a status report is a full snapshot that only
// raises it. Timestamps are not modelled.
type fakeRegistry struct {
	mu    sync.Mutex
	byID  map[string]*model.SDKInstance
	polls []polled
	err   error // injected store failure: Get, Register and RecordStatus return it
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
	if f.err != nil {
		return f.err
	}
	i, ok := f.byID[id]
	if !ok {
		return store.ErrNotFound
	}
	i.ObservedGeneration, i.ObservedEtag = max(i.ObservedGeneration, st.AppliedGeneration), st.Etag
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
type fakeDesiredRules struct {
	mu    sync.Mutex // a test may change the rules while a held poll reads them
	rules []*model.Rule
}

func (f *fakeDesiredRules) ForService(_ context.Context, svc string) ([]*model.Rule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.Rule
	for _, r := range f.rules {
		if r.Service == svc {
			out = append(out, r)
		}
	}
	return out, nil
}

// keep deletes every rule but the first n.
func (f *fakeDesiredRules) keep(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = f.rules[:n]
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
		broker: fx.gens.Broker(), logger: discardLogger()}
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

// isContentHash reports whether etag is a document's ETag: the content hash
// alone, 16 lowercase hex digits with no generation prefix.
func isContentHash(etag string) bool {
	if len(etag) != 16 || strings.ToLower(etag) != etag {
		return false
	}
	_, err := hex.DecodeString(etag)
	return err == nil
}

// quotedContentHash reports whether an ETag header value is the quoted
// content hash.
func quotedContentHash(header string) bool {
	return len(header) == 18 && header[0] == '"' && header[17] == '"' && isContentHash(header[1:17])
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
	if resp.Status != "registered" || resp.Version != 1 || !isContentHash(resp.Etag) {
		t.Errorf("status/version/etag = %s/%d/%s, want registered/1/<content hash>", resp.Status, resp.Version, resp.Etag)
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
	if resp.Version != 2 || !isContentHash(resp.Etag) {
		t.Errorf("version/etag = %d/%s, want the bumped generation 2 and a content-hash ETag", resp.Version, resp.Etag)
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
		ETag:         "0123456789abcdef",
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
		"etag":       `"0123456789abcdef"`,
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

	none := marshalRawJSON(t, ruleSyncFrom(desired.Document{Generation: 1, ETag: "0123456789abcdef",
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

func pollWith(t *testing.T, s *Server, query string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	return serveJSON(t, s, http.MethodGet, "/api/v1/sdk/rules?"+query, nil, hdr)
}

func TestHandlePollRules_200CarriesEtagAndGeneration(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Capabilities: []string{"inline"}})
	s := fx.server()
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=0", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	etag := w.Header().Get("ETag")
	if !quotedContentHash(etag) {
		t.Errorf("ETag = %q, want the quoted content hash (no generation prefix)", etag)
	}
	if got := w.Header().Get("X-Atropos-Generation"); got != "1" {
		t.Errorf("X-Atropos-Generation = %q, want 1", got)
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline" {
		t.Errorf("rules = %v, want [r-inline] (network and cache-box filtered for an inline-only host)", got)
	}
	var sync RuleSyncWire
	_ = json.Unmarshal(w.Body.Bytes(), &sync)
	if `"`+sync.Etag+`"` != etag || sync.Version != 1 {
		t.Errorf("body etag/version = %s/%d, header = %s", sync.Etag, sync.Version, etag)
	}
	if len(fx.reg.polls) != 1 || fx.reg.polls[0] != (polled{"cart-1", 0}) {
		t.Errorf("observed generation not recorded before answering: %+v", fx.reg.polls)
	}
}

func TestHandlePollRules_304OnMatchingEtag(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	first := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil)
	etag := first.Header().Get("ETag")
	for _, inm := range []string{etag, "W/" + etag, strings.Trim(etag, `"`)} {
		w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1", map[string]string{"If-None-Match": inm})
		if w.Code != http.StatusNotModified {
			t.Errorf("If-None-Match %s: status = %d, want 304", inm, w.Code)
		}
		if w.Header().Get("ETag") != etag || w.Header().Get("X-Atropos-Generation") != "1" {
			t.Errorf("304 headers = %v", w.Header())
		}
		if w.Body.Len() != 0 {
			t.Errorf("304 carried a body: %s", w.Body.String())
		}
	}
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1", map[string]string{"If-None-Match": `"garbage"`})
	if w.Code != http.StatusOK {
		t.Errorf("foreign ETag: status = %d, want 200", w.Code)
	}
}

func TestHandlePollRules_LongPollWakesOnBump(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")

	go func() {
		time.Sleep(100 * time.Millisecond)
		fx.rules.keep(1)                                         // the network rule is deleted...
		_, _ = fx.gens.Bump(context.Background(), "cartservice") // ...and the repo bumped
	}()
	start := time.Now()
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait=30", map[string]string{"If-None-Match": etag})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after the bump; body = %s", w.Code, w.Body.String())
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("long-poll took %v; it must return on the bump, not on wait", time.Since(start))
	}
	if got := w.Header().Get("ETag"); w.Header().Get("X-Atropos-Generation") != "2" || got == etag || !quotedContentHash(got) {
		t.Errorf("headers after bump = %v, want generation 2 and the new document's ETag (was %s)", w.Header(), etag)
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline,cachebox:replay:phase-iso" {
		t.Errorf("rules = %v, want the post-bump set", got)
	}
}

func TestHandlePollRules_LongPollTimesOutWith304(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
	start := time.Now()
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait=1", map[string]string{"If-None-Match": etag})
	if w.Code != http.StatusNotModified || time.Since(start) < time.Second {
		t.Fatalf("status = %d after %v, want 304 after ~1s", w.Code, time.Since(start))
	}
	if w.Header().Get("X-Atropos-Generation") != "1" {
		t.Errorf("X-Atropos-Generation = %q", w.Header().Get("X-Atropos-Generation"))
	}
}

// A generation move that leaves this instance's document unchanged keeps its
// ETag, the content hash alone: another instance's capabilities changed, or a
// bump landed between this instance's polls or while its document was being
// materialized. Its next poll names the current ETag at a version behind the
// generation and is answered at once: a 304 carrying the generation for the
// instance to adopt. Held on the matching ETag alone, it would sit out the
// whole wait with its observed generation stuck at the old one, which fails a
// convergence barrier that waits no longer than the SDK's poll.
func TestHandlePollRules_GenerationMovesButDocumentUnchanged(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
	_, _ = fx.gens.Bump(context.Background(), "cartservice")
	start := time.Now()
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait=30", map[string]string{"If-None-Match": etag})
	if elapsed := time.Since(start); w.Code != http.StatusNotModified || elapsed > 5*time.Second {
		t.Fatalf("status = %d after %v, want 304 at once (version 1 behind generation 2, document unchanged)", w.Code, elapsed)
	}
	if got := w.Header().Get("X-Atropos-Generation"); got != "2" {
		t.Errorf("X-Atropos-Generation = %q, want 2 for the instance to adopt", got)
	}
	if got := w.Header().Get("ETag"); got != etag {
		t.Errorf("ETag = %s, want %s: the bump left the document unchanged", got, etag)
	}
	if w.Body.Len() != 0 {
		t.Errorf("304 carried a body: %s", w.Body.String())
	}
}

// The other side of the hold condition: a poll naming the current document at
// the current generation is held. The next bump releases it (a 304 carrying
// the bumped generation when the bump left the document unchanged), and so
// does its wait elapsing (a 304 at the generation it holds).
func TestHandlePollRules_CurrentVersionWithMatchingEtagIsHeld(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bumpAt  time.Duration // 0: nothing bumps
		wait    string
		heldFor time.Duration // the answer comes no sooner
		wantGen string
	}{
		{name: "released by a bump", bumpAt: 300 * time.Millisecond, wait: "30", heldFor: 300 * time.Millisecond, wantGen: "2"},
		{name: "released by the wait elapsing", wait: "1", heldFor: time.Second, wantGen: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
			s := fx.server()
			etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
			start := time.Now()
			if tc.bumpAt > 0 {
				go func() {
					time.Sleep(tc.bumpAt)
					_, _ = fx.gens.Bump(context.Background(), "cartservice") // the document does not change
				}()
			}
			w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait="+tc.wait, map[string]string{"If-None-Match": etag})
			if elapsed := time.Since(start); w.Code != http.StatusNotModified || elapsed < tc.heldFor || elapsed > 5*time.Second {
				t.Fatalf("status = %d after %v, want 304 held for %v or more", w.Code, elapsed, tc.heldFor)
			}
			if got := w.Header().Get("X-Atropos-Generation"); got != tc.wantGen {
				t.Errorf("X-Atropos-Generation = %q, want %s", got, tc.wantGen)
			}
			if got := w.Header().Get("ETag"); got != etag {
				t.Errorf("ETag = %s, want %s (the document did not change)", got, etag)
			}
		})
	}
}

func TestHandlePollRules_UnknownInstanceStillServed(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	w := pollWith(t, s, "service=cartservice&instance_id=gone-1&version=0", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for an unknown instance (unfiltered document)", w.Code)
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); len(got) != 3 {
		t.Errorf("rules = %v, want all 3 (no capability filter without a registration)", got)
	}
}

func TestHandlePollRules_Validation(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	if w := pollWith(t, s, "instance_id=cart-1", nil); w.Code != http.StatusBadRequest {
		t.Errorf("missing service: status = %d, want 400", w.Code)
	}
	if got := parseWait("61"); got != maxPollWait {
		t.Errorf("parseWait(61) = %v, want clamp to %v", got, maxPollWait)
	}
	if got := parseWait("-3"); got != 0 {
		t.Errorf("parseWait(-3) = %v, want 0", got)
	}
	if got := parseWait("abc"); got != 0 {
		t.Errorf("parseWait(abc) = %v, want 0", got)
	}
	s.materializer = failingMaterializer{}
	if w := pollWith(t, s, "service=cartservice", nil); w.Code != http.StatusInternalServerError {
		t.Errorf("materialize failure: status = %d, want 500", w.Code)
	}
}

// Review Focus 1: cmd/manteion sets WriteTimeout=30s; a held poll longer than
// that must extend its write deadline or its answer is written into a dead
// connection. A real server with a 200ms WriteTimeout and a 700ms hold proves it;
// the bump that ends the hold leaves the document unchanged, so the answer is a
// 304 carrying the bumped generation.
// The client must not reuse connections: Go's transport silently retries a GET
// that a reused connection dropped, and the retry, sent after the bump, would
// be answered at once.
func TestHandlePollRules_HoldSurvivesServerWriteTimeout(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	mux := http.NewServeMux()
	s.routes(mux)
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.WriteTimeout = 200 * time.Millisecond
	srv.Start()
	t.Cleanup(srv.Close)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	first, err := client.Get(srv.URL + "/api/v1/sdk/rules?service=cartservice&instance_id=cart-1")
	if err != nil {
		t.Fatal(err)
	}
	first.Body.Close()
	etag := first.Header.Get("ETag")

	go func() {
		time.Sleep(700 * time.Millisecond)
		_, _ = fx.gens.Bump(context.Background(), "cartservice")
	}()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/sdk/rules?service=cartservice&instance_id=cart-1&version=1&wait=5", nil)
	req.Header.Set("If-None-Match", etag)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("held poll failed (write deadline not extended?): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotModified || resp.Header.Get("X-Atropos-Generation") != "2" {
		t.Fatalf("status = %d gen = %s, want 304 / 2 (the bump left the document unchanged)", resp.StatusCode, resp.Header.Get("X-Atropos-Generation"))
	}
}

// wait is whole seconds clamped to [0, maxPollWait], the clamp applied before
// the conversion so a huge value cannot overflow into a negative or short
// hold; anything unparseable is no hold at all.
func TestParseWait(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"": 0, "0": 0, "-3": 0, "abc": 0, "1.5": 0,
		"1": time.Second, "30": 30 * time.Second, "60": maxPollWait, "61": maxPollWait,
		"9999999999": maxPollWait, // 9999999999 s overflows time.Duration
	} {
		if got := parseWait(in); got != want {
			t.Errorf("parseWait(%q) = %v, want %v", in, got, want)
		}
	}
}

// Review Focus 3: If-None-Match names the document quoted, unquoted or weak
// (W/), alone or in a list; anything else (a foreign or garbled value, the
// wildcard, an empty header, the generation-prefixed form ETags had before
// the content hash alone was the ETag) is a mismatch, answered 200 and never
// held.
func TestEtagMatches(t *testing.T) {
	const etag = "0123456789abcdef"
	for header, want := range map[string]bool{
		`"0123456789abcdef"`:                       true,
		`W/"0123456789abcdef"`:                     true,
		`0123456789abcdef`:                         true,
		`"fedcba9876543210", W/"0123456789abcdef"`: true,
		`"fedcba9876543210"`:                       false,
		`"0123456789abcdee"`:                       false, // other content
		`"3-0123456789abcdef"`:                     false, // generation-prefixed
		`"garbage"`:                                false,
		`*`:                                        false,
		``:                                         false,
	} {
		if got := etagMatches(header, etag); got != want {
			t.Errorf("etagMatches(%q) = %v, want %v", header, got, want)
		}
	}
	if etagMatches(`""`, "") {
		t.Error("an empty ETag matched")
	}
}

// version is the generation the instance holds, recorded with the poll. Absent
// or unparseable reads as 0 and is still recorded (a bad version never costs
// the poll its last_poll_at). So does a value sdk_instances' BIGINT cannot hold
// (the write would fail): an SDK still holding a pre-long-poll version, which
// can exceed MaxInt64, must be served the document, never stranded on a 400.
func TestHandlePollRules_ObservedVersion(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	for _, tc := range []struct {
		query string
		want  uint64
	}{
		{"", 0},
		{"&version=", 0},
		{"&version=abc", 0},
		{"&version=-1", 0},
		{"&version=7", 7},
		{"&version=9223372036854775807", math.MaxInt64},
		{"&version=9223372036854775808", 0},  // MaxInt64+1
		{"&version=18446744073709551615", 0}, // MaxUint64
		{"&version=99999999999999999999", 0}, // overflows uint64
	} {
		fx.reg.polls = nil
		w := pollWith(t, s, "service=cartservice&instance_id=cart-1"+tc.query, nil)
		if w.Code != http.StatusOK {
			t.Errorf("%q: status = %d body = %s, want 200", tc.query, w.Code, w.Body.String())
		} else if got := ruleNamesOf(t, w.Body.Bytes()); len(got) != 3 {
			t.Errorf("%q: rules = %v, want the whole document", tc.query, got)
		}
		if want := []polled{{"cart-1", tc.want}}; !slices.Equal(fx.reg.polls, want) {
			t.Errorf("%q: recorded %+v, want %+v", tc.query, fx.reg.polls, want)
		}
	}
}

// Review Focus 2, the log half: the observed-state write a purged registration
// fails is only a warning. A registry that cannot be read is another matter:
// the instance's capability filter is unknown, so the poll answers 500 rather
// than a document that is not the instance's.
func TestHandlePollRules_RegistryFailures(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Capabilities: []string{"inline"}})
	s := fx.server()
	var logs bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logs, nil))

	if w := pollWith(t, s, "service=cartservice&instance_id=gone-1&version=4", nil); w.Code != http.StatusOK {
		t.Fatalf("unknown instance: status = %d, want 200", w.Code)
	}
	if want := []polled{{"gone-1", 4}}; !slices.Equal(fx.reg.polls, want) {
		t.Errorf("recorded %+v, want the attempt %+v", fx.reg.polls, want)
	}
	if got := logs.String(); !strings.Contains(got, `level=WARN msg="record poll failed" instance_id=gone-1`) || strings.Contains(got, "level=ERROR") {
		t.Errorf("logs = %q, want only the record-poll warning", got)
	}

	fx.reg.err = errors.New("db down")
	if w := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil); w.Code != http.StatusInternalServerError {
		t.Errorf("registry read failure: status = %d, want 500", w.Code)
	}
}

// A held poll records the generation the instance holds before it parks (the
// orchestrator observes convergence from it while the poll is held), and
// re-reads the registration once the generation moves: the instance may have
// re-registered with other capabilities while parked (the change that bumped),
// and its document must be filtered by them.
func TestHandlePollRules_HeldPollRecordsFirstAndRereadsTheInstance(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Capabilities: []string{"inline"}})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
	recordedWhileHeld := make(chan bool, 1)
	go func() {
		recorded := false // the poll cannot be answered before the bump below
		for deadline := time.Now().Add(5 * time.Second); !recorded && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			fx.reg.mu.Lock()
			recorded = slices.Contains(fx.reg.polls, polled{"cart-1", 1})
			fx.reg.mu.Unlock()
		}
		recordedWhileHeld <- recorded
		time.Sleep(100 * time.Millisecond) // let it park
		fx.reg.mu.Lock()
		fx.reg.byID["cart-1"].Capabilities = nil // every category now...
		fx.reg.mu.Unlock()
		_, _ = fx.gens.Bump(context.Background(), "cartservice") // ...and the change bumped
	}()
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait=30", map[string]string{"If-None-Match": etag})
	if !<-recordedWhileHeld {
		t.Error("the held poll's observed generation was not recorded before the hold")
	}
	if w.Code != http.StatusOK || w.Header().Get("X-Atropos-Generation") != "2" {
		t.Fatalf("status = %d gen = %s, want 200 / 2", w.Code, w.Header().Get("X-Atropos-Generation"))
	}
	if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline,r-net,cachebox:replay:phase-iso" {
		t.Errorf("rules = %v, want the unfiltered set of the re-read registration", got)
	}
}

// Review Focus 3, the handler half: a poll is held only when it names the
// current document at the current generation; anything else is answered at
// once, whatever wait asks for. A foreign If-None-Match gets a 200, even at
// the current version. The instance's own ETag at any other version gets a
// 304 carrying the generation to adopt: behind the generation (a bump left its
// document unchanged), ahead of it (the database was reset; == rather than >=
// in the hold condition) or absent (read as 0).
func TestHandlePollRules_MismatchIsNeverHeld(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
	_, _ = fx.gens.Bump(context.Background(), "cartservice")
	for _, tc := range []struct {
		inm      string
		version  string
		wantCode int
	}{
		{`"garbage"`, "&version=2", http.StatusOK},
		{etag, "&version=1", http.StatusNotModified},
		{etag, "&version=7", http.StatusNotModified},
		{etag, "", http.StatusNotModified},
	} {
		start := time.Now()
		w := pollWith(t, s, "service=cartservice&instance_id=cart-1&wait=30"+tc.version, map[string]string{"If-None-Match": tc.inm})
		if w.Code != tc.wantCode || time.Since(start) > 5*time.Second {
			t.Errorf("If-None-Match %s%s: status = %d after %v, want %d at once", tc.inm, tc.version, w.Code, time.Since(start), tc.wantCode)
		}
		if got := w.Header().Get("X-Atropos-Generation"); got != "2" {
			t.Errorf("If-None-Match %s%s: X-Atropos-Generation = %q, want 2", tc.inm, tc.version, got)
		}
	}
}

// A poll whose client has gone (its context is done) writes nothing: not when
// its hold ends that way, and not when a step fails because of it either
// (SpecResolver reports a cancelled read as a missing spec, so the error alone
// says nothing).
func TestHandlePollRules_ClientGoneWritesNothing(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	var logs bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logs, nil))
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")

	gone, cancel := context.WithCancel(context.Background())
	cancel() // the client hung up
	pollGone := func() *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequestWithContext(gone, http.MethodGet, "/api/v1/sdk/rules?service=cartservice&instance_id=cart-1&version=1&wait=30", nil)
		req.Header.Set("If-None-Match", etag)
		mux := http.NewServeMux()
		s.routes(mux)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}

	start := time.Now()
	if w := pollGone(); w.Body.Len() != 0 || len(w.Header()) != 0 {
		t.Errorf("hold ended by the client: wrote headers %v body %q, want nothing", w.Header(), w.Body.String())
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("hold outlived its client: %v", time.Since(start))
	}
	s.materializer = failingMaterializer{}
	if w := pollGone(); w.Body.Len() != 0 || len(w.Header()) != 0 {
		t.Errorf("materialize failure after the client left: wrote headers %v body %q, want nothing", w.Header(), w.Body.String())
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a client hang-up was logged as an error: %q", logs.String())
	}
}

// Generations.Wait fails on store errors as well as on the client leaving; a
// store failure mid-hold is a 500, not silence.
func TestHandlePollRules_GenerationStoreFailureMidHoldIs500(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
	s.generations = desired.NewGenerations(failingGenerationStore{}, desired.NewBroker()) // the materializer still reads fx.gens
	w := pollWith(t, s, "service=cartservice&instance_id=cart-1&version=1&wait=30", map[string]string{"If-None-Match": etag})
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

// shutdownSpyStore is a held poll's generation store in the shutdown tests.
// Every Get announces itself on read (the poll's Wait has subscribed and is
// about to park) and, like a database driver, fails on a cancelled context.
// With block set, Get parks inside the read until its context is cancelled: a
// store read the shutdown cuts short.
type shutdownSpyStore struct {
	desired.Store
	read  chan struct{}
	block bool
}

func (s shutdownSpyStore) Get(ctx context.Context, service string) (uint64, error) {
	select {
	case s.read <- struct{}{}:
	default:
	}
	if s.block {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("read generation: %w", err)
	}
	return s.Store.Get(ctx, service)
}

// The server's shutdown budget (10 s in cmd/manteion) is shorter than a hold
// (up to 60 s). Once the server begins shutting down, a held poll ends as if
// its wait had elapsed: it re-materializes and answers 304 when the document
// is unchanged or 200 with the new one, never through pollFailed (a 500, or
// nothing at all), even when the shutdown cut a store read short. A poll that
// arrives after shutdown began is answered without holding, unless its client
// has gone: that is still pollFailed's silence.
func TestHandlePollRules_ShutdownEndsHoldLikeATimeout(t *testing.T) {
	for _, tc := range []struct {
		name          string
		block         bool // the hold is inside a store read when shutdown begins
		change        bool // the document changes, without a bump, during the hold
		shutdownFirst bool // shutdown began before the poll arrived
		clientGone    bool // ...and the poll's client hung up
		wantCode      int  // 0: nothing written
	}{
		{name: "held, document unchanged: 304", wantCode: http.StatusNotModified},
		{name: "held, document changed without a bump: 200", change: true, wantCode: http.StatusOK},
		{name: "store read cut short: 304, not 500", block: true, wantCode: http.StatusNotModified},
		{name: "arriving after shutdown began: answered at once", shutdownFirst: true, wantCode: http.StatusNotModified},
		{name: "client gone during shutdown: nothing written", shutdownFirst: true, clientGone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
			s := fx.server()
			var logs bytes.Buffer
			s.logger = slog.New(slog.NewTextHandler(&logs, nil))
			etag := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil).Header().Get("ETag")
			// The hold waits on its own generations (the materializer keeps
			// fx.gens); both stores read generation 1.
			read := make(chan struct{}, 1)
			s.generations = desired.NewGenerations(shutdownSpyStore{Store: desired.NewMemoryStore(), read: read, block: tc.block}, desired.NewBroker())
			if tc.shutdownFirst {
				s.BeginShutdown()
			}

			reqCtx, hangUp := context.WithCancel(context.Background())
			defer hangUp()
			if tc.clientGone {
				hangUp()
			}
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				req := httptest.NewRequestWithContext(reqCtx, http.MethodGet, "/api/v1/sdk/rules?service=cartservice&instance_id=cart-1&version=1&wait=30", nil)
				req.Header.Set("If-None-Match", etag)
				mux := http.NewServeMux()
				s.routes(mux)
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, req)
				done <- w
			}()
			if !tc.shutdownFirst {
				select {
				case <-read: // the poll's Wait has subscribed: parked, or about to be
				case w := <-done:
					t.Fatalf("poll answered %d before it was held", w.Code)
				case <-time.After(5 * time.Second):
					t.Fatal("the poll never reached its hold")
				}
				if tc.change {
					fx.rules.keep(1) // the network rule is gone, and nothing bumped
				}
				s.BeginShutdown()
			}
			var w *httptest.ResponseRecorder
			select {
			case w = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the poll was still held 5s after shutdown began (wait=30)")
			}

			if tc.wantCode == 0 {
				if w.Body.Len() != 0 || len(w.Header()) != 0 {
					t.Errorf("wrote headers %v body %q to a client that had gone, want nothing", w.Header(), w.Body.String())
				}
				return
			}
			if w.Code != tc.wantCode {
				t.Fatalf("status = %d body = %q, want %d", w.Code, w.Body.String(), tc.wantCode)
			}
			if got := w.Header().Get("X-Atropos-Generation"); got != "1" {
				t.Errorf("X-Atropos-Generation = %q, want 1", got)
			}
			switch got := w.Header().Get("ETag"); {
			case tc.wantCode == http.StatusNotModified && (got != etag || w.Body.Len() != 0):
				t.Errorf("304: ETag = %s (want %s), body = %q (want none)", got, etag, w.Body.String())
			case tc.wantCode == http.StatusOK && (got == etag || !quotedContentHash(got)):
				t.Errorf("200: ETag = %s, want the changed document's ETag (was %s)", got, etag)
			}
			if tc.wantCode == http.StatusOK {
				if got := ruleNamesOf(t, w.Body.Bytes()); strings.Join(got, ",") != "r-inline,cachebox:replay:phase-iso" {
					t.Errorf("rules = %v, want the re-materialized set", got)
				}
			}
			if strings.Contains(logs.String(), "level=ERROR") {
				t.Errorf("the shutdown went through pollFailed: %q", logs.String())
			}
		})
	}
}

// End to end with net/http, wired as cmd/manteion wires it: Shutdown runs the
// RegisterOnShutdown hook before waiting for connections to go idle, a held
// poll is answered 304 over the wire (Shutdown does not cancel the request, so
// the re-materialization runs), an SDK's SSE stream ends cleanly, and Shutdown
// returns within its budget instead of waiting out a 30 s hold.
func TestServer_ShutdownAnswersHeldPollsAndEndsStreams(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	read := make(chan struct{}, 1)
	s.generations = desired.NewGenerations(shutdownSpyStore{Store: desired.NewMemoryStore(), read: read}, fx.gens.Broker())
	mux := http.NewServeMux()
	s.routes(mux)
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.RegisterOnShutdown(s.BeginShutdown)
	srv.Start()
	t.Cleanup(srv.Close)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	first, err := client.Get(srv.URL + "/api/v1/sdk/rules?service=cartservice&instance_id=cart-1")
	if err != nil {
		t.Fatal(err)
	}
	first.Body.Close()
	etag := first.Header.Get("ETag")

	sse, err := client.Get(srv.URL + "/api/v1/sdk/events?service=cartservice")
	if err != nil {
		t.Fatal(err)
	}
	defer sse.Body.Close()
	streamEnded := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, sse.Body); streamEnded <- err }()
	for deadline := time.Now().Add(2 * time.Second); s.broker.ClientCount() == 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the SSE handler never subscribed")
		}
	}

	type answer struct {
		resp *http.Response
		err  error
	}
	held := make(chan answer, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/sdk/rules?service=cartservice&instance_id=cart-1&version=1&wait=30", nil)
		req.Header.Set("If-None-Match", etag)
		resp, err := client.Do(req)
		held <- answer{resp, err}
	}()
	select {
	case <-read: // the poll is held
	case <-time.After(5 * time.Second):
		t.Fatal("the poll never reached its hold")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Config.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v (a held poll or a stream kept it waiting)", err)
	}
	a := <-held
	if a.err != nil {
		t.Fatalf("held poll: %v", a.err)
	}
	a.resp.Body.Close()
	if a.resp.StatusCode != http.StatusNotModified || a.resp.Header.Get("ETag") != etag {
		t.Errorf("held poll: status = %d ETag = %s, want 304 %s", a.resp.StatusCode, a.resp.Header.Get("ETag"), etag)
	}
	select {
	case err := <-streamEnded:
		if err != nil {
			t.Errorf("SSE stream ended with %v, want a clean end", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("the SSE stream outlived Shutdown")
	}
}

// oneInstanceResolver is an atrocontrol fleet with a single instance.
type oneInstanceResolver struct{ inst *model.SDKInstance }

func (r oneInstanceResolver) ForService(_ context.Context, service string) ([]*model.SDKInstance, error) {
	if service != r.inst.Service {
		return nil, nil
	}
	return []*model.SDKInstance{r.inst}, nil
}

func (r oneInstanceResolver) ForInstance(_ context.Context, id string) (*model.SDKInstance, error) {
	if id != r.inst.ID {
		return nil, store.ErrNotFound
	}
	return r.inst, nil
}

// Plan 1 serves freeze_cfg from the controller's intent tracker. enterPhase
// freezes a frozen service and then pushes the phase's rules to it, and
// endPhase pushes nil rules before it thaws. Neither rule push may disturb
// the freeze config: the tracker keeps reporting it and the poll keeps serving
// it to the service's cache-box instance until thaw clears it. (A rule push
// that replaced the whole intent dropped it, and the SDK clears its delay
// source on a document without freeze_cfg.)
func TestHandlePollRules_RulePushKeepsTheFreezeConfig(t *testing.T) {
	ctx := context.Background()
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Capabilities: []string{"inline", "cachebox"}})
	sdk := &model.SDKInstance{ID: "cart-1", Service: "cartservice", Address: atropostest.NewServer(t).URL}
	ctrl := atrocontrol.New(atropos.NewClient(), oneInstanceResolver{sdk},
		atrocontrol.WithDefaultTimeout(5*time.Second), atrocontrol.WithLogger(discardLogger()))
	fx.mat.Freeze = ctrl.IntentReader()
	s := fx.server()

	freeze := atroposdk.DelayRequest{Mu: 1.5, Sigma: 0.25, Seed: 7, Context: &atroposdk.CacheBoxContext{
		ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "canonical_v2", StrategyVersion: 2}}
	want, _ := json.Marshal(freeze)
	rules := []atroposdk.CompiledRule{{Name: "phase-rule", InjectionPoint: "egress", Mode: "inline",
		CacheBox: &atroposdk.CompiledCacheBox{Mode: "replay", KeyStrategy: "exact"}}}
	ok := func(step string, res atrocontrol.FanoutResult, err error) {
		t.Helper()
		if err != nil || !res.AllSucceeded() {
			t.Fatalf("%s: err = %v, failed = %v", step, err, res.Failed)
		}
	}
	// freezeCfgs reports the tracker's freeze config and the polled document's
	// freeze_cfg, each as JSON ("null" when absent).
	freezeCfgs := func(step string) (tracked, served string) {
		t.Helper()
		cfg, err := ctrl.IntentReader().FreezeConfigForService(ctx, "cartservice")
		if err != nil {
			t.Fatalf("%s: FreezeConfigForService: %v", step, err)
		}
		raw, _ := json.Marshal(cfg)
		w := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: poll status = %d body = %s", step, w.Code, w.Body.String())
		}
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
			t.Fatalf("%s: decode: %v", step, err)
		}
		served = "null"
		if fc, ok := doc["freeze_cfg"]; ok {
			served = string(fc)
		}
		return string(raw), served
	}

	// enterPhase: freeze, then push the phase's rules.
	res, err := ctrl.FreezeService(ctx, "cartservice", freeze)
	ok("freeze", res, err)
	res, err = ctrl.PushRules(ctx, "cartservice", rules)
	ok("push the phase rules", res, err)
	if tracked, served := freezeCfgs("after the rule push"); tracked != string(want) || served != string(want) {
		t.Errorf("after the rule push: tracker %s, served %s; want both %s", tracked, served, want)
	}
	if intent, _ := ctrl.IntentReader().Get("cartservice"); intent == nil || len(intent.Rules) != 1 {
		t.Errorf("the rule push itself was not recorded: %+v", intent)
	}

	// endPhase: the nil rule push before thaw keeps it as well.
	res, err = ctrl.PushRules(ctx, "cartservice", nil)
	ok("clear the rules", res, err)
	if tracked, served := freezeCfgs("after the nil rule push"); tracked != string(want) || served != string(want) {
		t.Errorf("after the nil rule push: tracker %s, served %s; want both %s", tracked, served, want)
	}

	// Thaw is what clears it.
	res, err = ctrl.ClearService(ctx, "cartservice")
	ok("thaw", res, err)
	if tracked, served := freezeCfgs("after thaw"); tracked != "null" || served != "null" {
		t.Errorf("after thaw: tracker %s, served %s; want neither", tracked, served)
	}
}

// ---------------------------------------------------------------------------
// POST /api/v1/sdk/status (desired-state protocol v1 §2)
// ---------------------------------------------------------------------------

func postStatus(t *testing.T, s *Server, body any) *httptest.ResponseRecorder {
	t.Helper()
	return serveJSON(t, s, http.MethodPost, "/api/v1/sdk/status", body, nil)
}

func TestHandleSDKStatus(t *testing.T) {
	ctx := context.Background()
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	w := postStatus(t, s, StatusReport{InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 3, Etag: "3-abc",
		Unsupported: []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}}, Errors: []string{"rule x: decode: boom"},
		ReplaySet: &model.ReplaySetStatus{ID: "sha256:aa", State: "committed"}})
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d body = %s, want 204", w.Code, w.Body.String())
	}
	if w.Body.Len() != 0 {
		t.Errorf("204 carried a body: %s", w.Body.String())
	}
	got, _ := fx.reg.Get(ctx, "cart-1")
	if got.ObservedGeneration != 3 || got.ObservedEtag != "3-abc" || len(got.ObservedUnsupported) != 1 ||
		len(got.ObservedErrors) != 1 || got.ReplaySetID != "sha256:aa" || got.ReplaySetState != "committed" {
		t.Errorf("stored = %+v", got)
	}

	// A report is a full snapshot: empty lists clear, and an absent replay_set
	// means the instance holds none, which clears the stored id and state.
	if w := postStatus(t, s, StatusReport{InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 4}); w.Code != http.StatusNoContent {
		t.Errorf("minimal report: status = %d", w.Code)
	}
	got, _ = fx.reg.Get(ctx, "cart-1")
	if got.ObservedGeneration != 4 || got.ObservedEtag != "" || len(got.ObservedUnsupported) != 0 || len(got.ObservedErrors) != 0 ||
		got.ReplaySetID != "" || got.ReplaySetState != "" {
		t.Errorf("after minimal report: %+v (empty lists clear, absent replay_set clears the id and state)", got)
	}

	w = postStatus(t, s, StatusReport{InstanceID: "nope", Service: "cartservice"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown instance: status = %d, want 404", w.Code)
	}
	assertErrorBody(t, w, "instance not found")
}

// Every refusal is decided before the store is touched, so the stored report
// survives it. Each case changes one thing in a report the store would take,
// and the report differs from the stored one in every column, so a 400 is that
// thing's and a write would show.
func TestHandleSDKStatus_Validation(t *testing.T) {
	ctx := context.Background()
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	if w := postStatus(t, s, StatusReport{InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 5, Etag: "5-abc",
		Unsupported: []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}}, Errors: []string{"first"},
		ReplaySet: &model.ReplaySetStatus{ID: "sha256:aa", State: "fetching"}}); w.Code != http.StatusNoContent {
		t.Fatalf("seed report: status = %d body = %s", w.Code, w.Body.String())
	}
	stored, _ := fx.reg.Get(ctx, "cart-1")

	report := func(edit func(*StatusReport)) StatusReport {
		r := StatusReport{InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 6, Etag: "6-def",
			Unsupported: []model.UnsupportedRule{}, Errors: []string{"second"},
			ReplaySet: &model.ReplaySetStatus{ID: "sha256:bb", State: "committed"}}
		edit(&r)
		return r
	}
	for _, tc := range []struct {
		name string
		body any
		want string // the error message starts with it
	}{
		{"malformed JSON", strings.NewReader("{"), "invalid JSON: "},
		{"negative applied_generation", strings.NewReader(`{"instance_id":"cart-1","service":"cartservice","applied_generation":-1}`), "invalid JSON: "},
		{"applied_generation beyond uint64", strings.NewReader(`{"instance_id":"cart-1","service":"cartservice","applied_generation":99999999999999999999}`), "invalid JSON: "},
		{"missing instance_id", report(func(r *StatusReport) { r.InstanceID = "" }), "instance_id required"},
		{"missing service", report(func(r *StatusReport) { r.Service = "" }), "service required"},
		// sdk_instances.observed_generation is a BIGINT: pgx cannot store more.
		{"applied_generation above MaxInt64", report(func(r *StatusReport) { r.AppliedGeneration = math.MaxInt64 + 1 }), "applied_generation out of range"},
		{"applied_generation MaxUint64", report(func(r *StatusReport) { r.AppliedGeneration = math.MaxUint64 }), "applied_generation out of range"},
		{"replay_set.state outside the vocabulary", report(func(r *StatusReport) { r.ReplaySet.State = "done" }), "replay_set.state must be fetching, committed or failed"},
		{"replay_set without a state", report(func(r *StatusReport) { r.ReplaySet = &model.ReplaySetStatus{ID: "sha256:bb"} }), "replay_set.state must be fetching, committed or failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := postStatus(t, s, tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body = %s, want 400", w.Code, w.Body.String())
			}
			var e ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || !strings.HasPrefix(e.Error, tc.want) {
				t.Errorf("body = %s, want an error starting %q", w.Body.String(), tc.want)
			}
			if got, _ := fx.reg.Get(ctx, "cart-1"); !reflect.DeepEqual(got, stored) {
				t.Errorf("a refused report changed the stored one: %+v, was %+v", got, stored)
			}
		})
	}

	// The BIGINT's own maximum is stored.
	if w := postStatus(t, s, report(func(r *StatusReport) { r.AppliedGeneration = math.MaxInt64 })); w.Code != http.StatusNoContent {
		t.Fatalf("applied_generation MaxInt64: status = %d body = %s, want 204", w.Code, w.Body.String())
	}
	if got, _ := fx.reg.Get(ctx, "cart-1"); got.ObservedGeneration != math.MaxInt64 {
		t.Errorf("stored generation = %d, want MaxInt64", got.ObservedGeneration)
	}
}

// The SDK always sends both lists, but a body that leaves them out or sends
// null is a report with nothing unsupported and no errors, and clears the
// previous ones just the same.
func TestHandleSDKStatus_AbsentListsClear(t *testing.T) {
	ctx := context.Background()
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	const head = `{"instance_id":"cart-1","service":"cartservice","applied_generation":2,"etag":"2-x"`
	for _, tc := range []struct{ name, body string }{
		{"keys left out", head + `}`},
		{"null", head + `,"unsupported":null,"errors":null}`},
		{"empty lists", head + `,"unsupported":[],"errors":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := postStatus(t, s, StatusReport{InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 1, Etag: "1-a",
				Unsupported: []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}}, Errors: []string{"boom"}}); w.Code != http.StatusNoContent {
				t.Fatalf("seed report: status = %d body = %s", w.Code, w.Body.String())
			}
			if w := postStatus(t, s, strings.NewReader(tc.body)); w.Code != http.StatusNoContent {
				t.Fatalf("status = %d body = %s, want 204", w.Code, w.Body.String())
			}
			got, _ := fx.reg.Get(ctx, "cart-1")
			if got.ObservedGeneration != 2 || got.ObservedEtag != "2-x" || len(got.ObservedUnsupported) != 0 || len(got.ObservedErrors) != 0 {
				t.Errorf("stored = %+v, want generation 2 with both lists cleared", got)
			}
		})
	}
}

// The body an SDK instance posts is its own StatusReport, encoded: the handler
// must take exactly that, whatever the manteion-local type's tags say.
func TestHandleSDKStatus_AcceptsTheSDKsReport(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	body, err := json.Marshal(atroposdk.StatusReport{
		InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 7, Etag: "7-abc",
		Unsupported: []atroposdk.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}},
		Errors:      []string{"rule x: decode: boom"},
		ReplaySet:   &atroposdk.ReplaySetStatus{ID: "sha256:aa", State: "fetching"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if w := postStatus(t, s, bytes.NewReader(body)); w.Code != http.StatusNoContent {
		t.Fatalf("status = %d body = %s, want 204 for %s", w.Code, w.Body.String(), body)
	}
	got, _ := fx.reg.Get(context.Background(), "cart-1")
	if got.ObservedGeneration != 7 || got.ObservedEtag != "7-abc" ||
		!slices.Equal(got.ObservedUnsupported, []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}}) ||
		!slices.Equal(got.ObservedErrors, []string{"rule x: decode: boom"}) ||
		got.ReplaySetID != "sha256:aa" || got.ReplaySetState != "fetching" {
		t.Errorf("stored = %+v from %s", got, body)
	}
}

// A store failure that is not a missing row is a 500 that keeps its cause out
// of the response and in the log.
func TestHandleSDKStatus_StoreFailureIs500(t *testing.T) {
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
	s := fx.server()
	var logs bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logs, nil))
	fx.reg.err = errors.New("db down")
	w := postStatus(t, s, StatusReport{InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 1})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body = %s, want 500", w.Code, w.Body.String())
	}
	assertErrorBody(t, w, "failed to record status")
	if got := logs.String(); !strings.Contains(got, `level=ERROR msg="sdk status: record failed" instance_id=cart-1`) || !strings.Contains(got, "db down") {
		t.Errorf("logs = %q, want the failure and its cause logged against the instance", got)
	}
}

// Against Postgres (MANTEION_TEST_DB=1): reports go through the real store and
// come back out of the instance detail, each replacing the previous one.
func TestHandleSDKStatus_DB(t *testing.T) {
	e := newGenEnv(t)
	inst := &model.SDKInstance{ID: "inst-" + e.tag, Service: "cart-" + e.tag, Version: "1.0.0", Address: "10.0.0.1:9090", PollIntervalMs: 10000}
	if err := e.s.sdk.Register(e.ctx, inst); err != nil {
		t.Fatalf("register instance: %v", err)
	}
	t.Cleanup(func() { _ = e.s.sdk.Deregister(context.Background(), inst.ID) })

	report := func(edit func(*StatusReport)) *httptest.ResponseRecorder {
		t.Helper()
		r := StatusReport{InstanceID: inst.ID, Service: inst.Service}
		edit(&r)
		return e.do(http.MethodPost, "/api/v1/sdk/status", r)
	}
	// detail reads the row back the way GET /sdk/instances/{id} serves it, with
	// the JSON keys it serves the observed state under.
	detail := func() (*model.SDKInstance, map[string]json.RawMessage) {
		t.Helper()
		w := e.do(http.MethodGet, "/api/v1/sdk/instances/"+inst.ID, nil)
		if w.Code != http.StatusOK {
			t.Fatalf("instance detail: status = %d body = %s", w.Code, w.Body.String())
		}
		var d SDKInstanceDetail
		if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
			t.Fatalf("decode instance detail: %v", err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
			t.Fatalf("decode instance detail keys: %v", err)
		}
		return d.SDKInstance, raw
	}
	unsupported := []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}}

	if w := report(func(r *StatusReport) {
		r.AppliedGeneration, r.Etag, r.Unsupported, r.Errors = 3, "3-abc", unsupported, []string{"rule x: decode: boom"}
		r.ReplaySet = &model.ReplaySetStatus{ID: "sha256:aa", State: "fetching"}
	}); w.Code != http.StatusNoContent {
		t.Fatalf("full report: status = %d body = %s, want 204", w.Code, w.Body.String())
	}
	got, keys := detail()
	if got.ObservedGeneration != 3 || got.ObservedEtag != "3-abc" || !slices.Equal(got.ObservedUnsupported, unsupported) ||
		!slices.Equal(got.ObservedErrors, []string{"rule x: decode: boom"}) ||
		got.ReplaySetID != "sha256:aa" || got.ReplaySetState != "fetching" || got.ObservedAt == nil {
		t.Errorf("after the full report: %+v", got)
	}
	for _, key := range []string{"observed_generation", "observed_etag", "observed_unsupported", "observed_errors", "replay_set_id", "replay_set_state", "observed_at"} {
		if _, ok := keys[key]; !ok {
			t.Errorf("instance detail lacks %q", key)
		}
	}

	// The replay set moves on: its state is overwritten, the rest repeated.
	if w := report(func(r *StatusReport) {
		r.AppliedGeneration, r.Etag, r.Unsupported, r.Errors = 3, "3-abc", unsupported, []string{}
		r.ReplaySet = &model.ReplaySetStatus{ID: "sha256:aa", State: "committed"}
	}); w.Code != http.StatusNoContent {
		t.Fatalf("committed report: status = %d body = %s", w.Code, w.Body.String())
	}
	if got, _ := detail(); got.ReplaySetState != "committed" || len(got.ObservedErrors) != 0 {
		t.Errorf("after the committed report: %+v, want the state overwritten and the errors cleared", got)
	}

	// null lists and no replay_set: a snapshot of an instance holding nothing.
	w := e.do(http.MethodPost, "/api/v1/sdk/status", json.RawMessage(
		`{"instance_id":"`+inst.ID+`","service":"`+inst.Service+`","applied_generation":4,"etag":"4-def","unsupported":null,"errors":null}`))
	if w.Code != http.StatusNoContent {
		t.Fatalf("null-lists report: status = %d body = %s", w.Code, w.Body.String())
	}
	if got, _ := detail(); got.ObservedGeneration != 4 || got.ObservedEtag != "4-def" || len(got.ObservedUnsupported) != 0 ||
		len(got.ObservedErrors) != 0 || got.ReplaySetID != "" || got.ReplaySetState != "" {
		t.Errorf("after the null-lists report: %+v, want nothing observed but generation 4", got)
	}

	// The BIGINT's maximum is stored; one more is refused before pgx sees it.
	if w := report(func(r *StatusReport) { r.AppliedGeneration = math.MaxInt64 }); w.Code != http.StatusNoContent {
		t.Fatalf("MaxInt64 report: status = %d body = %s", w.Code, w.Body.String())
	}
	if w := report(func(r *StatusReport) { r.AppliedGeneration = math.MaxInt64 + 1 }); w.Code != http.StatusBadRequest {
		t.Errorf("MaxInt64+1 report: status = %d body = %s, want 400", w.Code, w.Body.String())
	}
	if got, _ := detail(); got.ObservedGeneration != math.MaxInt64 {
		t.Errorf("generation = %d, want MaxInt64 kept", got.ObservedGeneration)
	}

	// A row that is not there is the store's ErrNotFound.
	if w := e.do(http.MethodPost, "/api/v1/sdk/status", StatusReport{InstanceID: "gone-" + e.tag, Service: inst.Service}); w.Code != http.StatusNotFound {
		t.Errorf("unknown instance: status = %d body = %s, want 404", w.Code, w.Body.String())
	}
}
