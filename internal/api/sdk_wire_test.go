package api

// Wire parity with the SDK (desired-state protocol v1). The bodies on the
// SDK-facing endpoints are the SDK's own atroposdk types, so these tests speak
// to the handlers with the SDK's encoder and decoder: whatever the SDK sends is
// what the handler must take, and whatever the handler answers is what the SDK
// decodes.

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"math"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
)

// unsetFields names the exported fields of the struct v (or what a pointer to
// it points at) that hold nothing: nil, an empty slice or map, or a zero value.
// The parity tests build fully populated values, so a field the SDK adds to
// its type that neither the fixture nor the handler fills is reported here
// instead of passing unnoticed.
func unsetFields(v any) []string {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	var unset []string
	for i := range rv.NumField() {
		field := rv.Type().Field(i)
		f := rv.Field(i)
		empty := f.IsZero() || ((f.Kind() == reflect.Slice || f.Kind() == reflect.Map) && f.Len() == 0)
		if field.IsExported() && empty {
			unset = append(unset, field.Name)
		}
	}
	return unset
}

func TestUnsetFields(t *testing.T) {
	type sample struct {
		Zero, Set                 int
		NilPtr, Ptr               *int
		NilSlice, Empty, Full     []string
		NilMap, EmptyMap, FullMap map[string]int
		hidden                    int // unexported: never reported
	}
	one := 1
	got := unsetFields(&sample{Set: 1, Ptr: &one, Empty: []string{}, Full: []string{"x"},
		EmptyMap: map[string]int{}, FullMap: map[string]int{"x": 1}})
	want := []string{"Zero", "NilPtr", "NilSlice", "Empty", "NilMap", "EmptyMap"}
	if !slices.Equal(got, want) {
		t.Errorf("unsetFields = %v, want %v", got, want)
	}
}

// decodeStrict decodes body into the SDK type v, refusing any key v does not
// know: the handler must not emit a field the SDK has no name for.
func decodeStrict(t *testing.T, body []byte, v any) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("the SDK's %T does not take the body: %v (%s)", v, err, body)
	}
}

// requireSameBytes fails unless the SDK's own encoding of the value it decoded
// is the handler's body, byte for byte (the handler writes one trailing
// newline, the encoder's way): nothing the handler wrote was lost on the way
// through the SDK type, and no key or tag differs from the SDK's.
func requireSameBytes(t *testing.T, body []byte, decoded any) {
	t.Helper()
	again, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.TrimSuffix(body, []byte("\n")); !bytes.Equal(got, again) {
		t.Errorf("the SDK re-encodes the body differently\n handler: %s\n SDK:     %s", got, again)
	}
}

// A fully populated document, polled and registered for, decodes as the SDK's
// own RuleSync with every field where the contract puts it, and the SDK
// encodes what it decoded to the handler's bytes. The instance is a sidecar
// with every category, running in a frozen phase that has an assigned artifact,
// so the document carries rules (inline, network, the synthesized cache-box
// rule), both active faults, freeze_cfg and replay_set.
func TestWireParity_DocumentDecodesAsTheSDKsRuleSync(t *testing.T) {
	caps := []string{"inline", "network", "resource", "cachebox"}
	fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice", Host: "sidecar", Capabilities: caps})
	mu, sigma := 1.5, 0.25
	frozen := &fakeFrozenPhase{
		phase: &model.ExperimentPhase{ID: "phase-iso", ExperimentID: "exp-1"},
		cfg: &model.CacheBoxConfig{Service: "cartservice", Mode: "replay_with_delay",
			SyntheticDelay: &model.SyntheticDelayConfig{FitMu: &mu, FitSigma: &sigma}},
		artifact: &model.ReplayArtifact{ID: "sha256:" + strings.Repeat("a", 64), EntryCount: 1234,
			W5SHA256: strings.Repeat("b", 64), SizeBytes: 5 << 20},
	}
	sources := &desired.PhaseSources{Phases: frozen, Artifacts: frozen}
	fx.mat.Freeze, fx.mat.ReplaySets = sources, sources
	s := fx.server()

	w := pollWith(t, s, "service=cartservice&instance_id=cart-1", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("poll: status = %d body = %s", w.Code, w.Body.String())
	}
	var doc atroposdk.RuleSync
	decodeStrict(t, w.Body.Bytes(), &doc)

	if doc.Version != 1 || !isContentHash(doc.Etag) || `"`+doc.Etag+`"` != w.Header().Get("ETag") {
		t.Errorf("version/etag = %d/%s, want generation 1 and the ETag header's content hash (header %s)",
			doc.Version, doc.Etag, w.Header().Get("ETag"))
	}
	var rules, faults []string
	for _, r := range doc.Rules {
		rules = append(rules, r.Name)
	}
	for _, f := range doc.ActiveFaults {
		faults = append(faults, f.ID)
	}
	if !slices.Equal(rules, []string{"r-inline", "r-net", "cachebox:replay:phase-iso"}) || !slices.Equal(faults, []string{"fc-inline", "fc-cpu"}) {
		t.Errorf("rules = %v, active_faults = %v", rules, faults)
	}
	wantFreeze := desired.FreezeDelayRequest(frozen.phase, *frozen.cfg)
	if !reflect.DeepEqual(doc.FreezeCfg, &wantFreeze) || doc.FreezeCfg.Mu != 1.5 || doc.FreezeCfg.Sigma != 0.25 ||
		doc.FreezeCfg.Context == nil || doc.FreezeCfg.Context.ExperimentID != "exp-1" || doc.FreezeCfg.Context.PhaseID != "phase-iso" {
		t.Errorf("freeze_cfg = %+v, want %+v", doc.FreezeCfg, wantFreeze)
	}
	wantSet := &atroposdk.ReplaySetRef{ID: frozen.artifact.ID, ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "canonical_v2",
		Count: 1234, SHA256: strings.Repeat("b", 64), SizeBytes: 5 << 20, URL: "/api/v1/cache/artifacts/" + frozen.artifact.ID}
	if !reflect.DeepEqual(doc.ReplaySet, wantSet) {
		t.Errorf("replay_set = %+v, want %+v", doc.ReplaySet, wantSet)
	}
	// Nothing the SDK's types carry is left unfilled by this document.
	if unset := unsetFields(doc); len(unset) > 0 {
		t.Errorf("RuleSync fields this document leaves unset: %v (extend the fixture or the handler)", unset)
	}
	if unset := unsetFields(doc.ReplaySet); len(unset) > 0 {
		t.Errorf("ReplaySetRef fields this document leaves unset: %v", unset)
	}
	requireSameBytes(t, w.Body.Bytes(), doc)

	// The register answer is the same document plus the status, as the SDK's
	// RegisterResponse (its RuleSync embedded). A fresh instance with the same
	// capabilities changes nobody's document: same generation, same ETag.
	reg := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register",
		map[string]any{"id": "cart-2", "service": "cartservice", "address": "10.0.0.6:9090", "host": "sidecar", "capabilities": caps}, nil)
	if reg.Code != http.StatusCreated {
		t.Fatalf("register: status = %d body = %s", reg.Code, reg.Body.String())
	}
	var resp atroposdk.RegisterResponse
	decodeStrict(t, reg.Body.Bytes(), &resp)
	if resp.Status != "registered" || !reflect.DeepEqual(resp.RuleSync, doc) {
		t.Errorf("register = %q + %+v, want registered + the polled document %+v", resp.Status, resp.RuleSync, doc)
	}
	requireSameBytes(t, reg.Body.Bytes(), resp)
}

// The register answer for a service whose desired state cannot be materialized
// is the contract's no-document body. The SDK decodes it as a RegisterResponse
// holding nothing (it applies nothing for a body with neither an etag nor
// rules), and the mirror the handler writes it with differs from the SDK's own
// encoding of that empty response in exactly one thing: the etag key, which the
// SDK tags omitempty and the contract (and the handler's other tests) keep.
func TestNoDocumentRegister_IsTheSDKsEmptyRegisterResponseWithTheEtagKey(t *testing.T) {
	fx := newDesiredFixture()
	s := fx.server()
	s.materializer = failingMaterializer{}
	w := serveJSON(t, s, http.MethodPost, "/api/v1/sdk/register",
		map[string]any{"id": "cart-1", "service": "cartservice", "address": "a"}, nil)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	const contractBody = `{"status":"registered","version":0,"etag":"","rules":null,"active_faults":null}`
	if got := strings.TrimSpace(w.Body.String()); got != contractBody {
		t.Errorf("body = %s, want %s", got, contractBody)
	}
	var resp atroposdk.RegisterResponse
	decodeStrict(t, w.Body.Bytes(), &resp)
	if !reflect.DeepEqual(resp, atroposdk.RegisterResponse{Status: "registered"}) {
		t.Errorf("the SDK decodes the no-document body as %+v, want the status and nothing else", resp)
	}

	ours := marshalRawJSON(t, noDocumentRegister{Status: "registered"})
	sdk := marshalRawJSON(t, atroposdk.RegisterResponse{Status: "registered"})
	sdk["etag"] = json.RawMessage(`""`) // present already once the SDK drops omitempty
	if !maps.EqualFunc(ours, sdk, func(a, b json.RawMessage) bool { return bytes.Equal(a, b) }) {
		t.Errorf("noDocumentRegister = %v, the SDK's empty RegisterResponse plus etag = %v: the mirror has drifted", ours, sdk)
	}
}

// statusSpy records the ObservedStatus the handler hands the store, ahead of the
// fake registry applying it.
type statusSpy struct {
	*fakeRegistry
	got []model.ObservedStatus
}

func (p *statusSpy) RecordStatus(ctx context.Context, id string, st model.ObservedStatus) error {
	p.got = append(p.got, st)
	return p.fakeRegistry.RecordStatus(ctx, id, st)
}

// The SDK's StatusReport, encoded by the SDK's own marshaller in each of the
// replay-set states it reports, goes through the status handler and reaches the
// store field by field: the generation and ETag, both unsupported rules, both
// errors, and the replay set's id and state.
func TestWireParity_SDKStatusReportReachesTheStore(t *testing.T) {
	for _, state := range []string{"fetching", "committed", "failed"} {
		t.Run(state, func(t *testing.T) {
			fx := newDesiredFixture(&model.SDKInstance{ID: "cart-1", Service: "cartservice"})
			spy := &statusSpy{fakeRegistry: fx.reg}
			s := fx.server()
			s.registry = spy

			report := atroposdk.StatusReport{
				InstanceID: "cart-1", Service: "cartservice", AppliedGeneration: 41, Etag: "0123456789abcdef",
				Unsupported: []atroposdk.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}, {Rule: "cb-1", Category: "composition"}},
				Errors:      []string{"rule a: decode: boom", "replay set sha256:aa: digest mismatch"},
				ReplaySet:   &atroposdk.ReplaySetStatus{ID: "sha256:aa", State: state},
			}
			// The report sets every field the SDK's types have.
			for name, v := range map[string]any{"StatusReport": report, "UnsupportedRule": report.Unsupported[0], "ReplaySetStatus": report.ReplaySet} {
				if unset := unsetFields(v); len(unset) > 0 {
					t.Fatalf("%s fields this report leaves unset: %v (extend the report, and the handler for them)", name, unset)
				}
			}
			body, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			w := postStatus(t, s, bytes.NewReader(body))
			if w.Code != http.StatusNoContent {
				t.Fatalf("status = %d body = %s, want 204 for %s", w.Code, w.Body.String(), body)
			}

			want := model.ObservedStatus{
				AppliedGeneration: 41, Etag: "0123456789abcdef",
				Unsupported: []model.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}, {Rule: "cb-1", Category: "composition"}},
				Errors:      []string{"rule a: decode: boom", "replay set sha256:aa: digest mismatch"},
				ReplaySet:   &model.ReplaySetStatus{ID: "sha256:aa", State: state},
			}
			if len(spy.got) != 1 || !reflect.DeepEqual(spy.got[0], want) {
				t.Errorf("the store received %+v, want exactly one %+v", spy.got, want)
			}
			stored, _ := fx.reg.Get(context.Background(), "cart-1")
			if stored.ObservedGeneration != 41 || stored.ObservedEtag != "0123456789abcdef" ||
				!slices.Equal(stored.ObservedUnsupported, want.Unsupported) || !slices.Equal(stored.ObservedErrors, want.Errors) ||
				stored.ReplaySetID != "sha256:aa" || stored.ReplaySetState != state {
				t.Errorf("stored = %+v, want generation 41, the ETag, both lists and replay set sha256:aa %s", stored, state)
			}
		})
	}
}

// ruleSyncFrom returns the SDK's own RuleSync: manteion's document field for
// field, the replay set converted from the stored ref.
func TestRuleSyncFrom_IsTheSDKsRuleSync(t *testing.T) {
	doc := desired.Document{
		Generation:   7,
		ETag:         "0123456789abcdef",
		Rules:        []atroposdk.CompiledRule{{Name: "r-inline"}},
		ActiveFaults: []atroposdk.FaultRequest{{ID: "fc-inline"}},
		FreezeCfg:    &atroposdk.DelayRequest{Mu: 1.5, Sigma: 0.25},
		ReplaySet: &model.ReplaySetRef{ID: "sha256:aa", ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "canonical_v2",
			Count: 3, SHA256: "bb", SizeBytes: 42, URL: "/api/v1/cache/artifacts/sha256:aa"},
	}
	got, err := ruleSyncFrom(doc)
	if err != nil {
		t.Fatal(err)
	}
	want := atroposdk.RuleSync{
		Version: 7, Etag: "0123456789abcdef",
		Rules:        []atroposdk.CompiledRule{{Name: "r-inline"}},
		ActiveFaults: []atroposdk.FaultRequest{{ID: "fc-inline"}},
		FreezeCfg:    &atroposdk.DelayRequest{Mu: 1.5, Sigma: 0.25},
		ReplaySet: &atroposdk.ReplaySetRef{ID: "sha256:aa", ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "canonical_v2",
			Count: 3, SHA256: "bb", SizeBytes: 42, URL: "/api/v1/cache/artifacts/sha256:aa"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ruleSyncFrom = %+v, want %+v", got, want)
	}

	// No replay set and no freeze config desired stay absent.
	none, err := ruleSyncFrom(desired.Document{Generation: 1, ETag: "0123456789abcdef"})
	if err != nil || none.ReplaySet != nil || none.FreezeCfg != nil {
		t.Errorf("document with neither freeze config nor replay set: %+v, %v", none, err)
	}
}

func TestReplaySetWire(t *testing.T) {
	if got, err := replaySetWire(nil); got != nil || err != nil {
		t.Errorf("replaySetWire(nil) = %+v, %v, want nil, nil", got, err)
	}

	ref := &model.ReplaySetRef{ID: "sha256:aa", ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "canonical_v2",
		Count: 3, SHA256: "bb", SizeBytes: 42, URL: "/api/v1/cache/artifacts/sha256:aa"}
	got, err := replaySetWire(ref)
	if err != nil {
		t.Fatal(err)
	}
	want := &atroposdk.ReplaySetRef{ID: "sha256:aa", ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "canonical_v2",
		Count: 3, SHA256: "bb", SizeBytes: 42, URL: "/api/v1/cache/artifacts/sha256:aa"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("replaySetWire = %+v, want %+v", got, want)
	}
	// The conversion fills every field of the SDK's ReplaySetRef: one the SDK
	// adds would come out unset here.
	if unset := unsetFields(got); len(unset) > 0 {
		t.Errorf("ReplaySetRef fields the conversion leaves unset: %v", unset)
	}

	// The platform's own limits convert exactly.
	for _, n := range []int{0, 1, math.MaxInt32, math.MaxInt, math.MinInt} {
		r := *ref
		r.Count = int64(n)
		if got, err := replaySetWire(&r); err != nil || got.Count != n {
			t.Errorf("count %d: got %+v, %v", n, got, err)
		}
	}
}

func TestNarrowCount(t *testing.T) {
	for _, tc := range []struct {
		n, lo, hi int64
		want      int
		wantErr   bool
	}{
		{n: 0, lo: math.MinInt32, hi: math.MaxInt32, want: 0},
		{n: math.MaxInt32, lo: math.MinInt32, hi: math.MaxInt32, want: math.MaxInt32},
		{n: math.MinInt32, lo: math.MinInt32, hi: math.MaxInt32, want: math.MinInt32},
		{n: math.MaxInt32 + 1, lo: math.MinInt32, hi: math.MaxInt32, wantErr: true},
		{n: math.MinInt32 - 1, lo: math.MinInt32, hi: math.MaxInt32, wantErr: true},
		{n: 1 << 40, lo: math.MinInt32, hi: math.MaxInt32, wantErr: true},
		{n: math.MaxInt64, lo: math.MinInt32, hi: math.MaxInt32, wantErr: true},
		{n: math.MinInt64, lo: math.MinInt32, hi: math.MaxInt32, wantErr: true},
	} {
		got, err := narrowCount(tc.n, tc.lo, tc.hi)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("narrowCount(%d, %d, %d) = %d, %v; want %d (error: %v)", tc.n, tc.lo, tc.hi, got, err, tc.want, tc.wantErr)
		}
	}
}
