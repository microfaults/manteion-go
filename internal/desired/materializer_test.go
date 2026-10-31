package desired

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

type fakeGen struct{ gen uint64 }

func (f *fakeGen) Get(context.Context, string) (uint64, error) { return f.gen, nil }

type fakeRules struct{ rules []*model.Rule }

func (f *fakeRules) ForService(_ context.Context, svc string) ([]*model.Rule, error) {
	var out []*model.Rule
	for _, r := range f.rules {
		if r.Service == svc {
			out = append(out, r)
		}
	}
	return out, nil
}

type fakeConfigs struct{ configs []*model.FaultConfig }

func (f *fakeConfigs) ListActiveForService(context.Context, string) ([]*model.FaultConfig, error) {
	return f.configs, nil
}

type fakeCacheBox struct{ ctx *model.CacheBoxRuleContext }

func (f *fakeCacheBox) ActiveCacheBoxPhaseForService(context.Context, string) (*model.CacheBoxRuleContext, error) {
	return f.ctx, nil
}

type fakeSpecs struct {
	specs map[string]*model.FaultSpec
	comps map[string]*model.FaultComposition
}

func (f *fakeSpecs) SpecResolver(context.Context) model.FaultSpecResolver {
	return func(id string) *model.FaultSpec { return f.specs[id] }
}
func (f *fakeSpecs) CompositionResolver(context.Context) model.CompositionResolver {
	return func(id string) *model.FaultComposition { return f.comps[id] }
}

type fakeFreeze struct{ cfg *atroposdk.DelayRequest }

func (f *fakeFreeze) FreezeConfigForService(context.Context, string) (*atroposdk.DelayRequest, error) {
	return f.cfg, nil
}

func rule(id, svc, typ, ref string) *model.Rule {
	r := &model.Rule{ID: id, Name: id, Service: svc, Enabled: true, Mode: "background"}
	switch typ {
	case "fault_spec":
		r.Action = model.RuleAction{Type: typ, FaultSpecID: ref}
	case "fault_composition":
		r.Action = model.RuleAction{Type: typ, FaultCompID: ref}
	case "cachebox":
		r.Action = model.RuleAction{Type: typ, CacheBox: &model.CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: "exact"}}
	}
	return r
}

func newTestMaterializer(gen uint64) (*Materializer, *fakeGen) {
	g := &fakeGen{gen: gen}
	m := &Materializer{
		Generations: g,
		Rules: &fakeRules{rules: []*model.Rule{
			rule("r-inline", "cart", "fault_spec", "spec-inline"),
			rule("r-net", "cart", "fault_spec", "spec-net"),
			rule("r-cpu", "cart", "fault_spec", "spec-cpu"),
			rule("r-comp", "cart", "fault_composition", "comp-mixed"),
			rule("r-cb", "cart", "cachebox", ""),
			rule("r-other", "checkout", "fault_spec", "spec-inline"),
		}},
		FaultConfigs: &fakeConfigs{configs: []*model.FaultConfig{
			{ID: "fc-inline", Service: "cart", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1s"}`)},
			{ID: "fc-mem", Service: "cart", Category: "resource", FaultType: "memory", Params: json.RawMessage(`{"target_bytes":1}`)},
			{ID: "fc-comp", Service: "cart", Category: "inline"}, // composition config: no params → skipped
		}},
		CacheBox: &fakeCacheBox{ctx: &model.CacheBoxRuleContext{ExperimentID: "exp-1", PhaseID: "phase-iso", Mode: "replay", KeyStrategy: "canonical_v2", StrategyVersion: 2}},
		Specs: &fakeSpecs{
			specs: map[string]*model.FaultSpec{
				"spec-inline": {ID: "spec-inline", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"100ms"}`)},
				"spec-net":    {ID: "spec-net", Category: "network", FaultType: "blackhole", Network: &model.NetworkEnvelope{Target: "redis", Direction: "upstream"}},
				"spec-cpu":    {ID: "spec-cpu", Category: "resource", FaultType: "cpu", Params: json.RawMessage(`{"target_load":0.5}`)},
			},
			comps: map[string]*model.FaultComposition{
				"comp-mixed": {ID: "comp-mixed", Name: "mixed", ExecutionMode: model.ExecutionParallel,
					Members: []model.FaultCompositionMember{{FaultSpecID: "spec-inline"}, {FaultSpecID: "spec-net"}}},
			},
		},
		Freeze: &fakeFreeze{cfg: &atroposdk.DelayRequest{Mu: 1.5, Sigma: 0.4}},
	}
	return m, g
}

func inst(caps ...string) *model.SDKInstance {
	return &model.SDKInstance{ID: "i1", Service: "cart", Capabilities: caps}
}

func ruleNames(d Document) string {
	var n []string
	for _, r := range d.Rules {
		n = append(n, r.Name)
	}
	return strings.Join(n, ",")
}

func TestMaterialize_UnfilteredForNilInstance(t *testing.T) {
	m, _ := newTestMaterializer(5)
	d, err := m.Materialize(context.Background(), "cart", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := ruleNames(d); got != "r-inline,r-net,r-cpu,r-comp,r-cb,cachebox:replay:phase-iso" {
		t.Errorf("rules = %s", got)
	}
	if len(d.ActiveFaults) != 2 || d.ActiveFaults[0].ID != "fc-inline" || d.ActiveFaults[1].ID != "fc-mem" {
		t.Errorf("active faults = %+v", d.ActiveFaults)
	}
	if d.FreezeCfg == nil || d.FreezeCfg.Mu != 1.5 {
		t.Errorf("freeze_cfg = %+v", d.FreezeCfg)
	}
	if d.Generation != 5 || !strings.HasPrefix(d.ETag, "5-") || len(d.ETag) != len("5-")+16 {
		t.Errorf("generation/etag = %d/%q", d.Generation, d.ETag)
	}
}

func TestMaterialize_FiltersByCapabilities(t *testing.T) {
	m, _ := newTestMaterializer(5)
	cases := []struct {
		caps       []string
		wantRules  string
		wantFaults int
		wantFreeze bool
	}{
		{[]string{"inline", "cachebox"}, "r-inline,r-cb,cachebox:replay:phase-iso", 1, true},
		{[]string{"inline", "network", "resource"}, "r-inline,r-net,r-cpu,r-comp", 2, false},
		{[]string{"inline"}, "r-inline", 1, false},
		{[]string{}, "r-inline,r-net,r-cpu,r-comp,r-cb,cachebox:replay:phase-iso", 2, true},
	}
	for _, tc := range cases {
		d, err := m.Materialize(context.Background(), "cart", inst(tc.caps...))
		if err != nil {
			t.Fatal(err)
		}
		if got := ruleNames(d); got != tc.wantRules {
			t.Errorf("caps %v: rules = %s, want %s", tc.caps, got, tc.wantRules)
		}
		if len(d.ActiveFaults) != tc.wantFaults || (d.FreezeCfg != nil) != tc.wantFreeze {
			t.Errorf("caps %v: faults=%d freeze=%v", tc.caps, len(d.ActiveFaults), d.FreezeCfg != nil)
		}
	}
}

func TestMaterialize_ETagStableAndContentSensitive(t *testing.T) {
	m, g := newTestMaterializer(5)
	ctx := context.Background()
	a, _ := m.Materialize(ctx, "cart", inst("inline"))
	b, _ := m.Materialize(ctx, "cart", inst("inline"))
	if a.ETag != b.ETag {
		t.Fatalf("same inputs, different ETags: %s vs %s", a.ETag, b.ETag)
	}
	g.gen = 6 // another instance's capabilities changed: generation moves, document does not
	c, _ := m.Materialize(ctx, "cart", inst("inline"))
	if !strings.HasPrefix(c.ETag, "6-") || c.ETag[2:] != a.ETag[2:] {
		t.Fatalf("hash must survive a generation move: %s vs %s", a.ETag, c.ETag)
	}
	d, _ := m.Materialize(ctx, "cart", inst("inline", "network"))
	if d.ETag[2:] == c.ETag[2:] {
		t.Fatal("different content, same hash")
	}
}

func TestMaterialize_EmptyIsNeverNull(t *testing.T) {
	m, _ := newTestMaterializer(1)
	d, err := m.Materialize(context.Background(), "nobody", inst("inline"))
	if err != nil {
		t.Fatal(err)
	}
	if d.Rules == nil || d.ActiveFaults == nil {
		t.Fatalf("Rules/ActiveFaults nil: %+v", d)
	}
}

func TestMaterialize_CompileErrorSurfaces(t *testing.T) {
	m, _ := newTestMaterializer(1)
	m.Rules = &fakeRules{rules: []*model.Rule{rule("r-bad", "cart", "fault_spec", "spec-missing")}}
	if _, err := m.Materialize(context.Background(), "cart", nil); err == nil {
		t.Fatal("expected a compile error for an unresolvable spec")
	}
}

type fakeReplaySets struct{ ref *model.ReplaySetRef }

func (f *fakeReplaySets) ReplaySetForService(context.Context, string) (*model.ReplaySetRef, error) {
	return f.ref, nil
}

var errBoom = errors.New("boom")

// failing is every error-returning source at once, and always fails.
type failing struct{}

func (failing) Get(context.Context, string) (uint64, error) { return 0, errBoom }
func (failing) ForService(context.Context, string) ([]*model.Rule, error) {
	return nil, errBoom
}
func (failing) ListActiveForService(context.Context, string) ([]*model.FaultConfig, error) {
	return nil, errBoom
}
func (failing) ActiveCacheBoxPhaseForService(context.Context, string) (*model.CacheBoxRuleContext, error) {
	return nil, errBoom
}
func (failing) FreezeConfigForService(context.Context, string) (*atroposdk.DelayRequest, error) {
	return nil, errBoom
}
func (failing) ReplaySetForService(context.Context, string) (*model.ReplaySetRef, error) {
	return nil, errBoom
}

// bumpingRules moves the generation while the rules are read, like a write
// landing in the middle of a materialization.
type bumpingRules struct{ g *fakeGen }

func (b *bumpingRules) ForService(context.Context, string) ([]*model.Rule, error) {
	b.g.gen++
	return nil, nil
}

// The replay set follows the freeze config: both go to a host that has a
// cache-box to install them in, and neither is even looked up for one that
// does not.
func TestMaterialize_FreezeAndReplaySetNeedACacheBox(t *testing.T) {
	ctx := context.Background()
	ref := &model.ReplaySetRef{ID: "sha256:ab12", ExperimentID: "exp-1", PhaseID: "phase-iso", KeyStrategy: "canonical_v2",
		Count: 3, SHA256: "cd34", SizeBytes: 120, URL: "/api/v1/sdk/replay-sets/sha256:ab12"}
	m, _ := newTestMaterializer(5)
	m.ReplaySets = &fakeReplaySets{ref: ref}

	for name, in := range map[string]*model.SDKInstance{
		"advertises cachebox":      inst("inline", "cachebox"),
		"advertises nothing (all)": inst(),
		"unknown instance (all)":   nil,
	} {
		d, err := m.Materialize(ctx, "cart", in)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(d.ReplaySet, ref) || d.FreezeCfg == nil {
			t.Errorf("%s: replay_set=%+v freeze_cfg=%+v, want both delivered", name, d.ReplaySet, d.FreezeCfg)
		}
	}

	m.Freeze, m.ReplaySets = failing{}, failing{}
	d, err := m.Materialize(ctx, "cart", inst("inline", "network", "resource"))
	if err != nil {
		t.Fatalf("a host without a cache-box must not consult the freeze or replay-set source: %v", err)
	}
	if d.ReplaySet != nil || d.FreezeCfg != nil {
		t.Errorf("replay_set=%+v freeze_cfg=%+v, want neither", d.ReplaySet, d.FreezeCfg)
	}
}

// The freeze and replay-set sources are optional until the orchestrator plan
// wires them; the document just carries neither.
func TestMaterialize_OptionalSourcesMayBeNil(t *testing.T) {
	m, _ := newTestMaterializer(5)
	m.Freeze, m.ReplaySets = nil, nil
	d, err := m.Materialize(context.Background(), "cart", nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.FreezeCfg != nil || d.ReplaySet != nil {
		t.Errorf("freeze_cfg=%+v replay_set=%+v, want neither", d.FreezeCfg, d.ReplaySet)
	}
}

// An instance converges on the ETag, so every part of the document it can act
// on has to move it: the delay source and the replay set as much as the rules.
func TestMaterialize_ETagCoversFreezeAndReplaySet(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestMaterializer(5)
	base, _ := m.Materialize(ctx, "cart", nil)

	m.ReplaySets = &fakeReplaySets{ref: &model.ReplaySetRef{ID: "sha256:ab12"}}
	withSet, _ := m.Materialize(ctx, "cart", nil)
	if withSet.ETag[2:] == base.ETag[2:] {
		t.Error("installing a replay set did not change the ETag")
	}

	m.Freeze = &fakeFreeze{cfg: &atroposdk.DelayRequest{Mu: 2, Sigma: 0.4}}
	retuned, _ := m.Materialize(ctx, "cart", nil)
	if retuned.ETag[2:] == withSet.ETag[2:] {
		t.Error("retuning the freeze delay did not change the ETag")
	}
}

// A bump that lands while the inputs are being read must not be claimed by a
// document that may predate it. The generation is read first, so the document
// carries the older number and the instance's next poll picks up the newer one.
func TestMaterialize_GenerationReadBeforeInputs(t *testing.T) {
	m, g := newTestMaterializer(5)
	m.Rules = &bumpingRules{g: g}
	d, err := m.Materialize(context.Background(), "cart", nil)
	if err != nil {
		t.Fatal(err)
	}
	if g.gen != 6 {
		t.Fatalf("generation = %d: the mid-read bump never happened, so this proves nothing", g.gen)
	}
	if d.Generation != 5 || !strings.HasPrefix(d.ETag, "5-") {
		t.Errorf("document claims generation %d (ETag %q), want the 5 it was read at", d.Generation, d.ETag)
	}
}

// Any source failing aborts the whole materialization: no partial document, so
// the caller answers an error and the instance keeps what it has.
func TestMaterialize_SourceErrorsAbort(t *testing.T) {
	cases := []struct {
		name     string
		sabotage func(*Materializer)
	}{
		{"generation", func(m *Materializer) { m.Generations = failing{} }},
		{"rules", func(m *Materializer) { m.Rules = failing{} }},
		{"cache-box role", func(m *Materializer) { m.CacheBox = failing{} }},
		{"active faults", func(m *Materializer) { m.FaultConfigs = failing{} }},
		{"freeze config", func(m *Materializer) { m.Freeze = failing{} }},
		{"replay set", func(m *Materializer) { m.ReplaySets = failing{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newTestMaterializer(5)
			tc.sabotage(m)
			d, err := m.Materialize(context.Background(), "cart", nil)
			if !errors.Is(err, errBoom) || !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("err = %v, want it to name %q and wrap the source error", err, tc.name)
			}
			if d.ETag != "" || d.Rules != nil {
				t.Errorf("a failed materialization returned a document: %+v", d)
			}
		})
	}
}

func TestFaultRequestsOf(t *testing.T) {
	latency := json.RawMessage(`{"delay":"1s"}`)
	errParams := json.RawMessage(`{"status_code":503}`)
	configs := []*model.FaultConfig{
		{ID: "net", Category: "network", FaultType: "latency", Params: latency, DurationMs: 3000, RampUpMs: 100, RampDownMs: 200,
			Network: &model.NetworkEnvelope{Target: "redis", Direction: "upstream", Scope: 0.5}},
		{ID: "comp", Category: "inline"}, // composition config: no params, not deliverable yet
		{ID: "err", Category: "inline", FaultType: "error", Params: errParams},
	}
	want := []atroposdk.FaultRequest{
		{ID: "net", Category: "network", FaultType: "latency", Params: latency, DurationMs: 3000, RampUpMs: 100, RampDownMs: 200,
			Network: &atroposdk.NetworkEnvelope{Target: "redis", Direction: "upstream", Scope: 0.5}},
		{ID: "err", Category: "inline", FaultType: "error", Params: errParams},
	}
	if got := FaultRequestsOf(configs); !reflect.DeepEqual(got, want) {
		t.Errorf("FaultRequestsOf =\n %+v\nwant\n %+v", got, want)
	}
	if got := FaultRequestsOf(nil); got == nil || len(got) != 0 {
		t.Errorf("no configs: got %#v, want an empty, non-nil slice", got)
	}
}
