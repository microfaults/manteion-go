package opaharness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/promql"
)

// --- fakes -------------------------------------------------------------------

type fakePolicies struct{ policies []*model.OPAPolicy }

func (f *fakePolicies) ListEnabled(_ context.Context) ([]*model.OPAPolicy, error) {
	var out []*model.OPAPolicy
	for _, p := range f.policies {
		if p.Enabled {
			out = append(out, p)
		}
	}
	return out, nil
}

type fakeDecisions struct {
	records    []*model.PolicyDecision
	lastChange time.Time
	hasLast    bool
	recordErr  error
}

func (f *fakeDecisions) Record(_ context.Context, d *model.PolicyDecision) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	cp := *d
	f.records = append(f.records, &cp)
	return nil
}

func (f *fakeDecisions) LastChangeAt(_ context.Context, _ string) (time.Time, bool, error) {
	return f.lastChange, f.hasLast, nil
}

type fakeRuleLister struct{ rules []*model.Rule }

func (f *fakeRuleLister) List(_ context.Context) ([]*model.Rule, error) { return f.rules, nil }

type fakeInstances struct {
	byService map[string][]*model.SDKInstance
}

func (f *fakeInstances) ForService(_ context.Context, service string) ([]*model.SDKInstance, error) {
	return f.byService[service], nil
}

// fakeProvider answers every input by name; err fails every fetch.
type fakeProvider struct {
	values map[string]any
	err    error
}

func (f fakeProvider) Fetch(_ context.Context, in model.PolicyInput) (any, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.values[in.Name], nil
}

type fakeMutator struct {
	calls   [][]Mutation
	outcome string // reported for every mutation; "" → created
	err     error
}

func (f *fakeMutator) Apply(_ context.Context, _ string, ms []Mutation) ([]MutationResult, error) {
	f.calls = append(f.calls, ms)
	if f.err != nil {
		return nil, f.err
	}
	out := make([]MutationResult, 0, len(ms))
	for _, m := range ms {
		svc, name := m.Key()
		o := f.outcome
		if o == "" {
			o = OutcomeCreated
		}
		out = append(out, MutationResult{Op: m.Op, Service: svc, Name: name, Outcome: o})
	}
	return out, nil
}

// --- fixtures ----------------------------------------------------------------

var t0 = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

// hotRego is the documented example policy (docs/opa-harness.md).
const hotRego = `package atropos

rule_name := "checkout-egress-latency"

hot if {
	input.inputs.decisions > 50
	input.inputs.p99 > 800
}

desired_rule := {
	"name": rule_name,
	"service": "checkout",
	"enabled": true,
	"priority": 100,
	"mode": "inline",
	"match": {"injection_point": "egress", "labels": {"atropos.http.path": "/cart/checkout"}},
	"action": {"type": "fault_spec", "fault_spec_id": "spec-egress-latency-200ms"},
}

mutations contains {"op": "upsert_rule", "rule": desired_rule} if hot

mutations contains {"op": "set_enabled", "service": "checkout", "name": rule_name, "enabled": false} if {
	not hot
	some r in input.rules
	r.service == "checkout"
	r.name == rule_name
	r.enabled
}
`

const outOfScopeRego = `package atropos

mutations contains {"op": "delete_rule", "service": "frontend", "name": "x"}

mutations contains {"op": "delete_rule", "service": "checkout", "name": "y"}
`

// shapeRego proves every field of the input document is populated: it emits
// one delete per rule it sees, and only if instances, services and now are there.
const shapeRego = `package atropos

mutations contains {"op": "delete_rule", "service": "checkout", "name": r.name} if {
	some r in input.rules
	some i in input.instances
	i.status == "alive"
	input.services == ["checkout"]
	input.now_ns > 0
	input.now != ""
}
`

const emptyRego = "package atropos\n\nmutations contains m if { false; m := {} }\n"

func hotPolicy() *model.OPAPolicy {
	return &model.OPAPolicy{
		ID: "opa-hot", Name: "checkout-hot", Enabled: true, Rego: hotRego, IntervalS: 10, CooldownS: 60,
		Inputs: []model.PolicyInput{
			{Name: "decisions", Source: "loki", Query: `sum(count_over_time({service_name="checkout"} |= "atropos.decision" [1m]))`, Kind: "scalar"},
			{Name: "p99", Source: "prometheus", Query: `histogram_quantile(0.99, sum by (le) (rate(duration_milliseconds_bucket{service_name="checkout", span_name="POST /cart/checkout"}[1m])))`, Kind: "scalar"},
		},
		Scope: model.PolicyScope{Services: []string{"checkout"}}, CreatedAt: t0, UpdatedAt: t0,
	}
}

func withRego(p *model.OPAPolicy, rego string) *model.OPAPolicy { p.Rego = rego; return p }

func hotValues() map[string]any  { return map[string]any{"decisions": 120.0, "p99": 1300.0} }
func coldValues() map[string]any { return map[string]any{"decisions": 3.0, "p99": 200.0} }

type testHarness struct {
	h   *Harness
	dec *fakeDecisions
	mut *fakeMutator
	now time.Time
}

func newTestHarness(policies []*model.OPAPolicy, prov InputProvider, mut *fakeMutator, dec *fakeDecisions, rules []*model.Rule) *testHarness {
	th := &testHarness{dec: dec, mut: mut, now: t0}
	th.h = New(Deps{
		Policies:  &fakePolicies{policies: policies},
		Decisions: dec,
		Rules:     &fakeRuleLister{rules: rules},
		Instances: &fakeInstances{byService: map[string][]*model.SDKInstance{"checkout": {{ID: "checkout-1", Service: "checkout", Status: "alive"}}}},
		Providers: Providers{"prometheus": prov, "loki": prov},
		Mutator:   mut,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:       func() time.Time { return th.now },
		NewID:     func(prefix string) string { return prefix + "-fixed" },
	})
	return th
}

func (th *testHarness) tickAt(d time.Duration) {
	th.now = t0.Add(d)
	th.h.tick(context.Background(), th.now)
}

// --- EvaluateOnce ------------------------------------------------------------

func TestEvaluateOnce_AppliesAndRecords(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if len(th.mut.calls) != 1 || len(th.mut.calls[0]) != 1 {
		t.Fatalf("mutator calls = %+v, want one call with one mutation", th.mut.calls)
	}
	m := th.mut.calls[0][0]
	if svc, name := m.Key(); m.Op != OpUpsertRule || svc != "checkout" || name != "checkout-egress-latency" || m.Rule == nil ||
		m.Rule.Match.Labels["atropos.http.path"] != "/cart/checkout" {
		t.Errorf("mutation = %+v, want the egress latency upsert", m)
	}
	if !ev.Applied || !ev.Changed || ev.Error != "" || len(ev.Results) != 1 || ev.Results[0].Outcome != OutcomeCreated {
		t.Errorf("evaluation = %+v, want applied+changed with one created result", ev)
	}
	if len(ev.InputDigest) != 64 || ev.Inputs["p99"] != 1300.0 || !ev.EvaluatedAt.Equal(t0) {
		t.Errorf("digest %q inputs %v at %v", ev.InputDigest, ev.Inputs, ev.EvaluatedAt)
	}
	if len(th.dec.records) != 1 {
		t.Fatalf("decisions recorded = %d, want 1", len(th.dec.records))
	}
	d := th.dec.records[0]
	var results []MutationResult
	if err := json.Unmarshal(d.Mutations, &results); err != nil || len(results) != 1 || results[0].Outcome != OutcomeCreated {
		t.Errorf("decision mutations = %s (%v), want one created result", d.Mutations, err)
	}
	if d.PolicyID != "opa-hot" || !d.Applied || d.Error != "" || !d.EvaluatedAt.Equal(t0) || d.InputDigest != ev.InputDigest || !strings.HasPrefix(d.ID, "pdec-") {
		t.Errorf("decision row = %+v", d)
	}
}

func TestEvaluateOnce_DryRunDoesNotApply(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev := th.h.EvaluateOnce(context.Background(), hotPolicy(), false)
	if len(th.mut.calls) != 0 || ev.Applied || ev.Changed || len(ev.Mutations) != 1 || len(ev.Results) != 0 {
		t.Errorf("dry run: calls %d, evaluation %+v; want the proposed mutation, nothing applied", len(th.mut.calls), ev)
	}
	if len(th.dec.records) != 1 || th.dec.records[0].Applied {
		t.Errorf("dry run must still be recorded, with applied=false: %+v", th.dec.records)
	}
}

func TestEvaluateOnce_ColdDisablesExistingRule(t *testing.T) {
	existing := &model.Rule{ID: "rule-1", Name: "checkout-egress-latency", Service: "checkout", Enabled: true, Mode: "inline",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-egress-latency-200ms"}}
	th := newTestHarness(nil, fakeProvider{values: coldValues()}, &fakeMutator{outcome: OutcomeUpdated}, &fakeDecisions{}, []*model.Rule{existing})
	ev := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if len(ev.Mutations) != 1 || ev.Mutations[0].Op != OpSetEnabled || ev.Mutations[0].Enabled == nil || *ev.Mutations[0].Enabled {
		t.Fatalf("mutations = %+v, want set_enabled false", ev.Mutations)
	}
}

func TestEvaluateOnce_InputErrorSkipsEval(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{err: errors.New("loki returned status 502")}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if !strings.Contains(ev.Error, "input decisions") || !strings.Contains(ev.Error, "502") || len(th.mut.calls) != 0 || ev.Applied {
		t.Errorf("evaluation = %+v, calls %d; want an input error and no apply", ev, len(th.mut.calls))
	}
	if len(th.dec.records) != 1 || th.dec.records[0].Error == "" {
		t.Errorf("errored evaluation must be recorded with its error: %+v", th.dec.records)
	}
}

func TestEvaluateOnce_RefusesOutOfScope(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev := th.h.EvaluateOnce(context.Background(), withRego(hotPolicy(), outOfScopeRego), true)
	if len(th.mut.calls) != 1 || len(th.mut.calls[0]) != 1 || th.mut.calls[0][0].Name != "y" {
		t.Fatalf("mutator received %+v, want only the checkout mutation", th.mut.calls)
	}
	var refused, created int
	for _, r := range ev.Results {
		switch r.Outcome {
		case OutcomeRefused:
			refused++
			if r.Service != "frontend" || !strings.Contains(r.Error, "outside policy scope") {
				t.Errorf("refusal = %+v", r)
			}
		case OutcomeCreated:
			created++
		}
	}
	if refused != 1 || created != 1 || len(ev.Mutations) != 2 {
		t.Errorf("results = %+v (mutations %d)", ev.Results, len(ev.Mutations))
	}
}

func TestEvaluateOnce_InputDocumentShape(t *testing.T) {
	rules := []*model.Rule{
		{ID: "rule-1", Name: "old-rule", Service: "checkout", Enabled: false, Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "s"}},
		{ID: "rule-2", Name: "fe-rule", Service: "frontend", Enabled: true, Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "s"}},
	}
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{outcome: OutcomeDeleted}, &fakeDecisions{}, rules)
	ev := th.h.EvaluateOnce(context.Background(), withRego(hotPolicy(), shapeRego), true)
	if ev.Error != "" || len(ev.Mutations) != 1 || ev.Mutations[0].Name != "old-rule" {
		t.Fatalf("evaluation = %+v; want exactly one delete for the in-scope rule (frontend's must not be in input.rules)", ev)
	}
}

func TestEvaluateOnce_CompileErrorIsRecorded(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev := th.h.EvaluateOnce(context.Background(), withRego(hotPolicy(), "package atropos\nmutations contains x if { x := }"), true)
	if !strings.Contains(ev.Error, "compile") || len(th.mut.calls) != 0 || len(th.dec.records) != 1 {
		t.Fatalf("evaluation = %+v, calls %d, records %d", ev, len(th.mut.calls), len(th.dec.records))
	}
}

func TestEvaluateOnce_RecordFailureIsReported(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{recordErr: errors.New("db down")}, nil)
	ev := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if ev.DecisionError != "record decision: db down" || ev.Error != "" {
		t.Errorf("decision_error %q error %q, want decision_error %q and no evaluation error", ev.DecisionError, ev.Error, "record decision: db down")
	}
	if !ev.Applied || !ev.Changed || len(ev.Results) != 1 || ev.Results[0].Outcome != OutcomeCreated || len(th.mut.calls) != 1 {
		t.Errorf("evaluation = %+v, want the applied results kept (they happened)", ev)
	}
}

// --- tick scheduling ---------------------------------------------------------

func TestTick_CooldownAfterChange(t *testing.T) {
	th := newTestHarness([]*model.OPAPolicy{hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	for _, step := range []struct {
		at        time.Duration
		wantEvals int
	}{
		{0, 1},                // due immediately; the applied set changed → cooldown 60s
		{10 * time.Second, 1}, // interval elapsed, cooldown has not
		{59 * time.Second, 1},
		{60 * time.Second, 2}, // cooldown elapsed; changes again → cooldown to 120s
		{119 * time.Second, 2},
		{120 * time.Second, 3},
	} {
		th.tickAt(step.at)
		if got := len(th.mut.calls); got != step.wantEvals {
			t.Fatalf("at +%s: evaluations = %d, want %d", step.at, got, step.wantEvals)
		}
	}
}

func TestTick_NoCooldownWhenUnchanged(t *testing.T) {
	th := newTestHarness([]*model.OPAPolicy{hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{outcome: OutcomeUnchanged}, &fakeDecisions{}, nil)
	for _, step := range []struct {
		at        time.Duration
		wantEvals int
	}{{0, 1}, {9 * time.Second, 1}, {10 * time.Second, 2}, {20 * time.Second, 3}} {
		th.tickAt(step.at)
		if got := len(th.mut.calls); got != step.wantEvals {
			t.Fatalf("at +%s: evaluations = %d, want %d", step.at, got, step.wantEvals)
		}
	}
}

func TestTick_CooldownRestoredFromDecisionLog(t *testing.T) {
	dec := &fakeDecisions{hasLast: true, lastChange: t0.Add(-30 * time.Second)}
	th := newTestHarness([]*model.OPAPolicy{hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{}, dec, nil)
	for _, step := range []struct {
		at        time.Duration
		wantEvals int
	}{{0, 0}, {29 * time.Second, 0}, {30 * time.Second, 1}} {
		th.tickAt(step.at)
		if got := len(th.mut.calls); got != step.wantEvals {
			t.Fatalf("at +%s: evaluations = %d, want %d (cooldown from the last recorded change)", step.at, got, step.wantEvals)
		}
	}
}

func TestTick_CompileErrorIsRecordedAndOthersRun(t *testing.T) {
	broken := withRego(hotPolicy(), "package atropos\nmutations contains x if { x := }")
	broken.ID, broken.Name = "opa-broken", "broken"
	th := newTestHarness([]*model.OPAPolicy{broken, hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	th.tickAt(0)
	if len(th.mut.calls) != 1 || len(th.dec.records) != 2 {
		t.Fatalf("calls %d records %d, want the healthy policy applied and both evaluations recorded", len(th.mut.calls), len(th.dec.records))
	}
	var errored int
	for _, d := range th.dec.records {
		if d.PolicyID == "opa-broken" && strings.Contains(d.Error, "compile") {
			errored++
		}
	}
	if errored != 1 {
		t.Errorf("records = %+v, want the broken policy's error recorded", th.dec.records)
	}
}

func TestTick_RecompilesOnRegoChange(t *testing.T) {
	p := hotPolicy()
	th := newTestHarness([]*model.OPAPolicy{p}, fakeProvider{values: hotValues()}, &fakeMutator{outcome: OutcomeUnchanged}, &fakeDecisions{}, nil)
	th.tickAt(0)
	p.Rego = emptyRego
	th.tickAt(10 * time.Second)
	if len(th.mut.calls) != 2 || len(th.mut.calls[0]) != 1 || len(th.mut.calls[1]) != 0 {
		t.Fatalf("calls = %+v, want the edited policy evaluated with its new source", th.mut.calls)
	}
}

func TestTick_DisabledPolicyIsSkipped(t *testing.T) {
	p := hotPolicy()
	p.Enabled = false
	th := newTestHarness([]*model.OPAPolicy{p}, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	th.tickAt(0)
	if len(th.mut.calls) != 0 || len(th.dec.records) != 0 {
		t.Fatalf("disabled policy was evaluated: calls %d records %d", len(th.mut.calls), len(th.dec.records))
	}
}

// --- carries: digest, dry-run record, partial apply, Run ---------------------

func TestInputDigest_ByteStable(t *testing.T) {
	a := map[string]any{"p99": 1300.0, "decisions": 120.0, "absent": nil,
		"vec": []promql.Sample{{Labels: map[string]string{"z": "1", "a": "2"}, Value: 1}}}
	b := map[string]any{"vec": []promql.Sample{{Labels: map[string]string{"a": "2", "z": "1"}, Value: 1}},
		"absent": nil, "decisions": 120.0, "p99": 1300.0}
	da, db := inputDigest(a), inputDigest(b)
	if da != db || len(da) != 64 {
		t.Fatalf("digests %q / %q, want equal 64-hex digests for equal inputs", da, db)
	}
	if inputDigest(map[string]any{"p99": 1300.5, "decisions": 120.0, "absent": nil, "vec": a["vec"]}) == da {
		t.Fatal("a different input value produced the same digest")
	}
	// Two evaluations with equal values record the same digest.
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev1 := th.h.EvaluateOnce(context.Background(), hotPolicy(), false)
	ev2 := th.h.EvaluateOnce(context.Background(), hotPolicy(), false)
	if ev1.InputDigest != ev2.InputDigest {
		t.Errorf("digests differ across runs: %s vs %s", ev1.InputDigest, ev2.InputDigest)
	}
}

func TestEvaluateOnce_DryRunRecordsWouldBeMutations(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev := th.h.EvaluateOnce(context.Background(), withRego(hotPolicy(), outOfScopeRego), false)
	if len(th.mut.calls) != 0 || len(ev.Mutations) != 2 {
		t.Fatalf("dry run: calls %d, mutations %+v", len(th.mut.calls), ev.Mutations)
	}
	d := th.dec.records[0]
	var results []MutationResult
	if err := json.Unmarshal(d.Mutations, &results); err != nil || len(results) != 2 || d.Applied {
		t.Fatalf("dry-run decision = %+v (%s, %v), want two results, applied=false", d, d.Mutations, err)
	}
	if r := results[0]; r.Outcome != OutcomeRefused || r.Service != "frontend" {
		t.Errorf("results[0] = %+v, want the frontend refusal", r)
	}
	if r := results[1]; r.Outcome != OutcomeDryRun || r.Op != OpDeleteRule || r.Service != "checkout" || r.Name != "y" || r.Changed() {
		t.Errorf("results[1] = %+v, want the checkout delete as dry_run", r)
	}
}

// partialMutator applies the first mutation, then fails like a List error.
type partialMutator struct{ calls int }

func (p *partialMutator) Apply(_ context.Context, _ string, ms []Mutation) ([]MutationResult, error) {
	p.calls++
	svc, name := ms[0].Key()
	return []MutationResult{{Op: ms[0].Op, Service: svc, Name: name, Outcome: OutcomeCreated}}, errors.New("list rules: db down")
}

func TestEvaluateOnce_PartialApplyKeepsResults(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	pm := &partialMutator{}
	th.h.deps.Mutator = pm
	ev := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if !strings.Contains(ev.Error, "apply: list rules") || len(ev.Results) != 1 || !ev.Changed || !ev.Applied {
		t.Fatalf("evaluation = %+v, want the partial result kept with the apply error", ev)
	}
	d := th.dec.records[0]
	var results []MutationResult
	if err := json.Unmarshal(d.Mutations, &results); err != nil || len(results) != 1 || results[0].Outcome != OutcomeCreated || d.Error == "" || !d.Applied {
		t.Errorf("decision = %+v (%s), want the partial result, applied, with the error", d, d.Mutations)
	}
}

func TestEvaluateOnce_UnknownSourceIsRecorded(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	p := hotPolicy()
	p.Inputs[0].Source = "graphite"
	ev := th.h.EvaluateOnce(context.Background(), p, true)
	if !strings.Contains(ev.Error, `no provider for source "graphite"`) || len(th.mut.calls) != 0 || len(th.dec.records) != 1 {
		t.Fatalf("evaluation = %+v", ev)
	}
}

func TestTick_PolicyRemovedDropsState(t *testing.T) {
	pol := &fakePolicies{policies: []*model.OPAPolicy{hotPolicy()}}
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	th.h.deps.Policies = pol
	th.tickAt(0)
	if len(th.h.nextAt) != 1 || len(th.h.prepared) != 1 {
		t.Fatalf("state after first tick: nextAt %d prepared %d", len(th.h.nextAt), len(th.h.prepared))
	}
	pol.policies = nil
	th.tickAt(time.Second)
	if len(th.h.nextAt) != 0 || len(th.h.prepared) != 0 {
		t.Fatalf("removed policy kept state: nextAt %d prepared %d", len(th.h.nextAt), len(th.h.prepared))
	}
}

func TestRun_ReturnsOnContextDone(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	th.h.resolution = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { th.h.Run(ctx); close(done) }()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx was cancelled")
	}
}

// --- carries: API-driven cooldown, shutdown mid-tick -------------------------

func TestEvaluateOnce_APIApplyMovesCooldown(t *testing.T) {
	th := newTestHarness([]*model.OPAPolicy{hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	// A dry run through the API changes nothing and leaves the schedule alone.
	th.h.EvaluateOnce(context.Background(), hotPolicy(), false)
	if len(th.h.nextAt) != 0 {
		t.Fatalf("dry run moved the schedule: %v", th.h.nextAt)
	}
	// An applying evaluation through the API that changes rules starts the cooldown.
	ev := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if !ev.Changed {
		t.Fatalf("apply: ev %+v, want a change", ev)
	}
	for _, step := range []struct {
		at        time.Duration
		wantEvals int
	}{
		{10 * time.Second, 1}, // interval elapsed, the API apply's cooldown has not
		{59 * time.Second, 1},
		{60 * time.Second, 2}, // cooldown elapsed
	} {
		th.tickAt(step.at)
		if got := len(th.mut.calls); got != step.wantEvals {
			t.Fatalf("at +%s: evaluations = %d, want %d", step.at, got, step.wantEvals)
		}
	}
}

// cancellingMutator cancels the tick's context on its first Apply.
type cancellingMutator struct {
	fakeMutator
	cancel context.CancelFunc
}

func (c *cancellingMutator) Apply(ctx context.Context, pid string, ms []Mutation) ([]MutationResult, error) {
	res, err := c.fakeMutator.Apply(ctx, pid, ms)
	c.cancel()
	return res, err
}

func TestTick_StopsOnCancelledContext(t *testing.T) {
	second := hotPolicy()
	second.ID, second.Name = "opa-hot-2", "checkout-hot-2"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mut := &cancellingMutator{cancel: cancel}
	dec := &fakeDecisions{}
	th := newTestHarness([]*model.OPAPolicy{hotPolicy(), second}, fakeProvider{values: hotValues()}, &mut.fakeMutator, dec, nil)
	th.h.deps.Mutator = mut
	th.h.tick(ctx, t0)
	if len(mut.calls) != 1 || len(dec.records) != 1 || dec.records[0].PolicyID != "opa-hot" {
		t.Fatalf("calls %d records %d, want only the first policy evaluated before the shutdown", len(mut.calls), len(dec.records))
	}
}

// --- non-finite samples ------------------------------------------------------

// nullCountRego reports, as the name of one delete, how many samples of the
// vector input v are null and how many are finite (guarded by != null).
const nullCountRego = `package atropos

nulls := count([s | some s in input.inputs.v; s.value == null])

finite := count([s | some s in input.inputs.v; s.value != null; s.value > 0])

mutations contains {"op": "delete_rule", "service": "checkout", "name": sprintf("nulls-%d-finite-%d-scalar-%v", [nulls, finite, input.inputs.s])}
`

func TestEvaluateOnce_NonFiniteSamplesAreNull(t *testing.T) {
	dec := &fakeDecisions{}
	th := newTestHarness(nil, nil, &fakeMutator{}, dec, nil)
	th.h.deps.Providers = Providers{
		"prometheus": VectorProvider{Querier: fakeQuerier{samples: nonFiniteSamples()}},
		"loki":       VectorProvider{Querier: fakeQuerier{samples: []promql.Sample{{Labels: map[string]string{}, Value: math.NaN()}}}},
	}
	p := withRego(hotPolicy(), nullCountRego)
	p.Inputs = []model.PolicyInput{
		{Name: "v", Source: "prometheus", Query: "q", Kind: "vector"},
		{Name: "s", Source: "loki", Query: "q", Kind: "scalar"},
	}
	ev := th.h.EvaluateOnce(context.Background(), p, false)
	if ev.Error != "" || ev.DecisionError != "" {
		t.Fatalf("evaluation error = %q / %q, want none (non-finite samples are null, not an error)", ev.Error, ev.DecisionError)
	}
	if len(ev.Mutations) != 1 || ev.Mutations[0].Name != "nulls-3-finite-1-scalar-null" {
		t.Fatalf("mutations = %+v, want the policy to see three null samples, one finite and a null scalar", ev.Mutations)
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("the evaluation must encode as JSON (the API answers it): %v", err)
	}
	if n := strings.Count(string(raw), `"value":null`); n != 3 {
		t.Errorf("evaluation JSON has %d null sample values, want 3: %s", n, raw)
	}
	if len(dec.records) != 1 || dec.records[0].InputDigest != ev.InputDigest || ev.InputDigest != inputDigest(ev.Inputs) ||
		strings.Contains(ev.InputDigest, "unencodable") {
		t.Errorf("recorded digest %v, evaluation digest %q: want the same digest over the null representation", dec.records, ev.InputDigest)
	}
}

// --- detached apply+record, waited shutdown ----------------------------------

type ctxKey struct{}

// ctxSeen is what a fake saw of the context it was called with.
type ctxSeen struct {
	called   bool
	err      error // ctx.Err() when called (after the caller cancelled, for the mutator)
	deadline time.Duration
	value    any
}

func seeCtx(ctx context.Context) ctxSeen {
	s := ctxSeen{called: true, err: ctx.Err(), value: ctx.Value(ctxKey{})}
	if dl, ok := ctx.Deadline(); ok {
		s.deadline = time.Until(dl)
	}
	return s
}

// ctxDecisions records like fakeDecisions and keeps the context Record saw.
type ctxDecisions struct {
	fakeDecisions
	seen ctxSeen
}

func (c *ctxDecisions) Record(ctx context.Context, d *model.PolicyDecision) error {
	c.seen = seeCtx(ctx)
	return c.fakeDecisions.Record(ctx, d)
}

// ctxProvider answers like fakeProvider and keeps the context Fetch saw.
type ctxProvider struct {
	fakeProvider
	seen *ctxSeen
}

func (c ctxProvider) Fetch(ctx context.Context, in model.PolicyInput) (any, error) {
	*c.seen = seeCtx(ctx)
	return c.fakeProvider.Fetch(ctx, in)
}

// blockingMutator applies like fakeMutator once release is closed. It
// signals started on entry, cancels the caller's context first when cancel is
// set, and keeps what its context looked like once released.
type blockingMutator struct {
	fakeMutator
	started chan struct{}
	release chan struct{}
	cancel  context.CancelFunc
	seen    ctxSeen
}

func (b *blockingMutator) Apply(ctx context.Context, pid string, ms []Mutation) ([]MutationResult, error) {
	if b.cancel != nil {
		b.cancel()
	}
	if b.started != nil {
		close(b.started)
	}
	if b.release != nil {
		<-b.release
	}
	b.seen = seeCtx(ctx)
	return b.fakeMutator.Apply(ctx, pid, ms)
}

func TestEvaluateOnce_ApplyAndRecordSurviveCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "caller"))
	defer cancel()
	dec := &ctxDecisions{}
	var fetched ctxSeen
	mut := &blockingMutator{cancel: cancel}
	th := newTestHarness(nil, ctxProvider{fakeProvider: fakeProvider{values: hotValues()}, seen: &fetched}, &mut.fakeMutator, &dec.fakeDecisions, nil)
	th.h.deps.Mutator, th.h.deps.Decisions = mut, dec

	ev := th.h.EvaluateOnce(ctx, hotPolicy(), true)
	if ev.Error != "" || ev.DecisionError != "" || !ev.Changed {
		t.Fatalf("evaluation = %+v; want the apply to complete although the caller cancelled", ev)
	}
	if fetched.err != nil || fetched.deadline != 0 || fetched.value != "caller" {
		t.Errorf("input fetch ctx = %+v, want the caller's own context (no apply deadline)", fetched)
	}
	if mut.seen.err != nil || mut.seen.value != "caller" || mut.seen.deadline <= 0 || mut.seen.deadline > applyTimeout {
		t.Errorf("apply ctx = %+v, want a live context detached from the caller's cancellation, carrying its values, bounded by %s", mut.seen, applyTimeout)
	}
	if !dec.seen.called || dec.seen.err != nil || dec.seen.value != "caller" || dec.seen.deadline <= 0 || dec.seen.deadline > applyTimeout {
		t.Errorf("record ctx = %+v, want a live detached context bounded by %s", dec.seen, applyTimeout)
	}
	if len(dec.records) != 1 || !dec.records[0].Applied {
		t.Errorf("records = %+v, want the applied decision recorded", dec.records)
	}
}

func TestEvaluateOnce_DryRunRecordSurvivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dec := &ctxDecisions{}
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &dec.fakeDecisions, nil)
	th.h.deps.Decisions = dec
	cancel()
	th.h.EvaluateOnce(ctx, hotPolicy(), false)
	if len(dec.records) != 1 || dec.seen.err != nil || dec.seen.deadline <= 0 {
		t.Errorf("records %d, record ctx %+v; want the dry run recorded under a live, bounded context", len(dec.records), dec.seen)
	}
}

func TestRun_ReturnsAfterTheInFlightTick(t *testing.T) {
	dec := &ctxDecisions{}
	mut := &blockingMutator{started: make(chan struct{}), release: make(chan struct{})}
	th := newTestHarness([]*model.OPAPolicy{hotPolicy()}, fakeProvider{values: hotValues()}, &mut.fakeMutator, &dec.fakeDecisions, nil)
	th.h.deps.Mutator, th.h.deps.Decisions = mut, dec
	th.h.resolution = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { th.h.Run(ctx); close(done) }()

	select {
	case <-mut.started:
	case <-time.After(2 * time.Second):
		t.Fatal("the tick never reached the mutator")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned while the tick's apply was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(mut.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the in-flight tick finished")
	}
	if len(mut.calls) != 1 || len(dec.records) != 1 || !dec.records[0].Applied || dec.seen.err != nil {
		t.Fatalf("calls %d, records %+v, record ctx %+v; want the in-flight evaluation applied and recorded under a live context",
			len(mut.calls), dec.records, dec.seen)
	}
}

// --- monotonic nextAt --------------------------------------------------------

// apiDuringTickMutator answers its first Apply (the tick's) with unchanged,
// after running an applying API evaluation of the same policy that changes
// something: the API's cooldown lands while the tick is in flight.
type apiDuringTickMutator struct {
	h     *Harness
	p     *model.OPAPolicy
	calls int
}

func (a *apiDuringTickMutator) Apply(ctx context.Context, _ string, ms []Mutation) ([]MutationResult, error) {
	a.calls++
	outcome := OutcomeCreated
	if a.calls == 1 {
		a.h.EvaluateOnce(ctx, a.p, true)
		outcome = OutcomeUnchanged
	}
	out := make([]MutationResult, 0, len(ms))
	for _, m := range ms {
		svc, name := m.Key()
		out = append(out, MutationResult{Op: m.Op, Service: svc, Name: name, Outcome: outcome})
	}
	return out, nil
}

func TestTick_KeepsALaterNextAtFromAConcurrentAPIApply(t *testing.T) {
	p := hotPolicy() // interval 10 s, cooldown 60 s
	th := newTestHarness([]*model.OPAPolicy{p}, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	mut := &apiDuringTickMutator{h: th.h, p: p}
	th.h.deps.Mutator = mut

	th.tickAt(0) // the tick computes +10 s (unchanged); the API apply inside it set +60 s
	if got := th.h.nextAt[p.ID]; !got.Equal(t0.Add(60 * time.Second)) {
		t.Fatalf("nextAt = %v, want %v: the tick's nearer time must not overwrite the API apply's cooldown", got, t0.Add(60*time.Second))
	}
	th.tickAt(10 * time.Second)
	th.tickAt(59 * time.Second)
	if mut.calls != 2 {
		t.Fatalf("mutator calls = %d before the cooldown lapsed, want 2 (the tick and the API apply)", mut.calls)
	}
	th.tickAt(60 * time.Second)
	if mut.calls != 3 {
		t.Fatalf("mutator calls = %d at +60s, want 3 (due once the cooldown lapsed)", mut.calls)
	}
}

func TestSetNextAt_KeepsTheLaterTime(t *testing.T) {
	h := New(Deps{})
	h.setNextAt("p", t0.Add(time.Minute))
	h.setNextAt("p", t0.Add(time.Second))
	if got := h.nextAt["p"]; !got.Equal(t0.Add(time.Minute)) {
		t.Fatalf("nextAt = %v, want the later %v", got, t0.Add(time.Minute))
	}
	h.setNextAt("p", t0.Add(2*time.Minute))
	if got := h.nextAt["p"]; !got.Equal(t0.Add(2 * time.Minute)) {
		t.Fatalf("nextAt = %v, want it moved forward to %v", got, t0.Add(2*time.Minute))
	}
}
