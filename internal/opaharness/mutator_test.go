package opaharness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

type fakeRules struct {
	rules                     []*model.Rule
	creates, updates, deletes int
	lists                     int
	listErrAfter              int   // List fails once it has answered this many times (0: never)
	createErr                 error // the next Create fails with it, once
}

func (f *fakeRules) List(_ context.Context) ([]*model.Rule, error) {
	if f.listErrAfter > 0 && f.lists >= f.listErrAfter {
		return nil, errors.New("db down")
	}
	f.lists++
	out := make([]*model.Rule, 0, len(f.rules))
	for _, r := range f.rules {
		cp := *r
		out = append(out, &cp)
	}
	return out, nil
}

func (f *fakeRules) Create(_ context.Context, r *model.Rule) error {
	if err := f.createErr; err != nil {
		f.createErr = nil
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	cp := *r
	f.rules = append(f.rules, &cp)
	f.creates++
	return nil
}

func (f *fakeRules) Update(_ context.Context, r *model.Rule) error {
	if err := r.Validate(); err != nil {
		return err
	}
	for i, existing := range f.rules {
		if existing.ID == r.ID {
			cp := *r
			f.rules[i] = &cp
			f.updates++
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeRules) Delete(_ context.Context, id string) error {
	for i, existing := range f.rules {
		if existing.ID == id {
			f.rules = append(f.rules[:i], f.rules[i+1:]...)
			f.deletes++
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeRules) get(service, name string) *model.Rule {
	for _, r := range f.rules {
		if r.Service == service && r.Name == name {
			return r
		}
	}
	return nil
}

type fakeSpecs struct {
	specs        map[string]*model.FaultSpec
	compositions []*model.FaultComposition // with members; ListCompositions strips them like the store
	updates      int
	updateErr    error
}

// ListCompositions mirrors (*store.FaultRepo).ListCompositions: members are
// not loaded.
func (f *fakeSpecs) ListCompositions(_ context.Context) ([]*model.FaultComposition, error) {
	out := make([]*model.FaultComposition, 0, len(f.compositions))
	for _, c := range f.compositions {
		cp := *c
		cp.Members = nil
		out = append(out, &cp)
	}
	return out, nil
}

func (f *fakeSpecs) GetComposition(_ context.Context, id string) (*model.FaultComposition, error) {
	for _, c := range f.compositions {
		if c.ID == id {
			cp := *c
			cp.Members = append([]model.FaultCompositionMember(nil), c.Members...)
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (f *fakeSpecs) GetSpec(_ context.Context, id string) (*model.FaultSpec, error) {
	if s, ok := f.specs[id]; ok {
		cp := *s
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeSpecs) UpdateSpec(_ context.Context, s *model.FaultSpec) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	if err := s.Validate(); err != nil {
		return err
	}
	cp := *s
	f.specs[s.ID] = &cp
	f.updates++
	return nil
}

var fixedTime = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

func newTestMutator(rules *fakeRules, specs *fakeSpecs) *RuleMutator {
	m := NewRuleMutator(rules, specs)
	m.newID = func(prefix string) string { return prefix + "-fixed" }
	m.now = func() time.Time { return fixedTime }
	return m
}

func desiredRule() *model.Rule {
	return &model.Rule{
		Name: "checkout-egress-latency", Service: "checkout", Enabled: true, Priority: 100, Mode: "inline",
		Match:  model.MatchCriteria{InjectionPoint: "egress", Labels: map[string]string{"atropos.http.path": "/cart/checkout"}},
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-lat"},
	}
}

func latencySpec(id string) *model.FaultSpec {
	return &model.FaultSpec{ID: id, Name: id, Category: "inline", FaultType: "latency", Host: "process", Params: json.RawMessage(`{"delay":"200ms"}`)}
}

func apply(t *testing.T, m *RuleMutator, ms ...Mutation) []MutationResult {
	t.Helper()
	res, err := m.Apply(context.Background(), "opa-1", ms)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(res) != len(ms) {
		t.Fatalf("results = %d, want one per mutation (%d)", len(res), len(ms))
	}
	return res
}

func TestRuleMutator_UpsertCreatesThenUnchangedThenUpdates(t *testing.T) {
	rules := &fakeRules{}
	m := newTestMutator(rules, &fakeSpecs{})

	res := apply(t, m, Mutation{Op: OpUpsertRule, Rule: desiredRule()})
	if res[0].Outcome != OutcomeCreated || res[0].RuleID != "rule-fixed" || rules.creates != 1 {
		t.Fatalf("first upsert = %+v (creates %d), want created rule-fixed", res[0], rules.creates)
	}
	stored := rules.get("checkout", "checkout-egress-latency")
	if stored == nil || stored.StartPolicy != "deduplicate_by_rule" || !stored.CreatedAt.Equal(fixedTime) {
		t.Fatalf("stored rule = %+v, want defaults applied and timestamps set", stored)
	}

	res = apply(t, m, Mutation{Op: OpUpsertRule, Rule: desiredRule()})
	if res[0].Outcome != OutcomeUnchanged || rules.updates != 0 || res[0].RuleID != "rule-fixed" {
		t.Fatalf("second upsert = %+v (updates %d), want unchanged, no write", res[0], rules.updates)
	}

	changed := desiredRule()
	changed.Priority = 200
	changed.ID = "rule-from-policy" // server-owned: must be ignored
	res = apply(t, m, Mutation{Op: OpUpsertRule, Rule: changed})
	stored = rules.get("checkout", "checkout-egress-latency")
	if res[0].Outcome != OutcomeUpdated || rules.updates != 1 || stored.Priority != 200 || stored.ID != "rule-fixed" ||
		!stored.CreatedAt.Equal(fixedTime) || len(rules.rules) != 1 {
		t.Fatalf("third upsert = %+v, stored %+v, want updated in place keeping id and created_at", res[0], stored)
	}
}

func TestRuleMutator_UpsertInvalidRuleFails(t *testing.T) {
	rules := &fakeRules{}
	m := newTestMutator(rules, &fakeSpecs{})
	bad := desiredRule()
	bad.Mode = "sideways"
	res := apply(t, m, Mutation{Op: OpUpsertRule, Rule: bad})
	if res[0].Outcome != OutcomeFailed || !strings.Contains(res[0].Error, "invalid mode") || rules.creates != 0 {
		t.Fatalf("result = %+v (creates %d), want failed with the validation error and no write", res[0], rules.creates)
	}
}

func TestRuleMutator_SetEnabled(t *testing.T) {
	existing := desiredRule()
	existing.ID = "rule-1"
	rules := &fakeRules{rules: []*model.Rule{existing}}
	m := newTestMutator(rules, &fakeSpecs{})
	off, on := false, true

	res := apply(t, m, Mutation{Op: OpSetEnabled, Service: "checkout", Name: existing.Name, Enabled: &off})
	if res[0].Outcome != OutcomeUpdated || rules.get("checkout", existing.Name).Enabled || rules.updates != 1 {
		t.Fatalf("disable = %+v, want updated and persisted disabled", res[0])
	}
	res = apply(t, m, Mutation{Op: OpSetEnabled, Service: "checkout", Name: existing.Name, Enabled: &off})
	if res[0].Outcome != OutcomeUnchanged || rules.updates != 1 {
		t.Fatalf("disable again = %+v, want unchanged", res[0])
	}
	res = apply(t, m, Mutation{Op: OpSetEnabled, Service: "checkout", Name: "missing", Enabled: &on})
	if res[0].Outcome != OutcomeFailed || !strings.Contains(res[0].Error, "not found") {
		t.Fatalf("unknown rule = %+v, want failed not found", res[0])
	}
}

func TestRuleMutator_DeleteRule(t *testing.T) {
	existing := desiredRule()
	existing.ID = "rule-1"
	rules := &fakeRules{rules: []*model.Rule{existing}}
	m := newTestMutator(rules, &fakeSpecs{})

	res := apply(t, m, Mutation{Op: OpDeleteRule, Service: "checkout", Name: existing.Name})
	if res[0].Outcome != OutcomeDeleted || res[0].RuleID != "rule-1" || rules.deletes != 1 || len(rules.rules) != 0 {
		t.Fatalf("delete = %+v, want deleted", res[0])
	}
	res = apply(t, m, Mutation{Op: OpDeleteRule, Service: "checkout", Name: existing.Name})
	if res[0].Outcome != OutcomeUnchanged || rules.deletes != 1 {
		t.Fatalf("delete again = %+v, want unchanged (idempotent)", res[0])
	}
}

func TestRuleMutator_SetParams(t *testing.T) {
	existing := desiredRule()
	existing.ID = "rule-1"
	rules := &fakeRules{rules: []*model.Rule{existing}}
	specs := &fakeSpecs{specs: map[string]*model.FaultSpec{"spec-lat": latencySpec("spec-lat")}}
	m := newTestMutator(rules, specs)
	key := Mutation{Op: OpSetParams, Service: "checkout", Name: existing.Name}

	mut := key
	mut.Params = json.RawMessage(`{"delay":"500ms"}`)
	res := apply(t, m, mut)
	if res[0].Outcome != OutcomeUpdated || specs.updates != 1 || rules.updates != 1 ||
		string(specs.specs["spec-lat"].Params) != `{"delay":"500ms"}` {
		t.Fatalf("set_params = %+v (spec updates %d, rule updates %d), want spec updated and the rule re-saved", res[0], specs.updates, rules.updates)
	}
	mut.Params = json.RawMessage(`{"delay": "500ms"}`) // same value, different whitespace
	if res = apply(t, m, mut); res[0].Outcome != OutcomeUnchanged || specs.updates != 1 || rules.updates != 1 {
		t.Fatalf("same params = %+v, want unchanged and no writes", res[0])
	}

	mut.Params = json.RawMessage(`{"delay":"500ms","bogus":1}`)
	if res = apply(t, m, mut); res[0].Outcome != OutcomeFailed || res[0].Error == "" || specs.updates != 1 {
		t.Fatalf("invalid params = %+v, want failed by the fault catalog and no write", res[0])
	}

	other := desiredRule()
	other.ID, other.Name, other.Service = "rule-2", "other", "frontend"
	rules.rules = append(rules.rules, other) // shares spec-lat
	mut.Params = json.RawMessage(`{"delay":"900ms"}`)
	if res = apply(t, m, mut); res[0].Outcome != OutcomeRefused || !strings.Contains(res[0].Error, "shared") || specs.updates != 1 {
		t.Fatalf("shared spec = %+v, want refused", res[0])
	}

	cb := &model.Rule{ID: "rule-cb", Name: "cb", Service: "checkout", Mode: "inline",
		Action: model.RuleAction{Type: "cachebox", CacheBox: &model.CacheBoxRuleConfig{Mode: "replay", KeyStrategy: "exact"}}}
	rules.rules = append(rules.rules, cb)
	if res = apply(t, m, Mutation{Op: OpSetParams, Service: "checkout", Name: "cb", Params: json.RawMessage(`{}`)}); res[0].Outcome != OutcomeFailed || !strings.Contains(res[0].Error, "fault_spec") {
		t.Fatalf("set_params on a cachebox rule = %+v, want failed", res[0])
	}
}

func TestRuleMutator_AppliesInOrderAndContinuesAfterFailure(t *testing.T) {
	rules := &fakeRules{}
	m := newTestMutator(rules, &fakeSpecs{})
	off := false
	res := apply(t, m,
		Mutation{Op: OpSetEnabled, Service: "checkout", Name: "absent", Enabled: &off},                  // fails
		Mutation{Op: OpUpsertRule, Rule: desiredRule()},                                                 // creates
		Mutation{Op: OpSetEnabled, Service: "checkout", Name: "checkout-egress-latency", Enabled: &off}, // sees the create
	)
	if res[0].Outcome != OutcomeFailed || res[1].Outcome != OutcomeCreated || res[2].Outcome != OutcomeUpdated {
		t.Fatalf("outcomes = %s %s %s, want failed created updated", res[0].Outcome, res[1].Outcome, res[2].Outcome)
	}
}

func TestRuleMutator_SetParamsRefusesSpecUsedByComposition(t *testing.T) {
	setParams := Mutation{Op: OpSetParams, Service: "checkout", Name: "checkout-egress-latency", Params: json.RawMessage(`{"delay":"900ms"}`)}
	comp := func(id string, members ...model.FaultCompositionMember) *model.FaultComposition {
		return &model.FaultComposition{ID: id, Name: id, ExecutionMode: "parallel", Members: members}
	}
	tests := []struct {
		name         string
		compositions []*model.FaultComposition
		wantOutcome  string
		wantError    string
	}{
		{"a composition uses the spec", []*model.FaultComposition{
			comp("comp-z-other", model.FaultCompositionMember{FaultSpecID: "spec-cpu"}),
			comp("comp-b-direct", model.FaultCompositionMember{FaultSpecID: "spec-cpu"}, model.FaultCompositionMember{FaultSpecID: "spec-lat"}),
		}, OutcomeRefused, "fault spec spec-lat is used by composition comp-b-direct; give the rule a dedicated spec"},
		{"a nested composition uses the spec", []*model.FaultComposition{
			comp("comp-b-inner", model.FaultCompositionMember{FaultSpecID: "spec-lat"}),
			comp("comp-a-outer", model.FaultCompositionMember{ChildCompositionID: "comp-b-inner"}),
		}, OutcomeRefused, "fault spec spec-lat is used by composition comp-a-outer; give the rule a dedicated spec"},
		{"a composition cycle not using the spec", []*model.FaultComposition{
			comp("comp-a", model.FaultCompositionMember{ChildCompositionID: "comp-b"}),
			comp("comp-b", model.FaultCompositionMember{ChildCompositionID: "comp-a"}, model.FaultCompositionMember{FaultSpecID: "spec-cpu"}),
		}, OutcomeUpdated, ""},
		{"no composition uses the spec", []*model.FaultComposition{
			comp("comp-other", model.FaultCompositionMember{FaultSpecID: "spec-cpu"}),
		}, OutcomeUpdated, ""},
		{"no compositions", nil, OutcomeUpdated, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			existing := desiredRule()
			existing.ID = "rule-1"
			rules := &fakeRules{rules: []*model.Rule{existing}}
			specs := &fakeSpecs{specs: map[string]*model.FaultSpec{"spec-lat": latencySpec("spec-lat")}, compositions: tc.compositions}
			res := apply(t, newTestMutator(rules, specs), setParams)
			if res[0].Outcome != tc.wantOutcome || res[0].Error != tc.wantError {
				t.Fatalf("result = %+v, want outcome %q error %q", res[0], tc.wantOutcome, tc.wantError)
			}
			if tc.wantOutcome == OutcomeRefused {
				if specs.updates != 0 || rules.updates != 0 || string(specs.specs["spec-lat"].Params) != `{"delay":"200ms"}` {
					t.Errorf("refused set_params wrote: spec updates %d, rule updates %d, params %s", specs.updates, rules.updates, specs.specs["spec-lat"].Params)
				}
			} else if specs.updates != 1 || rules.updates != 1 {
				t.Errorf("set_params: spec updates %d, rule updates %d, want 1 and 1", specs.updates, rules.updates)
			}
		})
	}
}

func TestRuleMutator_ListErrorReturnsGatheredResults(t *testing.T) {
	rules := &fakeRules{listErrAfter: 1}
	m := newTestMutator(rules, &fakeSpecs{})
	other := desiredRule()
	other.Name = "second"
	res, err := m.Apply(context.Background(), "opa-1", []Mutation{
		{Op: OpUpsertRule, Rule: desiredRule()},
		{Op: OpUpsertRule, Rule: other},
	})
	if err == nil || err.Error() != "list rules: db down" {
		t.Fatalf("err = %v, want %q", err, "list rules: db down")
	}
	if len(res) != 1 || res[0].Outcome != OutcomeCreated || res[0].Name != "checkout-egress-latency" || rules.creates != 1 {
		t.Fatalf("results = %+v (creates %d), want the first mutation's result only, the run aborted before the second", res, rules.creates)
	}
}

func TestRuleMutator_StoreWriteErrorFailsAndContinues(t *testing.T) {
	rules := &fakeRules{createErr: errors.New("insert rule: connection reset")}
	specs := &fakeSpecs{specs: map[string]*model.FaultSpec{"spec-lat": latencySpec("spec-lat")}, updateErr: errors.New("update spec: connection reset")}
	m := newTestMutator(rules, specs)
	other := desiredRule()
	other.Name = "second"
	res := apply(t, m,
		Mutation{Op: OpUpsertRule, Rule: desiredRule()},                                                           // Create fails
		Mutation{Op: OpUpsertRule, Rule: other},                                                                   // still applied
		Mutation{Op: OpSetParams, Service: "checkout", Name: "second", Params: json.RawMessage(`{"delay":"1s"}`)}, // UpdateSpec fails
		Mutation{Op: OpDeleteRule, Service: "checkout", Name: "second"},                                           // still applied
	)
	if res[0].Outcome != OutcomeFailed || res[0].Error != "insert rule: connection reset" {
		t.Errorf("results[0] = %+v, want failed with the store's error", res[0])
	}
	if res[1].Outcome != OutcomeCreated || rules.creates != 1 {
		t.Errorf("results[1] = %+v (creates %d), want the next mutation created", res[1], rules.creates)
	}
	if res[2].Outcome != OutcomeFailed || res[2].Error != "update spec: connection reset" || rules.updates != 0 {
		t.Errorf("results[2] = %+v (rule updates %d), want failed with the spec store's error and no rule re-save", res[2], rules.updates)
	}
	if res[3].Outcome != OutcomeDeleted || rules.deletes != 1 || rules.get("checkout", "second") != nil {
		t.Errorf("results[3] = %+v, want the delete after the failure applied", res[3])
	}
}
