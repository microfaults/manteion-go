package ruleconv

import (
	"encoding/json"
	"testing"

	"manteion-go/internal/model"
)

type mapSpecResolver map[string]*model.FaultSpec

func (m mapSpecResolver) GetFaultSpec(id string) (*model.FaultSpec, error) {
	return m[id], nil
}

type mapCompResolver map[string]*model.FaultComposition

func (m mapCompResolver) GetFaultComposition(id string) (*model.FaultComposition, error) {
	return m[id], nil
}

func TestCompileRules_FaultSpec(t *testing.T) {
	specs := mapSpecResolver{
		"spec-latency": {
			ID:        "spec-latency",
			Name:      "200ms latency",
			Category:  "inline",
			FaultType: "latency",
			Config:    json.RawMessage(`{"delay":"200ms"}`),
		},
	}

	rules := []*model.Rule{{
		ID:          "rule-1",
		Name:        "inject-latency",
		Service:     "productcatalog",
		Enabled:     true,
		Priority:    10,
		Match:       model.MatchCriteria{InjectionPoint: "egress", Labels: map[string]string{"svc": "cart"}},
		FaultSpecID: "spec-latency",
		Mode:        "inline",
	}}

	compiled, err := CompileRules(rules, specs)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}
	if len(compiled) != 1 {
		t.Fatalf("expected 1 compiled rule, got %d", len(compiled))
	}

	cr := compiled[0]
	if cr.Name != "inject-latency" {
		t.Errorf("Name: got %q", cr.Name)
	}
	if cr.Fault == nil {
		t.Fatal("expected Fault to be set")
	}
	if cr.Fault.Category != "inline" || cr.Fault.FaultType != "latency" {
		t.Errorf("Fault: got %s:%s", cr.Fault.Category, cr.Fault.FaultType)
	}

	// Verify JSON roundtrip.
	b, err := json.Marshal(compiled)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var roundtrip []CompiledRule
	if err := json.Unmarshal(b, &roundtrip); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if roundtrip[0].Fault.FaultType != "latency" {
		t.Errorf("roundtrip lost fault type")
	}
}

func TestCompileRule_DanglingSpec(t *testing.T) {
	specs := mapSpecResolver{}
	rules := []*model.Rule{{
		ID:          "rule-1",
		Name:        "dangling",
		FaultSpecID: "nonexistent",
		Mode:        "inline",
	}}

	_, err := CompileRules(rules, specs)
	if err == nil {
		t.Fatal("expected error for dangling fault spec")
	}
}

func TestCompileRules_Empty(t *testing.T) {
	specs := mapSpecResolver{}
	compiled, err := CompileRules(nil, specs)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}
	if compiled == nil || len(compiled) != 0 {
		t.Fatal("expected non-nil empty slice")
	}
}

func TestCompileRule_Composition(t *testing.T) {
	specs := mapSpecResolver{
		"spec-latency": {
			ID: "spec-latency", Name: "latency", Category: "inline",
			FaultType: "latency", Config: json.RawMessage(`{"delay":"100ms"}`),
			DurationMs: 100,
		},
		"spec-error": {
			ID: "spec-error", Name: "error", Category: "inline",
			FaultType: "error", Config: json.RawMessage(`{"status_code":500,"message":"fail"}`),
		},
	}

	comps := mapCompResolver{
		"comp-chaos": {
			ID: "comp-chaos", Name: "chaos-combo", ExecutionMode: "sequential",
			Members: []model.FaultCompositionMember{
				{FaultSpecID: "spec-latency"},
				{FaultSpecID: "spec-error"},
			},
		},
	}

	rules := []*model.Rule{{
		ID:                 "rule-comp",
		Name:               "chaos-rule",
		FaultCompositionID: "comp-chaos",
		Mode:               "inline",
	}}

	compiled, err := CompileRules(rules, specs, comps)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}
	if len(compiled) != 1 {
		t.Fatalf("expected 1 compiled rule, got %d", len(compiled))
	}

	cr := compiled[0]
	if cr.Composition == nil {
		t.Fatal("expected Composition to be set")
	}
	if cr.Fault != nil {
		t.Error("expected Fault to be nil for composition rule")
	}
	if cr.Composition.ExecutionMode != "sequential" {
		t.Errorf("ExecutionMode: %q", cr.Composition.ExecutionMode)
	}
	if len(cr.Composition.Members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(cr.Composition.Members))
	}

	m0 := cr.Composition.Members[0]
	if m0.Fault == nil || m0.Fault.FaultType != "latency" {
		t.Errorf("member[0]: expected latency fault, got %+v", m0.Fault)
	}
	m1 := cr.Composition.Members[1]
	if m1.Fault == nil || m1.Fault.FaultType != "error" {
		t.Errorf("member[1]: expected error fault, got %+v", m1.Fault)
	}

	// JSON roundtrip.
	b, err := json.Marshal(compiled)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var rt []CompiledRule
	if err := json.Unmarshal(b, &rt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rt[0].Composition.Members[0].Fault.FaultType != "latency" {
		t.Error("composition lost in roundtrip")
	}
}

func TestCompileRule_NestedComposition(t *testing.T) {
	specs := mapSpecResolver{
		"spec-a": {ID: "spec-a", Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"50ms"}`)},
		"spec-b": {ID: "spec-b", Category: "inline", FaultType: "error", Config: json.RawMessage(`{"status_code":500}`)},
		"spec-c": {ID: "spec-c", Category: "inline", FaultType: "hang", Config: json.RawMessage(`{"duration":"1s"}`)},
	}

	comps := mapCompResolver{
		"child-comp": {
			ID: "child-comp", Name: "child", ExecutionMode: "parallel",
			Members: []model.FaultCompositionMember{
				{FaultSpecID: "spec-a"},
				{FaultSpecID: "spec-b"},
			},
		},
		"parent-comp": {
			ID: "parent-comp", Name: "parent", ExecutionMode: "sequential",
			Members: []model.FaultCompositionMember{
				{ChildCompositionID: "child-comp"},
				{FaultSpecID: "spec-c"},
			},
		},
	}

	rules := []*model.Rule{{
		ID: "rule-nested", Name: "nested", FaultCompositionID: "parent-comp", Mode: "inline",
	}}

	compiled, err := CompileRules(rules, specs, comps)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}

	comp := compiled[0].Composition
	if comp.Members[0].Composition == nil {
		t.Fatal("expected nested composition in member[0]")
	}
	if comp.Members[0].Composition.ExecutionMode != "parallel" {
		t.Errorf("nested ExecutionMode: %q", comp.Members[0].Composition.ExecutionMode)
	}
	if comp.Members[1].Fault == nil || comp.Members[1].Fault.FaultType != "hang" {
		t.Errorf("member[1] expected hang fault")
	}
}

func TestCompileRule_CompositionDanglingSpec(t *testing.T) {
	specs := mapSpecResolver{}
	comps := mapCompResolver{
		"comp-bad": {
			ID: "comp-bad", Name: "bad", ExecutionMode: "parallel",
			Members: []model.FaultCompositionMember{
				{FaultSpecID: "nonexistent"},
				{FaultSpecID: "also-nonexistent"},
			},
		},
	}

	rules := []*model.Rule{{
		ID: "rule-bad", Name: "bad", FaultCompositionID: "comp-bad", Mode: "inline",
	}}

	_, err := CompileRules(rules, specs, comps)
	if err == nil {
		t.Fatal("expected error for dangling spec in composition")
	}
}

func TestCompileRule_CompositionWithDirection(t *testing.T) {
	specs := mapSpecResolver{
		"spec-net": {
			ID: "spec-net", Category: "network", FaultType: "latency",
			Config: json.RawMessage(`{"delay":"100ms"}`),
		},
		"spec-throttle": {
			ID: "spec-throttle", Category: "network", FaultType: "throttle",
			Config: json.RawMessage(`{"bytes_per_sec":1024}`),
		},
	}

	comps := mapCompResolver{
		"comp-net": {
			ID: "comp-net", Name: "net-combo", ExecutionMode: "parallel",
			Members: []model.FaultCompositionMember{
				{FaultSpecID: "spec-net", Direction: "upstream"},
				{FaultSpecID: "spec-throttle", Direction: "downstream"},
			},
		},
	}

	rules := []*model.Rule{{
		ID: "rule-net", Name: "net", FaultCompositionID: "comp-net", Mode: "inline",
	}}

	compiled, err := CompileRules(rules, specs, comps)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}

	members := compiled[0].Composition.Members
	if members[0].Direction != "upstream" {
		t.Errorf("member[0] direction: %q", members[0].Direction)
	}
	if members[1].Direction != "downstream" {
		t.Errorf("member[1] direction: %q", members[1].Direction)
	}
}
