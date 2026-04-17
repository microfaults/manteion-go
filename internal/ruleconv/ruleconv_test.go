package ruleconv

import (
	"encoding/json"
	"testing"

	"manteion-go/internal/model"
)

type mapResolver map[string]*model.FaultSpec

func (m mapResolver) GetFaultSpec(id string) (*model.FaultSpec, error) {
	return m[id], nil
}

func TestCompileRules(t *testing.T) {
	specs := mapResolver{
		"spec-latency": {
			ID:         "spec-latency",
			Name:       "200ms latency",
			Category:   "inline",
			FaultType:  "latency",
			Config:     json.RawMessage(`{"delay":"200ms"}`),
			DurationMs: 0,
		},
	}

	rules := []*model.Rule{
		{
			ID:          "rule-1",
			Name:        "inject-latency",
			Service:     "productcatalog",
			Enabled:     true,
			Priority:    10,
			Match:       model.MatchCriteria{InjectionPoint: "egress", Labels: map[string]string{"svc": "cart"}},
			FaultSpecID: "spec-latency",
			Mode:        "inline",
		},
	}

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
	if cr.InjectionPoint != "egress" {
		t.Errorf("InjectionPoint: got %q", cr.InjectionPoint)
	}
	if cr.Mode != "inline" {
		t.Errorf("Mode: got %q", cr.Mode)
	}
	if cr.Priority != 10 {
		t.Errorf("Priority: got %d", cr.Priority)
	}
	if cr.Fault == nil {
		t.Fatal("expected Fault to be set")
	}
	if cr.Fault.Category != "inline" || cr.Fault.FaultType != "latency" {
		t.Errorf("Fault: got %s:%s", cr.Fault.Category, cr.Fault.FaultType)
	}

	// Verify JSON roundtrip works (the whole point of CompiledRule).
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
	specs := mapResolver{}
	rules := []*model.Rule{
		{
			ID:          "rule-1",
			Name:        "dangling",
			FaultSpecID: "nonexistent",
			Mode:        "inline",
		},
	}

	_, err := CompileRules(rules, specs)
	if err == nil {
		t.Fatal("expected error for dangling fault spec")
	}
}

func TestCompileRules_Empty(t *testing.T) {
	specs := mapResolver{}
	compiled, err := CompileRules(nil, specs)
	if err != nil {
		t.Fatalf("CompileRules: %v", err)
	}
	if compiled == nil {
		t.Fatal("expected non-nil empty slice")
	}
	if len(compiled) != 0 {
		t.Fatalf("expected 0, got %d", len(compiled))
	}
}
