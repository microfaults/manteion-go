package model

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRule_Validate(t *testing.T) {
	base := func() Rule {
		return Rule{
			ID: "r1", Name: "slow-productcatalog", Service: "productcatalog",
			Enabled: true, Priority: 10,
			Action:    RuleAction{Type: "fault_spec", FaultSpecID: "f1"},
			Mode:      "inline",
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*Rule)
		wantErr bool
	}{
		{"valid with fault_spec", nil, false},
		{"valid with composition", func(r *Rule) {
			r.Action = RuleAction{Type: "fault_composition", FaultCompID: "c1"}
		}, false},
		{"both fault_spec and composition set", func(r *Rule) {
			r.Action = RuleAction{Type: "fault_spec", FaultSpecID: "f1", FaultCompID: "c1"}
		}, true},
		{"fault_spec type with empty id", func(r *Rule) {
			r.Action = RuleAction{Type: "fault_spec"}
		}, true},
		{"invalid action type", func(r *Rule) {
			r.Action = RuleAction{Type: "bad"}
		}, true},
		{"valid cachebox", func(r *Rule) {
			r.Action = RuleAction{Type: "cachebox", CacheBox: &CacheBoxRuleConfig{Mode: "replay", KeyStrategy: "exact"}}
		}, false},
		{"invalid mode", func(r *Rule) { r.Mode = "bad" }, true},
		{"missing service", func(r *Rule) { r.Service = "" }, true},
		{"background mode", func(r *Rule) { r.Mode = "background" }, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := base()
			if tt.modify != nil {
				tt.modify(&r)
			}
			err := r.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMatchCriteria_Validate(t *testing.T) {
	tests := []struct {
		name    string
		match   MatchCriteria
		wantErr bool
	}{
		{"empty (any)", MatchCriteria{}, false},
		{"ingress", MatchCriteria{InjectionPoint: "ingress"}, false},
		{"egress", MatchCriteria{InjectionPoint: "egress"}, false},
		{"transient", MatchCriteria{InjectionPoint: "transient"}, false},
		{"custom", MatchCriteria{InjectionPoint: "custom"}, false},
		{"with labels", MatchCriteria{Labels: map[string]string{"env": "prod"}}, false},
		{"invalid", MatchCriteria{InjectionPoint: "bad"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.match.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestRule_ServedPhaseIsNotOnTheWire pins that the running phase a rule is
// served through (set only by RuleRepo.ForService, read by the compiler) never
// crosses the REST boundary: it is not in a rule's JSON, and a request body
// cannot set it under either spelling.
func TestRule_ServedPhaseIsNotOnTheWire(t *testing.T) {
	out, err := json.Marshal(Rule{ID: "r1", ServedPhaseID: "phase-iso", ServedExperimentID: "exp-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, leaked := range []string{"phase-iso", "exp-1", "served"} {
		if strings.Contains(strings.ToLower(string(out)), leaked) {
			t.Errorf("marshalled rule %s contains %q", out, leaked)
		}
	}

	var in Rule
	body := `{"id":"r1","ServedPhaseID":"phase-x","served_phase_id":"phase-x","ServedExperimentID":"exp-x","served_experiment_id":"exp-x"}`
	if err := json.Unmarshal([]byte(body), &in); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if in.ServedPhaseID != "" || in.ServedExperimentID != "" {
		t.Errorf("a request body set the served pair: %+v", in)
	}
}
