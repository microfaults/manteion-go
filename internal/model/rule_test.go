package model

import (
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
