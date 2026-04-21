package model

import (
	"testing"
	"time"
)

func TestRule_Validate(t *testing.T) {
	base := func() Rule {
		return Rule{
			ID: "r1", Name: "slow-productcatalog", Service: "productcatalog",
			Enabled: true, Priority: 10, FaultSpecID: "f1", Mode: "inline",
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
			r.FaultSpecID = ""
			r.FaultCompositionID = "c1"
		}, false},
		{"both fault_spec and composition", func(r *Rule) {
			r.FaultCompositionID = "c1"
		}, true},
		{"neither fault_spec nor composition", func(r *Rule) {
			r.FaultSpecID = ""
		}, true},
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
