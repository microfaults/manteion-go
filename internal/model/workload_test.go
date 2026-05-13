package model

import (
	"testing"
	"time"
)

func TestAttack_Validate(t *testing.T) {
	base := func() Attack {
		return Attack{
			ID: "a1", Service: "productcatalog",
			TargetURL:    "http://productcatalog:3550/products",
			TargetMethod: "GET", Rate: 100, DurationMs: 30000,
			CreatedAt: time.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*Attack)
		wantErr bool
	}{
		{"valid attack", nil, false},
		{"missing service", func(a *Attack) { a.Service = "" }, true},
		{"missing target_url", func(a *Attack) { a.TargetURL = "" }, true},
		{"missing target_method", func(a *Attack) { a.TargetMethod = "" }, true},
		{"zero rate", func(a *Attack) { a.Rate = 0 }, true},
		{"zero duration", func(a *Attack) { a.DurationMs = 0 }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := base()
			if tt.modify != nil {
				tt.modify(&a)
			}
			err := a.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAttackResult_Validate(t *testing.T) {
	tests := []struct {
		name    string
		result  AttackResult
		wantErr bool
	}{
		{"valid", AttackResult{AttackID: "a1", Service: "frontend"}, false},
		{"missing attack_id", AttackResult{Service: "frontend"}, true},
		{"missing service", AttackResult{AttackID: "a1"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
