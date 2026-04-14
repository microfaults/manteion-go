package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestAttack_Validate(t *testing.T) {
	base := func() Attack {
		return Attack{
			ID: "a1", Service: "productcatalog", Role: "primary",
			TargetURL:    "http://productcatalog:3550/products",
			TargetMethod: "GET", Rate: 100, DurationMs: 30000,
			Status: "pending", CreatedAt: time.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*Attack)
		wantErr bool
	}{
		{"valid primary attack", nil, false},
		{"valid background attack", func(a *Attack) { a.Role = "background" }, false},
		{"missing service", func(a *Attack) { a.Service = "" }, true},
		{"invalid role", func(a *Attack) { a.Role = "other" }, true},
		{"missing target_url", func(a *Attack) { a.TargetURL = "" }, true},
		{"missing target_method", func(a *Attack) { a.TargetMethod = "" }, true},
		{"zero rate", func(a *Attack) { a.Rate = 0 }, true},
		{"zero duration", func(a *Attack) { a.DurationMs = 0 }, true},
		{"invalid status", func(a *Attack) { a.Status = "bad" }, true},
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

func TestWorkload_Validate(t *testing.T) {
	base := func() Workload {
		return Workload{
			ID: "w1", Name: "checkout-50rps", FlowID: "checkout",
			PersonaID: "aggressive", VUs: 10, Status: "pending",
			CreatedAt: time.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*Workload)
		wantErr bool
	}{
		{"valid", nil, false},
		{"missing flow_id", func(w *Workload) { w.FlowID = "" }, true},
		{"missing persona_id", func(w *Workload) { w.PersonaID = "" }, true},
		{"zero vus", func(w *Workload) { w.VUs = 0 }, true},
		{"invalid status", func(w *Workload) { w.Status = "bad" }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := base()
			if tt.modify != nil {
				tt.modify(&w)
			}
			err := w.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFlow_Validate(t *testing.T) {
	tests := []struct {
		name    string
		flow    Flow
		wantErr bool
	}{
		{
			"valid",
			Flow{ID: "f1", Name: "browse", Targets: []string{"frontend"}, Steps: json.RawMessage(`[{}]`)},
			false,
		},
		{"missing targets", Flow{ID: "f1", Name: "browse", Steps: json.RawMessage(`[{}]`)}, true},
		{"missing steps", Flow{ID: "f1", Name: "browse", Targets: []string{"frontend"}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.flow.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPersona_Validate(t *testing.T) {
	tests := []struct {
		name    string
		persona Persona
		wantErr bool
	}{
		{"valid", Persona{ID: "p1", Name: "aggressive", ThinkTimeMin: 100, ThinkTimeMax: 500}, false},
		{"min > max", Persona{ID: "p1", Name: "test", ThinkTimeMin: 500, ThinkTimeMax: 100}, true},
		{"negative min", Persona{ID: "p1", Name: "test", ThinkTimeMin: -1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.persona.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
