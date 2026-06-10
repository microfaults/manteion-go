package model

import (
	"testing"
	"time"
)

func TestExperiment_Validate(t *testing.T) {
	base := func() Experiment {
		return Experiment{
			ID:        "e1",
			Name:      "attribution-checkout",
			Status:    "planned",
			CreatedAt: time.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*Experiment)
		wantErr bool
	}{
		{"valid", nil, false},
		{"missing id", func(e *Experiment) { e.ID = "" }, true},
		{"missing name", func(e *Experiment) { e.Name = "" }, true},
		{"invalid status", func(e *Experiment) { e.Status = "bad" }, true},
		{"completed is valid", func(e *Experiment) { e.Status = "completed" }, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := base()
			if tt.modify != nil {
				tt.modify(&e)
			}
			err := e.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExperimentPhase_Validate(t *testing.T) {
	base := func() ExperimentPhase {
		return ExperimentPhase{
			ID:             "p1",
			ExperimentID:   "e1",
			Name:           "baseline",
			Position:       0,
			Status:         "pending",
			FrozenServices: nil,
		}
	}
	tests := []struct {
		name    string
		modify  func(*ExperimentPhase)
		wantErr bool
	}{
		{"valid baseline", nil, false},
		{"missing id", func(p *ExperimentPhase) { p.ID = "" }, true},
		{"missing experiment", func(p *ExperimentPhase) { p.ExperimentID = "" }, true},
		{"missing name", func(p *ExperimentPhase) { p.Name = "" }, true},
		{"negative position", func(p *ExperimentPhase) { p.Position = -1 }, true},
		{"invalid status", func(p *ExperimentPhase) { p.Status = "bad" }, true},
		{"running is valid", func(p *ExperimentPhase) { p.Status = "running" }, false},
		{"isolation with cachebox config", func(p *ExperimentPhase) {
			p.FrozenServices = []CacheBoxConfig{{
				Service:        "productcatalog",
				Mode:           "replay",
				KeyStrategy:    "exact",
				MutationPolicy: "deny",
			}}
		}, false},
		{"invalid cachebox mode", func(p *ExperimentPhase) {
			p.FrozenServices = []CacheBoxConfig{{Service: "x", Mode: "bogus", KeyStrategy: "exact", MutationPolicy: "deny"}}
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := base()
			if tt.modify != nil {
				tt.modify(&p)
			}
			err := p.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPhaseWorkflow_Validate(t *testing.T) {
	base := PhaseWorkflow{PhaseID: "p1", WorkflowID: "w1", VUs: 10, DurationSec: 60}
	tests := []struct {
		name    string
		modify  func(*PhaseWorkflow)
		wantErr bool
	}{
		{"valid", nil, false},
		{"missing phase", func(pw *PhaseWorkflow) { pw.PhaseID = "" }, true},
		{"missing workflow", func(pw *PhaseWorkflow) { pw.WorkflowID = "" }, true},
		{"zero vus", func(pw *PhaseWorkflow) { pw.VUs = 0 }, true},
		{"negative vus", func(pw *PhaseWorkflow) { pw.VUs = -1 }, true},
		{"zero duration", func(pw *PhaseWorkflow) { pw.DurationSec = 0 }, true},
		{"negative rate", func(pw *PhaseWorkflow) { pw.RateRPS = -1 }, true},
		{"rate is optional", func(pw *PhaseWorkflow) { pw.RateRPS = 0 }, false},
		{"rate set", func(pw *PhaseWorkflow) { pw.RateRPS = 25.5 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pw := base
			if tt.modify != nil {
				tt.modify(&pw)
			}
			err := pw.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPhaseRule_Validate(t *testing.T) {
	base := PhaseRule{PhaseID: "p1", RuleID: "r1", Position: 0}
	tests := []struct {
		name    string
		modify  func(*PhaseRule)
		wantErr bool
	}{
		{"valid", nil, false},
		{"missing phase", func(pr *PhaseRule) { pr.PhaseID = "" }, true},
		{"missing rule", func(pr *PhaseRule) { pr.RuleID = "" }, true},
		{"negative position", func(pr *PhaseRule) { pr.Position = -1 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := base
			if tt.modify != nil {
				tt.modify(&pr)
			}
			err := pr.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExperimentResults_Validate(t *testing.T) {
	base := ExperimentResults{ExperimentID: "e1", PhaseCount: 3, CompletedPhaseCount: 2}
	tests := []struct {
		name    string
		modify  func(*ExperimentResults)
		wantErr bool
	}{
		{"valid", nil, false},
		{"missing experiment", func(r *ExperimentResults) { r.ExperimentID = "" }, true},
		{"negative phase count", func(r *ExperimentResults) { r.PhaseCount = -1 }, true},
		{"completed > total", func(r *ExperimentResults) { r.CompletedPhaseCount = 5 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := base
			if tt.modify != nil {
				tt.modify(&r)
			}
			err := r.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
