package model

import (
	"testing"
	"time"
)

func TestExperiment_Validate(t *testing.T) {
	base := func() Experiment {
		return Experiment{
			ID: "e1", Name: "attribution-checkout",
			PrimaryWorkflowID: "w1", Status: "planned", CreatedAt: time.Now(),
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
		{"missing primary_workflow_id", func(e *Experiment) { e.PrimaryWorkflowID = "" }, true},
		{"invalid status", func(e *Experiment) { e.Status = "bad" }, true},
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

func TestExperimentRun_Validate(t *testing.T) {
	frozenProductcatalog := CacheBoxConfig{
		Service: "productcatalog", Mode: "replay",
		KeyStrategy: "exact", MutationPolicy: "deny",
	}

	tests := []struct {
		name    string
		run     ExperimentRun
		wantErr bool
	}{
		{
			"valid baseline",
			ExperimentRun{
				ID: "r1", ExperimentID: "e1", RunType: "baseline",
				Status: "pending", CreatedAt: time.Now(),
			},
			false,
		},
		{
			"valid isolation",
			ExperimentRun{
				ID: "r2", ExperimentID: "e1", RunType: "isolation",
				FrozenServices: []CacheBoxConfig{frozenProductcatalog},
				Status:         "pending", CreatedAt: time.Now(),
			},
			false,
		},
		{
			"valid combination",
			ExperimentRun{
				ID: "r3", ExperimentID: "e1", RunType: "combination",
				FrozenServices: []CacheBoxConfig{
					frozenProductcatalog,
					{Service: "currency", Mode: "replay", KeyStrategy: "exact_with_host", MutationPolicy: "deny"},
				},
				Status: "completed", CreatedAt: time.Now(),
			},
			false,
		},
		{
			"baseline with frozen services",
			ExperimentRun{
				ID: "r4", ExperimentID: "e1", RunType: "baseline",
				FrozenServices: []CacheBoxConfig{frozenProductcatalog},
				Status:         "pending",
			},
			true,
		},
		{
			"isolation without frozen services",
			ExperimentRun{
				ID: "r5", ExperimentID: "e1", RunType: "isolation",
				Status: "pending",
			},
			true,
		},
		{
			"invalid run_type",
			ExperimentRun{
				ID: "r6", ExperimentID: "e1", RunType: "bad", Status: "pending",
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestContributionResult_Validate(t *testing.T) {
	base := func() ContributionResult {
		return ContributionResult{
			ID: "cr1", ExperimentID: "e1", Service: "productcatalog",
			Workflow: "checkout", CacheBoxMode: "replay",
			BaselineRunID: "r1", IsolationRunID: "r2",
			DeltaP50Us: 5000, DeltaP95Us: 12000, DeltaP99Us: 25000,
		}
	}

	tests := []struct {
		name    string
		modify  func(*ContributionResult)
		wantErr bool
	}{
		{"valid replay", nil, false},
		{"valid replay_with_delay", func(c *ContributionResult) {
			c.CacheBoxMode = "replay_with_delay"
		}, false},
		{"invalid cachebox_mode", func(c *ContributionResult) {
			c.CacheBoxMode = "passthrough"
		}, true},
		{"missing service", func(c *ContributionResult) { c.Service = "" }, true},
		{"missing workflow", func(c *ContributionResult) { c.Workflow = "" }, true},
		{"missing baseline_run_id", func(c *ContributionResult) { c.BaselineRunID = "" }, true},
		{"missing isolation_run_id", func(c *ContributionResult) { c.IsolationRunID = "" }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base()
			if tt.modify != nil {
				tt.modify(&c)
			}
			err := c.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestContributionResult_DeltaComputation(t *testing.T) {
	baseline := WorkflowRunResult{
		ID: "wr1", ExperimentRunID: "r1", Workflow: "checkout",
		LatencyP50Us: 20000, LatencyP95Us: 45000, LatencyP99Us: 80000,
	}
	isolation := WorkflowRunResult{
		ID: "wr2", ExperimentRunID: "r2", Workflow: "checkout",
		LatencyP50Us: 15000, LatencyP95Us: 33000, LatencyP99Us: 55000,
	}

	cr := ContributionResult{
		ID: "cr1", ExperimentID: "e1", Service: "productcatalog",
		Workflow: "checkout", CacheBoxMode: "replay",
		BaselineRunID: baseline.ExperimentRunID, IsolationRunID: isolation.ExperimentRunID,
		DeltaP50Us: baseline.LatencyP50Us - isolation.LatencyP50Us,
		DeltaP95Us: baseline.LatencyP95Us - isolation.LatencyP95Us,
		DeltaP99Us: baseline.LatencyP99Us - isolation.LatencyP99Us,
	}

	if cr.DeltaP50Us != 5000 {
		t.Errorf("DeltaP50Us = %d, want 5000", cr.DeltaP50Us)
	}
	if cr.DeltaP95Us != 12000 {
		t.Errorf("DeltaP95Us = %d, want 12000", cr.DeltaP95Us)
	}
	if cr.DeltaP99Us != 25000 {
		t.Errorf("DeltaP99Us = %d, want 25000", cr.DeltaP99Us)
	}
}

func TestContributionResult_ContentionVsIntrinsic(t *testing.T) {
	// Two ContributionResults for the same service, different cache-box modes.
	// replay: removes ALL contribution (contention + intrinsic)
	// replay_with_delay: removes contention only (preserves intrinsic timing)
	totalDelta := ContributionResult{
		ID: "cr1", ExperimentID: "e1", Service: "productcatalog",
		Workflow: "checkout", CacheBoxMode: "replay",
		BaselineRunID: "r1", IsolationRunID: "r2",
		DeltaP99Us: 25000, // total contribution
	}
	contentionDelta := ContributionResult{
		ID: "cr2", ExperimentID: "e1", Service: "productcatalog",
		Workflow: "checkout", CacheBoxMode: "replay_with_delay",
		BaselineRunID: "r1", IsolationRunID: "r3",
		DeltaP99Us: 18000, // contention-only contribution
	}

	intrinsicCost := totalDelta.DeltaP99Us - contentionDelta.DeltaP99Us
	if intrinsicCost != 7000 {
		t.Errorf("intrinsic cost p99 = %d, want 7000", intrinsicCost)
	}
}

func TestWorkflowRunResult_Validate(t *testing.T) {
	tests := []struct {
		name    string
		result  WorkflowRunResult
		wantErr bool
	}{
		{"valid", WorkflowRunResult{ID: "wr1", ExperimentRunID: "r1", Workflow: "checkout"}, false},
		{"missing workflow", WorkflowRunResult{ID: "wr1", ExperimentRunID: "r1"}, true},
		{"missing run_id", WorkflowRunResult{ID: "wr1", Workflow: "checkout"}, true},
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

func TestServiceRunResult_Validate(t *testing.T) {
	tests := []struct {
		name    string
		result  ServiceRunResult
		wantErr bool
	}{
		{"valid", ServiceRunResult{ID: "sr1", ExperimentRunID: "r1", Service: "productcatalog"}, false},
		{"missing service", ServiceRunResult{ID: "sr1", ExperimentRunID: "r1"}, true},
		{"missing run_id", ServiceRunResult{ID: "sr1", Service: "productcatalog"}, true},
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

func TestValidateRunGraph(t *testing.T) {
	run := func(id, expID string, deps ...string) *ExperimentRun {
		return &ExperimentRun{
			ID: id, ExperimentID: expID, RunType: "baseline",
			Status: "pending", DependsOn: deps,
		}
	}

	tests := []struct {
		name    string
		runs    []*ExperimentRun
		wantErr bool
		errSub  string
	}{
		{
			name:    "valid linear DAG: A → B → C",
			runs:    []*ExperimentRun{run("A", "e1"), run("B", "e1", "A"), run("C", "e1", "B")},
			wantErr: false,
		},
		{
			name:    "valid fan-out: A → B, A → C",
			runs:    []*ExperimentRun{run("A", "e1"), run("B", "e1", "A"), run("C", "e1", "A")},
			wantErr: false,
		},
		{
			name: "valid diamond: A → B,C → D",
			runs: []*ExperimentRun{
				run("A", "e1"),
				run("B", "e1", "A"),
				run("C", "e1", "A"),
				run("D", "e1", "B", "C"),
			},
			wantErr: false,
		},
		{
			name:    "valid single entry point (no deps)",
			runs:    []*ExperimentRun{run("A", "e1")},
			wantErr: false,
		},
		{
			name:    "simple cycle: A → B → A",
			runs:    []*ExperimentRun{run("A", "e1", "B"), run("B", "e1", "A")},
			wantErr: true,
			errSub:  "cycle",
		},
		{
			name: "transitive cycle: A → B → C → A",
			runs: []*ExperimentRun{
				run("A", "e1", "C"),
				run("B", "e1", "A"),
				run("C", "e1", "B"),
			},
			wantErr: true,
			errSub:  "cycle",
		},
		{
			name:    "unknown dependency reference",
			runs:    []*ExperimentRun{run("A", "e1"), run("B", "e1", "Z")},
			wantErr: true,
			errSub:  "unknown run",
		},
		{
			name: "cross-experiment dependency",
			runs: []*ExperimentRun{
				run("A", "e1"),
				{ID: "B", ExperimentID: "e2", RunType: "baseline", Status: "pending", DependsOn: []string{"A"}},
			},
			wantErr: true,
			errSub:  "different experiment",
		},
		{
			name:    "empty run set",
			runs:    []*ExperimentRun{},
			wantErr: false,
		},
		{
			name: "isolation with frozen services but no baseline ancestor",
			runs: []*ExperimentRun{
				{ID: "A", ExperimentID: "e1", RunType: "isolation", Status: "pending",
					FrozenServices: []CacheBoxConfig{{Service: "svc2", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}}},
				{
					ID: "B", ExperimentID: "e1", RunType: "isolation",
					Status: "pending", DependsOn: []string{"A"},
					FrozenServices: []CacheBoxConfig{{Service: "svc", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
				},
			},
			wantErr: true,
			errSub:  "no baseline run",
		},
		{
			name: "isolation with frozen services and baseline ancestor",
			runs: []*ExperimentRun{
				{ID: "base", ExperimentID: "e1", RunType: "baseline", Status: "pending"},
				{
					ID: "iso", ExperimentID: "e1", RunType: "isolation",
					Status: "pending", DependsOn: []string{"base"},
					FrozenServices: []CacheBoxConfig{{Service: "svc", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
				},
			},
			wantErr: false,
		},
		{
			name: "combination with transitive baseline ancestor",
			runs: []*ExperimentRun{
				{ID: "base", ExperimentID: "e1", RunType: "baseline", Status: "pending"},
				{
					ID: "iso", ExperimentID: "e1", RunType: "isolation",
					Status: "pending", DependsOn: []string{"base"},
					FrozenServices: []CacheBoxConfig{{Service: "svc", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
				},
				{
					ID: "combo", ExperimentID: "e1", RunType: "combination",
					Status: "pending", DependsOn: []string{"iso"},
					FrozenServices: []CacheBoxConfig{
						{Service: "svc", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"},
						{Service: "svc2", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRunGraph(tt.runs)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateRunGraph() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errSub != "" {
				if got := err.Error(); !contains(got, tt.errSub) {
					t.Errorf("error %q should contain %q", got, tt.errSub)
				}
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsAt(s, sub))
}

func containsAt(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
