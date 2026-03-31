package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFaultSpec_Validate(t *testing.T) {
	base := func() FaultSpec {
		return FaultSpec{
			ID: "f1", Name: "test", Category: "inline", FaultType: "error",
			Config: json.RawMessage(`{"status_code":500}`), CreatedAt: time.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*FaultSpec)
		wantErr bool
	}{
		{"valid inline:error", nil, false},
		{"valid network:blackhole", func(f *FaultSpec) {
			f.Category = "network"
			f.FaultType = "blackhole"
		}, false},
		{"valid resource:cpu", func(f *FaultSpec) {
			f.Category = "resource"
			f.FaultType = "cpu"
		}, false},
		{"missing id", func(f *FaultSpec) { f.ID = "" }, true},
		{"missing name", func(f *FaultSpec) { f.Name = "" }, true},
		{"invalid category", func(f *FaultSpec) { f.Category = "bad" }, true},
		{"invalid fault_type for category", func(f *FaultSpec) {
			f.Category = "inline"
			f.FaultType = "blackhole"
		}, true},
		{"hang without duration", func(f *FaultSpec) {
			f.FaultType = "hang"
		}, true},
		{"hang with duration", func(f *FaultSpec) {
			f.FaultType = "hang"
			f.DurationMs = 5000
		}, false},
		{"missing config", func(f *FaultSpec) { f.Config = nil }, true},
		{"null config", func(f *FaultSpec) { f.Config = json.RawMessage("null") }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := base()
			if tt.modify != nil {
				tt.modify(&f)
			}
			err := f.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFaultComposition_Validate(t *testing.T) {
	tests := []struct {
		name    string
		comp    FaultComposition
		wantErr bool
	}{
		{
			name: "valid parallel composition",
			comp: FaultComposition{
				ID: "c1", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "f1", Direction: "upstream"},
					{Position: 1, FaultSpecID: "f2", Direction: "downstream"},
				},
				CreatedAt: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "valid sequential composition",
			comp: FaultComposition{
				ID: "c2", Name: "test", ExecutionMode: "sequential",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "f1"},
					{Position: 1, FaultSpecID: "f2"},
				},
				CreatedAt: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "valid nested composition",
			comp: FaultComposition{
				ID: "c3", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "f1"},
					{Position: 1, ChildCompositionID: "c1"},
				},
				CreatedAt: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "invalid execution_mode",
			comp: FaultComposition{
				ID: "c4", Name: "test", ExecutionMode: "bad",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "f1"},
					{Position: 1, FaultSpecID: "f2"},
				},
			},
			wantErr: true,
		},
		{
			name: "fewer than 2 members",
			comp: FaultComposition{
				ID: "c5", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "f1"},
				},
			},
			wantErr: true,
		},
		{
			name: "member with both fault and child",
			comp: FaultComposition{
				ID: "c6", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "f1", ChildCompositionID: "c1"},
					{Position: 1, FaultSpecID: "f2"},
				},
			},
			wantErr: true,
		},
		{
			name: "member with neither fault nor child",
			comp: FaultComposition{
				ID: "c7", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0},
					{Position: 1, FaultSpecID: "f2"},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid direction",
			comp: FaultComposition{
				ID: "c8", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "f1", Direction: "sideways"},
					{Position: 1, FaultSpecID: "f2"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.comp.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultIncompatibilities(t *testing.T) {
	incomp := DefaultIncompatibilities()
	if len(incomp) == 0 {
		t.Fatal("expected non-empty incompatibility list")
	}

	// Verify known hard incompatibilities are present.
	found := map[string]bool{}
	for _, i := range incomp {
		key := i.FaultTypeA + "+" + i.FaultTypeB + ":" + i.ConstraintType
		found[key] = true
	}

	hardCases := []string{
		"network:blackhole+network:*:hard",
		"network:drip+network:throttle:hard",
		"network:drip+network:latency:hard",
	}
	for _, c := range hardCases {
		if !found[c] {
			t.Errorf("missing hard incompatibility: %s", c)
		}
	}

	softCases := []string{
		"inline:hang+network:blackhole:soft",
		"resource:cpu+resource:memory:soft",
	}
	for _, c := range softCases {
		if !found[c] {
			t.Errorf("missing soft incompatibility: %s", c)
		}
	}
}
