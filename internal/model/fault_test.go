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
			Params: json.RawMessage(`{"status_code":500}`), CreatedAt: time.Now(),
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
			f.Params = json.RawMessage(`{}`)
			f.Network = &NetworkEnvelope{Target: "redis"}
		}, false},
		{"valid resource:cpu", func(f *FaultSpec) {
			f.Category = "resource"
			f.FaultType = "cpu"
			f.Params = json.RawMessage(`{"target_load":0.7}`)
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
			f.Params = json.RawMessage(`{"duration":"2s"}`)
		}, true},
		{"hang with duration", func(f *FaultSpec) {
			f.FaultType = "hang"
			f.Params = json.RawMessage(`{"duration":"2s"}`)
			f.DurationMs = 5000
		}, false},
		// Empty/absent params are valid when every field has a default —
		// the catalog decodes them as an all-defaults object (error → 500).
		{"missing params ok for all-default types", func(f *FaultSpec) { f.Params = nil }, false},
		{"params rejected by catalog", func(f *FaultSpec) {
			f.FaultType = "latency"
			f.Params = json.RawMessage(`{"delay":"bogus"}`)
		}, true},
		{"unknown param field rejected", func(f *FaultSpec) {
			f.Params = json.RawMessage(`{"status_code":500,"xtra":1}`)
		}, true},
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
					{FaultSpecID: "f1", Direction: "upstream"},
					{FaultSpecID: "f2", Direction: "downstream"},
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
					{FaultSpecID: "f1"},
					{FaultSpecID: "f2"},
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
					{FaultSpecID: "f1"},
					{ChildCompositionID: "c1"},
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
					{FaultSpecID: "f1"},
					{FaultSpecID: "f2"},
				},
			},
			wantErr: true,
		},
		{
			name: "fewer than 2 members",
			comp: FaultComposition{
				ID: "c5", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{FaultSpecID: "f1"},
				},
			},
			wantErr: true,
		},
		{
			name: "member with both fault and child",
			comp: FaultComposition{
				ID: "c6", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{FaultSpecID: "f1", ChildCompositionID: "c1"},
					{FaultSpecID: "f2"},
				},
			},
			wantErr: true,
		},
		{
			name: "member with neither fault nor child",
			comp: FaultComposition{
				ID: "c7", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{},
					{FaultSpecID: "f2"},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid direction",
			comp: FaultComposition{
				ID: "c8", Name: "test", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{FaultSpecID: "f1", Direction: "sideways"},
					{FaultSpecID: "f2"},
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
		key := i.Subject + "+" + i.Object + ":" + i.ConstraintType
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
