package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFaultConfig_Validate(t *testing.T) {
	compID := "comp-1"

	tests := []struct {
		name    string
		cfg     FaultConfig
		wantErr string
	}{
		{
			name: "valid request",
			cfg: FaultConfig{
				ID: "fc-1", Name: "t", Service: "svc",
				Category: "inline", FaultType: "latency",
				DurationMs: 1000, FaultReq: json.RawMessage(`{"delay":"100ms"}`),
			},
		},
		{
			name: "valid composition",
			cfg: FaultConfig{
				ID: "fc-2", Name: "t", Service: "svc",
				Category: "network", FaultType: "retransmit_delay",
				FaultCompositionID: &compID,
			},
		},
		{
			name:    "missing id",
			cfg:     FaultConfig{Name: "t", Service: "svc", Category: "inline", FaultType: "latency", FaultReq: json.RawMessage(`{}`)},
			wantErr: "id required",
		},
		{
			name:    "missing name",
			cfg:     FaultConfig{ID: "fc-1", Service: "svc", Category: "inline", FaultType: "latency", FaultReq: json.RawMessage(`{}`)},
			wantErr: "name required",
		},
		{
			name:    "missing service",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Category: "inline", FaultType: "latency", FaultReq: json.RawMessage(`{}`)},
			wantErr: "service required",
		},
		{
			name:    "invalid category",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "bogus", FaultType: "latency", FaultReq: json.RawMessage(`{}`)},
			wantErr: "invalid category",
		},
		{
			name:    "invalid type for category",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "bogus", FaultReq: json.RawMessage(`{}`)},
			wantErr: "invalid fault_type",
		},
		{
			name:    "stale network:loss rejected",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "network", FaultType: "loss", FaultReq: json.RawMessage(`{}`)},
			wantErr: "invalid fault_type",
		},
		{
			name:    "inline hang requires duration",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "hang", DurationMs: 0, FaultReq: json.RawMessage(`{}`)},
			wantErr: "inline:hang requires duration",
		},
		{
			name:    "missing req and comp",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency"},
			wantErr: "exactly one",
		},
		{
			name:    "both req and comp",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency", FaultReq: json.RawMessage(`{}`), FaultCompositionID: &compID},
			wantErr: "exactly one",
		},
		{
			name:    "negative duration",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency", FaultReq: json.RawMessage(`{}`), DurationMs: -1},
			wantErr: "duration_ms must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected nil, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestFaultConfig_CanFire(t *testing.T) {
	for _, st := range []FaultConfigStatus{FaultConfigReady, FaultConfigCompleted, FaultConfigCancelled} {
		if !(&FaultConfig{Status: st}).CanFire() {
			t.Errorf("status %q should be fireable", st)
		}
	}
	if (&FaultConfig{Status: FaultConfigActive}).CanFire() {
		t.Error("active config should not be fireable")
	}
}
