package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFaultConfig_Validate(t *testing.T) {
	compID := "comp-1"
	latParams := json.RawMessage(`{"delay":"100ms"}`)

	tests := []struct {
		name    string
		cfg     FaultConfig
		wantErr string
	}{
		{
			name: "valid params",
			cfg: FaultConfig{
				ID: "fc-1", Name: "t", Service: "svc",
				Category: "inline", FaultType: "latency",
				DurationMs: 1000, Params: latParams,
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
			name: "valid network with envelope",
			cfg: FaultConfig{
				ID: "fc-3", Name: "t", Service: "svc",
				Category: "network", FaultType: "latency",
				DurationMs: 1000, Params: latParams,
				Network: &NetworkEnvelope{Target: "redis"},
			},
		},
		{
			name:    "missing id",
			cfg:     FaultConfig{Name: "t", Service: "svc", Category: "inline", FaultType: "latency", Params: latParams},
			wantErr: "id required",
		},
		{
			name:    "missing name",
			cfg:     FaultConfig{ID: "fc-1", Service: "svc", Category: "inline", FaultType: "latency", Params: latParams},
			wantErr: "name required",
		},
		{
			name:    "missing service",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Category: "inline", FaultType: "latency", Params: latParams},
			wantErr: "service required",
		},
		{
			name:    "invalid category",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "bogus", FaultType: "latency", Params: latParams},
			wantErr: "unsupported fault",
		},
		{
			name:    "invalid type for category",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "bogus", Params: latParams},
			wantErr: "unsupported fault",
		},
		{
			name:    "stale network:loss rejected",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "network", FaultType: "loss", Params: latParams},
			wantErr: "unsupported fault",
		},
		{
			name: "params rejected by catalog",
			cfg: FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency",
				Params: json.RawMessage(`{"delay":"not-a-duration"}`)},
			wantErr: "invalid delay",
		},
		{
			name: "unknown param field rejected",
			cfg: FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency",
				Params: json.RawMessage(`{"delay":"100ms","dleay":"oops"}`)},
			wantErr: "unknown field",
		},
		{
			name: "network envelope on inline rejected",
			cfg: FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency",
				Params: latParams, Network: &NetworkEnvelope{Target: "redis"}},
			wantErr: "network envelope only valid",
		},
		{
			name: "inline hang requires duration",
			cfg: FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "hang",
				DurationMs: 0, Params: json.RawMessage(`{"duration":"2s"}`)},
			wantErr: "inline:hang requires duration",
		},
		{
			name:    "missing params and comp",
			cfg:     FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency"},
			wantErr: "exactly one",
		},
		{
			name: "both params and comp",
			cfg: FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency",
				Params: latParams, FaultCompositionID: &compID},
			wantErr: "exactly one",
		},
		{
			name: "negative duration",
			cfg: FaultConfig{ID: "fc-1", Name: "t", Service: "svc", Category: "inline", FaultType: "latency",
				Params: latParams, DurationMs: -1},
			wantErr: "durations must be >= 0",
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
