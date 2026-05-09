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
				ID:         "fc-1",
				Name:       "test-fault",
				Service:    "svc",
				Category:   "inline",
				FaultType:  "latency",
				DurationMs: 1000,
				FaultReq:   json.RawMessage(`{"delay":"100ms"}`),
			},
		},
		{
			name: "valid composition",
			cfg: FaultConfig{
				ID:                 "fc-2",
				Name:               "test-comp",
				Service:            "svc",
				Category:           "inline",
				FaultType:          "error", // type doesn't strictly matter for composition at the model validation level, but must be valid
				DurationMs:         1000,
				FaultCompositionID: &compID,
			},
		},
		{
			name: "missing id",
			cfg: FaultConfig{
				Name:       "test",
				Service:    "svc",
				Category:   "inline",
				FaultType:  "latency",
				FaultReq:   json.RawMessage(`{}`),
			},
			wantErr: "id required",
		},
		{
			name: "missing name",
			cfg: FaultConfig{
				ID:         "fc-1",
				Service:    "svc",
				Category:   "inline",
				FaultType:  "latency",
				FaultReq:   json.RawMessage(`{}`),
			},
			wantErr: "name required",
		},
		{
			name: "missing service",
			cfg: FaultConfig{
				ID:         "fc-1",
				Name:       "test",
				Category:   "inline",
				FaultType:  "latency",
				FaultReq:   json.RawMessage(`{}`),
			},
			wantErr: "service required",
		},
		{
			name: "invalid category",
			cfg: FaultConfig{
				ID:         "fc-1",
				Name:       "test",
				Service:    "svc",
				Category:   "bogus",
				FaultType:  "latency",
				FaultReq:   json.RawMessage(`{}`),
			},
			wantErr: "invalid category",
		},
		{
			name: "invalid type for category",
			cfg: FaultConfig{
				ID:         "fc-1",
				Name:       "test",
				Service:    "svc",
				Category:   "inline",
				FaultType:  "bogus",
				FaultReq:   json.RawMessage(`{}`),
			},
			wantErr: "invalid fault_type",
		},
		{
			name: "missing req and comp",
			cfg: FaultConfig{
				ID:         "fc-1",
				Name:       "test",
				Service:    "svc",
				Category:   "inline",
				FaultType:  "latency",
			},
			wantErr: "exactly one",
		},
		{
			name: "both req and comp",
			cfg: FaultConfig{
				ID:                 "fc-1",
				Name:               "test",
				Service:            "svc",
				Category:           "inline",
				FaultType:          "latency",
				FaultReq:           json.RawMessage(`{}`),
				FaultCompositionID: &compID,
			},
			wantErr: "exactly one",
		},
		{
			name: "negative duration",
			cfg: FaultConfig{
				ID:         "fc-1",
				Name:       "test",
				Service:    "svc",
				Category:   "inline",
				FaultType:  "latency",
				FaultReq:   json.RawMessage(`{}`),
				DurationMs: -1,
			},
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
			} else {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
				}
			}
		})
	}
}

func TestFaultConfig_IsInfiniteAllowed(t *testing.T) {
	allowlist := map[string]bool{
		"inline:latency": true,
		"inline:hang":    false,
	}

	cfg := FaultConfig{Category: "inline", FaultType: "latency", DurationMs: 1000}
	if !cfg.IsInfiniteAllowed(allowlist) {
		t.Error("expected true for duration > 0")
	}

	cfg = FaultConfig{Category: "inline", FaultType: "latency", DurationMs: 0}
	if !cfg.IsInfiniteAllowed(allowlist) {
		t.Error("expected true for duration == 0 when allowed")
	}

	cfg = FaultConfig{Category: "inline", FaultType: "hang", DurationMs: 0}
	if cfg.IsInfiniteAllowed(allowlist) {
		t.Error("expected false for duration == 0 when denied")
	}
}
