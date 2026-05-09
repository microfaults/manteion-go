package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type FaultConfigStatus string

const (
	FaultConfigReady             FaultConfigStatus = "ready"
	FaultConfigActive            FaultConfigStatus = "active"
	FaultConfigCompleted         FaultConfigStatus = "completed"
	FaultConfigManuallyCancelled FaultConfigStatus = "manually_cancelled"
	FaultConfigFailed            FaultConfigStatus = "failed"
	// FaultConfigScheduled — reserved for V6 (fire-at-T scheduling); not used here.
)

type FaultConfig struct {
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	Description        string            `json:"description,omitempty"`
	Service            string            `json:"service"`
	Category           string            `json:"category"`
	FaultType          string            `json:"fault_type"`

	// Exactly one of FaultReq or FaultCompositionID must be set.
	FaultReq           json.RawMessage   `json:"fault_request,omitempty"`
	FaultCompositionID *string           `json:"fault_composition_id,omitempty"` // J

	DurationMs         int64             `json:"duration_ms"` // 0 = infinite (whitelisted types only)
	ExperimentRunID    *string           `json:"experiment_run_id,omitempty"`     // #2

	Status             FaultConfigStatus `json:"status"`
	CreatedAt          time.Time         `json:"created_at"`
	UpdatedAt          time.Time         `json:"updated_at"`
	FiredAt            *time.Time        `json:"fired_at,omitempty"`
	CompletedAt        *time.Time        `json:"completed_at,omitempty"`
}

func (f *FaultConfig) Validate() error {
	if f.ID == "" {
		return errors.New("fault config: id required")
	}
	if f.Name == "" {
		return errors.New("fault config: name required")
	}
	if f.Service == "" {
		return errors.New("fault config: service required")
	}
	types, ok := validFaultTypes[f.Category]
	if !ok {
		return fmt.Errorf("fault config: invalid category %q", f.Category)
	}
	valid := false
	for _, t := range types {
		if t == f.FaultType {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("fault config: invalid fault_type %q for category %q",
			f.FaultType, f.Category)
	}
	hasReq := len(f.FaultReq) > 0 && string(f.FaultReq) != "null"
	if hasReq == (f.FaultCompositionID != nil) {
		return errors.New("fault config: exactly one of fault_request or fault_composition_id required")
	}
	if f.DurationMs < 0 {
		return errors.New("fault config: duration_ms must be >= 0")
	}
	return nil
}

func (f *FaultConfig) CanFire() bool   { return f.Status != FaultConfigActive }
func (f *FaultConfig) CanEdit() bool   { return f.Status != FaultConfigActive }
func (f *FaultConfig) CanDelete() bool { return f.Status != FaultConfigActive }

// ConflictKey is the (category:type) tuple used as the human-facing slot
// identifier and as the lookup key for the infinite-allowed whitelist.
// The DB-level uniqueness is on (service, category) only.
func (f *FaultConfig) ConflictKey() string { return f.Category + ":" + f.FaultType }

// IsInfiniteAllowed returns true if duration_ms > 0, OR if duration_ms == 0
// and this fault type is in the operator-configured whitelist.
func (f *FaultConfig) IsInfiniteAllowed(allowlist map[string]bool) bool {
	if f.DurationMs > 0 {
		return true
	}
	return allowlist[f.ConflictKey()]
}
// DefaultInfiniteAllowed — fault types safe to fire with duration_ms = 0.
// Operator config can override via a deployment-time map.
//
// Default-deny rationale:
//   inline:hang        — every matching request hangs forever; caller queue
//                        saturates in seconds.
//   network:blackhole  — same shape, network layer.
//   network:rst        — connection-reset storms; downstream retry
//                        amplification.
var DefaultInfiniteAllowed = map[string]bool{
	"inline:latency":   true,
	"inline:error":     true,
	"network:latency":  true,
	"network:throttle": true,
	"network:drip":     true,
	"network:loss":     true,
	"resource:cpu":     true,
	"resource:memory":  true,
	"resource:io":      true,
	"resource:disk":    true,
}
