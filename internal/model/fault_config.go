package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"manteion-go/internal/faultcatalog"
)

// FaultConfigStatus is the lifecycle state of a long-running fault.
type FaultConfigStatus string

const (
	FaultConfigReady     FaultConfigStatus = "ready"
	FaultConfigActive    FaultConfigStatus = "active"
	FaultConfigCompleted FaultConfigStatus = "completed"
	FaultConfigCancelled FaultConfigStatus = "cancelled"
)

// FaultConfig is a manually-managed, long-running fault — a side channel to the
// primary rule-attached fault path. It is fired explicitly, runs for an optional
// duration, and is delivered to SDK instances through the poll response's
// active_faults list (keyed by ID). The SDK reconciles desired vs applied faults
// on each poll and reaps stale ones via its watchdog; manteion only owns the
// desired state and flips status to completed when a duration elapses.
type FaultConfig struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Service     string `json:"service"`
	Category    string `json:"category"`
	FaultType   string `json:"fault_type"`

	// Exactly one of Params or FaultCompositionID is set. Params + Network
	// mirror the unified fault wire shape (atropos FaultRequest): Params is
	// the per-(category,fault_type) faultparams object; Network is the
	// envelope for network-category faults.
	Params             json.RawMessage  `json:"params,omitempty"`
	Network            *NetworkEnvelope `json:"network,omitempty"`
	FaultCompositionID *string          `json:"fault_composition_id,omitempty"`

	DurationMs int64   `json:"duration_ms"` // 0 = run until cancelled
	RampUpMs   int64   `json:"ramp_up_ms,omitempty"`
	RampDownMs int64   `json:"ramp_down_ms,omitempty"`
	PhaseID    *string `json:"phase_id,omitempty"` // optional "fired during this phase" tag

	Status      FaultConfigStatus `json:"status"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	FiredAt     *time.Time        `json:"fired_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
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
	hasParams := len(f.Params) > 0 && string(f.Params) != "null"
	if hasParams == (f.FaultCompositionID != nil) {
		return errors.New("fault config: exactly one of params or fault_composition_id required")
	}
	if hasParams {
		if err := faultcatalog.ValidateFault(f.Category, f.FaultType, f.Params, catalogEnvelope("", f.Network)); err != nil {
			return fmt.Errorf("fault config: %w", err)
		}
	} else if !faultcatalog.Supported(f.Category, f.FaultType) && f.FaultType != "" {
		return fmt.Errorf("fault config: unsupported fault %s/%s", f.Category, f.FaultType)
	}
	if f.Category == "inline" && f.FaultType == "hang" && f.DurationMs <= 0 {
		return errors.New("fault config: inline:hang requires duration_ms > 0")
	}
	if f.DurationMs < 0 || f.RampUpMs < 0 || f.RampDownMs < 0 {
		return errors.New("fault config: durations must be >= 0")
	}
	return nil
}

// CanFire reports whether the config can be fired — i.e. it is not already active.
func (f *FaultConfig) CanFire() bool { return f.Status != FaultConfigActive }
