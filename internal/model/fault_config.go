package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
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

	// Exactly one of FaultReq or FaultCompositionID is set.
	FaultReq           json.RawMessage `json:"fault_request,omitempty"`
	FaultCompositionID *string         `json:"fault_composition_id,omitempty"`

	DurationMs      int64   `json:"duration_ms"` // 0 = run until cancelled
	ExperimentRunID *string `json:"experiment_run_id,omitempty"`

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
		return fmt.Errorf("fault config: invalid fault_type %q for category %q", f.FaultType, f.Category)
	}
	if f.Category == "inline" && f.FaultType == "hang" && f.DurationMs <= 0 {
		return errors.New("fault config: inline:hang requires duration_ms > 0")
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

// CanFire reports whether the config can be fired — i.e. it is not already active.
func (f *FaultConfig) CanFire() bool { return f.Status != FaultConfigActive }
