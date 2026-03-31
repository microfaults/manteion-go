package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// FaultSpec is an atomic fault definition. Maps to exactly one atropos-go
// fault type. Three categories: inline, network, resource.
type FaultSpec struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Category   string          `json:"category"`   // "inline", "network", "resource"
	FaultType  string          `json:"fault_type"`  // e.g. "error", "blackhole", "cpu"
	Config     json.RawMessage `json:"config"`      // type-specific parameters
	DurationMs int64           `json:"duration_ms,omitempty"`
	RampUpMs   int64           `json:"ramp_up_ms,omitempty"`
	RampDownMs int64           `json:"ramp_down_ms,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

var validFaultTypes = map[string][]string{
	"inline":   {"error", "hang", "latency"},
	"network":  {"blackhole", "drip", "latency", "loss", "rst", "throttle"},
	"resource": {"cpu", "memory", "io"},
}

func (f *FaultSpec) Validate() error {
	if f.ID == "" {
		return errors.New("fault spec: id required")
	}
	if f.Name == "" {
		return errors.New("fault spec: name required")
	}
	types, ok := validFaultTypes[f.Category]
	if !ok {
		return fmt.Errorf("fault spec: invalid category %q", f.Category)
	}
	valid := false
	for _, t := range types {
		if t == f.FaultType {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("fault spec: invalid fault_type %q for category %q", f.FaultType, f.Category)
	}
	if f.Category == "inline" && f.FaultType == "hang" && f.DurationMs <= 0 {
		return errors.New("fault spec: inline:hang requires duration_ms > 0")
	}
	if len(f.Config) == 0 || string(f.Config) == "null" {
		return errors.New("fault spec: config required")
	}
	return nil
}

// FaultComposition groups faults for parallel or sequential execution.
// Max tree depth = 3 (atoms → groups → top-level composition).
type FaultComposition struct {
	ID            string                   `json:"id"`
	Name          string                   `json:"name"`
	ExecutionMode string                   `json:"execution_mode"` // "parallel" or "sequential"
	Members       []FaultCompositionMember `json:"members"`
	CreatedAt     time.Time                `json:"created_at"`
}

func (c *FaultComposition) Validate() error {
	if c.ID == "" {
		return errors.New("fault composition: id required")
	}
	if c.Name == "" {
		return errors.New("fault composition: name required")
	}
	if c.ExecutionMode != "parallel" && c.ExecutionMode != "sequential" {
		return fmt.Errorf("fault composition: invalid execution_mode %q", c.ExecutionMode)
	}
	if len(c.Members) < 2 {
		return errors.New("fault composition: at least 2 members required")
	}
	for i, m := range c.Members {
		if err := m.Validate(); err != nil {
			return fmt.Errorf("fault composition member[%d]: %w", i, err)
		}
	}
	return nil
}

// FaultCompositionMember is one slot in a composition.
// Exactly one of FaultSpecID or ChildCompositionID must be set.
type FaultCompositionMember struct {
	Position           int    `json:"position"`
	FaultSpecID        string `json:"fault_spec_id,omitempty"`
	ChildCompositionID string `json:"child_composition_id,omitempty"`
	Direction          string `json:"direction,omitempty"` // "upstream", "downstream", "" (non-network)
}

func (m *FaultCompositionMember) Validate() error {
	hasFault := m.FaultSpecID != ""
	hasChild := m.ChildCompositionID != ""
	if hasFault == hasChild {
		return errors.New("exactly one of fault_spec_id or child_composition_id must be set")
	}
	if m.Direction != "" && m.Direction != "upstream" && m.Direction != "downstream" {
		return fmt.Errorf("invalid direction %q", m.Direction)
	}
	return nil
}

// FaultIncompatibility defines a pair of fault types that cannot be composed.
// Stored as reference data; validated at composition creation time.
type FaultIncompatibility struct {
	FaultTypeA     string `json:"fault_type_a"`     // e.g. "network:blackhole"
	FaultTypeB     string `json:"fault_type_b"`     // e.g. "network:rst"
	Scope          string `json:"scope"`            // "parallel", "sequential", "any"
	ConstraintType string `json:"constraint_type"`  // "hard" or "soft"
	Reason         string `json:"reason"`
}

// DefaultIncompatibilities returns the known fault incompatibility rules
// derived from analysis of atropos-go proxy/fault code.
func DefaultIncompatibilities() []FaultIncompatibility {
	return []FaultIncompatibility{
		// Hard incompatibilities — physically undefined behavior.
		{
			FaultTypeA: "network:blackhole", FaultTypeB: "network:*",
			Scope: "parallel", ConstraintType: "hard",
			Reason: "blackhole hijacks pre-dial; second toxic's Pipe() never runs",
		},
		{
			FaultTypeA: "network:drip", FaultTypeB: "network:throttle",
			Scope: "parallel", ConstraintType: "hard",
			Reason: "both control stream timing; undefined which controls pacing",
		},
		{
			FaultTypeA: "network:drip", FaultTypeB: "network:latency",
			Scope: "parallel", ConstraintType: "hard",
			Reason: "both add per-chunk delays; double timing control",
		},
		// Soft incompatibilities — redundant/confusing but technically executable.
		{
			FaultTypeA: "inline:hang", FaultTypeB: "network:blackhole",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "both block the request; redundant",
		},
		{
			FaultTypeA: "network:loss", FaultTypeB: "network:rst",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "both can reset connection; intent is ambiguous",
		},
		{
			FaultTypeA: "inline:error", FaultTypeB: "inline:latency",
			Scope: "sequential", ConstraintType: "soft",
			Reason: "error first makes subsequent latency meaningless",
		},
		{
			FaultTypeA: "inline:error", FaultTypeB: "inline:hang",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "error completes immediately, hang blocks; conflicting intent",
		},
		{
			FaultTypeA: "resource:cpu", FaultTypeB: "resource:memory",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "memory allocation triggers GC which skews CPU duty-cycle measurements",
		},
	}
}
