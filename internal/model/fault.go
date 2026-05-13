package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ExecutionMode controls how composition members coordinate.
type ExecutionMode string

const (
	ExecutionParallel   ExecutionMode = "parallel"
	ExecutionSequential ExecutionMode = "sequential"
)

// IsValid reports whether the ExecutionMode is a recognized constant.
func (m ExecutionMode) IsValid() bool {
	switch m {
	case ExecutionParallel, ExecutionSequential:
		return true
	}
	return false
}

// Direction is an optional per-member tag for network faults indicating
// which atropos toxic pipe the fault attaches to. Empty means not applicable.
type Direction string

const (
	DirectionUpstream   Direction = "upstream"
	DirectionDownstream Direction = "downstream"
	// DirectionNone is the empty string, used explicitly for non-network fault
	// members where direction is not applicable. Included in IsValid() on purpose.
	DirectionNone Direction = ""
)

// IsValid reports whether the Direction is a recognized constant (or empty).
func (d Direction) IsValid() bool {
	switch d {
	case DirectionUpstream, DirectionDownstream, DirectionNone:
		return true
	}
	return false
}

// FaultSpec is an atomic fault definition. Maps to exactly one atropos-go
// fault type. Three categories: inline, network, resource.
//
// Host selects where the toxic runs:
//   - "proxy"   → TCP proxy sidecar; only valid for category="network"
//   - "inline"  → in-process RoundTripper response shaping (v6; only network)
//   - "process" → in-process Go fault (inline + resource categories)
//
// NetworkEnvelope (Target/Direction/Scope) is only populated for
// category="network"; enforced by DB CHECK constraint.
type FaultSpec struct {
	ID         string           `json:"id"`
	Name       string           `json:"name"`
	Category   string           `json:"category"`   // "inline", "network", "resource"
	FaultType  string           `json:"fault_type"` // e.g. "error", "blackhole", "cpu"
	Host       string           `json:"host,omitempty"`
	Network    *NetworkEnvelope `json:"network,omitempty"`
	Params     json.RawMessage  `json:"params"` // type-specific parameters
	DurationMs int64            `json:"duration_ms,omitempty"`
	RampUpMs   int64            `json:"ramp_up_ms,omitempty"`
	RampDownMs int64            `json:"ramp_down_ms,omitempty"`
	CreatedAt  time.Time        `json:"created_at"`
}

// NetworkEnvelope holds network-category-only envelope fields that select
// which traffic the toxic applies to. Stored as separate columns
// (network_target, network_direction, network_scope) on fault_specs.
type NetworkEnvelope struct {
	Target    string  `json:"target,omitempty"`
	Direction string  `json:"direction,omitempty"`
	Scope     float64 `json:"scope,omitempty"`
}

var validFaultTypes = map[string][]string{
	"inline":   {"error", "hang", "latency"},
	"network":  {"blackhole", "drip", "latency", "retransmit_delay", "rst", "throttle"},
	"resource": {"cpu", "disk", "io", "memory"},
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
	if len(f.Params) == 0 || string(f.Params) == "null" {
		return errors.New("fault spec: params required")
	}

	// Host vocabulary + category coupling.
	if f.Host != "" {
		switch f.Host {
		case "proxy", "inline":
			if f.Category != "network" {
				return fmt.Errorf("fault spec: host=%q only valid for network category, got %q", f.Host, f.Category)
			}
		case "process":
			if f.Category == "network" {
				return errors.New("fault spec: host=process invalid for network category")
			}
		default:
			return fmt.Errorf("fault spec: invalid host %q", f.Host)
		}
	}

	// NetworkEnvelope is forbidden for non-network categories.
	if f.Network != nil {
		if f.Category != "network" {
			return fmt.Errorf("fault spec: network envelope only valid for network category, got %q", f.Category)
		}
		if f.Network.Direction != "" &&
			f.Network.Direction != "upstream" &&
			f.Network.Direction != "downstream" {
			return fmt.Errorf("fault spec: invalid direction %q", f.Network.Direction)
		}
		if f.Network.Scope < 0 || f.Network.Scope > 1.0 {
			return fmt.Errorf("fault spec: scope %.2f out of [0,1]", f.Network.Scope)
		}
	}

	return nil
}

// FaultComposition groups faults for parallel or sequential execution.
// Max tree depth = 3 (atoms → groups → top-level composition).
type FaultComposition struct {
	ID            string                   `json:"id"`
	Name          string                   `json:"name"`
	ExecutionMode ExecutionMode            `json:"execution_mode"` // "parallel" or "sequential"
	DurationMs    int64                    `json:"duration_ms,omitempty"`
	RampUpMs      int64                    `json:"ramp_up_ms,omitempty"`
	RampDownMs    int64                    `json:"ramp_down_ms,omitempty"`
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
	if !c.ExecutionMode.IsValid() {
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
	FaultSpecID        string    `json:"fault_spec_id,omitempty"`
	ChildCompositionID string    `json:"child_composition_id,omitempty"`
	Direction          Direction `json:"direction,omitempty"` // "upstream", "downstream", "" (non-network)
}

func (m *FaultCompositionMember) Validate() error {
	hasFault := m.FaultSpecID != ""
	hasChild := m.ChildCompositionID != ""
	if hasFault == hasChild {
		return errors.New("exactly one of fault_spec_id or child_composition_id must be set")
	}
	if !m.Direction.IsValid() {
		return fmt.Errorf("invalid direction %q", m.Direction)
	}
	return nil
}

// FaultIncompatibility defines a pair of fault types that cannot be composed.
// Stored as reference data; validated at composition creation time.
type FaultIncompatibility struct {
	FaultTypeA     string `json:"fault_type_a"`    // e.g. "network:blackhole"
	FaultTypeB     string `json:"fault_type_b"`    // e.g. "network:rst"
	Scope          string `json:"scope"`           // "parallel", "sequential", "any"
	ConstraintType string `json:"constraint_type"` // "hard" or "soft"
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
			FaultTypeA: "network:retransmit_delay", FaultTypeB: "network:rst",
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
		{
			FaultTypeA: "inline:latency", FaultTypeB: "inline:hang",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "hang blocks indefinitely, making the latency delay invisible",
		},
	}
}
