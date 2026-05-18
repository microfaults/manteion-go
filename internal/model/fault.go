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
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Category    string           `json:"category"`   // "inline", "network", "resource"
	FaultType   string           `json:"fault_type"` // e.g. "error", "blackhole", "cpu"
	Host        string           `json:"host,omitempty"`
	Network     *NetworkEnvelope `json:"network,omitempty"`
	Params      json.RawMessage  `json:"params"`                // type-specific parameters
	Description string           `json:"description,omitempty"` // human-readable notes
	DurationMs  int64            `json:"duration_ms,omitempty"`
	RampUpMs    int64            `json:"ramp_up_ms,omitempty"`
	RampDownMs  int64            `json:"ramp_down_ms,omitempty"`
	CreatedAt   time.Time        `json:"created_at"`
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

// FaultConstraint is a single rule in the fault compatibility matrix.
//
// Kind discriminates between the three types of constraints we track:
//
//   - "pair": Subject and Object are both fault types ("category:fault_type").
//     The pair is incompatible when composed under Scope (parallel/sequential/any).
//     Subject and Object are symmetric except when Scope="sequential", in which
//     case Subject must come before Object.
//
//   - "host_mode": Subject is a host value ("host:proxy"), Object is a mode
//     value ("mode:inline" or "mode:background"). The combination of that
//     host with that mode is incompatible. Scope is always "always".
//
//   - "category_mode": Subject is a category ("category:resource"), Object
//     is a mode ("mode:inline" or "mode:background"). Scope is "always".
//
// Subject and Object are namespaced strings so the table can encode any pair
// of attributes without growing new columns. Wildcards "category:*" within
// the same kind are supported.
type FaultConstraint struct {
	Kind           string `json:"kind"`            // "pair" | "host_mode" | "category_mode"
	Subject        string `json:"subject"`         // e.g. "network:blackhole", "host:proxy", "category:resource"
	Object         string `json:"object"`          // pair: other type; mode constraints: "mode:inline"|"mode:background"
	Scope          string `json:"scope"`           // pair: "parallel"|"sequential"|"any"; mode: "always"
	ConstraintType string `json:"constraint_type"` // "hard" or "soft"
	Reason         string `json:"reason"`
}

// FaultIncompatibility is the legacy alias for FaultConstraint{Kind:"pair"},
// kept as a type alias so older API consumers compile; new code should use
// FaultConstraint directly.
//
// Deprecated: use FaultConstraint with Kind="pair".
type FaultIncompatibility = FaultConstraint

// DefaultConstraints returns the known fault compatibility rules derived
// from analysis of atropos-go fault execution semantics.
//
// Includes both pair-incompatibility (composition-level) and Mode×Host
// constraints (per-rule level). See FaultConstraint for the schema.
func DefaultConstraints() []FaultConstraint {
	return []FaultConstraint{
		// === Pair incompatibilities: hard (physically undefined behavior) ===
		{
			Kind:    "pair",
			Subject: "network:blackhole", Object: "network:*",
			Scope: "parallel", ConstraintType: "hard",
			Reason: "blackhole hijacks pre-dial; second toxic's Pipe() never runs",
		},
		{
			Kind:    "pair",
			Subject: "network:drip", Object: "network:throttle",
			Scope: "parallel", ConstraintType: "hard",
			Reason: "both control stream timing; undefined which controls pacing",
		},
		{
			Kind:    "pair",
			Subject: "network:drip", Object: "network:latency",
			Scope: "parallel", ConstraintType: "hard",
			Reason: "both add per-chunk delays; double timing control",
		},

		// === Pair incompatibilities: soft (redundant or confusing) ===
		{
			Kind:    "pair",
			Subject: "inline:hang", Object: "network:blackhole",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "both block the request; redundant",
		},
		{
			Kind:    "pair",
			Subject: "network:retransmit_delay", Object: "network:rst",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "both can reset connection; intent is ambiguous",
		},
		{
			Kind:    "pair",
			Subject: "inline:error", Object: "inline:latency",
			Scope: "sequential", ConstraintType: "soft",
			Reason: "error first makes subsequent latency meaningless",
		},
		{
			Kind:    "pair",
			Subject: "inline:error", Object: "inline:hang",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "error completes immediately, hang blocks; conflicting intent",
		},
		{
			Kind:    "pair",
			Subject: "resource:cpu", Object: "resource:memory",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "memory allocation triggers GC which skews CPU duty-cycle measurements",
		},
		{
			Kind:    "pair",
			Subject: "inline:latency", Object: "inline:hang",
			Scope: "parallel", ConstraintType: "soft",
			Reason: "hang blocks indefinitely, making the latency delay invisible",
		},

		// === Host×Mode constraints ===
		// A network.Proxy fault's Handle.Done fires only after the proxy's
		// listener lifetime elapses. Inline mode would block the request
		// goroutine for the full proxy duration (typically 30s+).
		{
			Kind:    "host_mode",
			Subject: "host:proxy", Object: "mode:inline",
			Scope: "always", ConstraintType: "hard",
			Reason: "inline mode blocks request for entire proxy listener lifetime",
		},

		// === Category×Mode constraints ===
		// Resource faults (cpu/memory/io/disk stress) run for seconds-to-minutes;
		// blocking the request for that duration is rarely intended.
		{
			Kind:    "category_mode",
			Subject: "category:resource", Object: "mode:inline",
			Scope: "always", ConstraintType: "soft",
			Reason: "resource stress typically runs longer than a request; blocking is rarely intended",
		},
		// Inline faults (latency/error/hang) are designed to affect the request
		// they fire on. background mode means the request continues without
		// waiting, defeating the purpose.
		{
			Kind:    "category_mode",
			Subject: "category:inline", Object: "mode:background",
			Scope: "always", ConstraintType: "soft",
			Reason: "inline faults are intended to affect the request; background mode means the request doesn't wait for them",
		},
	}
}

// DefaultIncompatibilities returns only the Kind="pair" constraints from
// DefaultConstraints(), preserving the old API for existing callers.
//
// Deprecated: use DefaultConstraints() and filter by Kind.
func DefaultIncompatibilities() []FaultConstraint {
	all := DefaultConstraints()
	var pairs []FaultConstraint
	for _, c := range all {
		if c.Kind == "pair" {
			pairs = append(pairs, c)
		}
	}
	return pairs
}
