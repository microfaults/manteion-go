package ruleconv

import (
	"encoding/json"
	"fmt"

	"manteion-go/internal/model"
)

// FaultSpecResolver looks up a FaultSpec by ID.
type FaultSpecResolver interface {
	GetFaultSpec(id string) (*model.FaultSpec, error)
}

// FaultCompositionResolver looks up a FaultComposition by ID.
type FaultCompositionResolver interface {
	GetFaultComposition(id string) (*model.FaultComposition, error)
}

// CompiledRule is the wire format for resolved rules served to SDKs.
// It inlines fault config so the SDK can construct evaluator rules
// without additional lookups. This exists because atroposdk.StaticRule's
// Decision.Fault is a Go interface that can't survive JSON roundtrip.
//
// Exactly one of Fault, Composition, or CacheBox must be set.
type CompiledRule struct {
	Name           string               `json:"name"`
	InjectionPoint string               `json:"injection_point,omitempty"`
	Labels         map[string]string    `json:"labels,omitempty"`
	Mode           string               `json:"mode"`
	Priority       int                  `json:"priority"`
	StartPolicy    string               `json:"start_policy,omitempty"`
	Fault          *CompiledFault       `json:"fault,omitempty"`
	Composition    *CompiledComposition `json:"composition,omitempty"`
	CacheBox       *CompiledCacheBox    `json:"cachebox,omitempty"`
}

// CompiledCacheBox is a resolved cache-box action for a rule.
type CompiledCacheBox struct {
	Mode        string `json:"mode"`         // "passthrough" | "replay" | "replay_with_delay"
	KeyStrategy string `json:"key_strategy"` // "exact" | "exact_with_host" | "exact_with_body"
}

// CompiledFault is a resolved FaultSpec on the wire.
//
// The category determines which sibling field is populated:
//   - "inline"   → Params holds the toxic-specific params; no envelope.
//   - "network"  → Network envelope holds host/target/direction/scope;
//     Params holds the toxic-specific params.
//   - "resource" → Params holds the toxic-specific params; no envelope.
type CompiledFault struct {
	Category   string `json:"category"`
	FaultType  string `json:"fault_type"`
	DurationMs int64  `json:"duration_ms,omitempty"`
	RampUpMs   int64  `json:"ramp_up_ms,omitempty"`
	RampDownMs int64  `json:"ramp_down_ms,omitempty"`

	Network *CompiledNetworkEnvelope `json:"network,omitempty"`
	Params  json.RawMessage          `json:"params,omitempty"`
}

// CompiledNetworkEnvelope is the network-category-only envelope on the wire.
// Matches atropos-go's NetworkEnvelope.
type CompiledNetworkEnvelope struct {
	Host      string  `json:"host"`
	Target    string  `json:"target,omitempty"`
	Direction string  `json:"direction,omitempty"`
	Scope     float64 `json:"scope,omitempty"`
}

// CompiledComposition is a resolved FaultComposition tree with all specs inlined.
// ExecutionMode and member Direction are plain strings (not model.ExecutionMode /
// model.Direction) to decouple the wire contract from internal Go type refactors.
type CompiledComposition struct {
	Name          string                      `json:"name"`
	ExecutionMode string                      `json:"execution_mode"`
	DurationMs    int64                       `json:"duration_ms,omitempty"`
	RampUpMs      int64                       `json:"ramp_up_ms,omitempty"`
	RampDownMs    int64                       `json:"ramp_down_ms,omitempty"`
	Members       []CompiledCompositionMember `json:"members"`
}

// CompiledCompositionMember is a resolved member — either a leaf fault or a nested composition.
type CompiledCompositionMember struct {
	Direction   string               `json:"direction,omitempty"`
	Fault       *CompiledFault       `json:"fault,omitempty"`
	Composition *CompiledComposition `json:"composition,omitempty"`
}

// CompileRules resolves FaultSpec/Composition references and produces wire-ready compiled rules.
func CompileRules(rules []*model.Rule, specs FaultSpecResolver, comps ...FaultCompositionResolver) ([]CompiledRule, error) {
	var compResolver FaultCompositionResolver
	if len(comps) > 0 {
		compResolver = comps[0]
	}

	var out []CompiledRule
	for _, r := range rules {
		compiled, err := compileRule(r, specs, compResolver)
		if err != nil {
			return nil, err
		}
		out = append(out, compiled)
	}
	if out == nil {
		out = []CompiledRule{}
	}
	return out, nil
}

// CompileRule resolves a single rule.
func CompileRule(r *model.Rule, specs FaultSpecResolver, comps ...FaultCompositionResolver) (CompiledRule, error) {
	var compResolver FaultCompositionResolver
	if len(comps) > 0 {
		compResolver = comps[0]
	}
	return compileRule(r, specs, compResolver)
}

func compileRule(r *model.Rule, specs FaultSpecResolver, comps FaultCompositionResolver) (CompiledRule, error) {
	cr := CompiledRule{
		Name:           r.Name,
		InjectionPoint: r.Match.InjectionPoint,
		Labels:         r.Match.Labels,
		Mode:           r.Mode,
		Priority:       r.Priority,
		StartPolicy:    r.StartPolicy,
	}

	switch r.Action.Type {
	case "fault_spec":
		f, err := resolveSpec(r.Action.FaultSpecID, specs)
		if err != nil {
			return CompiledRule{}, fmt.Errorf("rule %q: %w", r.ID, err)
		}
		cr.Fault = f

	case "fault_composition":
		if comps == nil {
			return CompiledRule{}, fmt.Errorf("rule %q: composition resolver required for fault_composition", r.ID)
		}
		cc, err := resolveComposition(r.Action.FaultCompID, specs, comps, 0)
		if err != nil {
			return CompiledRule{}, fmt.Errorf("rule %q: %w", r.ID, err)
		}
		cr.Composition = cc

	case "cachebox":
		cr.CacheBox = &CompiledCacheBox{
			Mode:        r.Action.CacheBox.Mode,
			KeyStrategy: r.Action.CacheBox.KeyStrategy,
		}
	}

	return cr, nil
}

func resolveSpec(id string, specs FaultSpecResolver) (*CompiledFault, error) {
	spec, err := specs.GetFaultSpec(id)
	if err != nil {
		return nil, fmt.Errorf("resolve fault spec %q: %w", id, err)
	}
	if spec == nil {
		return nil, fmt.Errorf("fault spec %q not found", id)
	}
	cf := &CompiledFault{
		Category:   spec.Category,
		FaultType:  spec.FaultType,
		Params:     spec.Params,
		DurationMs: spec.DurationMs,
		RampUpMs:   spec.RampUpMs,
		RampDownMs: spec.RampDownMs,
	}
	if spec.Category == "network" {
		host := spec.Host
		if host == "" {
			host = "proxy"
		}
		env := &CompiledNetworkEnvelope{Host: host}
		if spec.Network != nil {
			env.Target = spec.Network.Target
			env.Direction = spec.Network.Direction
			env.Scope = spec.Network.Scope
		}
		cf.Network = env
	}
	return cf, nil
}

const maxCompositionDepth = 3

func resolveComposition(id string, specs FaultSpecResolver, comps FaultCompositionResolver, depth int) (*CompiledComposition, error) {
	if depth >= maxCompositionDepth {
		return nil, fmt.Errorf(
			"composition %q nesting exceeds max depth %d (atoms→groups→top-level). "+
				"The cap is enforced at resolution time and may be raised in a future revision.",
			id, maxCompositionDepth,
		)
	}

	comp, err := comps.GetFaultComposition(id)
	if err != nil {
		return nil, fmt.Errorf("resolve composition %q: %w", id, err)
	}
	if comp == nil {
		return nil, fmt.Errorf("composition %q not found", id)
	}

	cc := &CompiledComposition{
		Name:          comp.Name,
		ExecutionMode: string(comp.ExecutionMode),
		DurationMs:    comp.DurationMs,
		RampUpMs:      comp.RampUpMs,
		RampDownMs:    comp.RampDownMs,
		Members:       make([]CompiledCompositionMember, len(comp.Members)),
	}

	for i, m := range comp.Members {
		member := CompiledCompositionMember{
			Direction: string(m.Direction),
		}

		switch {
		case m.FaultSpecID != "":
			f, err := resolveSpec(m.FaultSpecID, specs)
			if err != nil {
				return nil, fmt.Errorf("composition %q member[%d]: %w", id, i, err)
			}
			member.Fault = f

		case m.ChildCompositionID != "":
			child, err := resolveComposition(m.ChildCompositionID, specs, comps, depth+1)
			if err != nil {
				return nil, fmt.Errorf("composition %q member[%d]: %w", id, i, err)
			}
			member.Composition = child
		}

		cc.Members[i] = member
	}

	return cc, nil
}

// FuncResolver adapts a func(id string) *model.FaultSpec into FaultSpecResolver.
type FuncResolver struct {
	Fn func(id string) *model.FaultSpec
}

func (f *FuncResolver) GetFaultSpec(id string) (*model.FaultSpec, error) {
	spec := f.Fn(id)
	if spec == nil {
		return nil, nil
	}
	return spec, nil
}

// FuncCompositionResolver adapts a func into FaultCompositionResolver.
type FuncCompositionResolver struct {
	Fn func(id string) *model.FaultComposition
}

func (f *FuncCompositionResolver) GetFaultComposition(id string) (*model.FaultComposition, error) {
	comp := f.Fn(id)
	if comp == nil {
		return nil, nil
	}
	return comp, nil
}
