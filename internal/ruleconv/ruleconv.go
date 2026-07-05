// Package ruleconv compiles model.Rule + FaultSpec/FaultComposition rows into
// the wire format served to SDKs. The wire structs themselves are defined in
// atropos-go (CompiledRule, FaultRequest, NetworkEnvelope, ...) and imported
// here — both ends of the manteion↔SDK contract marshal the same types, so
// the field set cannot drift. This package's job is purely resolution:
// inlining spec/composition references so the SDK constructs evaluator rules
// without further lookups.
package ruleconv

import (
	"fmt"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

// Wire-type aliases so downstream packages (api, atrocontrol, policy) can
// keep referring to ruleconv.Compiled* without importing the SDK module.
type (
	CompiledRule              = atroposdk.CompiledRule
	CompiledFault             = atroposdk.CompiledFault // = atroposdk.FaultRequest
	CompiledCacheBox          = atroposdk.CompiledCacheBox
	CompiledNetworkEnvelope   = atroposdk.NetworkEnvelope
	CompiledComposition       = atroposdk.CompiledComposition
	CompiledCompositionMember = atroposdk.CompiledCompositionMember
)

// SynthesizeCacheBoxRule builds the wildcard-egress compiled cache-box rule
// that carries a service's authoritative CacheBoxContext (§W1) for a running
// phase. The SDK matches it on any egress request (empty labels) and records
// (passthrough) or replays (replay/replay_with_delay) tagged with the
// context's (experiment_id, phase_id).
//
// This synthesized rule is the sole recording/replay provenance since the
// global RecordingPhaseID signal was removed (MANT-4/INV-5): the SDK's
// ActiveRecordingPhases + CacheDrainTracker key off it appearing in — and, at
// drain, disappearing from — the polled rule set.
func SynthesizeCacheBoxRule(rc model.CacheBoxRuleContext) CompiledRule {
	return CompiledRule{
		Name:           "cachebox:" + rc.Mode + ":" + rc.PhaseID,
		InjectionPoint: "egress",
		Mode:           "inline",
		CacheBox: &CompiledCacheBox{
			Mode:        rc.Mode,
			KeyStrategy: rc.KeyStrategy,
			Context: &atroposdk.CacheBoxContext{
				ExperimentID:    rc.ExperimentID,
				PhaseID:         rc.PhaseID,
				KeyStrategy:     rc.KeyStrategy,
				StrategyVersion: rc.StrategyVersion,
				KeyHeaders:      rc.KeyHeaders,
			},
		},
	}
}

// FaultSpecResolver looks up a FaultSpec by ID.
type FaultSpecResolver interface {
	GetFaultSpec(id string) (*model.FaultSpec, error)
}

// FaultCompositionResolver looks up a FaultComposition by ID.
type FaultCompositionResolver interface {
	GetFaultComposition(id string) (*model.FaultComposition, error)
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
