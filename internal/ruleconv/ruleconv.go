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

// CompiledRule is the wire format for resolved rules served to SDKs.
// It inlines fault config so the SDK can construct evaluator rules
// without additional lookups. This exists because atroposdk.StaticRule's
// Decision.Fault is a Go interface that can't survive JSON roundtrip.
type CompiledRule struct {
	Name           string            `json:"name"`
	InjectionPoint string            `json:"injection_point,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Mode           string            `json:"mode"`
	Priority       int               `json:"priority"`
	Fault          *InlineFault      `json:"fault,omitempty"`
}

// InlineFault is a resolved FaultSpec with config inlined.
type InlineFault struct {
	Category   string          `json:"category"`
	FaultType  string          `json:"fault_type"`
	Config     json.RawMessage `json:"config"`
	DurationMs int64           `json:"duration_ms,omitempty"`
	RampUpMs   int64           `json:"ramp_up_ms,omitempty"`
	RampDownMs int64           `json:"ramp_down_ms,omitempty"`
}

// CompileRules resolves FaultSpec references and produces wire-ready compiled rules.
// Only enabled rules with FaultSpecID are supported; FaultCompositionID support is deferred.
func CompileRules(rules []*model.Rule, specs FaultSpecResolver) ([]CompiledRule, error) {
	var out []CompiledRule
	for _, r := range rules {
		compiled, err := CompileRule(r, specs)
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
func CompileRule(r *model.Rule, specs FaultSpecResolver) (CompiledRule, error) {
	cr := CompiledRule{
		Name:           r.Name,
		InjectionPoint: r.Match.InjectionPoint,
		Labels:         r.Match.Labels,
		Mode:           r.Mode,
		Priority:       r.Priority,
	}

	switch {
	case r.FaultSpecID != "":
		spec, err := specs.GetFaultSpec(r.FaultSpecID)
		if err != nil {
			return CompiledRule{}, fmt.Errorf("resolve fault spec %q for rule %q: %w", r.FaultSpecID, r.ID, err)
		}
		if spec == nil {
			return CompiledRule{}, fmt.Errorf("fault spec %q not found for rule %q", r.FaultSpecID, r.ID)
		}
		cr.Fault = &InlineFault{
			Category:   spec.Category,
			FaultType:  spec.FaultType,
			Config:     spec.Config,
			DurationMs: spec.DurationMs,
			RampUpMs:   spec.RampUpMs,
			RampDownMs: spec.RampDownMs,
		}

	case r.FaultCompositionID != "":
		return CompiledRule{}, fmt.Errorf("rule %q: FaultComposition compilation not yet implemented", r.ID)
	}

	return cr, nil
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
