package desired

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
	"manteion-go/internal/ruleconv"
)

// The Materializer reads its inputs through these seams so it stays a pure
// function of them: the real repositories satisfy them, and tests pass fakes.

// RuleSource returns the rules a service should currently enforce (enabled,
// phase-aware), in the order the SDK evaluates them.
type RuleSource interface {
	ForService(ctx context.Context, service string) ([]*model.Rule, error)
}

// FaultConfigSource returns the manual long-running faults active on a service.
type FaultConfigSource interface {
	ListActiveForService(ctx context.Context, service string) ([]*model.FaultConfig, error)
}

// CacheBoxSource returns the service's cache-box role in a running phase, or
// nil when it has none.
type CacheBoxSource interface {
	ActiveCacheBoxPhaseForService(ctx context.Context, service string) (*model.CacheBoxRuleContext, error)
}

// SpecSource resolves fault-spec and composition references for rule compilation.
type SpecSource interface {
	SpecResolver(ctx context.Context) model.FaultSpecResolver
	CompositionResolver(ctx context.Context) model.CompositionResolver
}

// FreezeSource returns the delay source a frozen service's cache-box should
// install, or nil when the service is not frozen.
type FreezeSource interface {
	FreezeConfigForService(ctx context.Context, service string) (*atroposdk.DelayRequest, error)
}

// ReplaySetSource returns the replay set a frozen service's instances must
// hold, or nil when none is desired.
type ReplaySetSource interface {
	ReplaySetForService(ctx context.Context, service string) (*model.ReplaySetRef, error)
}

// GenerationGetter reads a service's current generation.
type GenerationGetter interface {
	Get(ctx context.Context, service string) (uint64, error)
}

// Materializer builds the desired-state Document for one instance of a service.
type Materializer struct {
	Generations  GenerationGetter
	Rules        RuleSource
	FaultConfigs FaultConfigSource
	CacheBox     CacheBoxSource
	Specs        SpecSource
	Freeze       FreezeSource    // optional; nil → no freeze_cfg (plan 2 wires the frozen-phase source)
	ReplaySets   ReplaySetSource // optional; nil → no replay_set (plan 2)
}

// Document is the desired state for one instance: what the poll and register
// responses carry, already filtered to what the instance's host can execute.
type Document struct {
	Generation   uint64
	ETag         string                   // the body's content hash alone (see etag)
	Rules        []atroposdk.CompiledRule // never nil
	ActiveFaults []atroposdk.FaultRequest // never nil
	FreezeCfg    *atroposdk.DelayRequest
	ReplaySet    *model.ReplaySetRef
}

// Materialize reads the service's inputs and returns inst's Document. A nil
// inst (or one that advertised no capabilities) gets everything. The freeze
// config and replay set go only to a host with a cache-box, the one place that
// can install a delay source or a replay set. Any failure returns the zero
// Document and the error: there is no partial document to serve.
func (m *Materializer) Materialize(ctx context.Context, service string, inst *model.SDKInstance) (Document, error) {
	// Read the generation first so a document never claims a generation newer
	// than its data: a bump landing mid-read leaves the document at the older
	// number, and the instance's next poll sees the newer one.
	gen, err := m.Generations.Get(ctx, service)
	if err != nil {
		return Document{}, fmt.Errorf("generation: %w", err)
	}
	rules, err := m.Rules.ForService(ctx, service)
	if err != nil {
		return Document{}, fmt.Errorf("rules: %w", err)
	}
	compiled, err := ruleconv.CompileRules(rules,
		&ruleconv.FuncResolver{Fn: m.Specs.SpecResolver(ctx)},
		&ruleconv.FuncCompositionResolver{Fn: m.Specs.CompositionResolver(ctx)})
	if err != nil {
		return Document{}, fmt.Errorf("compile rules: %w", err)
	}
	cb, err := m.CacheBox.ActiveCacheBoxPhaseForService(ctx, service)
	if err != nil {
		return Document{}, fmt.Errorf("cache-box role: %w", err)
	}
	if cb != nil {
		compiled = append(compiled, ruleconv.SynthesizeCacheBoxRule(*cb))
	}
	configs, err := m.FaultConfigs.ListActiveForService(ctx, service)
	if err != nil {
		return Document{}, fmt.Errorf("active faults: %w", err)
	}
	doc := Document{Generation: gen}
	caps := CapabilitiesOf(inst)
	doc.Rules = FilterRules(compiled, caps)
	doc.ActiveFaults = FilterFaults(FaultRequestsOf(configs), caps)
	if caps.Allows("cachebox") {
		if m.Freeze != nil {
			if doc.FreezeCfg, err = m.Freeze.FreezeConfigForService(ctx, service); err != nil {
				return Document{}, fmt.Errorf("freeze config: %w", err)
			}
		}
		if m.ReplaySets != nil {
			if doc.ReplaySet, err = m.ReplaySets.ReplaySetForService(ctx, service); err != nil {
				return Document{}, fmt.Errorf("replay set: %w", err)
			}
		}
	}
	if doc.ETag, err = etag(doc); err != nil {
		return Document{}, err
	}
	return doc, nil
}

// etag is the content hash alone, "<sha256 hex[:16]>" over the canonical JSON
// of the document body (encoding/json is deterministic for these types: struct
// fields in declaration order, map keys sorted, JSONB params as Postgres
// normalized them). The generation is not an input: it travels in
// X-Atropos-Generation and the body's version, so a bump that leaves an
// instance's document unchanged keeps its ETag, and the instance is answered
// 304 with the new generation instead of being sent the same document again.
// Slice order is content: equal-priority rules are evaluated in the order
// delivered, so a reorder is a change.
func etag(d Document) (string, error) {
	body, err := json.Marshal(struct {
		Rules        []atroposdk.CompiledRule `json:"rules"`
		ActiveFaults []atroposdk.FaultRequest `json:"active_faults"`
		FreezeCfg    *atroposdk.DelayRequest  `json:"freeze_cfg,omitempty"`
		ReplaySet    *model.ReplaySetRef      `json:"replay_set,omitempty"`
	}{d.Rules, d.ActiveFaults, d.FreezeCfg, d.ReplaySet})
	if err != nil {
		return "", fmt.Errorf("canonical json: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])[:16], nil
}

// FaultRequestsOf maps active manual fault configs to their wire form.
// Composition configs (no Params) are skipped: they aren't deliverable yet.
// The result is never nil.
func FaultRequestsOf(configs []*model.FaultConfig) []atroposdk.FaultRequest {
	faults := make([]atroposdk.FaultRequest, 0, len(configs))
	for _, c := range configs {
		if len(c.Params) == 0 {
			continue // composition configs aren't deliverable yet
		}
		req := atroposdk.FaultRequest{
			ID:         c.ID,
			Category:   c.Category,
			FaultType:  c.FaultType,
			DurationMs: c.DurationMs,
			RampUpMs:   c.RampUpMs,
			RampDownMs: c.RampDownMs,
			Params:     c.Params,
		}
		if c.Network != nil {
			req.Network = &atroposdk.NetworkEnvelope{
				Target:    c.Network.Target,
				Direction: c.Network.Direction,
				Scope:     c.Network.Scope,
			}
		}
		faults = append(faults, req)
	}
	return faults
}
