package desired

import (
	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

// Capabilities is the set of fault categories an instance's host executes,
// built from what it advertised at registration. nil means the host advertised
// nothing — the wire contract's "absent = all" — so every category is allowed.
type Capabilities map[string]bool

// knownCategories are the capability strings the protocol defines. A host may
// advertise others (the sidecar may say "netem"); they name no category this
// control plane delivers, so they are dropped rather than carried.
var knownCategories = map[string]bool{
	"inline":   true,
	"network":  true,
	"resource": true,
	"cachebox": true,
}

// CapabilitiesOf returns the filter for inst. A nil instance or an empty list
// yields nil (allow everything). A non-empty list yields exactly its known
// members, so unknown strings neither unlock a category nor are rejected, and
// a list of only unknown strings allows nothing.
func CapabilitiesOf(inst *model.SDKInstance) Capabilities {
	if inst == nil || len(inst.Capabilities) == 0 {
		return nil
	}
	c := make(Capabilities, len(inst.Capabilities))
	for _, s := range inst.Capabilities {
		if knownCategories[s] {
			c[s] = true
		}
	}
	return c
}

// Allows reports whether the host executes category. nil allows every category.
func (c Capabilities) Allows(category string) bool {
	return c == nil || c[category]
}

func (c Capabilities) allowsAll(categories []string) bool {
	for _, category := range categories {
		if !c.Allows(category) {
			return false
		}
	}
	return true
}

// RuleCategories returns the categories a compiled rule needs its host to
// execute: a fault's effective category, every leaf of a composition (member
// order, depth first, one entry per leaf), "cachebox" for a cache-box rule.
func RuleCategories(cr atroposdk.CompiledRule) []string {
	var categories []string
	if cr.Fault != nil {
		categories = append(categories, cr.Fault.EffectiveCategory())
	}
	categories = appendCompositionCategories(categories, cr.Composition)
	if cr.CacheBox != nil {
		categories = append(categories, "cachebox")
	}
	return categories
}

func appendCompositionCategories(categories []string, c *atroposdk.CompiledComposition) []string {
	if c == nil {
		return categories
	}
	for _, m := range c.Members {
		if m.Fault != nil {
			categories = append(categories, m.Fault.EffectiveCategory())
		}
		categories = appendCompositionCategories(categories, m.Composition)
	}
	return categories
}

// FilterRules keeps the rules whose every category the host allows, so a
// composition survives only when all its leaves do. A rule that names no
// category is kept. The result is never nil.
func FilterRules(rules []atroposdk.CompiledRule, caps Capabilities) []atroposdk.CompiledRule {
	out := make([]atroposdk.CompiledRule, 0, len(rules))
	for _, r := range rules {
		if caps.allowsAll(RuleCategories(r)) {
			out = append(out, r)
		}
	}
	return out
}

// FilterFaults keeps the faults whose category the host allows. The result is
// never nil.
func FilterFaults(faults []atroposdk.FaultRequest, caps Capabilities) []atroposdk.FaultRequest {
	out := make([]atroposdk.FaultRequest, 0, len(faults))
	for _, f := range faults {
		if caps.Allows(f.EffectiveCategory()) {
			out = append(out, f)
		}
	}
	return out
}
