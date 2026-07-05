package model

import (
	"fmt"
	"slices"
	"strings"
)

// DefaultKeyStrategy is the cache-box keyer used when a frozen-service config
// leaves key_strategy empty. canonical_v2 is the length-prefixed SHA-256 keyer
// (design doc Q3) and the platform default from 2026-07 on.
const DefaultKeyStrategy = "canonical_v2"

// ResolveKeyStrategy returns the effective key strategy, defaulting an empty
// value to DefaultKeyStrategy.
func ResolveKeyStrategy(s string) string {
	if s == "" {
		return DefaultKeyStrategy
	}
	return s
}

// KeyStrategyVersion is the wire strategy_version for a key strategy: 2 for
// canonical_v2 (and the default), 1 for the legacy exact* strategies. Record
// and replay must agree on it (INV-2); manteion verifies at preload (MANT-5).
func KeyStrategyVersion(s string) int {
	if ResolveKeyStrategy(s) == "canonical_v2" {
		return 2
	}
	return 1
}

// CacheBoxRuleContext is the resolved authoritative context (§W1) for one
// service's active cache-box role in a running phase — the input from which
// the poll path synthesizes a compiled cache-box rule (MANT-4). Mode is
// "passthrough" (record on a baseline) or "replay"/"replay_with_delay" (an
// isolation freeze).
type CacheBoxRuleContext struct {
	ExperimentID    string
	PhaseID         string
	Mode            string
	KeyStrategy     string
	StrategyVersion int
	KeyHeaders      []string
}

// normalizeKeyHeaders lowercases and sorts a header allowlist so agreement
// comparisons ignore case and order (the SDK allowlist is case-insensitive).
func normalizeKeyHeaders(headers []string) []string {
	if len(headers) == 0 {
		return nil
	}
	out := make([]string, len(headers))
	for i, h := range headers {
		out[i] = strings.ToLower(h)
	}
	slices.Sort(out)
	return out
}

// ValidateCacheBoxStrategyAgreement enforces MANT-4(d): every frozen-service
// config for a given service, across all of an experiment's phases, must agree
// on (key_strategy, key_headers) — record and replay of that service must
// derive keys identically (INV-2). Empty key_strategy resolves to the default
// before comparison; key_headers are compared case- and order-insensitively.
func ValidateCacheBoxStrategyAgreement(phases []ExperimentPhase) error {
	type keying struct {
		strategy string
		headers  []string
	}
	seen := make(map[string]keying)
	for _, p := range phases {
		for _, fs := range p.FrozenServices {
			k := keying{
				strategy: ResolveKeyStrategy(fs.KeyStrategy),
				headers:  normalizeKeyHeaders(fs.KeyHeaders),
			}
			prev, ok := seen[fs.Service]
			if !ok {
				seen[fs.Service] = k
				continue
			}
			if prev.strategy != k.strategy {
				return fmt.Errorf(
					"cachebox strategy disagreement for service %q: %q vs %q (all phases touching a service must agree)",
					fs.Service, prev.strategy, k.strategy)
			}
			if !slices.Equal(prev.headers, k.headers) {
				return fmt.Errorf(
					"cachebox key_headers disagreement for service %q: %v vs %v",
					fs.Service, prev.headers, k.headers)
			}
		}
	}
	return nil
}
