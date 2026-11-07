package model

import (
	"strings"
	"testing"
)

func TestKeyStrategyResolveAndVersion(t *testing.T) {
	if got := ResolveKeyStrategy(""); got != "canonical_v2" {
		t.Fatalf("ResolveKeyStrategy(\"\") = %q, want canonical_v2", got)
	}
	if got := ResolveKeyStrategy("exact"); got != "exact" {
		t.Fatalf("ResolveKeyStrategy(exact) = %q, want exact", got)
	}
	if got := KeyStrategyVersion("canonical_v2"); got != 2 {
		t.Fatalf("KeyStrategyVersion(canonical_v2) = %d, want 2", got)
	}
	if got := KeyStrategyVersion("exact"); got != 1 {
		t.Fatalf("KeyStrategyVersion(exact) = %d, want 1", got)
	}
	if got := KeyStrategyVersion(""); got != 2 {
		t.Fatalf("KeyStrategyVersion(\"\") = %d, want 2 (default canonical_v2)", got)
	}
}

// TestExperimentCreate_RejectsStrategyDisagreement pins MANT-4(d): all phases
// referencing a service must agree on (key_strategy, key_headers) so record and
// replay derive keys identically (INV-2). Empty strategy defaults to
// canonical_v2; different services are independent.
func TestExperimentCreate_RejectsStrategyDisagreement(t *testing.T) {
	tests := []struct {
		name    string
		phases  []ExperimentPhase
		wantErr bool
	}{
		{
			name: "conflicting strategies for one service",
			phases: []ExperimentPhase{
				{FrozenServices: []CacheBoxConfig{{Service: "frontend", Mode: "replay", KeyStrategy: "exact"}}},
				{FrozenServices: []CacheBoxConfig{{Service: "frontend", Mode: "replay", KeyStrategy: "canonical_v2"}}},
			},
			wantErr: true,
		},
		{
			name: "same strategy agrees",
			phases: []ExperimentPhase{
				{FrozenServices: []CacheBoxConfig{{Service: "frontend", KeyStrategy: "exact"}}},
				{FrozenServices: []CacheBoxConfig{{Service: "frontend", KeyStrategy: "exact"}}},
			},
			wantErr: false,
		},
		{
			name: "empty defaults to canonical_v2 and agrees with explicit canonical_v2",
			phases: []ExperimentPhase{
				{FrozenServices: []CacheBoxConfig{{Service: "cart", KeyStrategy: ""}}},
				{FrozenServices: []CacheBoxConfig{{Service: "cart", KeyStrategy: "canonical_v2"}}},
			},
			wantErr: false,
		},
		{
			name: "different services are independent",
			phases: []ExperimentPhase{
				{FrozenServices: []CacheBoxConfig{{Service: "a", KeyStrategy: "exact"}}},
				{FrozenServices: []CacheBoxConfig{{Service: "b", KeyStrategy: "canonical_v2"}}},
			},
			wantErr: false,
		},
		{
			name: "key_headers disagreement",
			phases: []ExperimentPhase{
				{FrozenServices: []CacheBoxConfig{{Service: "x", KeyStrategy: "canonical_v2", KeyHeaders: []string{"X-Tenant"}}}},
				{FrozenServices: []CacheBoxConfig{{Service: "x", KeyStrategy: "canonical_v2", KeyHeaders: []string{"x-region"}}}},
			},
			wantErr: true,
		},
		{
			name: "key_headers agree modulo case and order",
			phases: []ExperimentPhase{
				{FrozenServices: []CacheBoxConfig{{Service: "x", KeyStrategy: "canonical_v2", KeyHeaders: []string{"X-Tenant", "X-Region"}}}},
				{FrozenServices: []CacheBoxConfig{{Service: "x", KeyStrategy: "canonical_v2", KeyHeaders: []string{"x-region", "x-tenant"}}}},
			},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCacheBoxStrategyAgreement(tc.phases)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// A user-authored cache-box rule attached to an experiment's phases keys with
// its own key_strategy and no key headers (it compiles with the served
// phase's context, ruleconv), so it must agree with the frozen-service config
// of its service: the same resolved strategy and no key_headers. A rule whose
// service no phase freezes, and a rule that is not a cache-box rule, are not
// constrained.
func TestValidateCacheBoxStrategyAgreement_UserCacheBoxRules(t *testing.T) {
	frozen := func(svc, strat string, headers ...string) []ExperimentPhase {
		return []ExperimentPhase{{FrozenServices: []CacheBoxConfig{{Service: svc, Mode: "replay", KeyStrategy: strat, KeyHeaders: headers}}}}
	}
	cbRule := func(name, svc, strat string) *Rule {
		return &Rule{ID: name, Name: name, Service: svc, Action: RuleAction{Type: "cachebox",
			CacheBox: &CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: strat}}}
	}
	tests := []struct {
		name    string
		phases  []ExperimentPhase
		rules   []*Rule
		wantErr string
	}{
		{"same strategy agrees", frozen("cart", "exact"), []*Rule{cbRule("r1", "cart", "exact")}, ""},
		{"the default resolves before comparison", frozen("cart", ""), []*Rule{cbRule("r1", "cart", "canonical_v2")}, ""},
		{"strategy mismatch", frozen("cart", "canonical_v2"), []*Rule{cbRule("r1", "cart", "exact")},
			`cachebox rule "r1" for service "cart" keys with "exact", but the experiment's frozen-service config for "cart" keys with "canonical_v2"`},
		{"key headers on the frozen config", frozen("cart", "exact", "X-Tenant"), []*Rule{cbRule("r1", "cart", "exact")},
			`cachebox rule "r1" for service "cart" keys with no key_headers, but the experiment's frozen-service config for "cart" keys with [x-tenant]`},
		{"a service no phase freezes is not constrained", frozen("cart", "canonical_v2"), []*Rule{cbRule("r1", "checkout", "exact")}, ""},
		{"a fault rule is not constrained", frozen("cart", "canonical_v2"),
			[]*Rule{{ID: "f1", Name: "f1", Service: "cart", Action: RuleAction{Type: "fault_spec", FaultSpecID: "spec-1"}}}, ""},
		{"no rules", frozen("cart", "exact"), nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCacheBoxStrategyAgreement(tc.phases, tc.rules...)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
