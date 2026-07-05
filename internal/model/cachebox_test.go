package model

import "testing"

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
