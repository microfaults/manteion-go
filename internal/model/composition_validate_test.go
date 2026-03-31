package model

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Test fixtures: fault specs covering all categories.
var testSpecs = map[string]*FaultSpec{
	"net-blackhole": {ID: "net-blackhole", Name: "blackhole", Category: "network", FaultType: "blackhole", Config: json.RawMessage(`{}`), CreatedAt: time.Now()},
	"net-latency":   {ID: "net-latency", Name: "latency", Category: "network", FaultType: "latency", Config: json.RawMessage(`{"delay_ms":100}`), CreatedAt: time.Now()},
	"net-throttle":  {ID: "net-throttle", Name: "throttle", Category: "network", FaultType: "throttle", Config: json.RawMessage(`{"bytes_per_sec":1024}`), CreatedAt: time.Now()},
	"net-drip":      {ID: "net-drip", Name: "drip", Category: "network", FaultType: "drip", Config: json.RawMessage(`{"chunk_size":1,"interval_ms":100}`), CreatedAt: time.Now()},
	"net-rst":       {ID: "net-rst", Name: "rst", Category: "network", FaultType: "rst", Config: json.RawMessage(`{"after_bytes":1024}`), CreatedAt: time.Now()},
	"net-loss":      {ID: "net-loss", Name: "loss", Category: "network", FaultType: "loss", Config: json.RawMessage(`{"rate":0.1}`), CreatedAt: time.Now()},
	"inline-latency": {ID: "inline-latency", Name: "latency", Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay_ms":50}`), CreatedAt: time.Now()},
	"inline-error":   {ID: "inline-error", Name: "error", Category: "inline", FaultType: "error", Config: json.RawMessage(`{"status_code":500}`), CreatedAt: time.Now()},
	"res-cpu":        {ID: "res-cpu", Name: "cpu", Category: "resource", FaultType: "cpu", Config: json.RawMessage(`{"target_load":0.8}`), CreatedAt: time.Now()},
	"res-io":         {ID: "res-io", Name: "io", Category: "resource", FaultType: "io", Config: json.RawMessage(`{"read_rate":1024}`), CreatedAt: time.Now()},
	"res-memory":     {ID: "res-memory", Name: "memory", Category: "resource", FaultType: "memory", Config: json.RawMessage(`{"target_load":0.5}`), CreatedAt: time.Now()},
}

func testFaultResolver(id string) *FaultSpec   { return testSpecs[id] }

var testCompositions = map[string]*FaultComposition{}

func testCompResolver(id string) *FaultComposition { return testCompositions[id] }

func TestValidateCompositionDepth(t *testing.T) {
	// Depth 1: flat composition of atoms.
	flat := &FaultComposition{
		ID: "flat", Name: "flat", ExecutionMode: "parallel",
		Members: []FaultCompositionMember{
			{Position: 0, FaultSpecID: "inline-latency"},
			{Position: 1, FaultSpecID: "res-cpu"},
		},
	}

	// Depth 2: one level of nesting.
	testCompositions["flat"] = flat
	nested2 := &FaultComposition{
		ID: "nested2", Name: "nested2", ExecutionMode: "parallel",
		Members: []FaultCompositionMember{
			{Position: 0, FaultSpecID: "res-io"},
			{Position: 1, ChildCompositionID: "flat"},
		},
	}

	// Depth 3: two levels of nesting (max allowed).
	testCompositions["nested2"] = nested2
	nested3 := &FaultComposition{
		ID: "nested3", Name: "nested3", ExecutionMode: "sequential",
		Members: []FaultCompositionMember{
			{Position: 0, FaultSpecID: "inline-error"},
			{Position: 1, ChildCompositionID: "nested2"},
		},
	}

	// Depth 4: exceeds max.
	testCompositions["nested3"] = nested3
	nested4 := &FaultComposition{
		ID: "nested4", Name: "nested4", ExecutionMode: "parallel",
		Members: []FaultCompositionMember{
			{Position: 0, FaultSpecID: "res-cpu"},
			{Position: 1, ChildCompositionID: "nested3"},
		},
	}

	tests := []struct {
		name    string
		comp    *FaultComposition
		max     int
		wantErr bool
	}{
		{"depth 1, max 3", flat, 3, false},
		{"depth 2, max 3", nested2, 3, false},
		{"depth 3, max 3", nested3, 3, false},
		{"depth 4, max 3 — rejected", nested4, 3, true},
		{"depth 2, max 1 — rejected", nested2, 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCompositionDepth(tt.comp, testCompResolver, tt.max)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCompositionDepth() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateNetworkDirections(t *testing.T) {
	tests := []struct {
		name    string
		comp    *FaultComposition
		wantErr bool
	}{
		{
			"valid: latency upstream + throttle downstream",
			&FaultComposition{
				ID: "ok-dir", Name: "ok", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-latency", Direction: "upstream"},
					{Position: 1, FaultSpecID: "net-throttle", Direction: "downstream"},
				},
			},
			false,
		},
		{
			"valid: network + non-network (no direction conflict)",
			&FaultComposition{
				ID: "ok-mixed", Name: "ok", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-latency", Direction: "upstream"},
					{Position: 1, FaultSpecID: "res-cpu"},
				},
			},
			false,
		},
		{
			"invalid: two network toxics same direction",
			&FaultComposition{
				ID: "bad-dir", Name: "bad", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-latency", Direction: "upstream"},
					{Position: 1, FaultSpecID: "net-throttle", Direction: "upstream"},
				},
			},
			true,
		},
		{
			"invalid: two network toxics unspecified direction",
			&FaultComposition{
				ID: "bad-nodir", Name: "bad", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-latency"},
					{Position: 1, FaultSpecID: "net-throttle"},
				},
			},
			true,
		},
		{
			"sequential: multiple network toxics OK (phase transitions)",
			&FaultComposition{
				ID: "seq-net", Name: "seq", ExecutionMode: "sequential",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-latency"},
					{Position: 1, FaultSpecID: "net-rst"},
				},
			},
			false, // sequential skips direction check
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNetworkDirections(tt.comp, testFaultResolver, testCompResolver)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateNetworkDirections() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateCompositionIncompatibilities(t *testing.T) {
	rules := DefaultIncompatibilities()

	tests := []struct {
		name         string
		comp         *FaultComposition
		wantHard     bool
		wantSoftAny  bool
	}{
		{
			"hard: blackhole + throttle parallel",
			&FaultComposition{
				ID: "bad-bh", Name: "bad", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-blackhole", Direction: "upstream"},
					{Position: 1, FaultSpecID: "net-throttle", Direction: "downstream"},
				},
			},
			true, false,
		},
		{
			"hard: blackhole + latency parallel",
			&FaultComposition{
				ID: "bad-bh-lat", Name: "bad", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-blackhole"},
					{Position: 1, FaultSpecID: "net-latency"},
				},
			},
			true, false,
		},
		{
			"hard: drip + throttle parallel",
			&FaultComposition{
				ID: "bad-drip", Name: "bad", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-drip"},
					{Position: 1, FaultSpecID: "net-throttle"},
				},
			},
			true, false,
		},
		{
			"soft: cpu + memory parallel",
			&FaultComposition{
				ID: "soft-res", Name: "soft", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "res-cpu"},
					{Position: 1, FaultSpecID: "res-memory"},
				},
			},
			false, true,
		},
		{
			"valid: inline latency + resource cpu parallel",
			&FaultComposition{
				ID: "ok-cross", Name: "ok", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "inline-latency"},
					{Position: 1, FaultSpecID: "res-cpu"},
				},
			},
			false, false,
		},
		{
			"valid: resource cpu + resource io parallel",
			&FaultComposition{
				ID: "ok-res", Name: "ok", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "res-cpu"},
					{Position: 1, FaultSpecID: "res-io"},
				},
			},
			false, false,
		},
		{
			"valid: network loss + resource io parallel (cross-category)",
			&FaultComposition{
				ID: "ok-loss-io", Name: "ok", ExecutionMode: "parallel",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "net-loss"},
					{Position: 1, FaultSpecID: "res-io"},
				},
			},
			false, false,
		},
		{
			"valid: inline latency then error sequential (order OK)",
			&FaultComposition{
				ID: "ok-seq", Name: "ok", ExecutionMode: "sequential",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "inline-latency"},
					{Position: 1, FaultSpecID: "inline-error"},
				},
			},
			false, false,
		},
		{
			"soft: inline error then latency sequential (error first = meaningless)",
			&FaultComposition{
				ID: "soft-seq", Name: "soft", ExecutionMode: "sequential",
				Members: []FaultCompositionMember{
					{Position: 0, FaultSpecID: "inline-error"},
					{Position: 1, FaultSpecID: "inline-latency"},
				},
			},
			false, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hardErr, softWarnings := ValidateCompositionIncompatibilities(
				tt.comp, testFaultResolver, testCompResolver, rules,
			)
			if (hardErr != nil) != tt.wantHard {
				t.Errorf("hardErr = %v, wantHard %v", hardErr, tt.wantHard)
			}
			if (len(softWarnings) > 0) != tt.wantSoftAny {
				t.Errorf("softWarnings = %v, wantSoftAny %v", softWarnings, tt.wantSoftAny)
			}
		})
	}
}

func TestMatchesType(t *testing.T) {
	tests := []struct {
		concrete string
		pattern  string
		want     bool
	}{
		{"network:blackhole", "network:blackhole", true},
		{"network:blackhole", "network:*", true},
		{"network:latency", "network:*", true},
		{"inline:error", "network:*", false},
		{"inline:error", "inline:error", true},
		{"resource:cpu", "resource:*", true},
	}

	for _, tt := range tests {
		name := tt.concrete + " vs " + tt.pattern
		t.Run(name, func(t *testing.T) {
			if got := matchesType(tt.concrete, tt.pattern); got != tt.want {
				t.Errorf("matchesType(%q, %q) = %v, want %v", tt.concrete, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestValidateComposition_Integration(t *testing.T) {
	// Clean up test compositions from other tests.
	saved := make(map[string]*FaultComposition)
	for k, v := range testCompositions {
		saved[k] = v
	}
	defer func() {
		testCompositions = saved
	}()

	// Reset.
	for k := range testCompositions {
		delete(testCompositions, k)
	}

	t.Run("valid composition passes all checks", func(t *testing.T) {
		comp := &FaultComposition{
			ID: "int-ok", Name: "ok", ExecutionMode: "parallel",
			Members: []FaultCompositionMember{
				{Position: 0, FaultSpecID: "net-latency", Direction: "upstream"},
				{Position: 1, FaultSpecID: "net-throttle", Direction: "downstream"},
			},
			CreatedAt: time.Now(),
		}
		err := ValidateComposition(comp, testFaultResolver, testCompResolver)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("blackhole + any network rejected", func(t *testing.T) {
		comp := &FaultComposition{
			ID: "int-bad", Name: "bad", ExecutionMode: "parallel",
			Members: []FaultCompositionMember{
				{Position: 0, FaultSpecID: "net-blackhole"},
				{Position: 1, FaultSpecID: "net-rst"},
			},
			CreatedAt: time.Now(),
		}
		err := ValidateComposition(comp, testFaultResolver, testCompResolver)
		if err == nil {
			t.Error("expected error for blackhole + rst")
		}
		if !strings.Contains(err.Error(), "incompatibility") && !strings.Contains(err.Error(), "network toxics") {
			// Could fail on direction check or incompatibility — both are correct.
			t.Logf("error (acceptable): %v", err)
		}
	})
}
