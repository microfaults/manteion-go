package model

import (
	"testing"
)

func TestCacheBoxConfig_Validate(t *testing.T) {
	base := func() CacheBoxConfig {
		return CacheBoxConfig{
			Service: "productcatalog", Mode: "replay",
			KeyStrategy: "exact", MutationPolicy: "deny",
		}
	}

	tests := []struct {
		name    string
		modify  func(*CacheBoxConfig)
		wantErr bool
	}{
		{"valid replay", nil, false},
		{"valid passthrough", func(c *CacheBoxConfig) { c.Mode = "passthrough" }, false},
		{"valid replay_with_delay", func(c *CacheBoxConfig) {
			c.Mode = "replay_with_delay"
			c.SyntheticDelay = &SyntheticDelayConfig{P50Us: 1000, P95Us: 5000, P99Us: 10000}
		}, false},
		{"replay_with_delay missing synthetic_delay", func(c *CacheBoxConfig) {
			c.Mode = "replay_with_delay"
		}, true},
		{"mutation deny with safe_methods", func(c *CacheBoxConfig) {
			c.SafeMethods = []string{"POST /search"}
		}, true},
		{"mutation allow with safe_methods", func(c *CacheBoxConfig) {
			c.MutationPolicy = "allow"
			c.SafeMethods = []string{"POST /search"}
		}, false},
		{"invalid mode", func(c *CacheBoxConfig) { c.Mode = "bad" }, true},
		{"invalid key_strategy", func(c *CacheBoxConfig) { c.KeyStrategy = "bad" }, true},
		{"invalid mutation_policy", func(c *CacheBoxConfig) { c.MutationPolicy = "bad" }, true},
		{"missing service", func(c *CacheBoxConfig) { c.Service = "" }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base()
			if tt.modify != nil {
				tt.modify(&c)
			}
			err := c.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTraceAnchor_Validate(t *testing.T) {
	tests := []struct {
		name    string
		anchor  TraceAnchor
		wantErr bool
	}{
		{
			"valid jaeger",
			TraceAnchor{ID: "t1", ExperimentRunID: "r1", MetaTraceID: "m1", Service: "frontend", Backend: "jaeger"},
			false,
		},
		{
			"valid prometheus",
			TraceAnchor{ID: "t2", ExperimentRunID: "r1", MetaTraceID: "m1", Service: "frontend", Backend: "prometheus"},
			false,
		},
		{"invalid backend", TraceAnchor{ID: "t3", ExperimentRunID: "r1", MetaTraceID: "m1", Service: "frontend", Backend: "bad"}, true},
		{"missing service", TraceAnchor{ID: "t4", ExperimentRunID: "r1", MetaTraceID: "m1", Backend: "jaeger"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.anchor.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
