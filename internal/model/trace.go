package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// TraceAnchor is a pointer into an external trace/metrics backend.
// Manteion stores when and where to look up trace data, not the data itself.
//
// Anchored at a phase (per migration #19 — previously experiment_run_id).
type TraceAnchor struct {
	ID          string          `json:"id"`
	PhaseID     string          `json:"phase_id"`
	MetaTraceID string          `json:"meta_trace_id"`
	Service     string          `json:"service"`
	Backend     string          `json:"backend"` // "jaeger", "prometheus", "tempo"
	QueryHint   json.RawMessage `json:"query_hint"`
	CollectedAt time.Time       `json:"collected_at"`
}

var validBackends = setOf(TraceBackendValues...)

func (t *TraceAnchor) Validate() error {
	if t.ID == "" {
		return errors.New("trace anchor: id required")
	}
	if t.PhaseID == "" {
		return errors.New("trace anchor: phase_id required")
	}
	if t.MetaTraceID == "" {
		return errors.New("trace anchor: meta_trace_id required")
	}
	if t.Service == "" {
		return errors.New("trace anchor: service required")
	}
	if !validBackends[t.Backend] {
		return fmt.Errorf("trace anchor: invalid backend %q", t.Backend)
	}
	return nil
}

// CacheBoxConfig describes cache-box state for a frozen service.
//
// The cache-box is the core research primitive: a service frozen at its SDK
// boundary to replay cached responses instead of doing real work. Zero CPU,
// zero queueing, but the call graph structure is preserved.
type CacheBoxConfig struct {
	Service          string                `json:"service"`
	Mode             string                `json:"mode"`                   // "passthrough", "replay", "replay_with_delay"
	WorkflowScope    string                `json:"workflow_scope"`         // meta-trace-id pattern, "" = all traffic
	KeyStrategy      string                `json:"key_strategy"`           // "exact", "exact_with_host", "exact_with_body" — matches SDK cachebox.KeyStrategy
	MutationPolicy   string                `json:"mutation_policy"`        // "deny" (default), "allow". Metadata only — not enforced by SDK. Records operator intent for experiment reproducibility.
	SafeMethods      []string              `json:"safe_methods,omitempty"` // Metadata only — not enforced by SDK.
	SyntheticDelay   *SyntheticDelayConfig `json:"synthetic_delay,omitempty"`
	WarmupDurationMs int64                 `json:"warmup_duration_ms,omitempty"`
	CacheTTLMs       int64                 `json:"cache_ttl_ms,omitempty"`
}

var (
	validCacheBoxModes = setOf(CacheBoxModeValues...)
	validKeyStrategies = setOf(CacheBoxKeyStrategyValues...)
	// mutation_policy lives inside the frozen_services JSONB (not a DB
	// enum), so it is Go-validated only.
	validMutationPolicies = setOf("deny", "allow")
)

func (c *CacheBoxConfig) Validate() error {
	if c.Service == "" {
		return errors.New("cachebox config: service required")
	}
	if !validCacheBoxModes[c.Mode] {
		return fmt.Errorf("cachebox config: invalid mode %q", c.Mode)
	}
	if !validKeyStrategies[c.KeyStrategy] {
		return fmt.Errorf("cachebox config: invalid key_strategy %q", c.KeyStrategy)
	}
	if !validMutationPolicies[c.MutationPolicy] {
		return fmt.Errorf("cachebox config: invalid mutation_policy %q", c.MutationPolicy)
	}
	if c.MutationPolicy == "deny" && len(c.SafeMethods) > 0 {
		return errors.New("cachebox config: safe_methods only valid when mutation_policy=allow")
	}
	if c.Mode == "replay_with_delay" && c.SyntheticDelay == nil {
		return errors.New("cachebox config: synthetic_delay required for replay_with_delay mode")
	}
	return nil
}

// SyntheticDelayConfig parameterizes the delay injected in replay_with_delay mode.
// Stores both summary percentiles and the full observed histogram with a
// parametric (lognormal) fit for sampling.
type SyntheticDelayConfig struct {
	P50Us            int64             `json:"p50_us"`
	P95Us            int64             `json:"p95_us"`
	P99Us            int64             `json:"p99_us"`
	HistogramBuckets []HistogramBucket `json:"histogram_buckets,omitempty"`
	FitMu            *float64          `json:"fit_mu,omitempty"`
	FitSigma         *float64          `json:"fit_sigma,omitempty"`
}

func (s *SyntheticDelayConfig) Validate() error {
	if s.P50Us < 0 || s.P95Us < 0 || s.P99Us < 0 {
		return errors.New("synthetic delay: percentiles must be non-negative")
	}
	if (s.FitMu == nil) != (s.FitSigma == nil) {
		return errors.New("synthetic delay: fit_mu and fit_sigma must both be present or both absent")
	}
	return nil
}

// HistogramBucket is one bucket in an observed latency histogram.
type HistogramBucket struct {
	UpperBoundUs int64 `json:"upper_bound_us"`
	Count        int64 `json:"count"`
}
