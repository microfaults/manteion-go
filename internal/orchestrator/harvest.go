package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/zeus"
)

// harvestPhase collects a completed phase's results: per-attack metrics from
// zeus into phase_workflow_results (+ phase_service_latency when the result
// names a service), and cache-box fidelity stats from the frozen services'
// SDK instances into phase_service_cache. Non-fatal throughout — missing or
// not-yet-ready results are logged and skipped so the phase still completes.
// Called exactly once per completed phase, by the finishPhase CAS winner.
func (o *Orchestrator) harvestPhase(ctx context.Context, p *model.ExperimentPhase, pws []model.PhaseWorkflow) {
	if o.zeusClient != nil {
		for _, pw := range pws {
			if pw.ZeusAttackID == "" {
				continue
			}
			if err := o.harvestAttack(ctx, p, pw); err != nil {
				o.logger.Warn("orchestrator: harvest attack result failed",
					"phase_id", p.ID, "workflow_id", pw.WorkflowID,
					"attack_id", pw.ZeusAttackID, "error", err)
			}
		}
	}
	o.harvestCacheStats(ctx, p)
}

// harvestAttack fetches the final metrics for one (phase, workflow) attack
// and upserts the result rows. Retries briefly when zeus hasn't finalized
// the result yet.
func (o *Orchestrator) harvestAttack(ctx context.Context, p *model.ExperimentPhase, pw model.PhaseWorkflow) error {
	var result *zeus.AttackResultInfo
	var err error
	backoff := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}
	for attempt := 0; attempt <= len(backoff); attempt++ {
		result, err = o.zeusClient.GetAttackResult(ctx, pw.ZeusAttackID)
		if err == nil {
			break
		}
		if !errors.Is(err, zeus.ErrAttackResultNotReady) {
			return fmt.Errorf("get attack result: %w", err)
		}
		if attempt < len(backoff) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff[attempt]):
			}
		}
	}
	if result == nil {
		o.logger.Warn("orchestrator: attack result not available after retries",
			"phase_id", p.ID, "attack_id", pw.ZeusAttackID)
		return nil
	}

	// Derive throughput from request count and duration if not provided.
	throughput := result.ThroughputRPS
	if throughput == 0 && result.DurationMs > 0 {
		throughput = float64(result.TotalRequests) / (float64(result.DurationMs) / 1000)
	}
	errorRate := 1.0 - result.SuccessRate
	if errorRate < 0 {
		errorRate = 0
	}
	errorCount := int64(math.Round(errorRate * float64(result.TotalRequests)))

	rawJSON, _ := json.Marshal(result)
	wfRes := &model.PhaseWorkflowResult{
		PhaseID:       p.ID,
		WorkflowID:    pw.WorkflowID,
		RequestCount:  result.TotalRequests,
		ErrorCount:    errorCount,
		ErrorRate:     errorRate,
		ThroughputRPS: throughput,
		LatencyP50Us:  result.LatencyP50Us,
		LatencyP95Us:  result.LatencyP95Us,
		LatencyP99Us:  result.LatencyP99Us,
		LatencyP999Us: result.LatencyP99Us, // p999 not reported by vegeta; use p99
		RawMetrics:    rawJSON,
	}
	if err := o.experiments.UpsertWorkflowResult(ctx, wfRes); err != nil {
		return fmt.Errorf("upsert workflow result: %w", err)
	}

	// Request-side per-service latency, when zeus attributes the attack to a
	// service. Only per-workflow rows are written — aggregating precomputed
	// percentiles across workflows into a service-wide (workflow_id='') row
	// would be statistically wrong (see the percentile-aggregation caveat).
	if result.Service != "" {
		svcRes := &model.PhaseServiceLatency{
			PhaseID:      p.ID,
			Service:      result.Service,
			WorkflowID:   pw.WorkflowID,
			LatencyP50Us: result.LatencyP50Us,
			LatencyP95Us: result.LatencyP95Us,
			LatencyP99Us: result.LatencyP99Us,
			RequestCount: result.TotalRequests,
		}
		if err := o.experiments.UpsertServiceLatency(ctx, svcRes); err != nil {
			o.logger.Warn("orchestrator: upsert service latency failed",
				"phase_id", p.ID, "service", result.Service, "error", err)
		}
	}

	o.logger.Info("orchestrator: harvested attack result",
		"phase_id", p.ID, "workflow_id", pw.WorkflowID, "attack_id", pw.ZeusAttackID,
		"p99_us", result.LatencyP99Us, "requests", result.TotalRequests)
	return nil
}

// harvestCacheStats snapshots cache-box counters from every frozen service's
// live SDK instances and upserts one phase_service_cache row per service.
//
// Metric mapping (the SDK exposes store hit/miss counters only):
//   - cache_hit_rate    = Σhits / (Σhits + Σmisses)
//   - cache_exact_match = same ratio — every hit is an exact key match under
//     the exact* key strategies, so the two coincide until fuzzier matching
//     exists.
//   - cache_staleness   = 0.0 — no staleness counter in the SDK yet.
//
// Row presence still carries the signal "this service was frozen in replay
// during this phase". Best-effort: a service with no reachable instances is
// skipped, not failed.
func (o *Orchestrator) harvestCacheStats(ctx context.Context, p *model.ExperimentPhase) {
	for _, fs := range p.FrozenServices {
		st, err := o.controller.StatusByService(ctx, fs.Service)
		if err != nil {
			o.logger.Warn("orchestrator: cache stats: status by service failed",
				"phase_id", p.ID, "service", fs.Service, "error", err)
			continue
		}
		var hits, misses int64
		seen := false
		for _, inst := range st.Instances {
			if inst.CacheBox == nil {
				continue
			}
			hits += inst.CacheBox.Store.Hits
			misses += inst.CacheBox.Store.Misses
			seen = true
		}
		if !seen {
			o.logger.Info("orchestrator: cache stats: no cache-box stats reachable",
				"phase_id", p.ID, "service", fs.Service)
			continue
		}
		hitRate := 0.0
		if total := hits + misses; total > 0 {
			hitRate = float64(hits) / float64(total)
		}
		res := &model.PhaseServiceCache{
			PhaseID:         p.ID,
			Service:         fs.Service,
			CacheHitRate:    hitRate,
			CacheExactMatch: hitRate,
			CacheStaleness:  0.0,
		}
		if err := o.experiments.UpsertServiceCache(ctx, res); err != nil {
			o.logger.Warn("orchestrator: upsert service cache failed",
				"phase_id", p.ID, "service", fs.Service, "error", err)
		}
	}
}
