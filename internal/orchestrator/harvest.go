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

// harvestCacheStats writes phase_service_cache fidelity rows. Two independent
// passes (mutually exclusive in the normal baseline/isolation split):
//
//   - Isolation (frozen services): snapshot live cache-box counters →
//     cache_hit_rate = Σhits/(Σhits+Σmisses), request_count = Σ(hits+misses)
//     (disambiguates "0 requests" from "all misses"), and recorded_entry_count
//     = the baseline coverage that was available to replay for this service.
//   - Baseline (persist_cache, no frozen): recording coverage — for each
//     service that ingested into this phase, recorded_entry_count = number of
//     entries captured (hit_rate/request_count = 0, no replay happened).
//
// cache_exact_match = hit_rate and cache_staleness = 0.0 (the SDK exposes no
// fuzzier-match or staleness counter yet — a documented follow-on). Best-effort
// throughout: an unreachable service or store error is logged, not fatal.
func (o *Orchestrator) harvestCacheStats(ctx context.Context, p *model.ExperimentPhase) {
	// Pass 1 — isolation replay stats.
	if len(p.FrozenServices) > 0 {
		baselineCoverage := o.baselineCoverage(ctx, p.ExperimentID)
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
			total := hits + misses
			hitRate := 0.0
			if total > 0 {
				hitRate = float64(hits) / float64(total)
			}
			res := &model.PhaseServiceCache{
				PhaseID:            p.ID,
				Service:            fs.Service,
				CacheHitRate:       hitRate,
				CacheExactMatch:    hitRate,
				CacheStaleness:     0.0,
				RequestCount:       total,
				RecordedEntryCount: baselineCoverage[fs.Service],
			}
			if err := o.experiments.UpsertServiceCache(ctx, res); err != nil {
				o.logger.Warn("orchestrator: upsert service cache failed",
					"phase_id", p.ID, "service", fs.Service, "error", err)
			}
		}
	}

	// Pass 2 — baseline recording coverage.
	if p.PersistCache && len(p.FrozenServices) == 0 {
		services, err := o.cacheStore.Services(p.ExperimentID, p.ID)
		if err != nil {
			o.logger.Warn("orchestrator: cache stats: list recorded services failed",
				"phase_id", p.ID, "error", err)
			return
		}
		for _, svc := range services {
			entries, err := o.cacheStore.Read(p.ExperimentID, p.ID, svc)
			if err != nil {
				o.logger.Warn("orchestrator: cache stats: read recorded entries failed",
					"phase_id", p.ID, "service", svc, "error", err)
				continue
			}
			res := &model.PhaseServiceCache{
				PhaseID:            p.ID,
				Service:            svc,
				RecordedEntryCount: int64(len(entries)),
			}
			if err := o.experiments.UpsertServiceCache(ctx, res); err != nil {
				o.logger.Warn("orchestrator: upsert recording coverage failed",
					"phase_id", p.ID, "service", svc, "error", err)
			}
		}
	}
}

// baselineCoverage returns, per service, the count of entries recorded by the
// experiment's baseline phase — the coverage available to replay. Empty when
// there is no completed baseline.
func (o *Orchestrator) baselineCoverage(ctx context.Context, experimentID string) map[string]int64 {
	out := map[string]int64{}
	baseline, err := o.baselinePhase(ctx, experimentID)
	if err != nil || baseline == nil {
		return out
	}
	services, err := o.cacheStore.Services(baseline.ExperimentID, baseline.ID)
	if err != nil {
		return out
	}
	for _, svc := range services {
		entries, err := o.cacheStore.Read(baseline.ExperimentID, baseline.ID, svc)
		if err != nil {
			continue
		}
		out[svc] = int64(len(entries))
	}
	return out
}
