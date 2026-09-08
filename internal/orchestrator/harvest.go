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

// harvestPhase collects a completed phase's results: per-workflow load
// metrics from zeus into phase_workflow_results (+ phase_service_latency when
// an attack result names a service), and cache-box fidelity stats from the
// frozen services' SDK instances into phase_service_cache. A workflow row
// carrying an attack handle is harvested from the attack result; one carrying
// only a k6 run handle (the common case — every expctl example phase) from the
// run's stats snapshot. A row with both keeps the attack result: one writer
// per (phase, workflow), never two. Non-fatal throughout — missing or
// not-yet-ready results are logged and skipped so the phase still completes.
// Called exactly once per completed phase, by the finishPhase CAS winner.
func (o *Orchestrator) harvestPhase(ctx context.Context, p *model.ExperimentPhase, pws []model.PhaseWorkflow, fidelity map[string]replayFidelity) {
	if o.zeusClient != nil {
		for _, pw := range pws {
			switch {
			case pw.ZeusAttackID != "":
				if err := o.harvestAttack(ctx, p, pw); err != nil {
					o.logger.Warn("orchestrator: harvest attack result failed",
						"phase_id", p.ID, "workflow_id", pw.WorkflowID,
						"attack_id", pw.ZeusAttackID, "error", err)
				}
			case pw.ZeusRunID != "":
				if err := o.harvestRun(ctx, p, pw); err != nil {
					o.logger.Warn("orchestrator: harvest run stats failed",
						"phase_id", p.ID, "workflow_id", pw.WorkflowID,
						"zeus_run_id", pw.ZeusRunID, "error", err)
				}
			}
		}
	}
	o.harvestCacheStats(ctx, p, fidelity)
}

// notReadyBackoff is the wait schedule between asks while zeus is still
// finalizing a load driver's result — shared by the attack and run paths so
// both give zeus the same ~7 s to flush before harvest gives up on the row.
var notReadyBackoff = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

// harvestAttack fetches the final metrics for one (phase, workflow) attack
// and upserts the result rows. Retries briefly when zeus hasn't finalized
// the result yet.
func (o *Orchestrator) harvestAttack(ctx context.Context, p *model.ExperimentPhase, pw model.PhaseWorkflow) error {
	var result *zeus.AttackResultInfo
	var err error
	backoff := notReadyBackoff
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

// harvestRun fetches the finalized stats snapshot for one (phase, workflow)
// k6 run and upserts the phase_workflow_results row. Retries on the same
// schedule as harvestAttack while zeus still serves the "no stats available
// yet" placeholder. A 404 — zeus restarted since the run completed and lost
// its in-memory run state — is tolerated: the row is skipped with a
// stats_unavailable warning and the phase completes without it (there is
// nothing true to write, and nothing a retry could recover).
func (o *Orchestrator) harvestRun(ctx context.Context, p *model.ExperimentPhase, pw model.PhaseWorkflow) error {
	var stats *zeus.RunStats
	var err error
	backoff := notReadyBackoff
	for attempt := 0; attempt <= len(backoff); attempt++ {
		stats, err = o.zeusClient.GetRunStats(ctx, pw.ZeusRunID)
		if err == nil {
			break
		}
		if errors.Is(err, zeus.ErrRunNotFound) {
			o.logger.Warn("orchestrator: run stats unavailable; skipping workflow result",
				"phase_id", p.ID, "workflow_id", pw.WorkflowID,
				"zeus_run_id", pw.ZeusRunID, "reason", "stats_unavailable")
			return nil
		}
		if !errors.Is(err, zeus.ErrRunStatsNotReady) {
			return fmt.Errorf("get run stats: %w", err)
		}
		if attempt < len(backoff) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff[attempt]):
			}
		}
	}
	if stats == nil {
		o.logger.Warn("orchestrator: run stats not available after retries",
			"phase_id", p.ID, "workflow_id", pw.WorkflowID, "zeus_run_id", pw.ZeusRunID)
		return nil
	}

	res := runStatsToResult(p.ID, pw.WorkflowID, stats)
	if err := o.experiments.UpsertWorkflowResult(ctx, res); err != nil {
		return fmt.Errorf("upsert workflow result: %w", err)
	}
	o.logger.Info("orchestrator: harvested run stats",
		"phase_id", p.ID, "workflow_id", pw.WorkflowID, "zeus_run_id", pw.ZeusRunID,
		"p99_us", res.LatencyP99Us, "requests", res.RequestCount)
	return nil
}

// runStatsToResult converts a zeus RunStats snapshot (durations in ns) into
// the phase_workflow_results row shape (latencies in µs):
//
//	request_count   = requests_sent
//	error_count     = requests_dropped
//	error_rate      = requests_dropped / requests_sent  (0 when nothing was sent)
//	throughput_rps  = requests_sent / duration_seconds  (0 when duration is 0)
//	latency_pXX_us  = latency_pXX / 1000                (integer truncation)
//	latency_p999_us = latency_p99_us                    (zeus reports no p999; the
//	                  column is NOT NULL — same convention as the vegeta path)
//
// raw_metrics is the RunStats document itself, nanoseconds untouched. Note
// what zeus puts behind the names: requests_dropped is k6's dropped_iterations
// (iterations the arrival-rate executor could not schedule), while HTTP-level
// failures are requests_sent − requests_ok — both survive in raw_metrics.
func runStatsToResult(phaseID, workflowID string, rs *zeus.RunStats) *model.PhaseWorkflowResult {
	errorRate := 0.0
	if rs.RequestsSent > 0 {
		errorRate = float64(rs.RequestsDropped) / float64(rs.RequestsSent)
	}
	throughput := 0.0
	if rs.Duration > 0 {
		throughput = float64(rs.RequestsSent) / time.Duration(rs.Duration).Seconds()
	}
	p99us := time.Duration(rs.LatencyP99).Microseconds()
	rawJSON, _ := json.Marshal(rs)
	return &model.PhaseWorkflowResult{
		PhaseID:       phaseID,
		WorkflowID:    workflowID,
		RequestCount:  rs.RequestsSent,
		ErrorCount:    rs.RequestsDropped,
		ErrorRate:     errorRate,
		ThroughputRPS: throughput,
		LatencyP50Us:  time.Duration(rs.LatencyP50).Microseconds(),
		LatencyP95Us:  time.Duration(rs.LatencyP95).Microseconds(),
		LatencyP99Us:  p99us,
		LatencyP999Us: p99us, // p999 not reported by zeus; use p99 (as the vegeta path does)
		RawMetrics:    rawJSON,
	}
}

// harvestCacheStats writes phase_service_cache fidelity rows. Two independent
// passes (mutually exclusive in the normal baseline/isolation split):
//
//   - Isolation (frozen services): the per-service replay aggregates from the
//     W6 fidelity snapshots the verdict pass pulled pre-thaw (MANT-6) —
//     cache_hit_rate = Σhits/(Σhits+Σmisses), request_count = Σ(hits+misses)
//     (disambiguates "0 requests" from "all misses"), and recorded_entry_count
//     = the baseline coverage that was available to replay for this service.
//     The fidelity snapshot is the ONLY authoritative replay-side counter: the
//     SDK's /admin/cachebox Store stats are record-half counters the replay
//     path never touches (record/replay split), and harvesting them is what
//     produced hit_rate=0 rows against verdicts with hundreds of hits.
//   - Baseline (persist_cache, no frozen): recording coverage — for each
//     service that ingested into this phase, recorded_entry_count = number of
//     entries captured (hit_rate/request_count = 0, no replay happened).
//
// cache_exact_match = hit_rate; cache_staleness = the mean replay age (ms).
// Best-effort throughout: missing telemetry or a store error is logged, not fatal.
func (o *Orchestrator) harvestCacheStats(ctx context.Context, p *model.ExperimentPhase, fidelity map[string]replayFidelity) {
	// Pass 1 — isolation replay stats.
	if len(p.FrozenServices) > 0 {
		baselineCoverage := o.baselineCoverage(ctx, p.ExperimentID)
		for _, fs := range p.FrozenServices {
			f, ok := fidelity[fs.Service]
			if !ok {
				// No snapshot reached the verdict pass either — the verdict is
				// already INVALID(telemetry_missing); there is nothing true to write.
				o.logger.Info("orchestrator: cache stats: no fidelity telemetry",
					"phase_id", p.ID, "service", fs.Service)
				continue
			}
			total := f.ReplayHits + f.ReplayMisses
			hitRate := 0.0
			if total > 0 {
				hitRate = float64(f.ReplayHits) / float64(total)
			}
			res := &model.PhaseServiceCache{
				PhaseID:            p.ID,
				Service:            fs.Service,
				CacheHitRate:       hitRate,
				CacheExactMatch:    hitRate,
				CacheStaleness:     f.AgeMeanMs,
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
