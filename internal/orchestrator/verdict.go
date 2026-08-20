package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

// fidelityPullTimeout bounds each per-instance W6 fidelity pull at phase finish.
const fidelityPullTimeout = 10 * time.Second

// replayFidelity is one frozen service's replay-side aggregate over the W6
// fidelity snapshots the verdict pass pulls (summed across live instances).
// It is the authoritative input to phase_service_cache: the SDK's
// /admin/cachebox Store counters harvest previously read are record-half
// counters the replay path stopped touching when the record/replay split
// landed (replay consults the ReplaySet; hits count in the FidelityRegistry),
// which is how isolation rows harvested hit_rate=0 while verdicts saw hits.
type replayFidelity struct {
	ReplayHits   int64
	ReplayMisses int64
	AgeMeanMs    float64 // mean replay age across instances (ms)
}

// collectFidelityVerdict pulls every frozen instance's W6 fidelity snapshot
// (before thaw, while counters are intact), computes the phase's first-class
// verdict (INV-6), and persists it. Returns the per-service replay aggregates
// (hits, misses, mean replay age) — the authoritative source harvest writes
// into phase_service_cache.
//
// Verdict: INVALID if any instance shows a replay miss, an uncommitted preload,
// a degraded source recording, or missing telemetry (a failed pull); else
// VALID_WITH_WARNINGS if the recording's divergent-collision rate exceeds 1%;
// else VALID.
func (o *Orchestrator) collectFidelityVerdict(ctx context.Context, p *model.ExperimentPhase) map[string]replayFidelity {
	fidelity := map[string]replayFidelity{}
	var snapshots []atroposdk.FidelitySnapshot
	anyMiss, preloadIncomplete, telemetryMissing := false, false, false
	var ageMaxMs, ageSumMs, ageCount int64

	for _, fs := range p.FrozenServices {
		instances, err := o.controller.InstancesForService(ctx, fs.Service)
		if err != nil || len(instances) == 0 {
			telemetryMissing = true
			o.logger.Warn("orchestrator: verdict: no instances for frozen service",
				"phase_id", p.ID, "service", fs.Service, "error", err)
			continue
		}
		var svcAgeSum, svcAgeCount, svcHits, svcMisses int64
		for _, inst := range instances {
			snap, err := o.controller.FetchFidelity(ctx, inst.Address, p.ExperimentID, p.ID, fidelityPullTimeout)
			if err != nil {
				telemetryMissing = true
				o.logger.Warn("orchestrator: verdict: fidelity pull failed",
					"phase_id", p.ID, "service", fs.Service, "instance", inst.ID, "error", err)
				continue
			}
			snapshots = append(snapshots, snap)
			// Miss classification: key_absent and body_buffer_failed are
			// coverage violations under load -- always INVALID. not_committed
			// misses are produced by the SDK's fail-closed ownership gate in
			// the setup window between the replay rule becoming poll-visible
			// (the phase is 'running') and the preload commit installing this
			// pair's set; once the pair's set IS committed the gate passes and
			// any further miss counts as key_absent, so not_committed with
			// Preload.Committed=true is provably pre-load-start noise (an
			// ambient request 503'd during setup, which fail-closed intends).
			// If the preload never committed, preloadIncomplete flags INVALID
			// regardless. Misses the SDK didn't classify (aggregate exceeds
			// the reason buckets -- e.g. a future reason string) stay INVALID:
			// an unexplained miss must never read as clean.
			classified := snap.MissReasons.KeyAbsent + snap.MissReasons.NotCommitted + snap.MissReasons.BodyBufferFailed
			if snap.MissReasons.KeyAbsent > 0 || snap.MissReasons.BodyBufferFailed > 0 ||
				snap.ReplayMisses > classified {
				anyMiss = true
			}
			if !snap.Preload.Committed {
				preloadIncomplete = true
			}
			if snap.ReplayAgeMs.Max > ageMaxMs {
				ageMaxMs = snap.ReplayAgeMs.Max
			}
			ageSumMs += snap.ReplayAgeMs.Mean
			ageCount++
			svcAgeSum += snap.ReplayAgeMs.Mean
			svcAgeCount++
			svcHits += snap.ReplayHits
			svcMisses += snap.ReplayMisses
		}
		if svcAgeCount > 0 {
			fidelity[fs.Service] = replayFidelity{
				ReplayHits:   svcHits,
				ReplayMisses: svcMisses,
				AgeMeanMs:    float64(svcAgeSum) / float64(svcAgeCount),
			}
		}
	}

	// Source recording quality: degraded drain (INVALID) and divergent-collision
	// rate (warning). Both come from the experiment's baseline recording.
	degradedBaseline := false
	collisionRate := 0.0
	if baseline, err := o.baselinePhase(ctx, p.ExperimentID); err == nil && baseline != nil {
		if drain, _ := o.experiments.GetPhaseDrain(ctx, baseline.ID); drain.Degraded() {
			degradedBaseline = true
		}
		div, ident := o.cacheStore.CollisionStats(baseline.ExperimentID, baseline.ID)
		if div+ident > 0 {
			collisionRate = float64(div) / float64(div+ident)
		}
	}

	var reasons []string
	if anyMiss {
		reasons = append(reasons, "fidelity_violation:replay_miss")
	}
	if preloadIncomplete {
		reasons = append(reasons, "preload_incomplete")
	}
	if degradedBaseline {
		reasons = append(reasons, "degraded_baseline")
	}
	if telemetryMissing {
		reasons = append(reasons, "telemetry_missing")
	}

	verdict := model.VerdictValid
	switch {
	case len(reasons) > 0:
		verdict = model.VerdictInvalid
	case collisionRate > 0.01:
		verdict = model.VerdictValidWithWarning
		reasons = append(reasons, fmt.Sprintf("collision_rate:%.4f", collisionRate))
	}

	var ageMeanMs int64
	if ageCount > 0 {
		ageMeanMs = ageSumMs / ageCount
	}
	rawSnaps, _ := json.Marshal(snapshots)
	pv := &model.PhaseVerdict{
		Verdict:         verdict,
		Reasons:         reasons,
		CollisionRate:   collisionRate,
		ReplayAgeMaxMs:  ageMaxMs,
		ReplayAgeMeanMs: ageMeanMs,
		Snapshots:       rawSnaps,
	}
	if err := o.experiments.UpsertPhaseVerdict(ctx, p.ID, pv); err != nil {
		o.logger.Warn("orchestrator: verdict: persist failed", "phase_id", p.ID, "error", err)
	}
	o.logger.Info("orchestrator: phase verdict",
		"phase_id", p.ID, "verdict", verdict, "reasons", reasons, "collision_rate", collisionRate)
	return fidelity
}
