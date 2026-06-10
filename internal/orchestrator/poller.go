package orchestrator

import (
	"context"
	"time"

	"manteion-go/internal/model"
)

const (
	defaultZeusPollInterval = 15 * time.Second
	defaultMaxPollDuration  = 30 * time.Minute
)

// pollPhaseAttacks runs as a background goroutine for each attack-driven
// phase, polling zeus for the status of the phase's attacks. When all
// attacks complete it finishes the phase "completed" (finishPhase harvests
// exactly once); an unexpected attack failure or the safety-net deadline
// finishes it "failed". Workflow rows are re-listed each tick so attack-id
// changes (e.g. after a resume) are picked up.
//
// Terminal calls pass context.Background(): finishPhase cancels this
// poller's ctx via cancelPoller, so the phase's cleanup must not run on the
// about-to-be-cancelled poll context.
func (o *Orchestrator) pollPhaseAttacks(ctx context.Context, phaseID string, phaseDuration time.Duration) {
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()

	// The safety net must outlast the configured attack duration, or a long
	// phase would be killed mid-flight by the default cap.
	timeout := o.maxPollDuration
	if d := phaseDuration + 5*time.Minute; d > timeout {
		timeout = d
	}
	deadline := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			o.logger.Error("orchestrator: zeus poll timeout exceeded; marking phase failed",
				"phase_id", phaseID, "timeout", timeout)
			o.finishPhase(context.Background(), phaseID, "failed", "running")
			return
		case <-ticker.C:
			pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID)
			if err != nil {
				o.logger.Warn("orchestrator: poll: list phase workflows failed",
					"phase_id", phaseID, "error", err)
				continue
			}
			done, failed := o.checkAttackStatuses(ctx, phaseID, pws)
			if failed {
				o.logger.Warn("orchestrator: zeus attack failed unexpectedly", "phase_id", phaseID)
				o.finishPhase(context.Background(), phaseID, "failed", "running")
				return
			}
			if done {
				o.logger.Info("orchestrator: zeus attacks completed naturally", "phase_id", phaseID)
				o.finishPhase(context.Background(), phaseID, "completed", "running")
				return
			}
		}
	}
}

// checkAttackStatuses polls every attack recorded on the phase's workflow
// rows. Returns (allDone, anyFailed). "completed" and "stopped" count as
// done ("stopped" is externally managed — we don't interfere);
// "pending"/"running" are in flight; anything unrecognized is a failure for
// defense-in-depth. Transient GetAttack errors leave the attack uncounted so
// the next tick retries.
func (o *Orchestrator) checkAttackStatuses(ctx context.Context, phaseID string, pws []model.PhaseWorkflow) (allDone bool, anyFailed bool) {
	if o.zeusClient == nil {
		return false, false
	}
	var attackIDs []string
	for _, pw := range pws {
		if pw.ZeusAttackID != "" {
			attackIDs = append(attackIDs, pw.ZeusAttackID)
		}
	}
	if len(attackIDs) == 0 {
		return false, false
	}

	completed := 0
	for _, id := range attackIDs {
		info, err := o.zeusClient.GetAttack(ctx, id)
		if err != nil {
			o.logger.Warn("orchestrator: get attack status failed",
				"phase_id", phaseID, "attack_id", id, "error", err)
			continue
		}
		switch info.Status {
		case "completed", "stopped":
			completed++
		case "pending", "running":
			// Still in progress — not counted.
		default:
			o.logger.Error("orchestrator: unexpected zeus attack status",
				"phase_id", phaseID, "attack_id", id, "status", info.Status)
			anyFailed = true
		}
	}

	return completed == len(attackIDs), anyFailed
}
