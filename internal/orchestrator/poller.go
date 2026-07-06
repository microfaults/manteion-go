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
			done, failed := o.checkLoadStatuses(ctx, phaseID, pws)
			if failed {
				o.logger.Warn("orchestrator: zeus load driver failed unexpectedly", "phase_id", phaseID)
				o.finishPhase(context.Background(), phaseID, "failed", "running")
				return
			}
			if done {
				o.logger.Info("orchestrator: zeus load drivers completed naturally", "phase_id", phaseID)
				o.finishPhase(context.Background(), phaseID, "completed", "running")
				return
			}
		}
	}
}

// checkLoadStatuses polls every load driver recorded on the phase's workflow
// rows -- both the k6 workflow RUNS and the additive vegeta ATTACKS -- and
// returns (allDone, anyFailed). The phase completes only when EVERY driver of
// both kinds is terminal, so an additive attack cannot end the phase while its
// workflow run is still generating traffic (or vice versa). A "stopped" driver
// counts as done (externally managed); a "failed"/"rejected"/unrecognized
// driver fails the phase; transient GetRun/GetAttack errors leave that driver
// uncounted so the next tick retries. A phase with no drivers at all returns
// not-done (the auto-complete path in enterPhase handles the driver-less case
// before the poller is ever spawned).
func (o *Orchestrator) checkLoadStatuses(ctx context.Context, phaseID string, pws []model.PhaseWorkflow) (allDone bool, anyFailed bool) {
	if o.zeusClient == nil {
		return false, false
	}

	total, completed := 0, 0
	for _, pw := range pws {
		if pw.ZeusRunID != "" {
			total++
			info, err := o.zeusClient.GetRun(ctx, pw.ZeusRunID)
			if err != nil {
				o.logger.Warn("orchestrator: get run status failed",
					"phase_id", phaseID, "run_id", pw.ZeusRunID, "error", err)
				continue // uncounted; retried next tick
			}
			switch info.Status {
			case "completed", "stopped":
				completed++
			case "starting", "validating", "running", "completing":
				// in flight
			default: // failed, rejected, or unknown
				o.logger.Error("orchestrator: zeus run in failure state",
					"phase_id", phaseID, "run_id", pw.ZeusRunID, "status", info.Status)
				anyFailed = true
			}
		}
		if pw.ZeusAttackID != "" {
			total++
			info, err := o.zeusClient.GetAttack(ctx, pw.ZeusAttackID)
			if err != nil {
				o.logger.Warn("orchestrator: get attack status failed",
					"phase_id", phaseID, "attack_id", pw.ZeusAttackID, "error", err)
				continue
			}
			switch info.Status {
			case "completed", "stopped":
				completed++
			case "pending", "running":
				// in flight
			default:
				o.logger.Error("orchestrator: unexpected zeus attack status",
					"phase_id", phaseID, "attack_id", pw.ZeusAttackID, "status", info.Status)
				anyFailed = true
			}
		}
	}

	if total == 0 {
		return false, anyFailed
	}
	return completed == total, anyFailed
}
