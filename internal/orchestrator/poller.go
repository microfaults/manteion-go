package orchestrator

import (
	"context"
	"time"

	"manteion-go/internal/model"
)

const (
	zeusPollingInterval    = 15 * time.Second
	defaultMaxPollDuration = 30 * time.Minute
)

// pollZeusStatus runs as a background goroutine for each active run, polling
// Zeus for the status of the run's attack IDs. When all attacks complete it
// finishes the run "completed" (finishRun harvests exactly once); an unexpected
// failure or the maxPollDuration safety-net finishes it "failed". It re-fetches
// the run each tick so attack-ID changes (e.g. after resume) are picked up.
//
// Terminal calls pass context.Background(): finishRun cancels this poller's ctx
// via cancelAll, so the run's cleanup must not run on the about-to-be-cancelled
// poll context.
func (o *Orchestrator) pollZeusStatus(ctx context.Context, runID string) {
	ticker := time.NewTicker(zeusPollingInterval)
	defer ticker.Stop()

	deadline := time.After(o.maxPollDuration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			o.logger.Error("orchestrator: zeus poll timeout exceeded; marking run failed",
				"run_id", runID, "timeout", defaultMaxPollDuration)
			o.finishRun(context.Background(), runID, "failed", "running")
			return
		case <-ticker.C:
			run, err := o.experiments.GetRun(ctx, runID)
			if err != nil {
				o.logger.Warn("orchestrator: poll: get run failed", "run_id", runID, "error", err)
				continue
			}
			done, failed := o.checkAttackStatuses(ctx, run)
			if failed {
				o.logger.Warn("orchestrator: zeus attack failed unexpectedly", "run_id", runID)
				o.finishRun(context.Background(), runID, "failed", "running")
				return
			}
			if done {
				o.logger.Info("orchestrator: zeus attacks completed naturally", "run_id", runID)
				o.finishRun(context.Background(), runID, "completed", "running")
				return
			}
		}
	}
}

// checkAttackStatuses polls all attack IDs on the run.
// Returns (allDone, anyFailed). Only considers the run "done" when all attacks
// had a fixed duration (DurationMs > 0 implied by "completed" status in Zeus).
// A "stopped" status is treated as externally managed — we don't interfere.
// Unrecognized statuses are treated as failures for defense-in-depth.
func (o *Orchestrator) checkAttackStatuses(ctx context.Context, run *model.ExperimentRun) (allDone bool, anyFailed bool) {
	attackIDs := run.ZeusAttackIDs
	if len(attackIDs) == 0 && run.ZeusAttackID != "" {
		attackIDs = []string{run.ZeusAttackID}
	}
	if len(attackIDs) == 0 || o.zeusClient == nil {
		return false, false
	}

	completedCount := 0
	for _, id := range attackIDs {
		info, err := o.zeusClient.GetAttack(ctx, id)
		if err != nil {
			o.logger.Warn("orchestrator: get attack status failed",
				"run_id", run.ID, "attack_id", id, "error", err)
			continue
		}
		switch info.Status {
		case "completed":
			completedCount++
		case "stopped":
			completedCount++
		case "pending", "running":
			// Still in progress — not counted.
		default:
			o.logger.Error("orchestrator: unexpected zeus attack status",
				"run_id", run.ID, "attack_id", id, "status", info.Status)
			anyFailed = true
		}
	}

	return completedCount == len(attackIDs), anyFailed
}
