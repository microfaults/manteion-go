package orchestrator

import (
	"context"
	"time"

	"manteion-go/internal/model"
)

const (
	zeusPollingInterval = 15 * time.Second
	defaultMaxPollDuration = 30 * time.Minute
)

// pollZeusStatus runs as a background goroutine for each active run.
// It polls Zeus for the status of all attack IDs associated with the run.
// When all attacks have completed (fixed-duration), it harvests results and
// transitions the run to "completed". If Zeus reports an unexpected failure
// the run is marked "failed". The poller gives up after maxPollDuration as
// a safety net against permanently stuck or unreachable attacks.
func (o *Orchestrator) pollZeusStatus(ctx context.Context, run *model.ExperimentRun) {
	ticker := time.NewTicker(zeusPollingInterval)
	defer ticker.Stop()

	deadline := time.After(o.maxPollDuration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			o.logger.Error("orchestrator: zeus poll timeout exceeded; marking run failed",
				"run_id", run.ID, "timeout", defaultMaxPollDuration)
			o.StopRun(ctx, run.ID, "failed")
			return
		case <-ticker.C:
			done, failed := o.checkAttackStatuses(ctx, run)
			if failed {
				o.logger.Warn("orchestrator: zeus attack failed unexpectedly",
					"run_id", run.ID)
				o.StopRun(ctx, run.ID, "failed")
				return
			}
			if done {
				o.logger.Info("orchestrator: zeus attacks completed naturally",
					"run_id", run.ID)
				// Re-fetch to get latest attack IDs before harvesting.
				latest, err := o.experiments.GetRun(ctx, run.ID)
				if err == nil {
					o.HarvestResults(ctx, latest)
				}
				o.StopRun(ctx, run.ID, "completed")
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
