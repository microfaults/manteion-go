package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"manteion-go/internal/model"
)

const (
	defaultZeusPollInterval = 15 * time.Second
	defaultMaxPollDuration  = 30 * time.Minute
	defaultPollGrace        = 5 * time.Minute
)

// pollPhaseAttacks runs as a background goroutine for each attack-driven
// phase, polling zeus for the status of the phase's attacks. When all
// attacks complete it finishes the phase "completed" (finishPhase harvests
// exactly once); an unexpected attack failure or the safety-net deadline
// fails it, recording which driver ended how (or that the deadline passed)
// as the phase's failure reason. Workflow rows are re-listed each tick so
// attack-id changes (e.g. after a resume) are picked up.
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
	if d := phaseDuration + o.pollGrace; d > timeout {
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
			o.failPhase(context.Background(), phaseID,
				fmt.Sprintf("phase exceeded safety-net deadline (%s): zeus load drivers never reached a terminal state", timeout),
				"running")
			return
		case <-ticker.C:
			pws, err := o.experiments.ListPhaseWorkflows(ctx, phaseID)
			if err != nil {
				o.logger.Warn("orchestrator: poll: list phase workflows failed",
					"phase_id", phaseID, "error", err)
				continue
			}
			done, failures := o.checkLoadStatuses(ctx, phaseID, pws)
			if len(failures) > 0 {
				reason := strings.Join(failures, "; ")
				o.logger.Warn("orchestrator: zeus load driver failed unexpectedly",
					"phase_id", phaseID, "reason", reason)
				o.failPhase(context.Background(), phaseID, reason, "running")
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
// returns whether every driver is terminal plus one operator-readable line
// per driver that ended badly (the phase's failure reason when non-empty).
// The phase completes only when EVERY driver of both kinds is terminal, so an
// additive attack cannot end the phase while its workflow run is still
// generating traffic (or vice versa). A "stopped" driver counts as done
// (externally managed); a "failed"/"rejected"/unrecognized driver fails the
// phase; transient GetRun/GetAttack errors leave that driver uncounted so the
// next tick retries. A phase with no drivers at all returns not-done (the
// auto-complete path in enterPhase handles the driver-less case before the
// poller is ever spawned).
func (o *Orchestrator) checkLoadStatuses(ctx context.Context, phaseID string, pws []model.PhaseWorkflow) (allDone bool, failures []string) {
	if o.zeusClient == nil {
		return false, nil
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
				// A breached k6 threshold (exit 99) completes the run with a
				// reason -- a load-health warning, never a phase failure: in
				// fault experiments a breached error-rate threshold IS the
				// measurement (e.g. fail-closed frozen phases).
				if info.Status == "completed" && info.Reason != "" {
					o.logger.Warn("orchestrator: zeus run completed with load-health warning",
						"phase_id", phaseID, "run_id", pw.ZeusRunID, "reason", info.Reason)
				}
			case "starting", "validating", "running", "completing":
				// in flight
			default: // failed, rejected, or unknown
				o.logger.Error("orchestrator: zeus run in failure state",
					"phase_id", phaseID, "run_id", pw.ZeusRunID, "status", info.Status, "reason", info.Reason)
				failures = append(failures, runFailureLine(pw.ZeusRunID, pw.WorkflowID, info.Status, info.Reason))
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
				failures = append(failures, fmt.Sprintf("zeus attack %s for workflow %s ended %s",
					pw.ZeusAttackID, pw.WorkflowID, info.Status))
			}
		}
	}

	if total == 0 {
		return false, failures
	}
	return completed == total, failures
}

// runFailureLine words a k6 run that zeus reports in a non-completing state:
// "rejected" is zeus refusing the run (after accepting the start), "failed"
// is k6 dying; zeus's reason, when it gives one, is appended verbatim.
func runFailureLine(runID, workflowID, status, reason string) string {
	var line string
	switch status {
	case "rejected":
		line = fmt.Sprintf("zeus rejected run %s for workflow %s", runID, workflowID)
	case "failed":
		line = fmt.Sprintf("k6 run %s for workflow %s ended failed", runID, workflowID)
	default:
		line = fmt.Sprintf("k6 run %s for workflow %s ended in unexpected state %q", runID, workflowID, status)
	}
	if reason != "" {
		line += ": " + reason
	}
	return line
}
