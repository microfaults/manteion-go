package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"manteion-go/internal/model"
	"manteion-go/internal/zeus"
)

// HarvestResults queries Zeus for the final metrics of all attacks associated with
// a completed run and stores them as WorkflowRunResult rows. Non-fatal: missing
// or not-yet-ready results are logged and skipped so the run can still complete.
func (o *Orchestrator) HarvestResults(ctx context.Context, run *model.ExperimentRun) {
	attackIDs := run.ZeusAttackIDs
	if len(attackIDs) == 0 && run.ZeusAttackID != "" {
		attackIDs = []string{run.ZeusAttackID}
	}
	if len(attackIDs) == 0 {
		return
	}

	for _, attackID := range attackIDs {
		if err := o.harvestOne(ctx, run, attackID); err != nil {
			o.logger.Warn("orchestrator: harvest attack result failed",
				"run_id", run.ID, "attack_id", attackID, "error", err)
		}
	}
}

func (o *Orchestrator) harvestOne(ctx context.Context, run *model.ExperimentRun, attackID string) error {
	var result *zeus.AttackResultInfo
	var err error
	backoff := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}
	for attempt := 0; attempt <= len(backoff); attempt++ {
		result, err = o.zeusClient.GetAttackResult(ctx, attackID)
		if err == nil {
			break
		}
		if !errors.Is(err, zeus.ErrAttackResultNotReady) {
			return fmt.Errorf("get attack result: %w", err)
		}
		if attempt < len(backoff) {
			o.logger.Info("orchestrator: attack result not yet available, retrying",
				"run_id", run.ID, "attack_id", attackID,
				"attempt", attempt+1, "backoff", backoff[attempt])
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff[attempt]):
			}
		}
	}
	if result == nil {
		o.logger.Warn("orchestrator: attack result not available after retries",
			"run_id", run.ID, "attack_id", attackID)
		return nil
	}

	// Derive throughput from request count and duration if not provided.
	throughput := result.ThroughputRPS
	if throughput == 0 && result.DurationMs > 0 {
		throughput = float64(result.TotalRequests) / (float64(result.DurationMs) / 1000)
	}
	errorRate := 1.0 - result.SuccessRate

	rawJSON, _ := json.Marshal(result)
	wfResult := &model.WorkflowRunResult{
		ID:              generateRunID(),
		ExperimentRunID: run.ID,
		Workflow:        result.Service,
		LatencyP50Us:    result.LatencyP50Us,
		LatencyP95Us:    result.LatencyP95Us,
		LatencyP99Us:    result.LatencyP99Us,
		LatencyP999Us:   result.LatencyP99Us, // p999 not reported by vegeta; use p99
		RequestCount:    result.TotalRequests,
		ErrorRate:       errorRate,
		ThroughputRPS:   throughput,
		RawMetrics:      rawJSON,
	}

	if err := o.experiments.CreateWorkflowResult(ctx, wfResult); err != nil {
		return fmt.Errorf("store workflow result: %w", err)
	}

	o.logger.Info("orchestrator: harvested attack result",
		"run_id", run.ID, "attack_id", attackID,
		"p99_us", result.LatencyP99Us, "requests", result.TotalRequests)
	return nil
}

// generateRunID produces a unique ID for result rows. Uses UUID so concurrent
// harvests can't collide on a nanosecond clock.
func generateRunID() string {
	return "res-" + uuid.NewString()
}
