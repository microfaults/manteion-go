package store

import (
	"context"
	"database/sql"
	"fmt"

	"manteion-go/internal/model"
)

// ExperimentRepo provides persistence for experiments, their runs, and results.
type ExperimentRepo struct {
	db *sql.DB
}

// NewExperimentRepo creates a new experiment repository.
func NewExperimentRepo(db *sql.DB) *ExperimentRepo {
	return &ExperimentRepo{db: db}
}

// --- Experiment ---

// Create inserts a new experiment.
func (r *ExperimentRepo) Create(ctx context.Context, exp *model.Experiment) error {
	if err := exp.Validate(); err != nil {
		return err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO experiments (id, name, description, experiment_type,
			primary_workload_id, status, created_at, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		exp.ID, exp.Name, nullString(exp.Description), exp.ExperimentType,
		exp.PrimaryWorkloadID, exp.Status, exp.CreatedAt, exp.StartedAt, exp.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert experiment: %w", err)
	}
	return nil
}

// Get returns an experiment by ID, or ErrNotFound.
func (r *ExperimentRepo) Get(ctx context.Context, id string) (*model.Experiment, error) {
	var exp model.Experiment
	var desc sql.NullString
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, description, experiment_type,
			primary_workload_id, status, created_at, started_at, completed_at
		FROM experiments WHERE id = $1`, id,
	).Scan(
		&exp.ID, &exp.Name, &desc, &exp.ExperimentType,
		&exp.PrimaryWorkloadID, &exp.Status, &exp.CreatedAt, &exp.StartedAt, &exp.CompletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment: %w", err)
	}
	exp.Description = fromNullString(desc)
	return &exp, nil
}

// List returns all experiments ordered by creation time (newest first).
func (r *ExperimentRepo) List(ctx context.Context) ([]*model.Experiment, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, description, experiment_type,
			primary_workload_id, status, created_at, started_at, completed_at
		FROM experiments ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list experiments: %w", err)
	}
	defer rows.Close()

	var result []*model.Experiment
	for rows.Next() {
		var exp model.Experiment
		var desc sql.NullString
		if err := rows.Scan(
			&exp.ID, &exp.Name, &desc, &exp.ExperimentType,
			&exp.PrimaryWorkloadID, &exp.Status, &exp.CreatedAt, &exp.StartedAt, &exp.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan experiment: %w", err)
		}
		exp.Description = fromNullString(desc)
		result = append(result, &exp)
	}
	return result, rows.Err()
}

// UpdateStatus changes the status of an experiment.
func (r *ExperimentRepo) UpdateStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE experiments SET status = $2 WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update experiment status: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// --- ExperimentRun ---

// CreateRun inserts a new experiment run.
func (r *ExperimentRepo) CreateRun(ctx context.Context, run *model.ExperimentRun) error {
	if err := run.Validate(); err != nil {
		return err
	}

	frozenJSON, err := jsonbMarshal(run.FrozenServices)
	if err != nil {
		return fmt.Errorf("marshal frozen_services: %w", err)
	}
	placementJSON, err := jsonbMarshal(run.NodePlacement)
	if err != nil {
		return fmt.Errorf("marshal node_placement: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO experiment_runs (id, experiment_id, run_type, run_index,
			frozen_services, meta_trace_id, status, node_placement,
			started_at, completed_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		run.ID, run.ExperimentID, run.RunType, run.RunIndex,
		frozenJSON, run.MetaTraceID, run.Status, placementJSON,
		run.StartedAt, run.CompletedAt, run.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert experiment_run: %w", err)
	}
	return nil
}

// GetRun returns an experiment run by ID, or ErrNotFound.
func (r *ExperimentRepo) GetRun(ctx context.Context, id string) (*model.ExperimentRun, error) {
	var run model.ExperimentRun
	var frozenJSON, placementJSON []byte

	err := r.db.QueryRowContext(ctx, `
		SELECT id, experiment_id, run_type, run_index,
			frozen_services, meta_trace_id, status, node_placement,
			started_at, completed_at, created_at
		FROM experiment_runs WHERE id = $1`, id,
	).Scan(
		&run.ID, &run.ExperimentID, &run.RunType, &run.RunIndex,
		&frozenJSON, &run.MetaTraceID, &run.Status, &placementJSON,
		&run.StartedAt, &run.CompletedAt, &run.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment_run: %w", err)
	}

	if err := jsonbScan(frozenJSON, &run.FrozenServices); err != nil {
		return nil, fmt.Errorf("unmarshal frozen_services: %w", err)
	}
	if err := jsonbScan(placementJSON, &run.NodePlacement); err != nil {
		return nil, fmt.Errorf("unmarshal node_placement: %w", err)
	}

	return &run, nil
}

// ListRunsByExperiment returns all runs for an experiment, ordered by run_index.
func (r *ExperimentRepo) ListRunsByExperiment(ctx context.Context, experimentID string) ([]*model.ExperimentRun, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_id, run_type, run_index,
			frozen_services, meta_trace_id, status, node_placement,
			started_at, completed_at, created_at
		FROM experiment_runs WHERE experiment_id = $1
		ORDER BY run_index`, experimentID)
	if err != nil {
		return nil, fmt.Errorf("list experiment runs: %w", err)
	}
	defer rows.Close()

	var result []*model.ExperimentRun
	for rows.Next() {
		var run model.ExperimentRun
		var frozenJSON, placementJSON []byte
		if err := rows.Scan(
			&run.ID, &run.ExperimentID, &run.RunType, &run.RunIndex,
			&frozenJSON, &run.MetaTraceID, &run.Status, &placementJSON,
			&run.StartedAt, &run.CompletedAt, &run.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan experiment run: %w", err)
		}
		if err := jsonbScan(frozenJSON, &run.FrozenServices); err != nil {
			return nil, err
		}
		if err := jsonbScan(placementJSON, &run.NodePlacement); err != nil {
			return nil, err
		}
		result = append(result, &run)
	}
	return result, rows.Err()
}

// UpdateRunStatus changes the status of an experiment run.
func (r *ExperimentRepo) UpdateRunStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE experiment_runs SET status = $2 WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// --- Results ---

// CreateWorkflowResult inserts a workflow-level latency measurement.
func (r *ExperimentRepo) CreateWorkflowResult(ctx context.Context, res *model.WorkflowRunResult) error {
	if err := res.Validate(); err != nil {
		return err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workflow_run_results (id, experiment_run_id, workflow,
			latency_p50_us, latency_p95_us, latency_p99_us, latency_p999_us,
			request_count, error_rate, throughput_rps, raw_metrics)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		res.ID, res.ExperimentRunID, res.Workflow,
		res.LatencyP50Us, res.LatencyP95Us, res.LatencyP99Us, res.LatencyP999Us,
		res.RequestCount, res.ErrorRate, res.ThroughputRPS, res.RawMetrics,
	)
	if err != nil {
		return fmt.Errorf("insert workflow_run_result: %w", err)
	}
	return nil
}

// CreateServiceResult inserts a per-service measurement.
func (r *ExperimentRepo) CreateServiceResult(ctx context.Context, res *model.ServiceRunResult) error {
	if err := res.Validate(); err != nil {
		return err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO service_run_results (id, experiment_run_id, service, workflow,
			latency_p50_us, latency_p95_us, latency_p99_us,
			cpu_millicores, memory_mb, cache_hit_rate, cache_exact_match,
			cache_staleness, raw_metrics)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		res.ID, res.ExperimentRunID, res.Service, nullString(res.Workflow),
		res.LatencyP50Us, res.LatencyP95Us, res.LatencyP99Us,
		res.CPUMillicores, res.MemoryMB, res.CacheHitRate, res.CacheExactMatch,
		res.CacheStaleness, res.RawMetrics,
	)
	if err != nil {
		return fmt.Errorf("insert service_run_result: %w", err)
	}
	return nil
}

// CreateContribution inserts a delta computation result.
func (r *ExperimentRepo) CreateContribution(ctx context.Context, c *model.ContributionResult) error {
	if err := c.Validate(); err != nil {
		return err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO contribution_results (id, experiment_id, service, workflow,
			cachebox_mode, baseline_run_id, isolation_run_id,
			delta_p50_us, delta_p95_us, delta_p99_us,
			interaction_effect, combination_run_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		c.ID, c.ExperimentID, c.Service, c.Workflow,
		c.CacheBoxMode, c.BaselineRunID, c.IsolationRunID,
		c.DeltaP50Us, c.DeltaP95Us, c.DeltaP99Us,
		c.InteractionEffect, nullString(c.CombinationRunID),
	)
	if err != nil {
		return fmt.Errorf("insert contribution_result: %w", err)
	}
	return nil
}

// ListContributions returns all contribution results for an experiment.
func (r *ExperimentRepo) ListContributions(ctx context.Context, experimentID string) ([]*model.ContributionResult, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_id, service, workflow, cachebox_mode,
			baseline_run_id, isolation_run_id,
			delta_p50_us, delta_p95_us, delta_p99_us,
			interaction_effect, combination_run_id
		FROM contribution_results WHERE experiment_id = $1
		ORDER BY service, workflow`, experimentID)
	if err != nil {
		return nil, fmt.Errorf("list contributions: %w", err)
	}
	defer rows.Close()

	var result []*model.ContributionResult
	for rows.Next() {
		var c model.ContributionResult
		var combRunID sql.NullString
		if err := rows.Scan(
			&c.ID, &c.ExperimentID, &c.Service, &c.Workflow, &c.CacheBoxMode,
			&c.BaselineRunID, &c.IsolationRunID,
			&c.DeltaP50Us, &c.DeltaP95Us, &c.DeltaP99Us,
			&c.InteractionEffect, &combRunID,
		); err != nil {
			return nil, fmt.Errorf("scan contribution: %w", err)
		}
		c.CombinationRunID = fromNullString(combRunID)
		result = append(result, &c)
	}
	return result, rows.Err()
}
