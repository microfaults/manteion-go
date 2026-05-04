package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
		INSERT INTO experiments (id, name, description,
			primary_workload_id, status, created_at, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		exp.ID, exp.Name, nullString(exp.Description),
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
		SELECT id, name, description,
			primary_workload_id, status, created_at, started_at, completed_at
		FROM experiments WHERE id = $1`, id,
	).Scan(
		&exp.ID, &exp.Name, &desc,
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
		SELECT id, name, description,
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
			&exp.ID, &exp.Name, &desc,
			&exp.PrimaryWorkloadID, &exp.Status, &exp.CreatedAt, &exp.StartedAt, &exp.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan experiment: %w", err)
		}
		exp.Description = fromNullString(desc)
		result = append(result, &exp)
	}
	return result, rows.Err()
}

// Delete removes an experiment and its runs (cascaded by DB FK).
func (r *ExperimentRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM experiments WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete experiment: %w", err)
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
	phaseRulesJSON, err := jsonbMarshal(run.PhaseRules)
	if err != nil {
		return fmt.Errorf("marshal phase_rules: %w", err)
	}
	transCondJSON, err := jsonbMarshal(run.TransitionCond)
	if err != nil {
		return fmt.Errorf("marshal transition_cond: %w", err)
	}
	workloadIDsJSON, err := jsonbMarshal(run.WorkloadIDs)
	if err != nil {
		return fmt.Errorf("marshal workload_ids: %w", err)
	}
	zeusAttackIDsJSON, err := jsonbMarshal(run.ZeusAttackIDs)
	if err != nil {
		return fmt.Errorf("marshal zeus_attack_ids: %w", err)
	}
	dependsOnJSON, err := jsonbMarshal(run.DependsOn)
	if err != nil {
		return fmt.Errorf("marshal depends_on: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO experiment_runs (id, experiment_id, run_type, run_index,
			frozen_services, meta_trace_id, status, node_placement,
			started_at, completed_at, created_at,
			phase_rules, transition_cond, current_phase,
			workload_ids, zeus_attack_ids, depends_on, persist_cache)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
		run.ID, run.ExperimentID, run.RunType, run.RunIndex,
		frozenJSON, run.MetaTraceID, run.Status, placementJSON,
		run.StartedAt, run.CompletedAt, run.CreatedAt,
		phaseRulesJSON, transCondJSON, run.CurrentPhase,
		workloadIDsJSON, zeusAttackIDsJSON, dependsOnJSON, run.PersistCache,
	)
	if err != nil {
		return fmt.Errorf("insert experiment_run: %w", err)
	}
	return nil
}

// GetRun returns an experiment run by ID, or ErrNotFound.
func (r *ExperimentRepo) GetRun(ctx context.Context, id string) (*model.ExperimentRun, error) {
	var run model.ExperimentRun
	var frozenJSON, placementJSON, phaseRulesJSON, transCondJSON []byte
	var workloadIDsJSON, zeusAttackIDsJSON, dependsOnJSON []byte
	var zeusAttackID sql.NullString

	err := r.db.QueryRowContext(ctx, `
		SELECT id, experiment_id, run_type, run_index,
			frozen_services, meta_trace_id, status, node_placement,
			started_at, completed_at, created_at,
			phase_rules, transition_cond, current_phase, zeus_attack_id,
			workload_ids, zeus_attack_ids, depends_on, persist_cache
		FROM experiment_runs WHERE id = $1`, id,
	).Scan(
		&run.ID, &run.ExperimentID, &run.RunType, &run.RunIndex,
		&frozenJSON, &run.MetaTraceID, &run.Status, &placementJSON,
		&run.StartedAt, &run.CompletedAt, &run.CreatedAt,
		&phaseRulesJSON, &transCondJSON, &run.CurrentPhase, &zeusAttackID,
		&workloadIDsJSON, &zeusAttackIDsJSON, &dependsOnJSON, &run.PersistCache,
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
	if err := jsonbScan(phaseRulesJSON, &run.PhaseRules); err != nil {
		return nil, fmt.Errorf("unmarshal phase_rules: %w", err)
	}
	if err := jsonbScan(transCondJSON, &run.TransitionCond); err != nil {
		return nil, fmt.Errorf("unmarshal transition_cond: %w", err)
	}
	if err := jsonbScan(workloadIDsJSON, &run.WorkloadIDs); err != nil {
		return nil, fmt.Errorf("unmarshal workload_ids: %w", err)
	}
	if err := jsonbScan(zeusAttackIDsJSON, &run.ZeusAttackIDs); err != nil {
		return nil, fmt.Errorf("unmarshal zeus_attack_ids: %w", err)
	}
	if err := jsonbScan(dependsOnJSON, &run.DependsOn); err != nil {
		return nil, fmt.Errorf("unmarshal depends_on: %w", err)
	}
	run.ZeusAttackID = fromNullString(zeusAttackID)

	return &run, nil
}

// GetBaselineRun returns the most recently completed baseline run for an
// experiment, or ErrNotFound if none exists.
func (r *ExperimentRepo) GetBaselineRun(ctx context.Context, experimentID string) (*model.ExperimentRun, error) {
	var id string
	err := r.db.QueryRowContext(ctx, `
		SELECT id FROM experiment_runs
		WHERE experiment_id = $1 AND run_type = 'baseline' AND status = 'completed'
		ORDER BY created_at DESC LIMIT 1`, experimentID,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get baseline run: %w", err)
	}
	return r.GetRun(ctx, id)
}

// UpdateRunZeusAttack stores the zeus attack ID on a run row.
func (r *ExperimentRepo) UpdateRunZeusAttack(ctx context.Context, id, attackID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE experiment_runs SET zeus_attack_id = $2 WHERE id = $1`, id, attackID)
	return err
}

// ListRunsByExperiment returns all runs for an experiment, ordered by run_index.
func (r *ExperimentRepo) ListRunsByExperiment(ctx context.Context, experimentID string) ([]*model.ExperimentRun, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_id, run_type, run_index,
			frozen_services, meta_trace_id, status, node_placement,
			started_at, completed_at, created_at,
			phase_rules, transition_cond, current_phase, zeus_attack_id,
			workload_ids, zeus_attack_ids, depends_on, persist_cache
		FROM experiment_runs WHERE experiment_id = $1
		ORDER BY run_index`, experimentID)
	if err != nil {
		return nil, fmt.Errorf("list experiment runs: %w", err)
	}
	defer rows.Close()

	var result []*model.ExperimentRun
	for rows.Next() {
		run, err := r.scanRun(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, run)
	}
	return result, rows.Err()
}

// scanRun scans a row from any query that selects the full experiment_runs column set.
func (r *ExperimentRepo) scanRun(rows *sql.Rows) (*model.ExperimentRun, error) {
	var run model.ExperimentRun
	var frozenJSON, placementJSON, phaseRulesJSON, transCondJSON []byte
	var workloadIDsJSON, zeusAttackIDsJSON, dependsOnJSON []byte
	var zeusAttackID sql.NullString
	if err := rows.Scan(
		&run.ID, &run.ExperimentID, &run.RunType, &run.RunIndex,
		&frozenJSON, &run.MetaTraceID, &run.Status, &placementJSON,
		&run.StartedAt, &run.CompletedAt, &run.CreatedAt,
		&phaseRulesJSON, &transCondJSON, &run.CurrentPhase, &zeusAttackID,
		&workloadIDsJSON, &zeusAttackIDsJSON, &dependsOnJSON, &run.PersistCache,
	); err != nil {
		return nil, fmt.Errorf("scan experiment run: %w", err)
	}
	if err := jsonbScan(frozenJSON, &run.FrozenServices); err != nil {
		return nil, err
	}
	if err := jsonbScan(placementJSON, &run.NodePlacement); err != nil {
		return nil, err
	}
	if err := jsonbScan(phaseRulesJSON, &run.PhaseRules); err != nil {
		return nil, err
	}
	if err := jsonbScan(transCondJSON, &run.TransitionCond); err != nil {
		return nil, err
	}
	if err := jsonbScan(workloadIDsJSON, &run.WorkloadIDs); err != nil {
		return nil, err
	}
	if err := jsonbScan(zeusAttackIDsJSON, &run.ZeusAttackIDs); err != nil {
		return nil, err
	}
	if err := jsonbScan(dependsOnJSON, &run.DependsOn); err != nil {
		return nil, err
	}
	run.ZeusAttackID = fromNullString(zeusAttackID)
	return &run, nil
}

// UpdateRunPhase atomically updates current_phase and status.
func (r *ExperimentRepo) UpdateRunPhase(ctx context.Context, id string, phase int, status string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE experiment_runs SET current_phase = $2, status = $3 WHERE id = $1`,
		id, phase, status)
	return err
}

// UpdateRunStatus changes the status of an experiment run and tracks timestamps.
// Sets started_at on first transition to "running"; sets completed_at on "completed" or "failed".
func (r *ExperimentRepo) UpdateRunStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiment_runs SET
			status       = $2,
			started_at   = CASE WHEN $2 = 'running'                    AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed') THEN now() ELSE completed_at END
		WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateRunZeusAttacks stores the primary and all attack IDs for multi-workflow runs.
func (r *ExperimentRepo) UpdateRunZeusAttacks(ctx context.Context, id string, attackIDs []string) error {
	idsJSON, err := jsonbMarshal(attackIDs)
	if err != nil {
		return fmt.Errorf("marshal zeus_attack_ids: %w", err)
	}
	var primaryID sql.NullString
	if len(attackIDs) > 0 {
		primaryID = sql.NullString{String: attackIDs[0], Valid: true}
	}
	_, err = r.db.ExecContext(ctx,
		`UPDATE experiment_runs SET zeus_attack_id = $2, zeus_attack_ids = $3 WHERE id = $1`,
		id, primaryID, idsJSON)
	return err
}

// AppendRunAttackID appends an attack ID to zeus_attack_ids before the Zeus
// call is issued, so a crash mid-flight leaves a known ID to reconcile against.
// If zeus_attack_id is null, also seeds it as the primary.
func (r *ExperimentRepo) AppendRunAttackID(ctx context.Context, id, attackID string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE experiment_runs SET
			zeus_attack_ids = COALESCE(zeus_attack_ids, '[]'::jsonb) || to_jsonb($2::text),
			zeus_attack_id  = COALESCE(zeus_attack_id, $2)
		WHERE id = $1`, id, attackID)
	if err != nil {
		return fmt.Errorf("append run attack id: %w", err)
	}
	return nil
}

// ListRunsByStatus returns all runs in any of the given statuses. Used at
// startup to find in-flight runs that need watcher/poller reconciliation.
func (r *ExperimentRepo) ListRunsByStatus(ctx context.Context, statuses []string) ([]*model.ExperimentRun, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(statuses))
	args := make([]any, len(statuses))
	for i, s := range statuses {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = s
	}
	query := `
		SELECT id, experiment_id, run_type, run_index,
			frozen_services, meta_trace_id, status, node_placement,
			started_at, completed_at, created_at,
			phase_rules, transition_cond, current_phase, zeus_attack_id,
			workload_ids, zeus_attack_ids, depends_on, persist_cache
		FROM experiment_runs WHERE status IN (` + strings.Join(placeholders, ",") + `)
		ORDER BY created_at`
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs by status: %w", err)
	}
	defer rows.Close()

	var result []*model.ExperimentRun
	for rows.Next() {
		run, err := r.scanRun(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, run)
	}
	return result, rows.Err()
}

// ListPendingRuns returns runs in "pending" state for an experiment.
func (r *ExperimentRepo) ListPendingRuns(ctx context.Context, experimentID string) ([]*model.ExperimentRun, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_id, run_type, run_index,
			frozen_services, meta_trace_id, status, node_placement,
			started_at, completed_at, created_at,
			phase_rules, transition_cond, current_phase, zeus_attack_id,
			workload_ids, zeus_attack_ids, depends_on, persist_cache
		FROM experiment_runs WHERE experiment_id = $1 AND status = 'pending'
		ORDER BY run_index`, experimentID)
	if err != nil {
		return nil, fmt.Errorf("list pending runs: %w", err)
	}
	defer rows.Close()

	var result []*model.ExperimentRun
	for rows.Next() {
		run, err := r.scanRun(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, run)
	}
	return result, rows.Err()
}

// UpdateStatus changes the status of an experiment and tracks timestamps.
func (r *ExperimentRepo) UpdateStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiments SET
			status       = $2,
			started_at   = CASE WHEN $2 = 'running'                              AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','cancelled') THEN now() ELSE completed_at END
		WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update experiment status: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ListWorkflowResults returns all workflow-level latency results for a run.
func (r *ExperimentRepo) ListWorkflowResults(ctx context.Context, runID string) ([]*model.WorkflowRunResult, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_run_id, workflow,
			latency_p50_us, latency_p95_us, latency_p99_us, latency_p999_us,
			request_count, error_rate, throughput_rps, raw_metrics
		FROM workflow_run_results WHERE experiment_run_id = $1
		ORDER BY workflow`, runID)
	if err != nil {
		return nil, fmt.Errorf("list workflow results: %w", err)
	}
	defer rows.Close()

	var result []*model.WorkflowRunResult
	for rows.Next() {
		var res model.WorkflowRunResult
		if err := rows.Scan(
			&res.ID, &res.ExperimentRunID, &res.Workflow,
			&res.LatencyP50Us, &res.LatencyP95Us, &res.LatencyP99Us, &res.LatencyP999Us,
			&res.RequestCount, &res.ErrorRate, &res.ThroughputRPS, &res.RawMetrics,
		); err != nil {
			return nil, fmt.Errorf("scan workflow result: %w", err)
		}
		result = append(result, &res)
	}
	return result, rows.Err()
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
