package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"manteion-go/internal/model"
)

// ExperimentRepo provides persistence for the phase-first experiment domain
// introduced by migration #19.
//
// Hierarchy:
//
//	experiments
//	  └── experiment_phases     (ordered by position)
//	        ├── phase_workflows  (per-(phase, workflow) attack config)
//	        ├── phase_rules      (rules active during the phase)
//	        └── results: phase_workflow_results,
//	                     phase_service_latency / resources / cache
//
// All list endpoints accept a Page struct so handlers can honor the
// pagination contract uniformly.
type ExperimentRepo struct {
	db *sql.DB
}

// NewExperimentRepo creates a new experiment repository.
func NewExperimentRepo(db *sql.DB) *ExperimentRepo {
	return &ExperimentRepo{db: db}
}

// ExperimentFilter narrows a list query. Empty values are no-ops.
// (Page, the pagination request shape, lives in helpers.go.)
type ExperimentFilter struct {
	Status string
}

// =========================================================================
// Experiment
// =========================================================================

// Create inserts a new experiment row. Workflows and phases are persisted
// via the dedicated Attach* / CreatePhase methods.
func (r *ExperimentRepo) Create(ctx context.Context, exp *model.Experiment) error {
	if err := exp.Validate(); err != nil {
		return err
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO experiments (id, name, description, hypothesis, status,
			created_by, created_at, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		exp.ID, exp.Name, nullString(exp.Description), nullString(exp.Hypothesis),
		exp.Status, nullString(exp.CreatedBy), exp.CreatedAt, exp.StartedAt, exp.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert experiment: %w", err)
	}
	return nil
}

// Get returns one experiment by id, or ErrNotFound.
func (r *ExperimentRepo) Get(ctx context.Context, id string) (*model.Experiment, error) {
	exp, err := scanExperimentRow(r.db.QueryRowContext(ctx, `
		SELECT id, name, description, hypothesis, status, created_by,
			created_at, started_at, completed_at
		FROM experiments WHERE id = $1`, id))
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment: %w", err)
	}
	return exp, nil
}

// List returns a page of experiments with the total row count. The total
// is computed in the same call so the handler can build the page envelope
// without a separate query.
func (r *ExperimentRepo) List(ctx context.Context, f ExperimentFilter, p Page) ([]*model.Experiment, int, error) {
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if p.Limit > 200 {
		p.Limit = 200
	}
	if p.Offset < 0 {
		p.Offset = 0
	}

	args := []any{p.Limit, p.Offset}
	where := ""
	if f.Status != "" {
		// status::text keeps the comparison graceful for unknown filter
		// values (matches nothing) instead of a 22P02 enum-cast error.
		where = "WHERE status::text = $3"
		args = append(args, f.Status)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, description, hypothesis, status, created_by,
			created_at, started_at, completed_at,
			COUNT(*) OVER () AS total_count
		FROM experiments
		`+where+`
		ORDER BY created_at DESC, id
		LIMIT $1 OFFSET $2`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list experiments: %w", err)
	}
	defer rows.Close()

	var (
		result []*model.Experiment
		total  int
	)
	for rows.Next() {
		var (
			exp                         model.Experiment
			desc, hypothesis, createdBy sql.NullString
			startedAt, completedAt      sql.NullTime
		)
		if err := rows.Scan(
			&exp.ID, &exp.Name, &desc, &hypothesis, &exp.Status, &createdBy,
			&exp.CreatedAt, &startedAt, &completedAt, &total,
		); err != nil {
			return nil, 0, fmt.Errorf("scan experiment: %w", err)
		}
		exp.Description = fromNullString(desc)
		exp.Hypothesis = fromNullString(hypothesis)
		exp.CreatedBy = fromNullString(createdBy)
		exp.StartedAt = nullTimeToPtr(startedAt)
		exp.CompletedAt = nullTimeToPtr(completedAt)
		result = append(result, &exp)
	}
	return result, total, rows.Err()
}

// Delete removes an experiment and cascades to its workflows/phases/results
// via FK ON DELETE CASCADE.
func (r *ExperimentRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM experiments WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete experiment: %w", err)
	}
	return affectedOrNotFound(res)
}

// UpdateStatus changes the experiment status and stamps started_at /
// completed_at as appropriate.
func (r *ExperimentRepo) UpdateStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiments SET
			status       = $2::experiment_status,
			started_at   = CASE WHEN $2 = 'running'                            AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','cancelled') THEN now()              ELSE completed_at END
		WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update experiment status: %w", err)
	}
	return affectedOrNotFound(res)
}

// TransitionExperiment atomically moves the experiment to `to` only when its
// current status is one of `from`. Returns whether the transition happened —
// the orchestrator FSM's compare-and-swap primitive: exactly one of N racing
// callers wins, and only the winner runs side effects. Timestamps are stamped
// like UpdateStatus.
func (r *ExperimentRepo) TransitionExperiment(ctx context.Context, id, to string, from ...string) (bool, error) {
	guard, args, err := fromStatusGuard(id, to, from)
	if err != nil {
		return false, fmt.Errorf("transition experiment: %w", err)
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiments SET
			status       = $2::experiment_status,
			started_at   = CASE WHEN $2 = 'running'                            AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','cancelled') THEN now()              ELSE completed_at END
		WHERE id = $1 AND status::text IN `+guard, args...)
	if err != nil {
		return false, fmt.Errorf("transition experiment: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("transition experiment: rows affected: %w", err)
	}
	return n > 0, nil
}

// TransitionPhase is the phase-level CAS twin of TransitionExperiment.
func (r *ExperimentRepo) TransitionPhase(ctx context.Context, id, to string, from ...string) (bool, error) {
	guard, args, err := fromStatusGuard(id, to, from)
	if err != nil {
		return false, fmt.Errorf("transition phase: %w", err)
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiment_phases SET
			status       = $2::phase_status,
			started_at   = CASE WHEN $2 = 'running'                              AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','skipped') THEN now()                  ELSE completed_at END
		WHERE id = $1 AND status::text IN `+guard, args...)
	if err != nil {
		return false, fmt.Errorf("transition phase: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("transition phase: rows affected: %w", err)
	}
	return n > 0, nil
}

// fromStatusGuard builds the parameterized "($3, $4, ...)" from-set guard and
// the full args slice (id, to, from...) for the Transition* CAS updates.
func fromStatusGuard(id, to string, from []string) (string, []any, error) {
	if len(from) == 0 {
		return "", nil, fmt.Errorf("empty from-set")
	}
	args := make([]any, 0, len(from)+2)
	args = append(args, id, to)
	ph := make([]string, len(from))
	for i, f := range from {
		ph[i] = fmt.Sprintf("$%d", i+3)
		args = append(args, f)
	}
	return "(" + strings.Join(ph, ", ") + ")", args, nil
}

// UpdateMetadata edits non-state fields. Empty arguments are no-ops.
func (r *ExperimentRepo) UpdateMetadata(ctx context.Context, id string, name, description, hypothesis string) error {
	sets := []string{}
	args := []any{id}
	idx := 2
	if name != "" {
		sets = append(sets, fmt.Sprintf("name = $%d", idx))
		args = append(args, name)
		idx++
	}
	if description != "" {
		sets = append(sets, fmt.Sprintf("description = $%d", idx))
		args = append(args, description)
		idx++
	}
	if hypothesis != "" {
		sets = append(sets, fmt.Sprintf("hypothesis = $%d", idx))
		args = append(args, hypothesis)
		idx++
	}
	if len(sets) == 0 {
		return nil
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE experiments SET `+strings.Join(sets, ", ")+` WHERE id = $1`, args...)
	if err != nil {
		return fmt.Errorf("update experiment metadata: %w", err)
	}
	return affectedOrNotFound(res)
}

// scanExperimentRow scans one experiments row.
func scanExperimentRow(scanner interface {
	Scan(dest ...any) error
}) (*model.Experiment, error) {
	var (
		exp                         model.Experiment
		desc, hypothesis, createdBy sql.NullString
		startedAt, completedAt      sql.NullTime
	)
	err := scanner.Scan(
		&exp.ID, &exp.Name, &desc, &hypothesis, &exp.Status, &createdBy,
		&exp.CreatedAt, &startedAt, &completedAt,
	)
	if err != nil {
		return nil, err
	}
	exp.Description = fromNullString(desc)
	exp.Hypothesis = fromNullString(hypothesis)
	exp.CreatedBy = fromNullString(createdBy)
	exp.StartedAt = nullTimeToPtr(startedAt)
	exp.CompletedAt = nullTimeToPtr(completedAt)
	return &exp, nil
}

// =========================================================================
// ExperimentPhase
// =========================================================================

// CreatePhase inserts one phase row. Caller is responsible for assigning
// the position (or letting NextPhasePosition do it).
func (r *ExperimentRepo) CreatePhase(ctx context.Context, p *model.ExperimentPhase) error {
	if err := p.Validate(); err != nil {
		return err
	}
	frozenJSON, err := json.Marshal(p.FrozenServices)
	if err != nil {
		return fmt.Errorf("marshal frozen_services: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO experiment_phases (id, experiment_id, name, position, status,
			frozen_services, persist_cache, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		p.ID, p.ExperimentID, p.Name, p.Position, p.Status,
		frozenJSON, p.PersistCache, p.StartedAt, p.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert experiment_phase: %w", err)
	}
	return nil
}

// GetPhase returns one phase by id.
func (r *ExperimentRepo) GetPhase(ctx context.Context, id string) (*model.ExperimentPhase, error) {
	p, err := scanPhaseRow(r.db.QueryRowContext(ctx, `
		SELECT id, experiment_id, name, position, status, frozen_services,
			persist_cache, started_at, completed_at
		FROM experiment_phases WHERE id = $1`, id))
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment_phase: %w", err)
	}
	return p, nil
}

// ListPhasesForExperiment returns phases ordered by position.
func (r *ExperimentRepo) ListPhasesForExperiment(ctx context.Context, experimentID string) ([]*model.ExperimentPhase, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_id, name, position, status, frozen_services,
			persist_cache, started_at, completed_at
		FROM experiment_phases
		WHERE experiment_id = $1
		ORDER BY position`, experimentID)
	if err != nil {
		return nil, fmt.Errorf("list experiment_phases: %w", err)
	}
	defer rows.Close()
	var out []*model.ExperimentPhase
	for rows.Next() {
		p, err := scanPhaseRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan experiment_phase: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeletePhase removes one phase and cascades to its workflows/rules/results.
func (r *ExperimentRepo) DeletePhase(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM experiment_phases WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete experiment_phase: %w", err)
	}
	return affectedOrNotFound(res)
}

// UpdatePhaseStatus stamps started_at / completed_at as appropriate.
func (r *ExperimentRepo) UpdatePhaseStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiment_phases SET
			status       = $2::phase_status,
			started_at   = CASE WHEN $2 = 'running'                              AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','skipped') THEN now()                  ELSE completed_at END
		WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update phase status: %w", err)
	}
	return affectedOrNotFound(res)
}

// NextPhasePosition returns the next free position for an experiment.
func (r *ExperimentRepo) NextPhasePosition(ctx context.Context, experimentID string) (int, error) {
	var next sql.NullInt32
	err := r.db.QueryRowContext(ctx,
		`SELECT MAX(position) + 1 FROM experiment_phases WHERE experiment_id = $1`,
		experimentID).Scan(&next)
	if err != nil {
		return 0, fmt.Errorf("next phase position: %w", err)
	}
	if !next.Valid {
		return 0, nil
	}
	return int(next.Int32), nil
}

func scanPhaseRow(scanner interface {
	Scan(dest ...any) error
}) (*model.ExperimentPhase, error) {
	var (
		p                      model.ExperimentPhase
		frozenJSON             []byte
		startedAt, completedAt sql.NullTime
	)
	if err := scanner.Scan(
		&p.ID, &p.ExperimentID, &p.Name, &p.Position, &p.Status, &frozenJSON,
		&p.PersistCache, &startedAt, &completedAt,
	); err != nil {
		return nil, err
	}
	if len(frozenJSON) > 0 {
		if err := json.Unmarshal(frozenJSON, &p.FrozenServices); err != nil {
			return nil, fmt.Errorf("decode frozen_services: %w", err)
		}
	}
	p.StartedAt = nullTimeToPtr(startedAt)
	p.CompletedAt = nullTimeToPtr(completedAt)
	return &p, nil
}

// =========================================================================
// PhaseWorkflow
// =========================================================================

// AttachPhaseWorkflows replaces the phase_workflows rows for the phase with
// the supplied slice. Use this on phase create / edit to keep the set in sync.
func (r *ExperimentRepo) AttachPhaseWorkflows(ctx context.Context, phaseID string, pws []model.PhaseWorkflow) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM phase_workflows WHERE phase_id = $1`, phaseID); err != nil {
		return fmt.Errorf("clear phase_workflows: %w", err)
	}
	for i, pw := range pws {
		pw.PhaseID = phaseID
		if err := (&pw).Validate(); err != nil {
			return fmt.Errorf("phase_workflows[%d]: %w", i, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO phase_workflows (phase_id, workflow_id, vus, rate_rps,
				duration_sec, target_url, target_method, zeus_attack_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			pw.PhaseID, pw.WorkflowID, pw.VUs, nullFloat(pw.RateRPS),
			pw.DurationSec, nullString(pw.TargetURL), nullString(pw.TargetMethod),
			nullString(pw.ZeusAttackID),
		); err != nil {
			return fmt.Errorf("insert phase_workflow: %w", err)
		}
	}
	return tx.Commit()
}

// ListPhaseWorkflows returns all phase_workflows rows for the phase.
func (r *ExperimentRepo) ListPhaseWorkflows(ctx context.Context, phaseID string) ([]model.PhaseWorkflow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, workflow_id, vus, rate_rps, duration_sec,
			target_url, target_method, zeus_attack_id
		FROM phase_workflows WHERE phase_id = $1
		ORDER BY workflow_id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_workflows: %w", err)
	}
	defer rows.Close()
	var out []model.PhaseWorkflow
	for rows.Next() {
		var (
			pw                              model.PhaseWorkflow
			rateRPS                         sql.NullFloat64
			targetURL, targetMethod, zeusID sql.NullString
		)
		if err := rows.Scan(
			&pw.PhaseID, &pw.WorkflowID, &pw.VUs, &rateRPS, &pw.DurationSec,
			&targetURL, &targetMethod, &zeusID,
		); err != nil {
			return nil, fmt.Errorf("scan phase_workflow: %w", err)
		}
		if rateRPS.Valid {
			pw.RateRPS = rateRPS.Float64
		}
		pw.TargetURL = fromNullString(targetURL)
		pw.TargetMethod = fromNullString(targetMethod)
		pw.ZeusAttackID = fromNullString(zeusID)
		out = append(out, pw)
	}
	return out, rows.Err()
}

// UpdatePhaseWorkflowZeusAttack stamps the zeus_attack_id on a single
// phase_workflows row once the orchestrator has registered the attack.
func (r *ExperimentRepo) UpdatePhaseWorkflowZeusAttack(ctx context.Context, phaseID, workflowID, zeusAttackID string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE phase_workflows SET zeus_attack_id = $3
		WHERE phase_id = $1 AND workflow_id = $2`,
		phaseID, workflowID, zeusAttackID)
	if err != nil {
		return fmt.Errorf("update phase_workflow zeus_attack_id: %w", err)
	}
	return affectedOrNotFound(res)
}

// =========================================================================
// PhaseRule
// =========================================================================

// AttachPhaseRules replaces the phase_rules rows for the phase. Position is
// taken from slice index.
func (r *ExperimentRepo) AttachPhaseRules(ctx context.Context, phaseID string, ruleIDs []string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM phase_rules WHERE phase_id = $1`, phaseID); err != nil {
		return fmt.Errorf("clear phase_rules: %w", err)
	}
	for i, rid := range ruleIDs {
		if rid == "" {
			return fmt.Errorf("attach phase rules: empty rule_id at position %d", i)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO phase_rules (phase_id, rule_id, position)
			VALUES ($1, $2, $3)`, phaseID, rid, i); err != nil {
			return fmt.Errorf("insert phase_rule: %w", err)
		}
	}
	return tx.Commit()
}

// ListPhaseRules returns phase_rules in position order.
func (r *ExperimentRepo) ListPhaseRules(ctx context.Context, phaseID string) ([]model.PhaseRule, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, rule_id, position
		FROM phase_rules WHERE phase_id = $1
		ORDER BY position`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_rules: %w", err)
	}
	defer rows.Close()
	var out []model.PhaseRule
	for rows.Next() {
		var pr model.PhaseRule
		if err := rows.Scan(&pr.PhaseID, &pr.RuleID, &pr.Position); err != nil {
			return nil, fmt.Errorf("scan phase_rule: %w", err)
		}
		out = append(out, pr)
	}
	return out, rows.Err()
}

// =========================================================================
// Results
// =========================================================================

// UpsertWorkflowResult inserts or updates the (phase, workflow) result row.
func (r *ExperimentRepo) UpsertWorkflowResult(ctx context.Context, res *model.PhaseWorkflowResult) error {
	if err := res.Validate(); err != nil {
		return err
	}
	if res.ComputedAt.IsZero() {
		res.ComputedAt = time.Now()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO phase_workflow_results (phase_id, workflow_id, request_count,
			error_count, error_rate, throughput_rps,
			latency_p50_us, latency_p95_us, latency_p99_us, latency_p999_us,
			computed_at, raw_metrics)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (phase_id, workflow_id) DO UPDATE SET
			request_count   = EXCLUDED.request_count,
			error_count     = EXCLUDED.error_count,
			error_rate      = EXCLUDED.error_rate,
			throughput_rps  = EXCLUDED.throughput_rps,
			latency_p50_us  = EXCLUDED.latency_p50_us,
			latency_p95_us  = EXCLUDED.latency_p95_us,
			latency_p99_us  = EXCLUDED.latency_p99_us,
			latency_p999_us = EXCLUDED.latency_p999_us,
			computed_at     = EXCLUDED.computed_at,
			raw_metrics     = EXCLUDED.raw_metrics`,
		res.PhaseID, res.WorkflowID, res.RequestCount,
		res.ErrorCount, res.ErrorRate, res.ThroughputRPS,
		res.LatencyP50Us, res.LatencyP95Us, res.LatencyP99Us, res.LatencyP999Us,
		res.ComputedAt, res.RawMetrics,
	)
	if err != nil {
		return fmt.Errorf("upsert phase_workflow_results: %w", err)
	}
	return nil
}

// ListWorkflowResultsForPhase returns all workflow results for one phase.
func (r *ExperimentRepo) ListWorkflowResultsForPhase(ctx context.Context, phaseID string) ([]*model.PhaseWorkflowResult, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, workflow_id, request_count, error_count, error_rate,
			throughput_rps, latency_p50_us, latency_p95_us, latency_p99_us, latency_p999_us,
			computed_at, raw_metrics
		FROM phase_workflow_results WHERE phase_id = $1
		ORDER BY workflow_id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_workflow_results: %w", err)
	}
	defer rows.Close()
	var out []*model.PhaseWorkflowResult
	for rows.Next() {
		var res model.PhaseWorkflowResult
		if err := rows.Scan(
			&res.PhaseID, &res.WorkflowID, &res.RequestCount, &res.ErrorCount, &res.ErrorRate,
			&res.ThroughputRPS, &res.LatencyP50Us, &res.LatencyP95Us, &res.LatencyP99Us, &res.LatencyP999Us,
			&res.ComputedAt, &res.RawMetrics,
		); err != nil {
			return nil, fmt.Errorf("scan phase_workflow_result: %w", err)
		}
		out = append(out, &res)
	}
	return out, rows.Err()
}

// UpsertServiceLatency inserts or updates a (phase, service, workflow_id) row.
func (r *ExperimentRepo) UpsertServiceLatency(ctx context.Context, res *model.PhaseServiceLatency) error {
	if err := res.Validate(); err != nil {
		return err
	}
	if res.ComputedAt.IsZero() {
		res.ComputedAt = time.Now()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO phase_service_latency (phase_id, service, workflow_id,
			latency_p50_us, latency_p95_us, latency_p99_us, request_count, computed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (phase_id, service, workflow_id) DO UPDATE SET
			latency_p50_us = EXCLUDED.latency_p50_us,
			latency_p95_us = EXCLUDED.latency_p95_us,
			latency_p99_us = EXCLUDED.latency_p99_us,
			request_count  = EXCLUDED.request_count,
			computed_at    = EXCLUDED.computed_at`,
		res.PhaseID, res.Service, res.WorkflowID,
		res.LatencyP50Us, res.LatencyP95Us, res.LatencyP99Us, res.RequestCount, res.ComputedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert phase_service_latency: %w", err)
	}
	return nil
}

// ListServiceLatencyForPhase returns per-service latency rows for a phase.
func (r *ExperimentRepo) ListServiceLatencyForPhase(ctx context.Context, phaseID string) ([]*model.PhaseServiceLatency, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, service, workflow_id, latency_p50_us, latency_p95_us,
			latency_p99_us, request_count, computed_at
		FROM phase_service_latency WHERE phase_id = $1
		ORDER BY service, workflow_id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_service_latency: %w", err)
	}
	defer rows.Close()
	var out []*model.PhaseServiceLatency
	for rows.Next() {
		var res model.PhaseServiceLatency
		if err := rows.Scan(
			&res.PhaseID, &res.Service, &res.WorkflowID, &res.LatencyP50Us, &res.LatencyP95Us,
			&res.LatencyP99Us, &res.RequestCount, &res.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("scan phase_service_latency: %w", err)
		}
		out = append(out, &res)
	}
	return out, rows.Err()
}

// UpsertServiceCache inserts or updates a (phase, service) cache stats row.
func (r *ExperimentRepo) UpsertServiceCache(ctx context.Context, res *model.PhaseServiceCache) error {
	if err := res.Validate(); err != nil {
		return err
	}
	if res.ComputedAt.IsZero() {
		res.ComputedAt = time.Now()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO phase_service_cache (phase_id, service, cache_hit_rate,
			cache_exact_match, cache_staleness, computed_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (phase_id, service) DO UPDATE SET
			cache_hit_rate    = EXCLUDED.cache_hit_rate,
			cache_exact_match = EXCLUDED.cache_exact_match,
			cache_staleness   = EXCLUDED.cache_staleness,
			computed_at       = EXCLUDED.computed_at`,
		res.PhaseID, res.Service, res.CacheHitRate, res.CacheExactMatch, res.CacheStaleness, res.ComputedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert phase_service_cache: %w", err)
	}
	return nil
}

// ListServiceCacheForPhase returns per-service cache rows for a phase.
func (r *ExperimentRepo) ListServiceCacheForPhase(ctx context.Context, phaseID string) ([]*model.PhaseServiceCache, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, service, cache_hit_rate, cache_exact_match, cache_staleness, computed_at
		FROM phase_service_cache WHERE phase_id = $1
		ORDER BY service`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_service_cache: %w", err)
	}
	defer rows.Close()
	var out []*model.PhaseServiceCache
	for rows.Next() {
		var res model.PhaseServiceCache
		if err := rows.Scan(
			&res.PhaseID, &res.Service, &res.CacheHitRate, &res.CacheExactMatch, &res.CacheStaleness, &res.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("scan phase_service_cache: %w", err)
		}
		out = append(out, &res)
	}
	return out, rows.Err()
}

// =========================================================================
// Experiment-level rollup
// =========================================================================

// GetExperimentResults returns the rollup row for one experiment.
func (r *ExperimentRepo) GetExperimentResults(ctx context.Context, experimentID string) (*model.ExperimentResults, error) {
	var res model.ExperimentResults
	err := r.db.QueryRowContext(ctx, `
		SELECT experiment_id, phase_count, completed_phase_count, total_request_count,
			total_error_count, overall_error_rate, worst_p99_us, worst_p99_phase_id,
			best_p99_us, best_p99_phase_id, computed_at
		FROM experiment_results WHERE experiment_id = $1`, experimentID,
	).Scan(
		&res.ExperimentID, &res.PhaseCount, &res.CompletedPhaseCount, &res.TotalRequestCount,
		&res.TotalErrorCount, &res.OverallErrorRate, &res.WorstP99Us, &res.WorstP99PhaseID,
		&res.BestP99Us, &res.BestP99PhaseID, &res.ComputedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment_results: %w", err)
	}
	return &res, nil
}

// RecomputeExperimentResults aggregates phase results into the rollup row.
// Safe to call any time; idempotent. Returns the freshly computed rollup
// (or nil if the experiment has no completed phases yet).
//
// NOTE on percentiles: worst_p99 / best_p99 are the max / min of per-phase
// p99 values, weighted by request count. This is NOT a true experiment-level
// percentile (which would need histograms); see the data-model doc's
// "percentile-aggregation caveat" section.
func (r *ExperimentRepo) RecomputeExperimentResults(ctx context.Context, experimentID string) (*model.ExperimentResults, error) {
	var (
		phaseCount, completedCount int
		totalReq, totalErr         int64
		worstP99, bestP99          sql.NullInt64
		worstPhase, bestPhase      sql.NullString
	)

	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*),
			COUNT(*) FILTER (WHERE status = 'completed')
		FROM experiment_phases WHERE experiment_id = $1`,
		experimentID,
	).Scan(&phaseCount, &completedCount)
	if err != nil {
		return nil, fmt.Errorf("count phases: %w", err)
	}

	err = r.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(pwr.request_count), 0),
			COALESCE(SUM(pwr.error_count),   0)
		FROM phase_workflow_results pwr
		JOIN experiment_phases p ON p.id = pwr.phase_id
		WHERE p.experiment_id = $1`, experimentID,
	).Scan(&totalReq, &totalErr)
	if err != nil {
		return nil, fmt.Errorf("sum result counters: %w", err)
	}

	// Worst phase: maximum p99 across phases (weighted by request count).
	// We pick the phase row with the highest representative p99; ties broken
	// by request count descending.
	err = r.db.QueryRowContext(ctx, `
		SELECT p.id, MAX(pwr.latency_p99_us)
		FROM phase_workflow_results pwr
		JOIN experiment_phases p ON p.id = pwr.phase_id
		WHERE p.experiment_id = $1
		GROUP BY p.id
		ORDER BY MAX(pwr.latency_p99_us) DESC NULLS LAST, SUM(pwr.request_count) DESC
		LIMIT 1`, experimentID,
	).Scan(&worstPhase, &worstP99)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("compute worst p99: %w", err)
	}

	err = r.db.QueryRowContext(ctx, `
		SELECT p.id, MIN(pwr.latency_p99_us)
		FROM phase_workflow_results pwr
		JOIN experiment_phases p ON p.id = pwr.phase_id
		WHERE p.experiment_id = $1 AND pwr.request_count > 0
		GROUP BY p.id
		ORDER BY MIN(pwr.latency_p99_us) ASC NULLS LAST, SUM(pwr.request_count) DESC
		LIMIT 1`, experimentID,
	).Scan(&bestPhase, &bestP99)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("compute best p99: %w", err)
	}

	// Nothing measured yet — no rollup possible.
	if !worstP99.Valid || !worstPhase.Valid || !bestP99.Valid || !bestPhase.Valid {
		return nil, nil
	}

	errorRate := 0.0
	if totalReq > 0 {
		errorRate = float64(totalErr) / float64(totalReq)
	}

	now := time.Now()
	res := &model.ExperimentResults{
		ExperimentID:        experimentID,
		PhaseCount:          phaseCount,
		CompletedPhaseCount: completedCount,
		TotalRequestCount:   totalReq,
		TotalErrorCount:     totalErr,
		OverallErrorRate:    errorRate,
		WorstP99Us:          worstP99.Int64,
		WorstP99PhaseID:     worstPhase.String,
		BestP99Us:           bestP99.Int64,
		BestP99PhaseID:      bestPhase.String,
		ComputedAt:          now,
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO experiment_results (experiment_id, phase_count, completed_phase_count,
			total_request_count, total_error_count, overall_error_rate,
			worst_p99_us, worst_p99_phase_id, best_p99_us, best_p99_phase_id, computed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (experiment_id) DO UPDATE SET
			phase_count           = EXCLUDED.phase_count,
			completed_phase_count = EXCLUDED.completed_phase_count,
			total_request_count   = EXCLUDED.total_request_count,
			total_error_count     = EXCLUDED.total_error_count,
			overall_error_rate    = EXCLUDED.overall_error_rate,
			worst_p99_us          = EXCLUDED.worst_p99_us,
			worst_p99_phase_id    = EXCLUDED.worst_p99_phase_id,
			best_p99_us           = EXCLUDED.best_p99_us,
			best_p99_phase_id     = EXCLUDED.best_p99_phase_id,
			computed_at           = EXCLUDED.computed_at`,
		res.ExperimentID, res.PhaseCount, res.CompletedPhaseCount,
		res.TotalRequestCount, res.TotalErrorCount, res.OverallErrorRate,
		res.WorstP99Us, res.WorstP99PhaseID, res.BestP99Us, res.BestP99PhaseID, res.ComputedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("upsert experiment_results: %w", err)
	}
	return res, nil
}
