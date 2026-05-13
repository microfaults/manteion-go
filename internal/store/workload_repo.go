package store

import (
	"context"
	"database/sql"
	"fmt"

	"manteion-go/internal/model"
)

// WorkloadRepo provides persistence for attacks and attack results backed
// by PostgreSQL. Flow/Persona/Workload tables have been removed — zeus's
// Workflow/Run/Dataset are canonical.
type WorkloadRepo struct {
	db *sql.DB
}

// NewWorkloadRepo creates a new workload repository.
func NewWorkloadRepo(db *sql.DB) *WorkloadRepo {
	return &WorkloadRepo{db: db}
}

// --- Attack ---

// CreateAttack inserts a new vegeta attack.
func (r *WorkloadRepo) CreateAttack(ctx context.Context, a *model.Attack) error {
	if err := a.Validate(); err != nil {
		return err
	}

	headersJSON, err := jsonbMarshal(a.TargetHeaders)
	if err != nil {
		return fmt.Errorf("marshal target_headers: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO attacks (id, experiment_run_id, policy_rule_id,
			service, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, zeus_attack_id,
			status, created_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'pending', $13, $14)`,
		a.ID, nullString(a.ExperimentRunID), nullString(a.PolicyRuleID),
		a.Service, a.TargetURL, a.TargetMethod, headersJSON,
		a.Rate, a.DurationMs, nullString(a.DedupBypass), nullString(a.MetaTraceID),
		nullString(a.ZeusAttackID),
		a.CreatedAt, a.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert attack: %w", err)
	}
	return nil
}

// GetAttack returns an attack by ID, or ErrNotFound.
func (r *WorkloadRepo) GetAttack(ctx context.Context, id string) (*model.Attack, error) {
	var a model.Attack
	var expRunID, policyRuleID, dedup, metaTrace, zeusID sql.NullString
	var headersJSON []byte

	err := r.db.QueryRowContext(ctx, `
		SELECT id, experiment_run_id, policy_rule_id,
			service, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, zeus_attack_id,
			created_at, completed_at
		FROM attacks WHERE id = $1`, id,
	).Scan(
		&a.ID, &expRunID, &policyRuleID,
		&a.Service, &a.TargetURL, &a.TargetMethod, &headersJSON,
		&a.Rate, &a.DurationMs, &dedup, &metaTrace, &zeusID,
		&a.CreatedAt, &a.CompletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get attack: %w", err)
	}
	a.ExperimentRunID = fromNullString(expRunID)
	a.PolicyRuleID = fromNullString(policyRuleID)
	a.DedupBypass = fromNullString(dedup)
	a.MetaTraceID = fromNullString(metaTrace)
	a.ZeusAttackID = fromNullString(zeusID)
	if err := jsonbScan(headersJSON, &a.TargetHeaders); err != nil {
		return nil, fmt.Errorf("unmarshal target_headers: %w", err)
	}
	return &a, nil
}

// ListAttacksByRun returns all attacks for an experiment run.
func (r *WorkloadRepo) ListAttacksByRun(ctx context.Context, runID string) ([]*model.Attack, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_run_id, policy_rule_id,
			service, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, zeus_attack_id,
			created_at, completed_at
		FROM attacks WHERE experiment_run_id = $1
		ORDER BY created_at`, runID)
	if err != nil {
		return nil, fmt.Errorf("list attacks by run: %w", err)
	}
	defer rows.Close()

	var result []*model.Attack
	for rows.Next() {
		var a model.Attack
		var expRunID, policyRuleID, dedup, metaTrace, zeusID sql.NullString
		var headersJSON []byte

		err := rows.Scan(
			&a.ID, &expRunID, &policyRuleID,
			&a.Service, &a.TargetURL, &a.TargetMethod, &headersJSON,
			&a.Rate, &a.DurationMs, &dedup, &metaTrace, &zeusID,
			&a.CreatedAt, &a.CompletedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan attack: %w", err)
		}
		a.ExperimentRunID = fromNullString(expRunID)
		a.PolicyRuleID = fromNullString(policyRuleID)
		a.DedupBypass = fromNullString(dedup)
		a.MetaTraceID = fromNullString(metaTrace)
		a.ZeusAttackID = fromNullString(zeusID)
		if err := jsonbScan(headersJSON, &a.TargetHeaders); err != nil {
			return nil, fmt.Errorf("unmarshal target_headers: %w", err)
		}
		result = append(result, &a)
	}
	return result, rows.Err()
}

// CreateAttackResult inserts attack outcome metrics.
func (r *WorkloadRepo) CreateAttackResult(ctx context.Context, res *model.AttackResult) error {
	if err := res.Validate(); err != nil {
		return err
	}

	statusCodesJSON, err := jsonbMarshal(res.StatusCodes)
	if err != nil {
		return fmt.Errorf("marshal status_codes: %w", err)
	}
	errorsJSON, err := jsonbMarshal(res.Errors)
	if err != nil {
		return fmt.Errorf("marshal errors: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO attack_results (attack_id, service, total_requests, duration_ms,
			rate_actual, success_rate, status_codes,
			latency_p50_us, latency_p90_us, latency_p95_us, latency_p99_us,
			latency_min_us, latency_max_us,
			bytes_in_total, bytes_out_total, errors, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
		res.AttackID, res.Service, res.TotalRequests, res.DurationMs,
		res.RateActual, res.SuccessRate, statusCodesJSON,
		res.LatencyP50Us, res.LatencyP90Us, res.LatencyP95Us, res.LatencyP99Us,
		res.LatencyMinUs, res.LatencyMaxUs,
		res.BytesInTotal, res.BytesOutTotal, errorsJSON, res.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert attack_result: %w", err)
	}
	return nil
}
