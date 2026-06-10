package store

import (
	"context"
	"database/sql"
	"fmt"

	"manteion-go/internal/model"
)

// WorkloadRepo provides persistence for attacks and attack results backed
// by PostgreSQL. Flow/Persona/Workload tables have been removed — zeus's
// Workflow/Run/Dataset are canonical. Per migration #19 the attacks table
// no longer carries experiment_run_id; phase-driven attacks are tracked
// via phase_workflows.zeus_attack_id instead.
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
		INSERT INTO attacks (id, policy_rule_id,
			service, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, zeus_attack_id,
			created_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		a.ID, nullString(a.PolicyRuleID),
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
	var policyRuleID, dedup, metaTrace, zeusID sql.NullString
	var headersJSON []byte

	err := r.db.QueryRowContext(ctx, `
		SELECT id, policy_rule_id,
			service, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, zeus_attack_id,
			created_at, completed_at
		FROM attacks WHERE id = $1`, id,
	).Scan(
		&a.ID, &policyRuleID,
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
	a.PolicyRuleID = fromNullString(policyRuleID)
	a.DedupBypass = fromNullString(dedup)
	a.MetaTraceID = fromNullString(metaTrace)
	a.ZeusAttackID = fromNullString(zeusID)
	if err := jsonbScan(headersJSON, &a.TargetHeaders); err != nil {
		return nil, fmt.Errorf("unmarshal target_headers: %w", err)
	}
	return &a, nil
}

// GetAttackByZeusID returns the manteion-side Attack record matching a zeus
// attack id (set on the phase_workflows row by the orchestrator).
func (r *WorkloadRepo) GetAttackByZeusID(ctx context.Context, zeusID string) (*model.Attack, error) {
	if zeusID == "" {
		return nil, ErrNotFound
	}
	var a model.Attack
	var policyRuleID, dedup, metaTrace, scannedZeusID sql.NullString
	var headersJSON []byte

	err := r.db.QueryRowContext(ctx, `
		SELECT id, policy_rule_id,
			service, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, zeus_attack_id,
			created_at, completed_at
		FROM attacks WHERE zeus_attack_id = $1`, zeusID,
	).Scan(
		&a.ID, &policyRuleID,
		&a.Service, &a.TargetURL, &a.TargetMethod, &headersJSON,
		&a.Rate, &a.DurationMs, &dedup, &metaTrace, &scannedZeusID,
		&a.CreatedAt, &a.CompletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get attack by zeus id: %w", err)
	}
	a.PolicyRuleID = fromNullString(policyRuleID)
	a.DedupBypass = fromNullString(dedup)
	a.MetaTraceID = fromNullString(metaTrace)
	a.ZeusAttackID = fromNullString(scannedZeusID)
	if err := jsonbScan(headersJSON, &a.TargetHeaders); err != nil {
		return nil, fmt.Errorf("unmarshal target_headers: %w", err)
	}
	return &a, nil
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
