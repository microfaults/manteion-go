package store

import (
	"context"
	"database/sql"
	"fmt"

	"manteion-go/internal/model"
)

// WorkloadRepo provides persistence for attack definitions and their
// execution records. Attacks are reusable definitions (no execution state,
// no experiment association); each trigger writes an attack_results row,
// optionally tagged with the phase it ran in.
type WorkloadRepo struct {
	db *sql.DB
}

// NewWorkloadRepo creates a new workload repository.
func NewWorkloadRepo(db *sql.DB) *WorkloadRepo {
	return &WorkloadRepo{db: db}
}

// --- Attack definitions ---

const attackColumns = `id, name, description, service, target_url, target_method,
	target_headers, rate, duration_ms, dedup_bypass, created_at, updated_at`

// CreateAttack inserts a new attack definition.
func (r *WorkloadRepo) CreateAttack(ctx context.Context, a *model.Attack) error {
	if err := a.Validate(); err != nil {
		return err
	}

	headersJSON, err := jsonbMarshal(a.TargetHeaders)
	if err != nil {
		return fmt.Errorf("marshal target_headers: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO attacks (id, name, description, service, target_url, target_method,
			target_headers, rate, duration_ms, dedup_bypass, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)`,
		a.ID, a.Name, a.Description, a.Service, a.TargetURL, a.TargetMethod,
		headersJSON, a.Rate, a.DurationMs, nullString(a.DedupBypass), a.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert attack: %w", err)
	}
	return nil
}

// GetAttack returns an attack definition by ID, or ErrNotFound.
func (r *WorkloadRepo) GetAttack(ctx context.Context, id string) (*model.Attack, error) {
	a, err := scanAttack(r.db.QueryRowContext(ctx,
		`SELECT `+attackColumns+` FROM attacks WHERE id = $1`, id))
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get attack: %w", err)
	}
	return a, nil
}

// ListAttacks returns all attack definitions, newest first.
func (r *WorkloadRepo) ListAttacks(ctx context.Context) ([]*model.Attack, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+attackColumns+` FROM attacks ORDER BY created_at DESC, id`)
	if err != nil {
		return nil, fmt.Errorf("list attacks: %w", err)
	}
	defer rows.Close()

	var out []*model.Attack
	for rows.Next() {
		a, err := scanAttack(rows)
		if err != nil {
			return nil, fmt.Errorf("scan attack row: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeleteAttack removes an attack definition; its execution records cascade.
func (r *WorkloadRepo) DeleteAttack(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM attacks WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete attack: %w", err)
	}
	return affectedOrNotFound(res)
}

func scanAttack(s interface{ Scan(...any) error }) (*model.Attack, error) {
	var a model.Attack
	var dedup sql.NullString
	var headersJSON []byte
	err := s.Scan(
		&a.ID, &a.Name, &a.Description, &a.Service, &a.TargetURL, &a.TargetMethod,
		&headersJSON, &a.Rate, &a.DurationMs, &dedup, &a.CreatedAt, &a.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	a.DedupBypass = fromNullString(dedup)
	if err := jsonbScan(headersJSON, &a.TargetHeaders); err != nil {
		return nil, fmt.Errorf("unmarshal target_headers: %w", err)
	}
	return &a, nil
}

// --- Attack execution records ---

const attackResultColumns = `id, attack_id, phase_id, zeus_attack_id, meta_trace_id, service,
	total_requests, duration_ms, rate_actual, success_rate, status_codes,
	latency_p50_us, latency_p90_us, latency_p95_us, latency_p99_us,
	latency_min_us, latency_max_us, bytes_in_total, bytes_out_total, errors,
	started_at, completed_at`

// CreateAttackResult inserts one execution record for an attack definition.
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
		INSERT INTO attack_results (id, attack_id, phase_id, zeus_attack_id, meta_trace_id, service,
			total_requests, duration_ms, rate_actual, success_rate, status_codes,
			latency_p50_us, latency_p90_us, latency_p95_us, latency_p99_us,
			latency_min_us, latency_max_us, bytes_in_total, bytes_out_total, errors,
			started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`,
		res.ID, res.AttackID, res.PhaseID, nullString(res.ZeusAttackID), nullString(res.MetaTraceID), res.Service,
		res.TotalRequests, res.DurationMs, res.RateActual, res.SuccessRate, statusCodesJSON,
		res.LatencyP50Us, res.LatencyP90Us, res.LatencyP95Us, res.LatencyP99Us,
		res.LatencyMinUs, res.LatencyMaxUs, res.BytesInTotal, res.BytesOutTotal, errorsJSON,
		res.StartedAt, res.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert attack_result: %w", err)
	}
	return nil
}

// ListResultsForAttack returns all execution records for a definition,
// newest first.
func (r *WorkloadRepo) ListResultsForAttack(ctx context.Context, attackID string) ([]*model.AttackResult, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT `+attackResultColumns+` FROM attack_results
		WHERE attack_id = $1 ORDER BY completed_at DESC NULLS LAST, id`, attackID)
	if err != nil {
		return nil, fmt.Errorf("list results for attack: %w", err)
	}
	return scanAttackResults(rows)
}

// ListResultsForPhase returns the execution records tagged with a phase.
func (r *WorkloadRepo) ListResultsForPhase(ctx context.Context, phaseID string) ([]*model.AttackResult, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT `+attackResultColumns+` FROM attack_results
		WHERE phase_id = $1 ORDER BY completed_at DESC NULLS LAST, id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list results for phase: %w", err)
	}
	return scanAttackResults(rows)
}

func scanAttackResults(rows *sql.Rows) ([]*model.AttackResult, error) {
	defer rows.Close()
	var out []*model.AttackResult
	for rows.Next() {
		var res model.AttackResult
		var zeusID, metaTrace sql.NullString
		var statusCodesJSON, errorsJSON []byte
		err := rows.Scan(
			&res.ID, &res.AttackID, &res.PhaseID, &zeusID, &metaTrace, &res.Service,
			&res.TotalRequests, &res.DurationMs, &res.RateActual, &res.SuccessRate, &statusCodesJSON,
			&res.LatencyP50Us, &res.LatencyP90Us, &res.LatencyP95Us, &res.LatencyP99Us,
			&res.LatencyMinUs, &res.LatencyMaxUs, &res.BytesInTotal, &res.BytesOutTotal, &errorsJSON,
			&res.StartedAt, &res.CompletedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan attack_result row: %w", err)
		}
		res.ZeusAttackID = fromNullString(zeusID)
		res.MetaTraceID = fromNullString(metaTrace)
		if err := jsonbScan(statusCodesJSON, &res.StatusCodes); err != nil {
			return nil, fmt.Errorf("unmarshal status_codes: %w", err)
		}
		if err := jsonbScan(errorsJSON, &res.Errors); err != nil {
			return nil, fmt.Errorf("unmarshal errors: %w", err)
		}
		out = append(out, &res)
	}
	return out, rows.Err()
}
