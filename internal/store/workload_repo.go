package store

import (
	"context"
	"database/sql"
	"fmt"

	"manteion-go/internal/model"
)

// WorkloadRepo provides persistence for flows, personas, workloads, attacks,
// and attack results.
type WorkloadRepo struct {
	db *sql.DB
}

// NewWorkloadRepo creates a new workload repository.
func NewWorkloadRepo(db *sql.DB) *WorkloadRepo {
	return &WorkloadRepo{db: db}
}

// --- Flow ---

// CreateFlow inserts a new k6 flow definition.
func (r *WorkloadRepo) CreateFlow(ctx context.Context, flow *model.Flow) error {
	if err := flow.Validate(); err != nil {
		return err
	}

	targetsJSON, err := jsonbMarshal(flow.Targets)
	if err != nil {
		return fmt.Errorf("marshal targets: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO flows (id, name, description, targets, estimated_rps_per_vu,
			steps, thresholds, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		flow.ID, flow.Name, nullString(flow.Description), targetsJSON,
		flow.EstimatedRPSPerVU, flow.Steps, flow.Thresholds, flow.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert flow: %w", err)
	}
	return nil
}

// GetFlow returns a flow by ID, or ErrNotFound.
func (r *WorkloadRepo) GetFlow(ctx context.Context, id string) (*model.Flow, error) {
	var flow model.Flow
	var desc sql.NullString
	var targetsJSON []byte

	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, description, targets, estimated_rps_per_vu,
			steps, thresholds, created_at
		FROM flows WHERE id = $1`, id,
	).Scan(
		&flow.ID, &flow.Name, &desc, &targetsJSON, &flow.EstimatedRPSPerVU,
		&flow.Steps, &flow.Thresholds, &flow.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get flow: %w", err)
	}
	flow.Description = fromNullString(desc)
	if err := jsonbScan(targetsJSON, &flow.Targets); err != nil {
		return nil, fmt.Errorf("unmarshal targets: %w", err)
	}
	return &flow, nil
}

// ListFlows returns all flows.
func (r *WorkloadRepo) ListFlows(ctx context.Context) ([]*model.Flow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, description, targets, estimated_rps_per_vu,
			steps, thresholds, created_at
		FROM flows ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list flows: %w", err)
	}
	defer rows.Close()

	var result []*model.Flow
	for rows.Next() {
		var flow model.Flow
		var desc sql.NullString
		var targetsJSON []byte
		if err := rows.Scan(
			&flow.ID, &flow.Name, &desc, &targetsJSON, &flow.EstimatedRPSPerVU,
			&flow.Steps, &flow.Thresholds, &flow.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan flow: %w", err)
		}
		flow.Description = fromNullString(desc)
		if err := jsonbScan(targetsJSON, &flow.Targets); err != nil {
			return nil, err
		}
		result = append(result, &flow)
	}
	return result, rows.Err()
}

// --- Persona ---

// CreatePersona inserts a new behavioral profile.
func (r *WorkloadRepo) CreatePersona(ctx context.Context, p *model.Persona) error {
	if err := p.Validate(); err != nil {
		return err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO personas (id, name, description,
			explore_prob, engage_prob, commit_prob, repeat_prob,
			think_time_min, think_time_max)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		p.ID, p.Name, nullString(p.Description),
		p.ExploreProb, p.EngageProb, p.CommitProb, p.RepeatProb,
		p.ThinkTimeMin, p.ThinkTimeMax,
	)
	if err != nil {
		return fmt.Errorf("insert persona: %w", err)
	}
	return nil
}

// GetPersona returns a persona by ID, or ErrNotFound.
func (r *WorkloadRepo) GetPersona(ctx context.Context, id string) (*model.Persona, error) {
	var p model.Persona
	var desc sql.NullString
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, description,
			explore_prob, engage_prob, commit_prob, repeat_prob,
			think_time_min, think_time_max
		FROM personas WHERE id = $1`, id,
	).Scan(
		&p.ID, &p.Name, &desc,
		&p.ExploreProb, &p.EngageProb, &p.CommitProb, &p.RepeatProb,
		&p.ThinkTimeMin, &p.ThinkTimeMax,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get persona: %w", err)
	}
	p.Description = fromNullString(desc)
	return &p, nil
}

// ListPersonas returns all personas.
func (r *WorkloadRepo) ListPersonas(ctx context.Context) ([]*model.Persona, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, description,
			explore_prob, engage_prob, commit_prob, repeat_prob,
			think_time_min, think_time_max
		FROM personas ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list personas: %w", err)
	}
	defer rows.Close()

	var result []*model.Persona
	for rows.Next() {
		var p model.Persona
		var desc sql.NullString
		if err := rows.Scan(
			&p.ID, &p.Name, &desc,
			&p.ExploreProb, &p.EngageProb, &p.CommitProb, &p.RepeatProb,
			&p.ThinkTimeMin, &p.ThinkTimeMax,
		); err != nil {
			return nil, fmt.Errorf("scan persona: %w", err)
		}
		p.Description = fromNullString(desc)
		result = append(result, &p)
	}
	return result, rows.Err()
}

// --- Workload ---

// CreateWorkload inserts a new k6 workload session.
func (r *WorkloadRepo) CreateWorkload(ctx context.Context, w *model.Workload) error {
	if err := w.Validate(); err != nil {
		return err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workloads (id, name, flow_id, persona_id, vus, rate,
			meta_trace_id, status, started_at, completed_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		w.ID, w.Name, w.FlowID, w.PersonaID, w.VUs, w.Rate,
		nullString(w.MetaTraceID), w.Status, w.StartedAt, w.CompletedAt, w.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert workload: %w", err)
	}
	return nil
}

// GetWorkload returns a workload by ID, or ErrNotFound.
func (r *WorkloadRepo) GetWorkload(ctx context.Context, id string) (*model.Workload, error) {
	var w model.Workload
	var metaTraceID sql.NullString
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, flow_id, persona_id, vus, rate,
			meta_trace_id, status, started_at, completed_at, created_at
		FROM workloads WHERE id = $1`, id,
	).Scan(
		&w.ID, &w.Name, &w.FlowID, &w.PersonaID, &w.VUs, &w.Rate,
		&metaTraceID, &w.Status, &w.StartedAt, &w.CompletedAt, &w.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get workload: %w", err)
	}
	w.MetaTraceID = fromNullString(metaTraceID)
	return &w, nil
}

// ListWorkloads returns all workloads ordered by creation time.
func (r *WorkloadRepo) ListWorkloads(ctx context.Context) ([]*model.Workload, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, flow_id, persona_id, vus, rate,
			meta_trace_id, status, started_at, completed_at, created_at
		FROM workloads ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list workloads: %w", err)
	}
	defer rows.Close()

	var result []*model.Workload
	for rows.Next() {
		var w model.Workload
		var metaTraceID sql.NullString
		if err := rows.Scan(
			&w.ID, &w.Name, &w.FlowID, &w.PersonaID, &w.VUs, &w.Rate,
			&metaTraceID, &w.Status, &w.StartedAt, &w.CompletedAt, &w.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workload: %w", err)
		}
		w.MetaTraceID = fromNullString(metaTraceID)
		result = append(result, &w)
	}
	return result, rows.Err()
}

// UpdateWorkloadStatus changes the status of a workload.
func (r *WorkloadRepo) UpdateWorkloadStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE workloads SET status = $2 WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update workload status: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
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
		INSERT INTO attacks (id, workload_id, experiment_run_id, auto_rule_id,
			service, role, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, status,
			started_at, completed_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
		a.ID, nullString(a.WorkloadID), nullString(a.ExperimentRunID), nullString(a.AutoRuleID),
		a.Service, a.Role, a.TargetURL, a.TargetMethod, headersJSON,
		a.Rate, a.DurationMs, nullString(a.DedupBypass), nullString(a.MetaTraceID), a.Status,
		a.StartedAt, a.CompletedAt, a.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert attack: %w", err)
	}
	return nil
}

// GetAttack returns an attack by ID, or ErrNotFound.
func (r *WorkloadRepo) GetAttack(ctx context.Context, id string) (*model.Attack, error) {
	var a model.Attack
	var workloadID, expRunID, autoRuleID, dedup, metaTrace sql.NullString
	var headersJSON []byte

	err := r.db.QueryRowContext(ctx, `
		SELECT id, workload_id, experiment_run_id, auto_rule_id,
			service, role, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, status,
			started_at, completed_at, created_at
		FROM attacks WHERE id = $1`, id,
	).Scan(
		&a.ID, &workloadID, &expRunID, &autoRuleID,
		&a.Service, &a.Role, &a.TargetURL, &a.TargetMethod, &headersJSON,
		&a.Rate, &a.DurationMs, &dedup, &metaTrace, &a.Status,
		&a.StartedAt, &a.CompletedAt, &a.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get attack: %w", err)
	}
	a.WorkloadID = fromNullString(workloadID)
	a.ExperimentRunID = fromNullString(expRunID)
	a.AutoRuleID = fromNullString(autoRuleID)
	a.DedupBypass = fromNullString(dedup)
	a.MetaTraceID = fromNullString(metaTrace)
	if err := jsonbScan(headersJSON, &a.TargetHeaders); err != nil {
		return nil, fmt.Errorf("unmarshal target_headers: %w", err)
	}
	return &a, nil
}

// ListAttacksByWorkload returns all attacks for a workload.
func (r *WorkloadRepo) ListAttacksByWorkload(ctx context.Context, workloadID string) ([]*model.Attack, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, workload_id, experiment_run_id, auto_rule_id,
			service, role, target_url, target_method, target_headers,
			rate, duration_ms, dedup_bypass, meta_trace_id, status,
			started_at, completed_at, created_at
		FROM attacks WHERE workload_id = $1
		ORDER BY created_at`, workloadID)
	if err != nil {
		return nil, fmt.Errorf("list attacks by workload: %w", err)
	}
	defer rows.Close()

	var result []*model.Attack
	for rows.Next() {
		var a model.Attack
		var workloadIDN, expRunID, autoRuleID sql.NullString
		var dedup, metaTrace sql.NullString
		var headersJSON []byte

		err := rows.Scan(
			&a.ID, &workloadIDN, &expRunID, &autoRuleID,
			&a.Service, &a.Role, &a.TargetURL, &a.TargetMethod, &headersJSON,
			&a.Rate, &a.DurationMs, &dedup, &metaTrace, &a.Status,
			&a.StartedAt, &a.CompletedAt, &a.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan attack: %w", err)
		}
		a.WorkloadID = fromNullString(workloadIDN)
		a.ExperimentRunID = fromNullString(expRunID)
		a.AutoRuleID = fromNullString(autoRuleID)
		a.DedupBypass = fromNullString(dedup)
		a.MetaTraceID = fromNullString(metaTrace)
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
