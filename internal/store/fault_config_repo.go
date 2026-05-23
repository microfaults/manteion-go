package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"manteion-go/internal/model"
)

// FaultConfigRepo persists long-running manual fault configs.
type FaultConfigRepo struct {
	db *sql.DB
}

func NewFaultConfigRepo(db *sql.DB) *FaultConfigRepo {
	return &FaultConfigRepo{db: db}
}

const faultConfigColumns = `id, name, description, service, category, fault_type,
	fault_request, fault_composition_id, duration_ms, experiment_run_id,
	status, created_at, updated_at, fired_at, completed_at`

func scanFaultConfig(s interface{ Scan(...any) error }) (*model.FaultConfig, error) {
	var f model.FaultConfig
	err := s.Scan(
		&f.ID, &f.Name, &f.Description, &f.Service, &f.Category, &f.FaultType,
		&f.FaultReq, &f.FaultCompositionID, &f.DurationMs, &f.ExperimentRunID,
		&f.Status, &f.CreatedAt, &f.UpdatedAt, &f.FiredAt, &f.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	return &f, nil
}

func scanFaultConfigs(rows *sql.Rows) ([]*model.FaultConfig, error) {
	defer rows.Close()
	var out []*model.FaultConfig
	for rows.Next() {
		f, err := scanFaultConfig(rows)
		if err != nil {
			return nil, fmt.Errorf("scan fault config: %w", err)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (r *FaultConfigRepo) Create(ctx context.Context, f *model.FaultConfig) error {
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO fault_configs (
			id, name, description, service, category, fault_type,
			fault_request, fault_composition_id, duration_ms, experiment_run_id,
			status, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now(), now())
		RETURNING created_at, updated_at`,
		f.ID, f.Name, f.Description, f.Service, f.Category, f.FaultType,
		f.FaultReq, f.FaultCompositionID, f.DurationMs, f.ExperimentRunID, string(f.Status),
	).Scan(&f.CreatedAt, &f.UpdatedAt)
	if err != nil {
		return fmt.Errorf("create fault config: %w", err)
	}
	return nil
}

func (r *FaultConfigRepo) Get(ctx context.Context, id string) (*model.FaultConfig, error) {
	f, err := scanFaultConfig(r.db.QueryRowContext(ctx,
		`SELECT `+faultConfigColumns+` FROM fault_configs WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get fault config: %w", err)
	}
	return f, nil
}

// List returns fault configs filtered by optional service and status (empty
// string = no filter on that field), newest first.
func (r *FaultConfigRepo) List(ctx context.Context, service string, status model.FaultConfigStatus) ([]*model.FaultConfig, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT `+faultConfigColumns+` FROM fault_configs
		WHERE ($1 = '' OR service = $1)
		  AND ($2 = '' OR status = $2)
		ORDER BY created_at DESC`,
		service, string(status),
	)
	if err != nil {
		return nil, fmt.Errorf("list fault configs: %w", err)
	}
	return scanFaultConfigs(rows)
}

// ListActiveForService returns the faults that should currently be applied to a
// service: active and not past their duration. This is the poll-response set the
// SDK reconciles against.
func (r *FaultConfigRepo) ListActiveForService(ctx context.Context, service string) ([]*model.FaultConfig, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT `+faultConfigColumns+` FROM fault_configs
		WHERE service = $1 AND status = 'active'
		  AND (duration_ms = 0 OR fired_at + (duration_ms * INTERVAL '1 millisecond') > now())
		ORDER BY created_at`,
		service,
	)
	if err != nil {
		return nil, fmt.Errorf("list active fault configs: %w", err)
	}
	return scanFaultConfigs(rows)
}

// ListExpired returns active faults whose finite duration has elapsed, for the
// reaper to mark completed.
func (r *FaultConfigRepo) ListExpired(ctx context.Context) ([]*model.FaultConfig, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT `+faultConfigColumns+` FROM fault_configs
		WHERE status = 'active' AND duration_ms > 0
		  AND fired_at + (duration_ms * INTERVAL '1 millisecond') <= now()`,
	)
	if err != nil {
		return nil, fmt.Errorf("list expired fault configs: %w", err)
	}
	return scanFaultConfigs(rows)
}

func (r *FaultConfigRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM fault_configs WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete fault config: %w", err)
	}
	return affectedOrNotFound(res)
}

func (r *FaultConfigRepo) MarkFired(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE fault_configs SET status = 'active', fired_at = now(), updated_at = now()
		WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark fired: %w", err)
	}
	return affectedOrNotFound(res)
}

func (r *FaultConfigRepo) MarkCompleted(ctx context.Context, id string) error {
	return r.markTerminal(ctx, id, model.FaultConfigCompleted)
}

func (r *FaultConfigRepo) MarkCancelled(ctx context.Context, id string) error {
	return r.markTerminal(ctx, id, model.FaultConfigCancelled)
}

func (r *FaultConfigRepo) markTerminal(ctx context.Context, id string, status model.FaultConfigStatus) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE fault_configs SET status = $1, completed_at = now(), updated_at = now()
		WHERE id = $2`, string(status), id)
	if err != nil {
		return fmt.Errorf("mark %s: %w", status, err)
	}
	return affectedOrNotFound(res)
}
