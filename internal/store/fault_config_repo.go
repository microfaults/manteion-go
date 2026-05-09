package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"manteion-go/internal/model"
)

var ErrConflict = errors.New("store: conflict")

type ListFaultConfigFilters struct {
	Service         string
	Status          model.FaultConfigStatus
	ExperimentRunID *string // nil = no filter; "" = manual only (NULL); "x" = run-x only
}

type FaultConfigRepo struct {
	db *sql.DB
}

func NewFaultConfigRepo(db *sql.DB) *FaultConfigRepo {
	return &FaultConfigRepo{db: db}
}

func (r *FaultConfigRepo) Create(ctx context.Context, f *model.FaultConfig) error {
	const query = `
		INSERT INTO fault_configs (
			id, name, description, service, category, fault_type,
			fault_request, fault_composition_id, duration_ms,
			experiment_run_id, status, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), now()
		)
		RETURNING created_at, updated_at
	`
	err := r.db.QueryRowContext(ctx, query,
		f.ID, f.Name, f.Description, f.Service, f.Category, f.FaultType,
		f.FaultReq, f.FaultCompositionID, f.DurationMs,
		f.ExperimentRunID, f.Status,
	).Scan(&f.CreatedAt, &f.UpdatedAt)

	if err != nil {
		return fmt.Errorf("create fault config: %w", err)
	}
	return nil
}

func (r *FaultConfigRepo) Get(ctx context.Context, id string) (*model.FaultConfig, error) {
	const query = `
		SELECT id, name, description, service, category, fault_type,
			fault_request, fault_composition_id, duration_ms,
			experiment_run_id, status, created_at, updated_at,
			fired_at, completed_at
		FROM fault_configs
		WHERE id = $1
	`
	var f model.FaultConfig
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&f.ID, &f.Name, &f.Description, &f.Service, &f.Category, &f.FaultType,
		&f.FaultReq, &f.FaultCompositionID, &f.DurationMs,
		&f.ExperimentRunID, &f.Status, &f.CreatedAt, &f.UpdatedAt,
		&f.FiredAt, &f.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get fault config: %w", err)
	}
	return &f, nil
}

func (r *FaultConfigRepo) List(ctx context.Context, filter ListFaultConfigFilters) ([]*model.FaultConfig, error) {
	var where []string
	var args []any
	argID := 1

	if filter.Service != "" {
		where = append(where, fmt.Sprintf("service = $%d", argID))
		args = append(args, filter.Service)
		argID++
	}
	if filter.Status != "" {
		where = append(where, fmt.Sprintf("status = $%d", argID))
		args = append(args, filter.Status)
		argID++
	}
	if filter.ExperimentRunID != nil {
		if *filter.ExperimentRunID == "" {
			where = append(where, "experiment_run_id IS NULL")
		} else {
			where = append(where, fmt.Sprintf("experiment_run_id = $%d", argID))
			args = append(args, *filter.ExperimentRunID)
			argID++
		}
	}

	query := `
		SELECT id, name, description, service, category, fault_type,
			fault_request, fault_composition_id, duration_ms,
			experiment_run_id, status, created_at, updated_at,
			fired_at, completed_at
		FROM fault_configs
	`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY created_at DESC"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list fault configs: %w", err)
	}
	defer rows.Close()

	var results []*model.FaultConfig
	for rows.Next() {
		var f model.FaultConfig
		if err := rows.Scan(
			&f.ID, &f.Name, &f.Description, &f.Service, &f.Category, &f.FaultType,
			&f.FaultReq, &f.FaultCompositionID, &f.DurationMs,
			&f.ExperimentRunID, &f.Status, &f.CreatedAt, &f.UpdatedAt,
			&f.FiredAt, &f.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan fault config: %w", err)
		}
		results = append(results, &f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate fault configs: %w", err)
	}
	return results, nil
}

func (r *FaultConfigRepo) Update(ctx context.Context, f *model.FaultConfig) error {
	const query = `
		UPDATE fault_configs
		SET name = $1, description = $2, duration_ms = $3,
			fault_request = $4, fault_composition_id = $5,
			updated_at = now()
		WHERE id = $6 AND status != 'active'
		RETURNING updated_at
	`
	err := r.db.QueryRowContext(ctx, query,
		f.Name, f.Description, f.DurationMs,
		f.FaultReq, f.FaultCompositionID, f.ID,
	).Scan(&f.UpdatedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Check if it exists but is active
			exists, checkErr := r.checkExists(ctx, f.ID)
			if checkErr != nil {
				return checkErr
			}
			if exists {
				return ErrConflict
			}
			return ErrNotFound
		}
		return fmt.Errorf("update fault config: %w", err)
	}
	return nil
}

func (r *FaultConfigRepo) checkExists(ctx context.Context, id string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fault_configs WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check exists: %w", err)
	}
	return exists, nil
}

func (r *FaultConfigRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM fault_configs WHERE id = $1 AND status != 'active'`, id)
	if err != nil {
		return fmt.Errorf("delete fault config: %w", err)
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		exists, err := r.checkExists(ctx, id)
		if err != nil {
			return err
		}
		if exists {
			return ErrConflict
		}
		return ErrNotFound
	}
	return nil
}

func (r *FaultConfigRepo) MarkFired(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE fault_configs
		SET status = 'active', fired_at = now(), updated_at = now()
		WHERE id = $1 AND status != 'active'
	`, id)
	if err != nil {
		return fmt.Errorf("mark fired: %w", err)
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		exists, err := r.checkExists(ctx, id)
		if err != nil {
			return err
		}
		if exists {
			return ErrConflict
		}
		return ErrNotFound
	}
	return nil
}

func (r *FaultConfigRepo) markStatus(ctx context.Context, id string, status model.FaultConfigStatus, setCompletedAt bool) error {
	query := `UPDATE fault_configs SET status = $1, updated_at = now()`
	if setCompletedAt {
		query += `, completed_at = now()`
	}
	query += ` WHERE id = $2`
	_, err := r.db.ExecContext(ctx, query, status, id)
	if err != nil {
		return fmt.Errorf("mark status %s: %w", status, err)
	}
	return nil
}

func (r *FaultConfigRepo) MarkCompleted(ctx context.Context, id string) error {
	return r.markStatus(ctx, id, model.FaultConfigCompleted, true)
}

func (r *FaultConfigRepo) MarkManuallyCancelled(ctx context.Context, id string) error {
	return r.markStatus(ctx, id, model.FaultConfigManuallyCancelled, true)
}

func (r *FaultConfigRepo) MarkFailed(ctx context.Context, id string) error {
	return r.markStatus(ctx, id, model.FaultConfigFailed, false)
}

func (r *FaultConfigRepo) HasActiveConflict(ctx context.Context, service, category, excludeID string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM fault_configs
			WHERE service = $1 AND category = $2 AND status = 'active' AND id != $3
		)
	`, service, category, excludeID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check active conflict: %w", err)
	}
	return exists, nil
}

func (r *FaultConfigRepo) ListExpired(ctx context.Context) ([]*model.FaultConfig, error) {
	const query = `
		SELECT id, name, description, service, category, fault_type,
			fault_request, fault_composition_id, duration_ms,
			experiment_run_id, status, created_at, updated_at,
			fired_at, completed_at
		FROM fault_configs
		WHERE status = 'active'
		  AND duration_ms > 0
		  AND fired_at + (duration_ms * INTERVAL '1 millisecond') <= now()
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list expired fault configs: %w", err)
	}
	defer rows.Close()

	var results []*model.FaultConfig
	for rows.Next() {
		var f model.FaultConfig
		if err := rows.Scan(
			&f.ID, &f.Name, &f.Description, &f.Service, &f.Category, &f.FaultType,
			&f.FaultReq, &f.FaultCompositionID, &f.DurationMs,
			&f.ExperimentRunID, &f.Status, &f.CreatedAt, &f.UpdatedAt,
			&f.FiredAt, &f.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan expired fault config: %w", err)
		}
		results = append(results, &f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired fault configs: %w", err)
	}
	return results, nil
}
