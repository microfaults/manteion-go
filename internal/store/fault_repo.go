package store

import (
	"context"
	"database/sql"
	"fmt"

	"manteion-go/internal/model"
)

// FaultRepo provides CRUD for FaultSpecs and FaultCompositions.
type FaultRepo struct {
	db *sql.DB
}

// NewFaultRepo creates a new fault repository.
func NewFaultRepo(db *sql.DB) *FaultRepo {
	return &FaultRepo{db: db}
}

// --- FaultSpec ---

// CreateSpec inserts a new atomic fault specification.
func (r *FaultRepo) CreateSpec(ctx context.Context, spec *model.FaultSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}

	var target, direction sql.NullString
	var scope sql.NullFloat64
	if spec.Network != nil {
		if spec.Network.Target != "" {
			target = sql.NullString{String: spec.Network.Target, Valid: true}
		}
		if spec.Network.Direction != "" {
			direction = sql.NullString{String: spec.Network.Direction, Valid: true}
		}
		if spec.Network.Scope > 0 {
			scope = sql.NullFloat64{Float64: spec.Network.Scope, Valid: true}
		}
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO fault_specs (
			id, name, category, fault_type, host, params,
			network_target, network_direction, network_scope,
			duration_ms, ramp_up_ms, ramp_down_ms, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		spec.ID, spec.Name, spec.Category, spec.FaultType,
		nullString(spec.Host), spec.Params,
		target, direction, scope,
		spec.DurationMs, spec.RampUpMs, spec.RampDownMs, spec.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert fault_spec: %w", err)
	}
	return nil
}

// GetSpec returns a fault spec by ID, or ErrNotFound.
func (r *FaultRepo) GetSpec(ctx context.Context, id string) (*model.FaultSpec, error) {
	var spec model.FaultSpec
	var host, target, direction sql.NullString
	var scope sql.NullFloat64
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, category, fault_type, host, params,
			network_target, network_direction, network_scope,
			duration_ms, ramp_up_ms, ramp_down_ms, created_at
		FROM fault_specs WHERE id = $1`, id,
	).Scan(
		&spec.ID, &spec.Name, &spec.Category, &spec.FaultType, &host, &spec.Params,
		&target, &direction, &scope,
		&spec.DurationMs, &spec.RampUpMs, &spec.RampDownMs, &spec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get fault_spec: %w", err)
	}
	spec.Host = fromNullString(host)
	hydrateNetworkEnvelope(&spec, target, direction, scope)
	return &spec, nil
}

// ListSpecs returns all fault specifications.
func (r *FaultRepo) ListSpecs(ctx context.Context) ([]*model.FaultSpec, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, category, fault_type, host, params,
			network_target, network_direction, network_scope,
			duration_ms, ramp_up_ms, ramp_down_ms, created_at
		FROM fault_specs ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list fault_specs: %w", err)
	}
	defer rows.Close()

	var result []*model.FaultSpec
	for rows.Next() {
		var spec model.FaultSpec
		var host, target, direction sql.NullString
		var scope sql.NullFloat64
		err := rows.Scan(
			&spec.ID, &spec.Name, &spec.Category, &spec.FaultType, &host, &spec.Params,
			&target, &direction, &scope,
			&spec.DurationMs, &spec.RampUpMs, &spec.RampDownMs, &spec.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan fault_spec: %w", err)
		}
		spec.Host = fromNullString(host)
		hydrateNetworkEnvelope(&spec, target, direction, scope)
		result = append(result, &spec)
	}
	return result, rows.Err()
}

// hydrateNetworkEnvelope populates spec.Network from nullable columns iff
// any envelope field is set. Keeps Network=nil for non-network specs so
// JSON omits the field instead of emitting "network":{}.
func hydrateNetworkEnvelope(spec *model.FaultSpec, target, direction sql.NullString, scope sql.NullFloat64) {
	if !target.Valid && !direction.Valid && !scope.Valid {
		return
	}
	env := &model.NetworkEnvelope{}
	if target.Valid {
		env.Target = target.String
	}
	if direction.Valid {
		env.Direction = direction.String
	}
	if scope.Valid {
		env.Scope = scope.Float64
	}
	spec.Network = env
}

// DeleteSpec removes a fault spec by ID. Fails if referenced by a rule.
func (r *FaultRepo) DeleteSpec(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM fault_specs WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete fault_spec: %w", err)
	}
	return affectedOrNotFound(res)
}

// --- FaultComposition ---

// CreateComposition inserts a composition and all its members in a single transaction.
func (r *FaultRepo) CreateComposition(ctx context.Context, comp *model.FaultComposition) error {
	if err := comp.Validate(); err != nil {
		return err
	}

	return execTx(ctx, r.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO fault_compositions (id, name, execution_mode, duration_ms, ramp_up_ms, ramp_down_ms, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			comp.ID, comp.Name, comp.ExecutionMode,
			comp.DurationMs, comp.RampUpMs, comp.RampDownMs,
			comp.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("insert fault_composition: %w", err)
		}

		for i, m := range comp.Members {
			_, err := tx.ExecContext(ctx, `
				INSERT INTO fault_composition_members
					(composition_id, position, fault_spec_id, child_composition_id, direction)
				VALUES ($1, $2, $3, $4, $5)`,
				comp.ID, i,
				nullString(m.FaultSpecID), nullString(m.ChildCompositionID),
				nullString(string(m.Direction)),
			)
			if err != nil {
				return fmt.Errorf("insert composition member[%d]: %w", i, err)
			}
		}

		return nil
	})
}

// GetComposition returns a composition with its members, or ErrNotFound.
func (r *FaultRepo) GetComposition(ctx context.Context, id string) (*model.FaultComposition, error) {
	var comp model.FaultComposition
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, execution_mode, duration_ms, ramp_up_ms, ramp_down_ms, created_at
		FROM fault_compositions WHERE id = $1`, id,
	).Scan(&comp.ID, &comp.Name, &comp.ExecutionMode,
		&comp.DurationMs, &comp.RampUpMs, &comp.RampDownMs,
		&comp.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get fault_composition: %w", err)
	}

	// Load members.
	rows, err := r.db.QueryContext(ctx, `
		SELECT position, fault_spec_id, child_composition_id, direction
		FROM fault_composition_members
		WHERE composition_id = $1 ORDER BY position`, id)
	if err != nil {
		return nil, fmt.Errorf("get composition members: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var m model.FaultCompositionMember
		var pos int
		var faultSpecID, childCompID, direction sql.NullString
		if err := rows.Scan(&pos, &faultSpecID, &childCompID, &direction); err != nil {
			return nil, fmt.Errorf("scan composition member: %w", err)
		}
		_ = pos // slice order = insert order via ORDER BY position
		m.FaultSpecID = fromNullString(faultSpecID)
		m.ChildCompositionID = fromNullString(childCompID)
		m.Direction = model.Direction(fromNullString(direction))
		comp.Members = append(comp.Members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &comp, nil
}

// ListCompositions returns all compositions (without members loaded).
func (r *FaultRepo) ListCompositions(ctx context.Context) ([]*model.FaultComposition, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, execution_mode, duration_ms, ramp_up_ms, ramp_down_ms, created_at
		FROM fault_compositions ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list fault_compositions: %w", err)
	}
	defer rows.Close()

	var result []*model.FaultComposition
	for rows.Next() {
		var comp model.FaultComposition
		if err := rows.Scan(&comp.ID, &comp.Name, &comp.ExecutionMode,
			&comp.DurationMs, &comp.RampUpMs, &comp.RampDownMs,
			&comp.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan fault_composition: %w", err)
		}
		result = append(result, &comp)
	}
	return result, rows.Err()
}

// DeleteComposition removes a composition and its members (via CASCADE).
func (r *FaultRepo) DeleteComposition(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM fault_compositions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete fault_composition: %w", err)
	}
	return affectedOrNotFound(res)
}

// SpecResolver returns a FaultSpecResolver backed by this repository.
func (r *FaultRepo) SpecResolver(ctx context.Context) model.FaultSpecResolver {
	return func(id string) *model.FaultSpec {
		spec, err := r.GetSpec(ctx, id)
		if err != nil {
			return nil
		}
		return spec
	}
}

// CompositionResolver returns a CompositionResolver backed by this repository.
func (r *FaultRepo) CompositionResolver(ctx context.Context) model.CompositionResolver {
	return func(id string) *model.FaultComposition {
		comp, err := r.GetComposition(ctx, id)
		if err != nil {
			return nil
		}
		return comp
	}
}
