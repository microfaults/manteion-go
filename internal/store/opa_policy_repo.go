package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"manteion-go/internal/model"
)

// OPAPolicyRepo persists OPA harness policies (opa_policies, migration 15).
// Names are unique: a write that collides on one returns ErrConflict.
type OPAPolicyRepo struct {
	db *sql.DB
}

// NewOPAPolicyRepo returns a repository over db.
func NewOPAPolicyRepo(db *sql.DB) *OPAPolicyRepo {
	return &OPAPolicyRepo{db: db}
}

// opaPolicyColumns is the column list every SELECT reads; scanOPAPolicy
// scans in this order.
const opaPolicyColumns = `id, name, enabled, rego, interval_s, cooldown_s, inputs, scope, created_at, updated_at`

// Create validates p (applying its defaults in place) and inserts it.
// A name already in use returns ErrConflict.
func (r *OPAPolicyRepo) Create(ctx context.Context, p *model.OPAPolicy) error {
	inputs, scope, err := marshalOPAPolicy(p)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO opa_policies (`+opaPolicyColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		p.ID, p.Name, p.Enabled, p.Rego, p.IntervalS, p.CooldownS, inputs, scope,
		p.CreatedAt, p.UpdatedAt,
	)
	if err != nil {
		if code, _ := pgViolation(err); code == pgUniqueViolation {
			return fmt.Errorf("opa policy name %q already exists: %w", p.Name, ErrConflict)
		}
		return fmt.Errorf("insert opa policy: %w", err)
	}
	return nil
}

// Get returns one policy by id, or ErrNotFound.
func (r *OPAPolicyRepo) Get(ctx context.Context, id string) (*model.OPAPolicy, error) {
	p, err := scanOPAPolicy(r.db.QueryRowContext(ctx,
		`SELECT `+opaPolicyColumns+` FROM opa_policies WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get opa policy %s: %w", id, err)
	}
	return p, nil
}

// Update validates p (applying its defaults in place) and replaces the stored
// policy with id p.ID, setting updated_at to now(); created_at is kept.
// ErrNotFound when no such policy exists, ErrConflict when the new name is
// taken by another policy.
func (r *OPAPolicyRepo) Update(ctx context.Context, p *model.OPAPolicy) error {
	inputs, scope, err := marshalOPAPolicy(p)
	if err != nil {
		return err
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE opa_policies
		SET name = $2, enabled = $3, rego = $4, interval_s = $5, cooldown_s = $6,
			inputs = $7, scope = $8, updated_at = now()
		WHERE id = $1`,
		p.ID, p.Name, p.Enabled, p.Rego, p.IntervalS, p.CooldownS, inputs, scope,
	)
	if err != nil {
		if code, _ := pgViolation(err); code == pgUniqueViolation {
			return fmt.Errorf("opa policy name %q already exists: %w", p.Name, ErrConflict)
		}
		return fmt.Errorf("update opa policy %s: %w", p.ID, err)
	}
	return affectedOrNotFound(res)
}

// Delete removes a policy by id; its decision log goes with it (ON DELETE
// CASCADE). ErrNotFound when no such policy exists.
func (r *OPAPolicyRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM opa_policies WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete opa policy %s: %w", id, err)
	}
	return affectedOrNotFound(res)
}

// List returns every policy, oldest created first.
func (r *OPAPolicyRepo) List(ctx context.Context) ([]*model.OPAPolicy, error) {
	return r.list(ctx, `SELECT `+opaPolicyColumns+` FROM opa_policies ORDER BY created_at, id`)
}

// ListEnabled returns the enabled policies, oldest created first: the set the
// harness evaluates.
func (r *OPAPolicyRepo) ListEnabled(ctx context.Context) ([]*model.OPAPolicy, error) {
	return r.list(ctx, `SELECT `+opaPolicyColumns+` FROM opa_policies WHERE enabled ORDER BY created_at, id`)
}

func (r *OPAPolicyRepo) list(ctx context.Context, query string) ([]*model.OPAPolicy, error) {
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list opa policies: %w", err)
	}
	defer rows.Close()
	out := []*model.OPAPolicy{}
	for rows.Next() {
		p, err := scanOPAPolicy(rows)
		if err != nil {
			return nil, fmt.Errorf("scan opa policy: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// marshalOPAPolicy validates p and encodes its JSONB columns. A nil Inputs
// is stored as [] (the column default), not JSON null.
func marshalOPAPolicy(p *model.OPAPolicy) (inputs, scope []byte, err error) {
	if err := p.Validate(); err != nil {
		return nil, nil, validationError(err)
	}
	in := p.Inputs
	if in == nil {
		in = []model.PolicyInput{}
	}
	if inputs, err = jsonbMarshal(in); err != nil {
		return nil, nil, fmt.Errorf("encode opa policy inputs: %w", err)
	}
	if scope, err = jsonbMarshal(p.Scope); err != nil {
		return nil, nil, fmt.Errorf("encode opa policy scope: %w", err)
	}
	return inputs, scope, nil
}

// scanOPAPolicy scans one opa_policies row in opaPolicyColumns order.
// Shared by Get and the lists.
func scanOPAPolicy(scanner interface {
	Scan(dest ...any) error
}) (*model.OPAPolicy, error) {
	var (
		p         model.OPAPolicy
		inputsRaw []byte
		scopeRaw  []byte
	)
	if err := scanner.Scan(&p.ID, &p.Name, &p.Enabled, &p.Rego, &p.IntervalS, &p.CooldownS,
		&inputsRaw, &scopeRaw, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return nil, err
	}
	if err := jsonbScan(inputsRaw, &p.Inputs); err != nil {
		return nil, fmt.Errorf("decode inputs for %s: %w", p.ID, err)
	}
	if err := jsonbScan(scopeRaw, &p.Scope); err != nil {
		return nil, fmt.Errorf("decode scope for %s: %w", p.ID, err)
	}
	return &p, nil
}
