package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"manteion-go/internal/model"
)

// ErrInUse is returned when a delete is blocked by a RESTRICT foreign key —
// e.g. a workflow referenced by phase_workflows, or a rule referenced by
// phase_rules. Handlers map it to 409 Conflict.
var ErrInUse = errors.New("store: in use")

// ErrDuplicateName is returned when a unique name constraint is violated.
var ErrDuplicateName = errors.New("store: duplicate name")

// WorkflowRepo provides persistence for manteion-owned workflow definitions.
//
// Manteion holds the DEFINITION (the full DSL v2 document); zeus validates
// and executes. Phase start materializes the definition into zeus before
// triggering runs, so zeus's in-memory store is a cache of this table.
type WorkflowRepo struct {
	db *sql.DB
}

// NewWorkflowRepo creates a new workflow repository.
func NewWorkflowRepo(db *sql.DB) *WorkflowRepo {
	return &WorkflowRepo{db: db}
}

// WorkflowFilter narrows a list query. Empty values are no-ops.
// (Reserved for future name-search; today the list is short enough
// that the UI filters client-side.)
type WorkflowFilter struct {
	NameContains string
}

const workflowColumns = `id, name, version, description, dsl, created_at, updated_at`

// Create inserts a new workflow definition.
func (r *WorkflowRepo) Create(ctx context.Context, wf *model.Workflow) error {
	if err := wf.Validate(); err != nil {
		return err
	}
	if wf.Version == "" {
		wf.Version = "2"
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workflows (id, name, version, description, dsl, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6)`,
		wf.ID, wf.Name, wf.Version, wf.Description, []byte(wf.DSL), wf.CreatedAt,
	)
	if isUniqueViolation(err) {
		return fmt.Errorf("workflow name %q: %w", wf.Name, ErrDuplicateName)
	}
	if err != nil {
		return fmt.Errorf("insert workflow: %w", err)
	}
	return nil
}

// Get returns a workflow definition by ID, or ErrNotFound.
func (r *WorkflowRepo) Get(ctx context.Context, id string) (*model.Workflow, error) {
	wf, err := scanWorkflow(r.db.QueryRowContext(ctx,
		`SELECT `+workflowColumns+` FROM workflows WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get workflow: %w", err)
	}
	return wf, nil
}

// List returns a page of workflows + total count, newest first.
func (r *WorkflowRepo) List(ctx context.Context, f WorkflowFilter, p Page) ([]*model.Workflow, int, error) {
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if p.Limit > 200 {
		p.Limit = 200
	}
	if p.Offset < 0 {
		p.Offset = 0
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT `+workflowColumns+`, COUNT(*) OVER () AS total_count
		FROM workflows
		ORDER BY created_at DESC, id
		LIMIT $1 OFFSET $2`, p.Limit, p.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list workflows: %w", err)
	}
	defer rows.Close()

	var (
		out   []*model.Workflow
		total int
	)
	for rows.Next() {
		var wf model.Workflow
		var dsl []byte
		if err := rows.Scan(&wf.ID, &wf.Name, &wf.Version, &wf.Description, &dsl,
			&wf.CreatedAt, &wf.UpdatedAt, &total); err != nil {
			return nil, 0, fmt.Errorf("scan workflow row: %w", err)
		}
		wf.DSL = dsl
		out = append(out, &wf)
	}
	return out, total, rows.Err()
}

// Update replaces the definition (name, version, description, dsl) and bumps
// updated_at.
func (r *WorkflowRepo) Update(ctx context.Context, wf *model.Workflow) error {
	if err := wf.Validate(); err != nil {
		return err
	}
	if wf.Version == "" {
		wf.Version = "2"
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE workflows SET name=$2, version=$3, description=$4, dsl=$5, updated_at=$6
		WHERE id = $1`,
		wf.ID, wf.Name, wf.Version, wf.Description, []byte(wf.DSL), time.Now(),
	)
	if isUniqueViolation(err) {
		return fmt.Errorf("workflow name %q: %w", wf.Name, ErrDuplicateName)
	}
	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}
	return affectedOrNotFound(res)
}

// Delete removes a workflow definition. Returns ErrInUse when the workflow
// is still referenced by phase_workflows (FK RESTRICT) — the experimental
// record protects its provenance.
func (r *WorkflowRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM workflows WHERE id = $1`, id)
	if isFKViolation(err) {
		return fmt.Errorf("workflow %q referenced by experiment phases: %w", id, ErrInUse)
	}
	if err != nil {
		return fmt.Errorf("delete workflow: %w", err)
	}
	return affectedOrNotFound(res)
}

func scanWorkflow(row *sql.Row) (*model.Workflow, error) {
	var wf model.Workflow
	var dsl []byte
	if err := row.Scan(&wf.ID, &wf.Name, &wf.Version, &wf.Description, &dsl,
		&wf.CreatedAt, &wf.UpdatedAt); err != nil {
		return nil, err
	}
	wf.DSL = dsl
	return &wf, nil
}

// isUniqueViolation reports whether err is a Postgres unique_violation (23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// isFKViolation reports whether err is a Postgres foreign_key_violation (23503).
func isFKViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}
