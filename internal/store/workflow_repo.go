package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"manteion-go/internal/model"
)

// WorkflowRepo provides persistence for manteion-owned workflow definitions.
//
// Manteion holds the DEFINITION (the DSL spec); zeus holds EXECUTION
// (runs, attacks, validation). When a workflow is started the API layer
// inlines this definition into the proxied call to zeus.
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

// Page is the pagination request shape — same shape as ExperimentRepo
// so the api layer's pagination helper can drive both.
type WorkflowPage = Page

// Create inserts a new workflow definition.
func (r *WorkflowRepo) Create(ctx context.Context, wf *model.Workflow) error {
	if err := wf.Validate(); err != nil {
		return err
	}
	targetsJSON, err := json.Marshal(wf.Targets)
	if err != nil {
		return fmt.Errorf("marshal targets: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO workflows (id, name, description, targets,
			estimated_rps_per_vu, steps, thresholds, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
		wf.ID, wf.Name, nullString(wf.Description), targetsJSON,
		wf.EstimatedRPSPerVU,
		jsonbBytes(wf.Steps), jsonbBytesOrNull(wf.Thresholds),
		wf.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert workflow: %w", err)
	}
	return nil
}

// Get returns a workflow definition by ID, or ErrNotFound.
func (r *WorkflowRepo) Get(ctx context.Context, id string) (*model.Workflow, error) {
	wf, err := scanWorkflowRow(r.db.QueryRowContext(ctx, `
		SELECT id, name, description, targets, estimated_rps_per_vu,
			steps, thresholds, created_at, updated_at
		FROM workflows WHERE id = $1`, id))
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get workflow: %w", err)
	}
	return wf, nil
}

// List returns a page of workflows + total count.
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
		SELECT id, name, description, targets, estimated_rps_per_vu,
			steps, thresholds, created_at, updated_at,
			COUNT(*) OVER () AS total_count
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
		wf, t, err := scanWorkflowRowWithTotal(rows)
		if err != nil {
			return nil, 0, err
		}
		total = t
		out = append(out, wf)
	}
	return out, total, rows.Err()
}

// Update edits an existing workflow definition. Empty fields on the input
// (other than Steps/Thresholds, which always overwrite) are treated as
// "no change" so callers can PATCH without re-sending the whole row.
func (r *WorkflowRepo) Update(ctx context.Context, wf *model.Workflow) error {
	if wf.ID == "" {
		return fmt.Errorf("update workflow: id required")
	}
	if len(wf.Steps) == 0 {
		return fmt.Errorf("update workflow: steps required")
	}
	if len(wf.Targets) == 0 {
		return fmt.Errorf("update workflow: targets required")
	}
	targetsJSON, err := json.Marshal(wf.Targets)
	if err != nil {
		return fmt.Errorf("marshal targets: %w", err)
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE workflows SET
			name                 = $2,
			description          = $3,
			targets              = $4,
			estimated_rps_per_vu = $5,
			steps                = $6,
			thresholds           = $7,
			updated_at           = now()
		WHERE id = $1`,
		wf.ID, wf.Name, nullString(wf.Description), targetsJSON,
		wf.EstimatedRPSPerVU,
		jsonbBytes(wf.Steps), jsonbBytesOrNull(wf.Thresholds),
	)
	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}
	return affectedOrNotFound(res)
}

// Delete removes a workflow definition.
func (r *WorkflowRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM workflows WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete workflow: %w", err)
	}
	return affectedOrNotFound(res)
}

func scanWorkflowRow(scanner interface {
	Scan(dest ...any) error
}) (*model.Workflow, error) {
	var (
		wf           model.Workflow
		desc         sql.NullString
		targetsRaw   []byte
		stepsRaw     []byte
		thresholdRaw []byte
	)
	if err := scanner.Scan(
		&wf.ID, &wf.Name, &desc, &targetsRaw, &wf.EstimatedRPSPerVU,
		&stepsRaw, &thresholdRaw, &wf.CreatedAt, &wf.UpdatedAt,
	); err != nil {
		return nil, err
	}
	wf.Description = fromNullString(desc)
	if len(targetsRaw) > 0 {
		if err := json.Unmarshal(targetsRaw, &wf.Targets); err != nil {
			return nil, fmt.Errorf("decode targets: %w", err)
		}
	}
	if len(stepsRaw) > 0 {
		wf.Steps = json.RawMessage(stepsRaw)
	}
	if len(thresholdRaw) > 0 {
		wf.Thresholds = json.RawMessage(thresholdRaw)
	}
	return &wf, nil
}

func scanWorkflowRowWithTotal(rows *sql.Rows) (*model.Workflow, int, error) {
	var (
		wf           model.Workflow
		desc         sql.NullString
		targetsRaw   []byte
		stepsRaw     []byte
		thresholdRaw []byte
		total        int
	)
	if err := rows.Scan(
		&wf.ID, &wf.Name, &desc, &targetsRaw, &wf.EstimatedRPSPerVU,
		&stepsRaw, &thresholdRaw, &wf.CreatedAt, &wf.UpdatedAt, &total,
	); err != nil {
		return nil, 0, fmt.Errorf("scan workflow: %w", err)
	}
	wf.Description = fromNullString(desc)
	if len(targetsRaw) > 0 {
		if err := json.Unmarshal(targetsRaw, &wf.Targets); err != nil {
			return nil, 0, fmt.Errorf("decode targets: %w", err)
		}
	}
	if len(stepsRaw) > 0 {
		wf.Steps = json.RawMessage(stepsRaw)
	}
	if len(thresholdRaw) > 0 {
		wf.Thresholds = json.RawMessage(thresholdRaw)
	}
	return &wf, total, nil
}

// jsonbBytes returns the raw JSON bytes as-is for postgres JSONB inserts.
// json.RawMessage already satisfies driver.Valuer, but we make the intent
// explicit (postgres will reject empty bytes).
func jsonbBytes(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return []byte("null")
	}
	return raw
}

// jsonbBytesOrNull returns SQL NULL when the input is empty.
func jsonbBytesOrNull(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return []byte(raw)
}
