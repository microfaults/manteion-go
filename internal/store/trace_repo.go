package store

import (
	"context"
	"database/sql"
	"fmt"

	"manteion-go/internal/model"
)

// TraceRepo provides persistence for trace anchors — pointers into
// external trace/metrics backends (Jaeger, Prometheus, Tempo).
type TraceRepo struct {
	db *sql.DB
}

// NewTraceRepo creates a new trace anchor repository.
func NewTraceRepo(db *sql.DB) *TraceRepo {
	return &TraceRepo{db: db}
}

// Create inserts a new trace anchor.
func (r *TraceRepo) Create(ctx context.Context, anchor *model.TraceAnchor) error {
	if err := anchor.Validate(); err != nil {
		return err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO trace_anchors (id, phase_id, meta_trace_id,
			service, backend, query_hint, collected_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		anchor.ID, anchor.PhaseID, anchor.MetaTraceID,
		anchor.Service, anchor.Backend, anchor.QueryHint, anchor.CollectedAt,
	)
	if err != nil {
		return fmt.Errorf("insert trace_anchor: %w", err)
	}
	return nil
}

// ListByPhase returns all trace anchors for a given experiment phase.
func (r *TraceRepo) ListByPhase(ctx context.Context, phaseID string) ([]*model.TraceAnchor, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, phase_id, meta_trace_id,
			service, backend, query_hint, collected_at
		FROM trace_anchors WHERE phase_id = $1
		ORDER BY service, backend`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list trace_anchors: %w", err)
	}
	defer rows.Close()

	var result []*model.TraceAnchor
	for rows.Next() {
		var a model.TraceAnchor
		if err := rows.Scan(
			&a.ID, &a.PhaseID, &a.MetaTraceID,
			&a.Service, &a.Backend, &a.QueryHint, &a.CollectedAt,
		); err != nil {
			return nil, fmt.Errorf("scan trace_anchor: %w", err)
		}
		result = append(result, &a)
	}
	return result, rows.Err()
}
