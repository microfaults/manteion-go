package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"manteion-go/internal/model"
)

// PhaseFaultEventRepo persists the per-phase fault apply→clear audit trail
// (phase_fault_events). Rows are opened by the orchestrator when it pushes a
// rule or freezes a cache-box service, and closed (ended_at) when the phase
// finishes.
type PhaseFaultEventRepo struct {
	db *sql.DB
}

func NewPhaseFaultEventRepo(db *sql.DB) *PhaseFaultEventRepo {
	return &PhaseFaultEventRepo{db: db}
}

// Create inserts one fault event (ended_at left NULL = still active).
func (r *PhaseFaultEventRepo) Create(ctx context.Context, e *model.PhaseFaultEvent) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if e.StartedAt.IsZero() {
		e.StartedAt = time.Now()
	}
	detail := e.Detail
	if len(detail) == 0 {
		detail = []byte("{}")
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO phase_fault_events (id, phase_id, source, service, kind, detail, started_at, ended_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.ID, e.PhaseID, e.Source, e.Service, e.Kind, []byte(detail), e.StartedAt, e.EndedAt)
	if err != nil {
		return fmt.Errorf("insert phase_fault_event: %w", err)
	}
	return nil
}

// EndOpenForPhase stamps ended_at on every still-open event of the phase.
func (r *PhaseFaultEventRepo) EndOpenForPhase(ctx context.Context, phaseID string, endedAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE phase_fault_events SET ended_at = $2
		WHERE phase_id = $1 AND ended_at IS NULL`, phaseID, endedAt)
	if err != nil {
		return fmt.Errorf("end open phase_fault_events: %w", err)
	}
	return nil
}

// ListForPhase returns the phase's events in apply order.
func (r *PhaseFaultEventRepo) ListForPhase(ctx context.Context, phaseID string) ([]*model.PhaseFaultEvent, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, phase_id, source, service, kind, detail, started_at, ended_at
		FROM phase_fault_events WHERE phase_id = $1
		ORDER BY started_at, id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_fault_events: %w", err)
	}
	defer rows.Close()
	var out []*model.PhaseFaultEvent
	for rows.Next() {
		var (
			e       model.PhaseFaultEvent
			detail  []byte
			endedAt sql.NullTime
		)
		if err := rows.Scan(&e.ID, &e.PhaseID, &e.Source, &e.Service, &e.Kind, &detail, &e.StartedAt, &endedAt); err != nil {
			return nil, fmt.Errorf("scan phase_fault_event: %w", err)
		}
		e.Detail = detail
		e.EndedAt = nullTimeToPtr(endedAt)
		out = append(out, &e)
	}
	return out, rows.Err()
}
