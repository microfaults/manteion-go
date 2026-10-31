package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"manteion-go/internal/model"
)

// ReplayArtifactRepo persists the replay-set artifacts built for frozen
// isolation phases (replay_artifacts, migration 13): one row per (phase,
// service) pointing at the content-addressed NDJSON file on disk. The id
// repeats across phases that replay the same recording; the pair is the key.
type ReplayArtifactRepo struct {
	db *sql.DB
}

func NewReplayArtifactRepo(db *sql.DB) *ReplayArtifactRepo {
	return &ReplayArtifactRepo{db: db}
}

// replayArtifactColumns is the column list scanReplayArtifact scans, in order.
const replayArtifactColumns = `id, experiment_id, phase_id, service,
	entry_count, w5_sha256, size_bytes, path, created_at`

// scanReplayArtifact scans one replay_artifacts row selected with
// replayArtifactColumns. Shared by GetByID and ForPhaseService.
func scanReplayArtifact(s interface{ Scan(...any) error }) (*model.ReplayArtifact, error) {
	var a model.ReplayArtifact
	if err := s.Scan(&a.ID, &a.ExperimentID, &a.PhaseID, &a.Service,
		&a.EntryCount, &a.W5SHA256, &a.SizeBytes, &a.Path, &a.CreatedAt); err != nil {
		return nil, err
	}
	return &a, nil
}

// Upsert assigns the artifact to its (phase, service). The first call inserts
// the row; a later call for the same pair replaces it (a rebuilt set, a new
// id) and restamps created_at. created_at belongs to the database: the value
// on a is not written and not read back.
func (r *ReplayArtifactRepo) Upsert(ctx context.Context, a *model.ReplayArtifact) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO replay_artifacts (id, experiment_id, phase_id, service,
			entry_count, w5_sha256, size_bytes, path)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (phase_id, service) DO UPDATE SET
			id            = EXCLUDED.id,
			experiment_id = EXCLUDED.experiment_id,
			entry_count   = EXCLUDED.entry_count,
			w5_sha256     = EXCLUDED.w5_sha256,
			size_bytes    = EXCLUDED.size_bytes,
			path          = EXCLUDED.path,
			created_at    = now()`,
		a.ID, a.ExperimentID, a.PhaseID, a.Service,
		a.EntryCount, a.W5SHA256, a.SizeBytes, a.Path,
	)
	if err != nil {
		return fmt.Errorf("upsert replay_artifact (phase %q, service %q): %w", a.PhaseID, a.Service, err)
	}
	return nil
}

// GetByID returns the artifact with the given id, or ErrNotFound. Several
// (phase, service) rows can share an id (two phases replaying the same
// recording); they describe byte-identical files, so the newest row stands
// for all of them.
func (r *ReplayArtifactRepo) GetByID(ctx context.Context, id string) (*model.ReplayArtifact, error) {
	a, err := scanReplayArtifact(r.db.QueryRowContext(ctx, `
		SELECT `+replayArtifactColumns+` FROM replay_artifacts
		WHERE id = $1
		ORDER BY created_at DESC
		LIMIT 1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get replay_artifact %q: %w", id, err)
	}
	return a, nil
}

// ForPhaseService returns the artifact assigned to the (phase, service) pair.
// A pair with no artifact assigned is not an error: it returns nil, nil.
func (r *ReplayArtifactRepo) ForPhaseService(ctx context.Context, phaseID, service string) (*model.ReplayArtifact, error) {
	a, err := scanReplayArtifact(r.db.QueryRowContext(ctx, `
		SELECT `+replayArtifactColumns+` FROM replay_artifacts
		WHERE phase_id = $1 AND service = $2`, phaseID, service))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get replay_artifact for phase %q, service %q: %w", phaseID, service, err)
	}
	return a, nil
}
