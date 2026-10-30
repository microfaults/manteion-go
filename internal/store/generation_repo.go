package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// GenerationRepo persists per-service desired-state generations
// (service_generations, migration 12). It satisfies desired.Store.
type GenerationRepo struct{ db *sql.DB }

func NewGenerationRepo(db *sql.DB) *GenerationRepo { return &GenerationRepo{db: db} }

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Get returns the service's generation; a service without a row is at 1.
func (r *GenerationRepo) Get(ctx context.Context, service string) (uint64, error) {
	var gen uint64
	err := r.db.QueryRowContext(ctx, `SELECT generation FROM service_generations WHERE service = $1`, service).Scan(&gen)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get generation %q: %w", service, err)
	}
	return gen, nil
}

// Bump increments the generation (first bump creates the row at 2) inside tx
// when given, so a rule write and its bump commit or roll back together.
func (r *GenerationRepo) Bump(ctx context.Context, tx *sql.Tx, service string) (uint64, error) {
	var q rowQuerier = r.db
	if tx != nil {
		q = tx
	}
	var gen uint64
	err := q.QueryRowContext(ctx, `
		INSERT INTO service_generations (service, generation) VALUES ($1, 2)
		ON CONFLICT (service) DO UPDATE
		SET generation = service_generations.generation + 1, updated_at = now()
		RETURNING generation`, service).Scan(&gen)
	if err != nil {
		return 0, fmt.Errorf("bump generation %q: %w", service, err)
	}
	return gen, nil
}
