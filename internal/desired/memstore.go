package desired

import (
	"context"
	"database/sql"
	"sync"
)

// memoryStore is an in-memory Store for tests with the same contract as the
// service_generations table: an absent service is at 1 and its first bump
// yields 2. It ignores tx.
type memoryStore struct {
	mu   sync.Mutex
	gens map[string]uint64
}

// NewMemoryStore returns an empty in-memory Store.
func NewMemoryStore() Store { return &memoryStore{gens: map[string]uint64{}} }

func (m *memoryStore) Get(_ context.Context, service string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen, ok := m.gens[service]; ok {
		return gen, nil
	}
	return 1, nil
}

func (m *memoryStore) Bump(_ context.Context, _ *sql.Tx, service string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	gen, ok := m.gens[service]
	if !ok {
		gen = 1
	}
	gen++
	m.gens[service] = gen
	return gen, nil
}
