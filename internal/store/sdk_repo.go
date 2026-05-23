package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"manteion-go/internal/model"
)

// SDKRepo provides persistence for atropos-go SDK instance registrations.
type SDKRepo struct {
	db *sql.DB
}

// NewSDKRepo creates a new SDK instance repository.
func NewSDKRepo(db *sql.DB) *SDKRepo {
	return &SDKRepo{db: db}
}

// statusExpr computes liveness from last_poll_at and poll_interval_ms:
//
//	alive: within 2× poll interval
//	stale: between 2× and 5× poll interval
//	dead:  beyond 5× poll interval
const statusExpr = `CASE
	WHEN EXTRACT(EPOCH FROM now() - last_poll_at) * 1000 > poll_interval_ms * 5 THEN 'dead'
	WHEN EXTRACT(EPOCH FROM now() - last_poll_at) * 1000 > poll_interval_ms * 2 THEN 'stale'
	ELSE 'alive'
END`

// selectColumns is the column list shared by all read queries. Status is
// computed from poll staleness, not stored.
var selectColumns = fmt.Sprintf(
	`id, service, version, address, poll_interval_ms, routes, registered_at, last_poll_at, %s AS status`,
	statusExpr,
)

// Register inserts or re-registers an SDK instance (upsert).
// On conflict, updates service/version/address/routes/poll_interval_ms and
// resets timestamps.
func (r *SDKRepo) Register(ctx context.Context, inst *model.SDKInstance) error {
	if err := inst.Validate(); err != nil {
		return err
	}

	// routesJSON is SQL NULL when the service publishes no routes (e.g. gRPC
	// services), a JSON array otherwise. Typed nil → NULL via lib/pq.
	var routesJSON any
	if len(inst.Routes) > 0 {
		b, err := json.Marshal(inst.Routes)
		if err != nil {
			return fmt.Errorf("marshal sdk routes: %w", err)
		}
		routesJSON = b
	}

	pollMs := inst.PollIntervalMs
	if pollMs <= 0 {
		pollMs = 10000 // default 10s
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO sdk_instances (id, service, version, address, poll_interval_ms, routes, registered_at, last_poll_at)
		VALUES ($1, $2, $3, $4, $5, $6, now(), now())
		ON CONFLICT (id) DO UPDATE SET
			service = EXCLUDED.service,
			version = EXCLUDED.version,
			address = EXCLUDED.address,
			poll_interval_ms = EXCLUDED.poll_interval_ms,
			routes = EXCLUDED.routes,
			registered_at = now(),
			last_poll_at = now()`,
		inst.ID, inst.Service, inst.Version, inst.Address, pollMs, routesJSON,
	)
	if err != nil {
		return fmt.Errorf("register sdk instance: %w", err)
	}
	return nil
}

// Deregister removes an SDK instance by ID.
func (r *SDKRepo) Deregister(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM sdk_instances WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deregister sdk instance: %w", err)
	}
	return affectedOrNotFound(res)
}

// scanInstance scans one sdk_instances row including the nullable routes
// JSONB and the computed status. Shared by Get/List/ForService/ListLive;
// column order must match selectColumns.
func scanInstance(scanner interface {
	Scan(dest ...any) error
}) (*model.SDKInstance, error) {
	var (
		inst      model.SDKInstance
		routesRaw []byte
	)
	if err := scanner.Scan(&inst.ID, &inst.Service, &inst.Version, &inst.Address,
		&inst.PollIntervalMs, &routesRaw, &inst.RegisteredAt, &inst.LastPollAt, &inst.Status); err != nil {
		return nil, err
	}
	if len(routesRaw) > 0 {
		if err := json.Unmarshal(routesRaw, &inst.Routes); err != nil {
			return nil, fmt.Errorf("decode routes for %s: %w", inst.ID, err)
		}
	}
	return &inst, nil
}

// Get returns an SDK instance by ID, or ErrNotFound.
func (r *SDKRepo) Get(ctx context.Context, id string) (*model.SDKInstance, error) {
	inst, err := scanInstance(r.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT %s FROM sdk_instances WHERE id = $1`, selectColumns), id))
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get sdk instance: %w", err)
	}
	return inst, nil
}

// List returns all registered SDK instances (no liveness filter — callers
// that need only fresh instances should use ListLive).
func (r *SDKRepo) List(ctx context.Context) ([]*model.SDKInstance, error) {
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s FROM sdk_instances ORDER BY service, id`, selectColumns))
	if err != nil {
		return nil, fmt.Errorf("list sdk instances: %w", err)
	}
	defer rows.Close()

	var result []*model.SDKInstance
	for rows.Next() {
		inst, err := scanInstance(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sdk instance: %w", err)
		}
		result = append(result, inst)
	}
	return result, rows.Err()
}

// ListLive returns SDK instances whose last_poll_at is within the freshness
// window. The catalog handler uses this so the route list never includes
// pods that have stopped polling.
func (r *SDKRepo) ListLive(ctx context.Context, freshness time.Duration) ([]*model.SDKInstance, error) {
	seconds := int(freshness.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s FROM sdk_instances
		WHERE last_poll_at > now() - make_interval(secs => $1)
		ORDER BY service, id`, selectColumns), seconds)
	if err != nil {
		return nil, fmt.Errorf("list live sdk instances: %w", err)
	}
	defer rows.Close()

	var result []*model.SDKInstance
	for rows.Next() {
		inst, err := scanInstance(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sdk instance: %w", err)
		}
		result = append(result, inst)
	}
	return result, rows.Err()
}

// ForService returns SDK instances for a specific service.
func (r *SDKRepo) ForService(ctx context.Context, service string) ([]*model.SDKInstance, error) {
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s FROM sdk_instances WHERE service = $1 ORDER BY id`, selectColumns), service)
	if err != nil {
		return nil, fmt.Errorf("sdk instances for service: %w", err)
	}
	defer rows.Close()

	var result []*model.SDKInstance
	for rows.Next() {
		inst, err := scanInstance(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sdk instance: %w", err)
		}
		result = append(result, inst)
	}
	return result, rows.Err()
}

// TouchPoll updates the last_poll_at timestamp for an instance.
// Called on every SDK poll to track liveness.
func (r *SDKRepo) TouchPoll(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE sdk_instances SET last_poll_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("touch poll: %w", err)
	}
	return affectedOrNotFound(res)
}

// PurgeDead removes instances inactive for longer than (5× poll_interval) + grace.
func (r *SDKRepo) PurgeDead(ctx context.Context, grace time.Duration) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
		DELETE FROM sdk_instances
		WHERE EXTRACT(EPOCH FROM now() - last_poll_at) * 1000 > (poll_interval_ms * 5) + $1`,
		grace.Milliseconds(),
	)
	if err != nil {
		return 0, fmt.Errorf("purge dead instances: %w", err)
	}
	return res.RowsAffected()
}

// Count returns the total number of registered instances.
func (r *SDKRepo) Count(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sdk_instances`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count sdk instances: %w", err)
	}
	return count, nil
}
