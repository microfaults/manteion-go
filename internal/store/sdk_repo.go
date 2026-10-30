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
// computed from poll staleness, not stored. scanInstance scans in this order.
var selectColumns = fmt.Sprintf(
	`id, service, version, address, poll_interval_ms, routes, registered_at, last_poll_at,
	 host_kind, capabilities, observed_generation, observed_etag, observed_unsupported, observed_errors,
	 replay_set_id, replay_set_state, observed_at, %s AS status`,
	statusExpr,
)

// Register inserts or re-registers an SDK instance (upsert).
// On conflict, updates service/version/address/routes/poll_interval_ms and
// host/capabilities, resets timestamps, and clears the observed state: a
// re-registering instance starts from nothing, and its register response
// hands it the current desired-state document.
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

	// capsJSON is SQL NULL when the host advertised no capabilities (absent =
	// all, the wire contract), a JSON array otherwise.
	capsJSON, err := jsonbOrNull(inst.Capabilities)
	if err != nil {
		return fmt.Errorf("marshal sdk capabilities: %w", err)
	}

	pollMs := inst.PollIntervalMs
	if pollMs <= 0 {
		pollMs = 10000 // default 10s
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO sdk_instances (id, service, version, address, poll_interval_ms, routes, host_kind, capabilities, registered_at, last_poll_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
		ON CONFLICT (id) DO UPDATE SET
			service = EXCLUDED.service,
			version = EXCLUDED.version,
			address = EXCLUDED.address,
			poll_interval_ms = EXCLUDED.poll_interval_ms,
			routes = EXCLUDED.routes,
			host_kind = EXCLUDED.host_kind,
			capabilities = EXCLUDED.capabilities,
			observed_generation = 0,
			observed_etag = '',
			observed_unsupported = NULL,
			observed_errors = NULL,
			replay_set_id = '',
			replay_set_state = '',
			observed_at = NULL,
			registered_at = now(),
			last_poll_at = now()`,
		inst.ID, inst.Service, inst.Version, inst.Address, pollMs, routesJSON, inst.Host, capsJSON,
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

// scanInstance scans one sdk_instances row including the nullable JSONB
// columns (routes, capabilities, observed_unsupported, observed_errors),
// observed_at and the computed status. Shared by Get/List/ForService/ListLive;
// column order must match selectColumns.
func scanInstance(scanner interface {
	Scan(dest ...any) error
}) (*model.SDKInstance, error) {
	var (
		inst           model.SDKInstance
		routesRaw      []byte
		capsRaw        []byte
		unsupportedRaw []byte
		errorsRaw      []byte
		observedAt     sql.NullTime
	)
	if err := scanner.Scan(&inst.ID, &inst.Service, &inst.Version, &inst.Address,
		&inst.PollIntervalMs, &routesRaw, &inst.RegisteredAt, &inst.LastPollAt,
		&inst.Host, &capsRaw, &inst.ObservedGeneration, &inst.ObservedEtag, &unsupportedRaw, &errorsRaw,
		&inst.ReplaySetID, &inst.ReplaySetState, &observedAt, &inst.Status); err != nil {
		return nil, err
	}
	if len(routesRaw) > 0 {
		if err := json.Unmarshal(routesRaw, &inst.Routes); err != nil {
			return nil, fmt.Errorf("decode routes for %s: %w", inst.ID, err)
		}
	}
	if err := jsonbScan(capsRaw, &inst.Capabilities); err != nil {
		return nil, fmt.Errorf("decode capabilities for %s: %w", inst.ID, err)
	}
	if err := jsonbScan(unsupportedRaw, &inst.ObservedUnsupported); err != nil {
		return nil, fmt.Errorf("decode observed_unsupported for %s: %w", inst.ID, err)
	}
	if err := jsonbScan(errorsRaw, &inst.ObservedErrors); err != nil {
		return nil, fmt.Errorf("decode observed_errors for %s: %w", inst.ID, err)
	}
	inst.ObservedAt = nullTimeToPtr(observedAt)
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

// RecordPoll is the per-poll write (desired-state v1): the poll timestamp that
// drives liveness and the generation the instance says it holds. Runs BEFORE
// the handler decides 200/304 — most polls are no-change, and this is how the
// orchestrator observes convergence without another round trip.
func (r *SDKRepo) RecordPoll(ctx context.Context, id string, observedGeneration uint64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE sdk_instances SET last_poll_at = now(), observed_generation = $2 WHERE id = $1`, id, observedGeneration)
	if err != nil {
		return fmt.Errorf("record poll: %w", err)
	}
	return affectedOrNotFound(res)
}

// RecordStatus stores an instance's POST /api/v1/sdk/status report. The report
// is a full snapshot of what the instance holds, so every observed column is
// overwritten: an absent ReplaySet means the instance holds no set and clears
// the stored id and state (keeping them would leave a stale committed/failed
// state behind after a thaw or phase end, and artifact ids are content hashes
// that can recur).
func (r *SDKRepo) RecordStatus(ctx context.Context, id string, st model.ObservedStatus) error {
	unsupported, err := jsonbOrNull(st.Unsupported)
	if err != nil {
		return fmt.Errorf("marshal observed unsupported: %w", err)
	}
	errs, err := jsonbOrNull(st.Errors)
	if err != nil {
		return fmt.Errorf("marshal observed errors: %w", err)
	}
	replayID, replayState := "", ""
	if st.ReplaySet != nil {
		replayID, replayState = st.ReplaySet.ID, st.ReplaySet.State
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE sdk_instances SET
			observed_generation = $2, observed_etag = $3,
			observed_unsupported = $4, observed_errors = $5,
			replay_set_id = $6, replay_set_state = $7,
			observed_at = now()
		WHERE id = $1`,
		id, st.AppliedGeneration, st.Etag, unsupported, errs, replayID, replayState)
	if err != nil {
		return fmt.Errorf("record status: %w", err)
	}
	return affectedOrNotFound(res)
}

// jsonbOrNull encodes a list for a nullable JSONB column: an empty (or nil)
// list is SQL NULL, anything else its JSON array bytes. jsonbMarshal would
// write the JSON value null for a nil slice, which is not SQL NULL.
func jsonbOrNull[T any](s []T) (any, error) {
	if len(s) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return b, nil
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
