package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"manteion-go/internal/model"
)

// ExperimentRepo provides persistence for the phase-first experiment domain
// introduced by migration #19.
//
// Hierarchy:
//
//	experiments
//	  └── experiment_phases     (ordered by position)
//	        ├── phase_workflows  (per-(phase, workflow) attack config)
//	        ├── phase_rules      (rules active during the phase)
//	        └── results: phase_workflow_results,
//	                     phase_service_latency / resources / cache
//
// All list endpoints accept a Page struct so handlers can honor the
// pagination contract uniformly.
type ExperimentRepo struct {
	db *sql.DB
}

// NewExperimentRepo creates a new experiment repository.
func NewExperimentRepo(db *sql.DB) *ExperimentRepo {
	return &ExperimentRepo{db: db}
}

// ExperimentFilter narrows a list query. Empty values are no-ops.
// (Page, the pagination request shape, lives in helpers.go.)
type ExperimentFilter struct {
	Status string
}

// =========================================================================
// Experiment
// =========================================================================

// Create inserts a new experiment row. Workflows and phases are persisted
// via the dedicated Attach* / CreatePhase methods.
func (r *ExperimentRepo) Create(ctx context.Context, exp *model.Experiment) error {
	if err := exp.Validate(); err != nil {
		return validationError(err)
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO experiments (id, name, description, hypothesis, status,
			created_by, created_at, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		exp.ID, exp.Name, nullString(exp.Description), nullString(exp.Hypothesis),
		exp.Status, nullString(exp.CreatedBy), exp.CreatedAt, exp.StartedAt, exp.CompletedAt,
	)
	if err != nil {
		if code, _ := pgViolation(err); code == pgUniqueViolation {
			return fmt.Errorf("experiment id %q already exists: %w", exp.ID, ErrConflict)
		}
		return fmt.Errorf("insert experiment: %w", err)
	}
	return nil
}

// Get returns one experiment by id, or ErrNotFound.
func (r *ExperimentRepo) Get(ctx context.Context, id string) (*model.Experiment, error) {
	exp, err := scanExperimentRow(r.db.QueryRowContext(ctx, `
		SELECT id, name, description, hypothesis, status, created_by,
			created_at, started_at, completed_at, failure_reason
		FROM experiments WHERE id = $1`, id))
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment: %w", err)
	}
	return exp, nil
}

// List returns a page of experiments with the total row count. The total
// is computed in the same call so the handler can build the page envelope
// without a separate query.
func (r *ExperimentRepo) List(ctx context.Context, f ExperimentFilter, p Page) ([]*model.Experiment, int, error) {
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if p.Limit > 200 {
		p.Limit = 200
	}
	if p.Offset < 0 {
		p.Offset = 0
	}

	args := []any{p.Limit, p.Offset}
	where := ""
	if f.Status != "" {
		// status::text keeps the comparison graceful for unknown filter
		// values (matches nothing) instead of a 22P02 enum-cast error.
		where = "WHERE status::text = $3"
		args = append(args, f.Status)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, description, hypothesis, status, created_by,
			created_at, started_at, completed_at, failure_reason,
			COUNT(*) OVER () AS total_count
		FROM experiments
		`+where+`
		ORDER BY created_at DESC, id
		LIMIT $1 OFFSET $2`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list experiments: %w", err)
	}
	defer rows.Close()

	var (
		result []*model.Experiment
		total  int
	)
	for rows.Next() {
		var (
			exp                                        model.Experiment
			desc, hypothesis, createdBy, failureReason sql.NullString
			startedAt, completedAt                     sql.NullTime
		)
		if err := rows.Scan(
			&exp.ID, &exp.Name, &desc, &hypothesis, &exp.Status, &createdBy,
			&exp.CreatedAt, &startedAt, &completedAt, &failureReason, &total,
		); err != nil {
			return nil, 0, fmt.Errorf("scan experiment: %w", err)
		}
		exp.Description = fromNullString(desc)
		exp.Hypothesis = fromNullString(hypothesis)
		exp.CreatedBy = fromNullString(createdBy)
		exp.StartedAt = nullTimeToPtr(startedAt)
		exp.CompletedAt = nullTimeToPtr(completedAt)
		exp.FailureReason = fromNullString(failureReason)
		result = append(result, &exp)
	}
	return result, total, rows.Err()
}

// Delete removes an experiment and cascades to its workflows/phases/results
// via FK ON DELETE CASCADE.
func (r *ExperimentRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM experiments WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete experiment: %w", err)
	}
	return affectedOrNotFound(res)
}

// UpdateStatus changes the experiment status and stamps started_at /
// completed_at as appropriate.
func (r *ExperimentRepo) UpdateStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiments SET
			status       = $2::experiment_status,
			started_at   = CASE WHEN $2 = 'running'                            AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','cancelled') THEN now()              ELSE completed_at END
		WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update experiment status: %w", err)
	}
	return affectedOrNotFound(res)
}

// TransitionExperiment atomically moves the experiment to `to` only when its
// current status is one of `from`. Returns whether the transition happened —
// the orchestrator FSM's compare-and-swap primitive: exactly one of N racing
// callers wins, and only the winner runs side effects. Timestamps are stamped
// like UpdateStatus.
func (r *ExperimentRepo) TransitionExperiment(ctx context.Context, id, to string, from ...string) (bool, error) {
	guard, args, err := fromStatusGuard(id, to, from)
	if err != nil {
		return false, fmt.Errorf("transition experiment: %w", err)
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiments SET
			status       = $2::experiment_status,
			started_at   = CASE WHEN $2 = 'running'                            AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','cancelled') THEN now()              ELSE completed_at END
		WHERE id = $1 AND status::text IN `+guard, args...)
	if err != nil {
		return false, fmt.Errorf("transition experiment: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("transition experiment: rows affected: %w", err)
	}
	return n > 0, nil
}

// TransitionPhase is the phase-level CAS twin of TransitionExperiment.
func (r *ExperimentRepo) TransitionPhase(ctx context.Context, id, to string, from ...string) (bool, error) {
	guard, args, err := fromStatusGuard(id, to, from)
	if err != nil {
		return false, fmt.Errorf("transition phase: %w", err)
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiment_phases SET
			status       = $2::phase_status,
			started_at   = CASE WHEN $2 = 'running'                              AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','skipped') THEN now()                  ELSE completed_at END
		WHERE id = $1 AND status::text IN `+guard, args...)
	if err != nil {
		return false, fmt.Errorf("transition phase: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("transition phase: rows affected: %w", err)
	}
	return n > 0, nil
}

// FailExperiment is TransitionExperiment to 'failed' that records why in the
// same statement, so an experiment can never be failed without its reason
// (migration 11). Same CAS contract: true only for the caller that moved the
// row out of a `from` status; a loser writes neither the status nor the
// reason. An empty reason is stored as NULL.
func (r *ExperimentRepo) FailExperiment(ctx context.Context, id, reason string, from ...string) (bool, error) {
	guard, args, err := fromStatusGuard(id, reason, from)
	if err != nil {
		return false, fmt.Errorf("fail experiment: %w", err)
	}
	args[1] = nullString(reason)
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiments SET
			status         = 'failed',
			failure_reason = $2,
			completed_at   = now()
		WHERE id = $1 AND status::text IN `+guard, args...)
	if err != nil {
		return false, fmt.Errorf("fail experiment: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("fail experiment: rows affected: %w", err)
	}
	return n > 0, nil
}

// FailPhase is the phase-level twin of FailExperiment: TransitionPhase to
// 'failed' plus the reason, in one statement.
func (r *ExperimentRepo) FailPhase(ctx context.Context, id, reason string, from ...string) (bool, error) {
	guard, args, err := fromStatusGuard(id, reason, from)
	if err != nil {
		return false, fmt.Errorf("fail phase: %w", err)
	}
	args[1] = nullString(reason)
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiment_phases SET
			status         = 'failed',
			failure_reason = $2,
			completed_at   = now()
		WHERE id = $1 AND status::text IN `+guard, args...)
	if err != nil {
		return false, fmt.Errorf("fail phase: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("fail phase: rows affected: %w", err)
	}
	return n > 0, nil
}

// fromStatusGuard builds the parameterized "($3, $4, ...)" from-set guard and
// the full args slice (id, second, from...) for the Transition* / Fail* CAS
// updates: $2 is the target status for Transition* and the failure reason
// for Fail* (whose target status is the literal 'failed').
func fromStatusGuard(id, second string, from []string) (string, []any, error) {
	if len(from) == 0 {
		return "", nil, fmt.Errorf("empty from-set")
	}
	args := make([]any, 0, len(from)+2)
	args = append(args, id, second)
	ph := make([]string, len(from))
	for i, f := range from {
		ph[i] = fmt.Sprintf("$%d", i+3)
		args = append(args, f)
	}
	return "(" + strings.Join(ph, ", ") + ")", args, nil
}

// UpdateMetadata edits the non-state fields of an experiment. A nil argument
// leaves that column untouched; an empty description / hypothesis clears it
// (NULL, as on Create). Name is required, so an empty one is rejected rather
// than written. The planned-only guard is the handler's.
func (r *ExperimentRepo) UpdateMetadata(ctx context.Context, id string, name, description, hypothesis *string) error {
	sets := []string{}
	args := []any{id}
	set := func(col string, v any) {
		args = append(args, v)
		sets = append(sets, fmt.Sprintf("%s = $%d", col, len(args)))
	}
	if name != nil {
		if *name == "" {
			return validationError(errors.New("experiment: name required"))
		}
		set("name", *name)
	}
	if description != nil {
		set("description", nullString(*description))
	}
	if hypothesis != nil {
		set("hypothesis", nullString(*hypothesis))
	}
	if len(sets) == 0 {
		return nil
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE experiments SET `+strings.Join(sets, ", ")+` WHERE id = $1`, args...)
	if err != nil {
		return fmt.Errorf("update experiment metadata: %w", err)
	}
	return affectedOrNotFound(res)
}

// scanExperimentRow scans one experiments row.
func scanExperimentRow(scanner interface {
	Scan(dest ...any) error
}) (*model.Experiment, error) {
	var (
		exp                                        model.Experiment
		desc, hypothesis, createdBy, failureReason sql.NullString
		startedAt, completedAt                     sql.NullTime
	)
	err := scanner.Scan(
		&exp.ID, &exp.Name, &desc, &hypothesis, &exp.Status, &createdBy,
		&exp.CreatedAt, &startedAt, &completedAt, &failureReason,
	)
	if err != nil {
		return nil, err
	}
	exp.Description = fromNullString(desc)
	exp.Hypothesis = fromNullString(hypothesis)
	exp.CreatedBy = fromNullString(createdBy)
	exp.StartedAt = nullTimeToPtr(startedAt)
	exp.CompletedAt = nullTimeToPtr(completedAt)
	exp.FailureReason = fromNullString(failureReason)
	return &exp, nil
}

// =========================================================================
// ExperimentPhase
// =========================================================================

// CreatePhase inserts one phase row. Caller is responsible for assigning
// the position (or letting NextPhasePosition do it).
func (r *ExperimentRepo) CreatePhase(ctx context.Context, p *model.ExperimentPhase) error {
	if err := p.Validate(); err != nil {
		return validationError(err)
	}
	frozenJSON, err := json.Marshal(p.FrozenServices)
	if err != nil {
		return fmt.Errorf("marshal frozen_services: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO experiment_phases (id, experiment_id, name, position, status,
			frozen_services, persist_cache, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		p.ID, p.ExperimentID, p.Name, p.Position, p.Status,
		frozenJSON, p.PersistCache, p.StartedAt, p.CompletedAt,
	)
	if err != nil {
		if code, constraint := pgViolation(err); code == pgForeignKeyViolation && constraint == "experiment_phases_experiment_id_fkey" {
			return fmt.Errorf("experiment %q: %w", p.ExperimentID, ErrNotFound)
		}
		return phaseConflict(fmt.Errorf("insert experiment_phase: %w", err), p)
	}
	return nil
}

// GetPhase returns one phase by id.
func (r *ExperimentRepo) GetPhase(ctx context.Context, id string) (*model.ExperimentPhase, error) {
	p, err := scanPhaseRow(r.db.QueryRowContext(ctx, `
		SELECT id, experiment_id, name, position, status, frozen_services,
			persist_cache, started_at, completed_at, failure_reason
		FROM experiment_phases WHERE id = $1`, id))
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment_phase: %w", err)
	}
	return p, nil
}

// PhaseEdit carries the join-row replacements applied atomically with an
// UpdatePhase row write. A nil slice leaves that join table untouched; an
// empty non-nil slice clears it.
type PhaseEdit struct {
	Workflows []model.PhaseWorkflow
	RuleIDs   []string
}

// UpdatePhase rewrites a phase's editable columns (name, position,
// frozen_services, persist_cache) and, for each non-nil PhaseEdit slice,
// replaces its phase_workflows / phase_rules rows — all in one transaction,
// so a rejected attachment leaves the row untouched. The status guards
// (pending phase, planned experiment) are the handler's; this is the write.
// Unique-constraint collisions (name, position, duplicate ids) surface as
// ErrConflict; the position constraint is deferred, so that one is only
// raised at commit.
func (r *ExperimentRepo) UpdatePhase(ctx context.Context, p *model.ExperimentPhase, edit PhaseEdit) error {
	if err := p.Validate(); err != nil {
		return validationError(err)
	}
	frozenJSON, err := json.Marshal(p.FrozenServices)
	if err != nil {
		return fmt.Errorf("marshal frozen_services: %w", err)
	}
	err = execTx(ctx, r.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE experiment_phases SET
				name = $2, position = $3, frozen_services = $4, persist_cache = $5
			WHERE id = $1`,
			p.ID, p.Name, p.Position, frozenJSON, p.PersistCache)
		if err != nil {
			return fmt.Errorf("update experiment_phase: %w", err)
		}
		if err := affectedOrNotFound(res); err != nil {
			return err
		}
		if edit.Workflows != nil {
			if err := attachPhaseWorkflows(ctx, tx, p.ID, edit.Workflows); err != nil {
				return err
			}
		}
		if edit.RuleIDs != nil {
			if err := attachPhaseRules(ctx, tx, p.ID, edit.RuleIDs); err != nil {
				return err
			}
		}
		return nil
	})
	return phaseConflict(err, p)
}

// phaseConflict maps a unique_violation raised by a phase write to
// ErrConflict, naming the colliding column; any other error passes through.
func phaseConflict(err error, p *model.ExperimentPhase) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return err
	}
	switch pgErr.ConstraintName {
	case "experiment_phases_experiment_id_name_key":
		return fmt.Errorf("phase name %q already used in this experiment: %w", p.Name, ErrConflict)
	case "experiment_phases_position_unique":
		return fmt.Errorf("phase position %d already used in this experiment: %w", p.Position, ErrConflict)
	}
	detail := pgErr.Detail
	if detail == "" {
		detail = pgErr.Message
	}
	return fmt.Errorf("%s: %w", detail, ErrConflict)
}

// RunningExperimentIDs returns the ids of every experiment currently in status
// 'running' — the set the admission controller checks a starting experiment's
// service footprint against (MANT-5).
func (r *ExperimentRepo) RunningExperimentIDs(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id FROM experiments WHERE status = 'running'`)
	if err != nil {
		return nil, fmt.Errorf("list running experiments: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan running experiment: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ListRunningPhases returns every phase currently in status 'running' across
// all experiments (newest-started first), each with its frozen_services — the
// input to per-service cache-box rule synthesis (MANT-4). Small in practice:
// at most one running phase per active experiment.
func (r *ExperimentRepo) ListRunningPhases(ctx context.Context) ([]*model.ExperimentPhase, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_id, name, position, status, frozen_services,
			persist_cache, started_at, completed_at, failure_reason
		FROM experiment_phases
		WHERE status = 'running'
		ORDER BY started_at DESC NULLS LAST`)
	if err != nil {
		return nil, fmt.Errorf("list running phases: %w", err)
	}
	defer rows.Close()
	var out []*model.ExperimentPhase
	for rows.Next() {
		p, err := scanPhaseRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan running phase: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ActiveCacheBoxPhaseForService resolves the cache-box role a service is
// currently playing, if any — the context from which the poll path synthesizes
// the service's compiled cache-box rule (MANT-4, replacing the deleted global
// RecordingPhaseID signal). Provenance is phase-scoped (INV-5): there is no
// ambient "most recently started phase" fallback.
//
//   - Replay: the service is frozen in a running phase → its freeze mode +
//     strategy (an isolation phase).
//   - Record: the service is frozen somewhere in an experiment that has a
//     running persist_cache (baseline) phase → passthrough into that baseline,
//     so exactly the services that will be replayed are recorded.
//
// Returns nil when the service has no active cache-box role.
func (r *ExperimentRepo) ActiveCacheBoxPhaseForService(ctx context.Context, service string) (*model.CacheBoxRuleContext, error) {
	running, err := r.ListRunningPhases(ctx)
	if err != nil {
		return nil, err
	}

	baselineByExp := make(map[string]*model.ExperimentPhase)
	var baselineExpIDs []string
	for _, p := range running {
		for i := range p.FrozenServices {
			if p.FrozenServices[i].Service == service {
				return cacheBoxRuleContext(p.ExperimentID, p.ID, p.FrozenServices[i]), nil
			}
		}
		if p.PersistCache {
			if _, ok := baselineByExp[p.ExperimentID]; !ok {
				baselineByExp[p.ExperimentID] = p
				baselineExpIDs = append(baselineExpIDs, p.ExperimentID)
			}
		}
	}

	for _, expID := range baselineExpIDs {
		phases, err := r.ListPhasesForExperiment(ctx, expID)
		if err != nil {
			return nil, err
		}
		for _, ph := range phases {
			for i := range ph.FrozenServices {
				if ph.FrozenServices[i].Service == service {
					bp := baselineByExp[expID]
					rc := cacheBoxRuleContext(bp.ExperimentID, bp.ID, ph.FrozenServices[i])
					rc.Mode = "passthrough" // record on the baseline, regardless of the isolation freeze mode
					return rc, nil
				}
			}
		}
	}
	return nil, nil
}

// cacheBoxRuleContext builds a resolved rule context from a frozen-service
// config, defaulting the key strategy and deriving its wire version.
func cacheBoxRuleContext(experimentID, phaseID string, fs model.CacheBoxConfig) *model.CacheBoxRuleContext {
	strat := model.ResolveKeyStrategy(fs.KeyStrategy)
	return &model.CacheBoxRuleContext{
		ExperimentID:    experimentID,
		PhaseID:         phaseID,
		Mode:            fs.Mode,
		KeyStrategy:     strat,
		StrategyVersion: model.KeyStrategyVersion(strat),
		KeyHeaders:      fs.KeyHeaders,
	}
}

// UpsertPhaseDrain persists a recording phase's drain outcome (MANT-2),
// idempotent on phase_id so the drain gate can write it exactly once.
func (r *ExperimentRepo) UpsertPhaseDrain(ctx context.Context, phaseID string, result *model.PhaseDrainResult) error {
	detail, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal drain result: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO phase_drain (phase_id, status, detail, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (phase_id) DO UPDATE SET status = $2, detail = $3, updated_at = now()`,
		phaseID, result.Status, detail)
	if err != nil {
		return fmt.Errorf("upsert phase_drain: %w", err)
	}
	return nil
}

// GetPhaseDrain returns a phase's persisted drain outcome, or nil when the phase
// never drained (e.g. an isolation phase, or one that never completed).
func (r *ExperimentRepo) GetPhaseDrain(ctx context.Context, phaseID string) (*model.PhaseDrainResult, error) {
	var detail []byte
	err := r.db.QueryRowContext(ctx,
		`SELECT detail FROM phase_drain WHERE phase_id = $1`, phaseID).Scan(&detail)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get phase_drain: %w", err)
	}
	var out model.PhaseDrainResult
	if len(detail) > 0 {
		if err := json.Unmarshal(detail, &out); err != nil {
			return nil, fmt.Errorf("decode drain detail: %w", err)
		}
	}
	return &out, nil
}

// UpsertPhaseVerdict persists a phase's fidelity verdict (MANT-6), idempotent
// on phase_id so the finish-time computation writes it exactly once.
func (r *ExperimentRepo) UpsertPhaseVerdict(ctx context.Context, phaseID string, v *model.PhaseVerdict) error {
	detail, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal verdict: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO phase_verdict (phase_id, verdict, detail, computed_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (phase_id) DO UPDATE SET verdict = $2, detail = $3, computed_at = now()`,
		phaseID, v.Verdict, detail)
	if err != nil {
		return fmt.Errorf("upsert phase_verdict: %w", err)
	}
	return nil
}

// GetPhaseVerdict returns a phase's stored fidelity verdict, or nil when none
// was computed (e.g. a baseline/non-frozen phase).
func (r *ExperimentRepo) GetPhaseVerdict(ctx context.Context, phaseID string) (*model.PhaseVerdict, error) {
	var detail []byte
	err := r.db.QueryRowContext(ctx,
		`SELECT detail FROM phase_verdict WHERE phase_id = $1`, phaseID).Scan(&detail)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get phase_verdict: %w", err)
	}
	var out model.PhaseVerdict
	if len(detail) > 0 {
		if err := json.Unmarshal(detail, &out); err != nil {
			return nil, fmt.Errorf("decode verdict detail: %w", err)
		}
	}
	return &out, nil
}

// ListPhasesForExperiment returns phases ordered by position.
func (r *ExperimentRepo) ListPhasesForExperiment(ctx context.Context, experimentID string) ([]*model.ExperimentPhase, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, experiment_id, name, position, status, frozen_services,
			persist_cache, started_at, completed_at, failure_reason
		FROM experiment_phases
		WHERE experiment_id = $1
		ORDER BY position`, experimentID)
	if err != nil {
		return nil, fmt.Errorf("list experiment_phases: %w", err)
	}
	defer rows.Close()
	var out []*model.ExperimentPhase
	for rows.Next() {
		p, err := scanPhaseRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan experiment_phase: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ListPhasesPaged returns a cross-experiment page of phase list rows (the
// "runs" list), newest-started first, each with its experiment name, workflow
// ids, and frozen-service count. workflow_ids is aggregated as JSONB to avoid
// PG-array scanning.
func (r *ExperimentRepo) ListPhasesPaged(ctx context.Context, p Page) ([]*model.PhaseListItem, int, error) {
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
		SELECT p.id, p.experiment_id, e.name, p.name, p.position, p.status,
			p.started_at, p.completed_at, p.failure_reason,
			CASE WHEN jsonb_typeof(p.frozen_services) = 'array'
				THEN jsonb_array_length(p.frozen_services)
				ELSE 0
			END AS frozen_service_count,
			COALESCE(jsonb_agg(pw.workflow_id ORDER BY pw.workflow_id)
				FILTER (WHERE pw.workflow_id IS NOT NULL), '[]'::jsonb) AS workflow_ids,
			COUNT(*) OVER () AS total_count
		FROM experiment_phases p
		JOIN experiments e ON e.id = p.experiment_id
		LEFT JOIN phase_workflows pw ON pw.phase_id = p.id
		GROUP BY p.id, e.name
		ORDER BY p.started_at DESC NULLS LAST, p.position
		LIMIT $1 OFFSET $2`, p.Limit, p.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list phases paged: %w", err)
	}
	defer rows.Close()
	var (
		out   []*model.PhaseListItem
		total int
	)
	for rows.Next() {
		var (
			it                     model.PhaseListItem
			startedAt, completedAt sql.NullTime
			failureReason          sql.NullString
			workflowIDs            []byte
		)
		if err := rows.Scan(
			&it.ID, &it.ExperimentID, &it.ExperimentName, &it.Name, &it.Position, &it.Status,
			&startedAt, &completedAt, &failureReason, &it.FrozenServiceCount, &workflowIDs, &total,
		); err != nil {
			return nil, 0, fmt.Errorf("scan phase list item: %w", err)
		}
		it.StartedAt = nullTimeToPtr(startedAt)
		it.CompletedAt = nullTimeToPtr(completedAt)
		it.FailureReason = fromNullString(failureReason)
		if len(workflowIDs) > 0 {
			if err := json.Unmarshal(workflowIDs, &it.WorkflowIDs); err != nil {
				return nil, 0, fmt.Errorf("decode workflow_ids: %w", err)
			}
		}
		out = append(out, &it)
	}
	return out, total, rows.Err()
}

// RecentPhaseIDsForService returns the ids of the most recent phases whose
// footprint includes service — frozen in the phase's cache-box set, or
// targeted by a rule the phase attaches — newest-started first (the order
// ListPhasesPaged uses), never-started phases after, newest-created first
// among ties (ids are uuidv7). Backs the SDK instance detail's recent_run_ids.
func (r *ExperimentRepo) RecentPhaseIDsForService(ctx context.Context, service string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 5
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT p.id
		FROM experiment_phases p
		WHERE p.frozen_services @> jsonb_build_array(jsonb_build_object('service', $1::text))
		   OR EXISTS (SELECT 1 FROM phase_rules pr
		              JOIN rules r ON r.id = pr.rule_id
		              WHERE pr.phase_id = p.id AND r.service = $1::text)
		ORDER BY p.started_at DESC NULLS LAST, p.id DESC
		LIMIT $2`, service, limit)
	if err != nil {
		return nil, fmt.Errorf("recent phases for service: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan recent phase id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// DeletePhase removes one phase and cascades to its workflows/rules/results.
func (r *ExperimentRepo) DeletePhase(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM experiment_phases WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete experiment_phase: %w", err)
	}
	return affectedOrNotFound(res)
}

// UpdatePhaseStatus stamps started_at / completed_at as appropriate.
func (r *ExperimentRepo) UpdatePhaseStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE experiment_phases SET
			status       = $2::phase_status,
			started_at   = CASE WHEN $2 = 'running'                              AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $2 IN ('completed','failed','skipped') THEN now()                  ELSE completed_at END
		WHERE id = $1`, id, status)
	if err != nil {
		return fmt.Errorf("update phase status: %w", err)
	}
	return affectedOrNotFound(res)
}

// NextPhasePosition returns the next free position for an experiment.
func (r *ExperimentRepo) NextPhasePosition(ctx context.Context, experimentID string) (int, error) {
	var next sql.NullInt32
	err := r.db.QueryRowContext(ctx,
		`SELECT MAX(position) + 1 FROM experiment_phases WHERE experiment_id = $1`,
		experimentID).Scan(&next)
	if err != nil {
		return 0, fmt.Errorf("next phase position: %w", err)
	}
	if !next.Valid {
		return 0, nil
	}
	return int(next.Int32), nil
}

func scanPhaseRow(scanner interface {
	Scan(dest ...any) error
}) (*model.ExperimentPhase, error) {
	var (
		p                      model.ExperimentPhase
		frozenJSON             []byte
		startedAt, completedAt sql.NullTime
		failureReason          sql.NullString
	)
	if err := scanner.Scan(
		&p.ID, &p.ExperimentID, &p.Name, &p.Position, &p.Status, &frozenJSON,
		&p.PersistCache, &startedAt, &completedAt, &failureReason,
	); err != nil {
		return nil, err
	}
	if len(frozenJSON) > 0 {
		if err := json.Unmarshal(frozenJSON, &p.FrozenServices); err != nil {
			return nil, fmt.Errorf("decode frozen_services: %w", err)
		}
	}
	p.StartedAt = nullTimeToPtr(startedAt)
	p.CompletedAt = nullTimeToPtr(completedAt)
	p.FailureReason = fromNullString(failureReason)
	return &p, nil
}

// =========================================================================
// PhaseWorkflow
// =========================================================================

// AttachPhaseWorkflows replaces the phase_workflows rows for the phase with
// the supplied slice. Use this on phase create / edit to keep the set in sync.
func (r *ExperimentRepo) AttachPhaseWorkflows(ctx context.Context, phaseID string, pws []model.PhaseWorkflow) error {
	return execTx(ctx, r.db, func(tx *sql.Tx) error {
		return attachPhaseWorkflows(ctx, tx, phaseID, pws)
	})
}

// attachPhaseWorkflows is the delete-reinsert body of AttachPhaseWorkflows,
// run inside the caller's transaction so UpdatePhase can compose it.
func attachPhaseWorkflows(ctx context.Context, tx *sql.Tx, phaseID string, pws []model.PhaseWorkflow) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM phase_workflows WHERE phase_id = $1`, phaseID); err != nil {
		return fmt.Errorf("clear phase_workflows: %w", err)
	}
	for i, pw := range pws {
		pw.PhaseID = phaseID
		if err := (&pw).Validate(); err != nil {
			return validationError(fmt.Errorf("phase_workflows[%d]: %w", i, err))
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO phase_workflows (phase_id, workflow_id, vus, rate_rps,
				duration_sec, target_url, target_method, zeus_attack_id, zeus_run_id,
				dataset_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			pw.PhaseID, pw.WorkflowID, pw.VUs, nullFloat(pw.RateRPS),
			pw.DurationSec, nullString(pw.TargetURL), nullString(pw.TargetMethod),
			nullString(pw.ZeusAttackID), nullString(pw.ZeusRunID),
			nullString(pw.DatasetID),
		); err != nil {
			switch code, constraint := pgViolation(err); {
			case code == pgForeignKeyViolation && constraint == "phase_workflows_workflow_id_fkey":
				return validationError(fmt.Errorf("phase_workflows[%d]: unknown workflow_id %q", i, pw.WorkflowID))
			case code == pgUniqueViolation:
				// The phase's rows were just cleared, so the collision is within this list.
				return validationError(fmt.Errorf("phase_workflows[%d]: duplicate workflow_id %q", i, pw.WorkflowID))
			}
			return fmt.Errorf("insert phase_workflow: %w", err)
		}
	}
	return nil
}

// ListPhaseWorkflows returns all phase_workflows rows for the phase.
func (r *ExperimentRepo) ListPhaseWorkflows(ctx context.Context, phaseID string) ([]model.PhaseWorkflow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, workflow_id, vus, rate_rps, duration_sec,
			target_url, target_method, zeus_attack_id, zeus_run_id, dataset_id
		FROM phase_workflows WHERE phase_id = $1
		ORDER BY workflow_id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_workflows: %w", err)
	}
	defer rows.Close()
	var out []model.PhaseWorkflow
	for rows.Next() {
		var (
			pw                                                model.PhaseWorkflow
			rateRPS                                           sql.NullFloat64
			targetURL, targetMethod, zeusID, runID, datasetID sql.NullString
		)
		if err := rows.Scan(
			&pw.PhaseID, &pw.WorkflowID, &pw.VUs, &rateRPS, &pw.DurationSec,
			&targetURL, &targetMethod, &zeusID, &runID, &datasetID,
		); err != nil {
			return nil, fmt.Errorf("scan phase_workflow: %w", err)
		}
		if rateRPS.Valid {
			pw.RateRPS = rateRPS.Float64
		}
		pw.TargetURL = fromNullString(targetURL)
		pw.TargetMethod = fromNullString(targetMethod)
		pw.ZeusAttackID = fromNullString(zeusID)
		pw.ZeusRunID = fromNullString(runID)
		pw.DatasetID = fromNullString(datasetID)
		out = append(out, pw)
	}
	return out, rows.Err()
}

// UpdatePhaseWorkflowZeusAttack stamps the zeus_attack_id on a single
// phase_workflows row once the orchestrator has registered the attack.
func (r *ExperimentRepo) UpdatePhaseWorkflowZeusAttack(ctx context.Context, phaseID, workflowID, zeusAttackID string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE phase_workflows SET zeus_attack_id = $3
		WHERE phase_id = $1 AND workflow_id = $2`,
		phaseID, workflowID, zeusAttackID)
	if err != nil {
		return fmt.Errorf("update phase_workflow zeus_attack_id: %w", err)
	}
	return affectedOrNotFound(res)
}

// UpdatePhaseWorkflowZeusRun stamps the zeus_run_id on a single
// phase_workflows row once the orchestrator has started the workflow run.
func (r *ExperimentRepo) UpdatePhaseWorkflowZeusRun(ctx context.Context, phaseID, workflowID, zeusRunID string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE phase_workflows SET zeus_run_id = $3
		WHERE phase_id = $1 AND workflow_id = $2`,
		phaseID, workflowID, zeusRunID)
	if err != nil {
		return fmt.Errorf("update phase_workflow zeus_run_id: %w", err)
	}
	return affectedOrNotFound(res)
}

// =========================================================================
// PhaseRule
// =========================================================================

// AttachPhaseRules replaces the phase_rules rows for the phase. Position is
// taken from slice index.
func (r *ExperimentRepo) AttachPhaseRules(ctx context.Context, phaseID string, ruleIDs []string) error {
	return execTx(ctx, r.db, func(tx *sql.Tx) error {
		return attachPhaseRules(ctx, tx, phaseID, ruleIDs)
	})
}

// attachPhaseRules is the delete-reinsert body of AttachPhaseRules, run
// inside the caller's transaction so UpdatePhase can compose it.
func attachPhaseRules(ctx context.Context, tx *sql.Tx, phaseID string, ruleIDs []string) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM phase_rules WHERE phase_id = $1`, phaseID); err != nil {
		return fmt.Errorf("clear phase_rules: %w", err)
	}
	for i, rid := range ruleIDs {
		if rid == "" {
			return validationError(fmt.Errorf("attach phase rules: empty rule_id at position %d", i))
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO phase_rules (phase_id, rule_id, position)
			VALUES ($1, $2, $3)`, phaseID, rid, i); err != nil {
			switch code, constraint := pgViolation(err); {
			case code == pgForeignKeyViolation && constraint == "phase_rules_rule_id_fkey":
				return validationError(fmt.Errorf("rule_ids[%d]: unknown rule_id %q", i, rid))
			case code == pgUniqueViolation:
				return validationError(fmt.Errorf("rule_ids[%d]: duplicate rule_id %q", i, rid))
			}
			return fmt.Errorf("insert phase_rule: %w", err)
		}
	}
	return nil
}

// ListPhaseRules returns phase_rules in position order.
func (r *ExperimentRepo) ListPhaseRules(ctx context.Context, phaseID string) ([]model.PhaseRule, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, rule_id, position
		FROM phase_rules WHERE phase_id = $1
		ORDER BY position`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_rules: %w", err)
	}
	defer rows.Close()
	var out []model.PhaseRule
	for rows.Next() {
		var pr model.PhaseRule
		if err := rows.Scan(&pr.PhaseID, &pr.RuleID, &pr.Position); err != nil {
			return nil, fmt.Errorf("scan phase_rule: %w", err)
		}
		out = append(out, pr)
	}
	return out, rows.Err()
}

// =========================================================================
// Results
// =========================================================================

// UpsertWorkflowResult inserts or updates the (phase, workflow) result row.
func (r *ExperimentRepo) UpsertWorkflowResult(ctx context.Context, res *model.PhaseWorkflowResult) error {
	if err := res.Validate(); err != nil {
		return err
	}
	if res.ComputedAt.IsZero() {
		res.ComputedAt = time.Now()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO phase_workflow_results (phase_id, workflow_id, request_count,
			error_count, error_rate, throughput_rps,
			latency_p50_us, latency_p95_us, latency_p99_us, latency_p999_us,
			computed_at, raw_metrics)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (phase_id, workflow_id) DO UPDATE SET
			request_count   = EXCLUDED.request_count,
			error_count     = EXCLUDED.error_count,
			error_rate      = EXCLUDED.error_rate,
			throughput_rps  = EXCLUDED.throughput_rps,
			latency_p50_us  = EXCLUDED.latency_p50_us,
			latency_p95_us  = EXCLUDED.latency_p95_us,
			latency_p99_us  = EXCLUDED.latency_p99_us,
			latency_p999_us = EXCLUDED.latency_p999_us,
			computed_at     = EXCLUDED.computed_at,
			raw_metrics     = EXCLUDED.raw_metrics`,
		res.PhaseID, res.WorkflowID, res.RequestCount,
		res.ErrorCount, res.ErrorRate, res.ThroughputRPS,
		res.LatencyP50Us, res.LatencyP95Us, res.LatencyP99Us, res.LatencyP999Us,
		res.ComputedAt, res.RawMetrics,
	)
	if err != nil {
		return fmt.Errorf("upsert phase_workflow_results: %w", err)
	}
	return nil
}

// ListWorkflowResultsForPhase returns all workflow results for one phase.
func (r *ExperimentRepo) ListWorkflowResultsForPhase(ctx context.Context, phaseID string) ([]*model.PhaseWorkflowResult, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, workflow_id, request_count, error_count, error_rate,
			throughput_rps, latency_p50_us, latency_p95_us, latency_p99_us, latency_p999_us,
			computed_at, raw_metrics
		FROM phase_workflow_results WHERE phase_id = $1
		ORDER BY workflow_id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_workflow_results: %w", err)
	}
	defer rows.Close()
	var out []*model.PhaseWorkflowResult
	for rows.Next() {
		var res model.PhaseWorkflowResult
		if err := rows.Scan(
			&res.PhaseID, &res.WorkflowID, &res.RequestCount, &res.ErrorCount, &res.ErrorRate,
			&res.ThroughputRPS, &res.LatencyP50Us, &res.LatencyP95Us, &res.LatencyP99Us, &res.LatencyP999Us,
			&res.ComputedAt, &res.RawMetrics,
		); err != nil {
			return nil, fmt.Errorf("scan phase_workflow_result: %w", err)
		}
		out = append(out, &res)
	}
	return out, rows.Err()
}

// UpsertServiceLatency inserts or updates a (phase, service, workflow_id) row.
func (r *ExperimentRepo) UpsertServiceLatency(ctx context.Context, res *model.PhaseServiceLatency) error {
	if err := res.Validate(); err != nil {
		return err
	}
	if res.ComputedAt.IsZero() {
		res.ComputedAt = time.Now()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO phase_service_latency (phase_id, service, workflow_id,
			latency_p50_us, latency_p95_us, latency_p99_us, request_count, computed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (phase_id, service, workflow_id) DO UPDATE SET
			latency_p50_us = EXCLUDED.latency_p50_us,
			latency_p95_us = EXCLUDED.latency_p95_us,
			latency_p99_us = EXCLUDED.latency_p99_us,
			request_count  = EXCLUDED.request_count,
			computed_at    = EXCLUDED.computed_at`,
		res.PhaseID, res.Service, res.WorkflowID,
		res.LatencyP50Us, res.LatencyP95Us, res.LatencyP99Us, res.RequestCount, res.ComputedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert phase_service_latency: %w", err)
	}
	return nil
}

// ListServiceLatencyForPhase returns per-service latency rows for a phase.
func (r *ExperimentRepo) ListServiceLatencyForPhase(ctx context.Context, phaseID string) ([]*model.PhaseServiceLatency, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, service, workflow_id, latency_p50_us, latency_p95_us,
			latency_p99_us, request_count, computed_at
		FROM phase_service_latency WHERE phase_id = $1
		ORDER BY service, workflow_id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_service_latency: %w", err)
	}
	defer rows.Close()
	var out []*model.PhaseServiceLatency
	for rows.Next() {
		var res model.PhaseServiceLatency
		if err := rows.Scan(
			&res.PhaseID, &res.Service, &res.WorkflowID, &res.LatencyP50Us, &res.LatencyP95Us,
			&res.LatencyP99Us, &res.RequestCount, &res.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("scan phase_service_latency: %w", err)
		}
		out = append(out, &res)
	}
	return out, rows.Err()
}

// UpsertServiceCache inserts or updates a (phase, service) cache stats row.
func (r *ExperimentRepo) UpsertServiceCache(ctx context.Context, res *model.PhaseServiceCache) error {
	if err := res.Validate(); err != nil {
		return err
	}
	if res.ComputedAt.IsZero() {
		res.ComputedAt = time.Now()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO phase_service_cache (phase_id, service, cache_hit_rate,
			cache_exact_match, cache_staleness, request_count, recorded_entry_count, computed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (phase_id, service) DO UPDATE SET
			cache_hit_rate       = EXCLUDED.cache_hit_rate,
			cache_exact_match    = EXCLUDED.cache_exact_match,
			cache_staleness      = EXCLUDED.cache_staleness,
			request_count        = EXCLUDED.request_count,
			recorded_entry_count = EXCLUDED.recorded_entry_count,
			computed_at          = EXCLUDED.computed_at`,
		res.PhaseID, res.Service, res.CacheHitRate, res.CacheExactMatch, res.CacheStaleness,
		res.RequestCount, res.RecordedEntryCount, res.ComputedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert phase_service_cache: %w", err)
	}
	return nil
}

// ListServiceCacheForPhase returns per-service cache rows for a phase.
func (r *ExperimentRepo) ListServiceCacheForPhase(ctx context.Context, phaseID string) ([]*model.PhaseServiceCache, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT phase_id, service, cache_hit_rate, cache_exact_match, cache_staleness,
			request_count, recorded_entry_count, computed_at
		FROM phase_service_cache WHERE phase_id = $1
		ORDER BY service`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_service_cache: %w", err)
	}
	defer rows.Close()
	var out []*model.PhaseServiceCache
	for rows.Next() {
		var res model.PhaseServiceCache
		if err := rows.Scan(
			&res.PhaseID, &res.Service, &res.CacheHitRate, &res.CacheExactMatch, &res.CacheStaleness,
			&res.RequestCount, &res.RecordedEntryCount, &res.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("scan phase_service_cache: %w", err)
		}
		out = append(out, &res)
	}
	return out, rows.Err()
}

// =========================================================================
// Experiment-level rollup
// =========================================================================

// GetExperimentResults returns the rollup row for one experiment.
func (r *ExperimentRepo) GetExperimentResults(ctx context.Context, experimentID string) (*model.ExperimentResults, error) {
	var res model.ExperimentResults
	err := r.db.QueryRowContext(ctx, `
		SELECT experiment_id, phase_count, completed_phase_count, total_request_count,
			total_error_count, overall_error_rate, worst_p99_us, worst_p99_phase_id,
			best_p99_us, best_p99_phase_id, computed_at
		FROM experiment_results WHERE experiment_id = $1`, experimentID,
	).Scan(
		&res.ExperimentID, &res.PhaseCount, &res.CompletedPhaseCount, &res.TotalRequestCount,
		&res.TotalErrorCount, &res.OverallErrorRate, &res.WorstP99Us, &res.WorstP99PhaseID,
		&res.BestP99Us, &res.BestP99PhaseID, &res.ComputedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment_results: %w", err)
	}
	return &res, nil
}

// RecomputeExperimentResults aggregates phase results into the rollup row.
// Safe to call any time; idempotent. Returns the freshly computed rollup
// (or nil if the experiment has no completed phases yet).
//
// NOTE on percentiles: worst_p99 / best_p99 are the max / min of per-phase
// p99 values, weighted by request count. This is NOT a true experiment-level
// percentile (which would need histograms); see the data-model doc's
// "percentile-aggregation caveat" section.
func (r *ExperimentRepo) RecomputeExperimentResults(ctx context.Context, experimentID string) (*model.ExperimentResults, error) {
	var (
		phaseCount, completedCount int
		totalReq, totalErr         int64
		worstP99, bestP99          sql.NullInt64
		worstPhase, bestPhase      sql.NullString
	)

	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*),
			COUNT(*) FILTER (WHERE status = 'completed')
		FROM experiment_phases WHERE experiment_id = $1`,
		experimentID,
	).Scan(&phaseCount, &completedCount)
	if err != nil {
		return nil, fmt.Errorf("count phases: %w", err)
	}

	err = r.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(pwr.request_count), 0),
			COALESCE(SUM(pwr.error_count),   0)
		FROM phase_workflow_results pwr
		JOIN experiment_phases p ON p.id = pwr.phase_id
		WHERE p.experiment_id = $1`, experimentID,
	).Scan(&totalReq, &totalErr)
	if err != nil {
		return nil, fmt.Errorf("sum result counters: %w", err)
	}

	// Worst phase: maximum p99 across phases (weighted by request count).
	// We pick the phase row with the highest representative p99; ties broken
	// by request count descending.
	err = r.db.QueryRowContext(ctx, `
		SELECT p.id, MAX(pwr.latency_p99_us)
		FROM phase_workflow_results pwr
		JOIN experiment_phases p ON p.id = pwr.phase_id
		WHERE p.experiment_id = $1
		GROUP BY p.id
		ORDER BY MAX(pwr.latency_p99_us) DESC NULLS LAST, SUM(pwr.request_count) DESC
		LIMIT 1`, experimentID,
	).Scan(&worstPhase, &worstP99)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("compute worst p99: %w", err)
	}

	err = r.db.QueryRowContext(ctx, `
		SELECT p.id, MIN(pwr.latency_p99_us)
		FROM phase_workflow_results pwr
		JOIN experiment_phases p ON p.id = pwr.phase_id
		WHERE p.experiment_id = $1 AND pwr.request_count > 0
		GROUP BY p.id
		ORDER BY MIN(pwr.latency_p99_us) ASC NULLS LAST, SUM(pwr.request_count) DESC
		LIMIT 1`, experimentID,
	).Scan(&bestPhase, &bestP99)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("compute best p99: %w", err)
	}

	// Nothing measured yet — no rollup possible.
	if !worstP99.Valid || !worstPhase.Valid || !bestP99.Valid || !bestPhase.Valid {
		return nil, nil
	}

	errorRate := 0.0
	if totalReq > 0 {
		errorRate = float64(totalErr) / float64(totalReq)
	}

	now := time.Now()
	res := &model.ExperimentResults{
		ExperimentID:        experimentID,
		PhaseCount:          phaseCount,
		CompletedPhaseCount: completedCount,
		TotalRequestCount:   totalReq,
		TotalErrorCount:     totalErr,
		OverallErrorRate:    errorRate,
		WorstP99Us:          worstP99.Int64,
		WorstP99PhaseID:     worstPhase.String,
		BestP99Us:           bestP99.Int64,
		BestP99PhaseID:      bestPhase.String,
		ComputedAt:          now,
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO experiment_results (experiment_id, phase_count, completed_phase_count,
			total_request_count, total_error_count, overall_error_rate,
			worst_p99_us, worst_p99_phase_id, best_p99_us, best_p99_phase_id, computed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (experiment_id) DO UPDATE SET
			phase_count           = EXCLUDED.phase_count,
			completed_phase_count = EXCLUDED.completed_phase_count,
			total_request_count   = EXCLUDED.total_request_count,
			total_error_count     = EXCLUDED.total_error_count,
			overall_error_rate    = EXCLUDED.overall_error_rate,
			worst_p99_us          = EXCLUDED.worst_p99_us,
			worst_p99_phase_id    = EXCLUDED.worst_p99_phase_id,
			best_p99_us           = EXCLUDED.best_p99_us,
			best_p99_phase_id     = EXCLUDED.best_p99_phase_id,
			computed_at           = EXCLUDED.computed_at`,
		res.ExperimentID, res.PhaseCount, res.CompletedPhaseCount,
		res.TotalRequestCount, res.TotalErrorCount, res.OverallErrorRate,
		res.WorstP99Us, res.WorstP99PhaseID, res.BestP99Us, res.BestP99PhaseID, res.ComputedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("upsert experiment_results: %w", err)
	}
	return res, nil
}
