package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"manteion-go/internal/model"
)

// DefaultDecisionRetention is the number of decisions PolicyDecisionRepo keeps
// per policy unless WithRetention says otherwise.
const DefaultDecisionRetention = 1000

// decisionChangedPredicate is the SQL condition of a decision that applied a
// change: applied, with a mutation whose outcome is created, updated or
// deleted (a mutations value that is not a JSON array counts as no change).
// The newest row matching it is the cooldown witness: LastChangeAt reads it
// and Record's prune never deletes it, both through this one fragment so they
// cannot drift.
const decisionChangedPredicate = `applied AND EXISTS (
	SELECT 1 FROM jsonb_array_elements(
		CASE WHEN jsonb_typeof(mutations) = 'array' THEN mutations ELSE '[]'::jsonb END) m
	WHERE m->>'outcome' IN ('created', 'updated', 'deleted'))`

// PolicyDecisionRepo persists the OPA harness decision log (policy_decisions,
// migration 15). Retention is per policy: every Record prunes the policy's
// log to the newest keep rows (by evaluated_at, then id), plus the cooldown
// witness (the newest decision matching decisionChangedPredicate) when it is
// older than those, so a restart keeps the cooldown however many unchanged
// or dry-run evaluations followed the last change. Rows go with their policy
// (ON DELETE CASCADE).
type PolicyDecisionRepo struct {
	db   *sql.DB
	keep int
}

// NewPolicyDecisionRepo returns a repository over db keeping
// DefaultDecisionRetention decisions per policy.
func NewPolicyDecisionRepo(db *sql.DB) *PolicyDecisionRepo {
	return &PolicyDecisionRepo{db: db, keep: DefaultDecisionRetention}
}

// WithRetention returns a copy of r that keeps the newest keep decisions per
// policy; keep < 1 means DefaultDecisionRetention.
func (r *PolicyDecisionRepo) WithRetention(keep int) *PolicyDecisionRepo {
	if keep < 1 {
		keep = DefaultDecisionRetention
	}
	cp := *r
	cp.keep = keep
	return &cp
}

// Record inserts d and, in the same transaction, prunes d.PolicyID's log to
// the retention, never deleting the cooldown witness. A nil Mutations is
// stored as []; an empty Error as NULL.
func (r *PolicyDecisionRepo) Record(ctx context.Context, d *model.PolicyDecision) error {
	mutations := []byte(d.Mutations)
	if mutations == nil {
		mutations = []byte("[]")
	}
	return execTx(ctx, r.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO policy_decisions (id, policy_id, evaluated_at, input_digest, mutations, applied, error)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			d.ID, d.PolicyID, d.EvaluatedAt, d.InputDigest, mutations, d.Applied, nullString(d.Error),
		); err != nil {
			if code, _ := pgViolation(err); code == pgUniqueViolation {
				return fmt.Errorf("policy decision id %q already exists: %w", d.ID, ErrConflict)
			}
			return fmt.Errorf("insert policy decision: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM policy_decisions
			WHERE policy_id = $1 AND id NOT IN (
				SELECT id FROM policy_decisions WHERE policy_id = $1
				ORDER BY evaluated_at DESC, id DESC LIMIT $2)
			AND id NOT IN (
				SELECT id FROM policy_decisions WHERE policy_id = $1 AND `+decisionChangedPredicate+`
				ORDER BY evaluated_at DESC, id DESC LIMIT 1)`,
			d.PolicyID, r.keep,
		); err != nil {
			return fmt.Errorf("prune policy decisions for %s: %w", d.PolicyID, err)
		}
		return nil
	})
}

// List returns a page of policyID's decisions, newest first (evaluated_at,
// then id, descending), and the policy's total decision count. The page is
// clamped like ListPhasesPaged: limit <= 0 → 20, > 200 → 200, offset < 0 → 0.
// No rows yields a non-nil empty slice and total 0.
func (r *PolicyDecisionRepo) List(ctx context.Context, policyID string, p Page) ([]*model.PolicyDecision, int, error) {
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
		SELECT id, policy_id, evaluated_at, input_digest, mutations, applied, error,
			COUNT(*) OVER () AS total_count
		FROM policy_decisions
		WHERE policy_id = $1
		ORDER BY evaluated_at DESC, id DESC
		LIMIT $2 OFFSET $3`, policyID, p.Limit, p.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list policy decisions: %w", err)
	}
	defer rows.Close()
	out := []*model.PolicyDecision{}
	total := 0
	for rows.Next() {
		var (
			d         model.PolicyDecision
			mutations []byte
			errText   sql.NullString
		)
		if err := rows.Scan(&d.ID, &d.PolicyID, &d.EvaluatedAt, &d.InputDigest, &mutations,
			&d.Applied, &errText, &total); err != nil {
			return nil, 0, fmt.Errorf("scan policy decision: %w", err)
		}
		d.Mutations = mutations
		d.Error = fromNullString(errText)
		out = append(out, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("list policy decisions: %w", err)
	}
	return out, total, nil
}

// LastChangeAt returns the evaluated_at of policyID's newest decision
// matching decisionChangedPredicate (applied, with a created, updated or
// deleted mutation): the cooldown witness, which Record's prune keeps. ok is
// false when there is none.
func (r *PolicyDecisionRepo) LastChangeAt(ctx context.Context, policyID string) (time.Time, bool, error) {
	var at time.Time
	err := r.db.QueryRowContext(ctx, `
		SELECT evaluated_at FROM policy_decisions
		WHERE policy_id = $1 AND `+decisionChangedPredicate+`
		ORDER BY evaluated_at DESC, id DESC LIMIT 1`, policyID).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("last change for policy %s: %w", policyID, err)
	}
	return at, true, nil
}
