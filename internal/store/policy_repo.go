package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"manteion-go/internal/model"
)

// PolicyRepo provides persistence for metric-triggered policy rules.
type PolicyRepo struct {
	db *sql.DB
}

// NewPolicyRepo creates a new policy repository.
func NewPolicyRepo(db *sql.DB) *PolicyRepo {
	return &PolicyRepo{db: db}
}

// Create inserts a new policy rule. Condition and Action are stored as JSONB.
func (r *PolicyRepo) Create(ctx context.Context, rule *model.PolicyRule) error {
	if err := rule.Validate(); err != nil {
		return err
	}

	condJSON, err := json.Marshal(rule.Condition)
	if err != nil {
		return fmt.Errorf("marshal condition: %w", err)
	}
	actionJSON, err := json.Marshal(rule.Action)
	if err != nil {
		return fmt.Errorf("marshal action: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO policy_rules (id, name, enabled, condition, action, cooldown_ns, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		rule.ID, rule.Name, rule.Enabled, condJSON, actionJSON,
		int64(rule.Cooldown), rule.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert policy_rule: %w", err)
	}
	return nil
}

// Get returns a policy rule by ID, or ErrNotFound.
func (r *PolicyRepo) Get(ctx context.Context, id string) (*model.PolicyRule, error) {
	var rule model.PolicyRule
	var condJSON, actionJSON []byte
	var cooldownNs int64

	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, enabled, condition, action, cooldown_ns, created_at
		FROM policy_rules WHERE id = $1`, id,
	).Scan(
		&rule.ID, &rule.Name, &rule.Enabled, &condJSON, &actionJSON,
		&cooldownNs, &rule.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get policy_rule: %w", err)
	}

	if err := json.Unmarshal(condJSON, &rule.Condition); err != nil {
		return nil, fmt.Errorf("unmarshal condition: %w", err)
	}
	if err := json.Unmarshal(actionJSON, &rule.Action); err != nil {
		return nil, fmt.Errorf("unmarshal action: %w", err)
	}
	rule.Cooldown = durationFromNs(cooldownNs)

	return &rule, nil
}

// List returns all policy rules.
func (r *PolicyRepo) List(ctx context.Context) ([]*model.PolicyRule, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, enabled, condition, action, cooldown_ns, created_at
		FROM policy_rules ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list policy_rules: %w", err)
	}
	defer rows.Close()

	return r.scanPolicyRules(rows)
}

// ListEnabled returns only enabled policy rules.
func (r *PolicyRepo) ListEnabled(ctx context.Context) ([]*model.PolicyRule, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, enabled, condition, action, cooldown_ns, created_at
		FROM policy_rules WHERE enabled = true
		ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list enabled policy_rules: %w", err)
	}
	defer rows.Close()

	return r.scanPolicyRules(rows)
}

// Delete removes a policy rule by ID.
func (r *PolicyRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM policy_rules WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete policy_rule: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *PolicyRepo) scanPolicyRules(rows *sql.Rows) ([]*model.PolicyRule, error) {
	var result []*model.PolicyRule
	for rows.Next() {
		var rule model.PolicyRule
		var condJSON, actionJSON []byte
		var cooldownNs int64

		if err := rows.Scan(
			&rule.ID, &rule.Name, &rule.Enabled, &condJSON, &actionJSON,
			&cooldownNs, &rule.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan policy_rule: %w", err)
		}

		if err := json.Unmarshal(condJSON, &rule.Condition); err != nil {
			return nil, fmt.Errorf("unmarshal condition: %w", err)
		}
		if err := json.Unmarshal(actionJSON, &rule.Action); err != nil {
			return nil, fmt.Errorf("unmarshal action: %w", err)
		}
		rule.Cooldown = durationFromNs(cooldownNs)

		result = append(result, &rule)
	}
	return result, rows.Err()
}
