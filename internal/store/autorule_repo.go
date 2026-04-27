package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"manteion-go/internal/model"
)

// AutoRuleRepo provides persistence for metric-triggered auto rules.
type AutoRuleRepo struct {
	db *sql.DB
}

// NewAutoRuleRepo creates a new auto rule repository.
func NewAutoRuleRepo(db *sql.DB) *AutoRuleRepo {
	return &AutoRuleRepo{db: db}
}

// Create inserts a new auto rule. Condition and Action are stored as JSONB.
func (r *AutoRuleRepo) Create(ctx context.Context, rule *model.AutoRule) error {
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
		INSERT INTO auto_rules (id, name, enabled, condition, action, cooldown_ns, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		rule.ID, rule.Name, rule.Enabled, condJSON, actionJSON,
		int64(rule.Cooldown), rule.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert auto_rule: %w", err)
	}
	return nil
}

// Get returns an auto rule by ID, or ErrNotFound.
func (r *AutoRuleRepo) Get(ctx context.Context, id string) (*model.AutoRule, error) {
	var rule model.AutoRule
	var condJSON, actionJSON []byte
	var cooldownNs int64

	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, enabled, condition, action, cooldown_ns, created_at
		FROM auto_rules WHERE id = $1`, id,
	).Scan(
		&rule.ID, &rule.Name, &rule.Enabled, &condJSON, &actionJSON,
		&cooldownNs, &rule.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get auto_rule: %w", err)
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

// List returns all auto rules.
func (r *AutoRuleRepo) List(ctx context.Context) ([]*model.AutoRule, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, enabled, condition, action, cooldown_ns, created_at
		FROM auto_rules ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list auto_rules: %w", err)
	}
	defer rows.Close()

	return r.scanAutoRules(rows)
}

// ListEnabled returns only enabled auto rules.
func (r *AutoRuleRepo) ListEnabled(ctx context.Context) ([]*model.AutoRule, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, enabled, condition, action, cooldown_ns, created_at
		FROM auto_rules WHERE enabled = true
		ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list enabled auto_rules: %w", err)
	}
	defer rows.Close()

	return r.scanAutoRules(rows)
}

// Delete removes an auto rule by ID.
func (r *AutoRuleRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM auto_rules WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete auto_rule: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *AutoRuleRepo) scanAutoRules(rows *sql.Rows) ([]*model.AutoRule, error) {
	var result []*model.AutoRule
	for rows.Next() {
		var rule model.AutoRule
		var condJSON, actionJSON []byte
		var cooldownNs int64

		if err := rows.Scan(
			&rule.ID, &rule.Name, &rule.Enabled, &condJSON, &actionJSON,
			&cooldownNs, &rule.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan auto_rule: %w", err)
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

// durationFromNs converts nanoseconds stored in DB to time.Duration.
func durationFromNs(ns int64) time.Duration {
	return time.Duration(ns)
}
