package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"manteion-go/internal/model"
)

// RuleRepo provides CRUD operations for fault injection rules backed by PostgreSQL.
// Every mutation bumps the rule_version counter in the same transaction so that
// SDK polling can use version-based 304 responses.
type RuleRepo struct {
	db *sql.DB
}

// NewRuleRepo creates a new rule repository.
func NewRuleRepo(db *sql.DB) *RuleRepo {
	return &RuleRepo{db: db}
}

// Create inserts a new rule and bumps the rule version.
func (r *RuleRepo) Create(ctx context.Context, rule *model.Rule) error {
	if err := rule.Validate(); err != nil {
		return err
	}

	labels, err := jsonbMarshal(rule.Match.Labels)
	if err != nil {
		return fmt.Errorf("marshal match_labels: %w", err)
	}

	return execTx(ctx, r.db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO rules (id, name, service, enabled, priority, injection_point,
				match_labels, fault_spec_id, fault_composition_id, mode, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			rule.ID, rule.Name, rule.Service, rule.Enabled, rule.Priority,
			nullString(rule.Match.InjectionPoint), labels,
			nullString(rule.FaultSpecID), nullString(rule.FaultCompositionID),
			rule.Mode, rule.CreatedAt, rule.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("insert rule: %w", err)
		}

		return r.bumpVersion(ctx, tx)
	})
}

// Get returns a rule by ID, or ErrNotFound.
func (r *RuleRepo) Get(ctx context.Context, id string) (*model.Rule, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, name, service, enabled, priority, injection_point,
			match_labels, fault_spec_id, fault_composition_id, mode, created_at, updated_at
		FROM rules WHERE id = $1`, id)

	return r.scanRule(row)
}

// Update replaces an existing rule and bumps the rule version.
func (r *RuleRepo) Update(ctx context.Context, rule *model.Rule) error {
	if err := rule.Validate(); err != nil {
		return err
	}

	labels, err := jsonbMarshal(rule.Match.Labels)
	if err != nil {
		return fmt.Errorf("marshal match_labels: %w", err)
	}

	return execTx(ctx, r.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE rules SET name=$2, service=$3, enabled=$4, priority=$5, injection_point=$6,
				match_labels=$7, fault_spec_id=$8, fault_composition_id=$9, mode=$10, updated_at=$11
			WHERE id = $1`,
			rule.ID, rule.Name, rule.Service, rule.Enabled, rule.Priority,
			nullString(rule.Match.InjectionPoint), labels,
			nullString(rule.FaultSpecID), nullString(rule.FaultCompositionID),
			rule.Mode, time.Now(),
		)
		if err != nil {
			return fmt.Errorf("update rule: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}

		return r.bumpVersion(ctx, tx)
	})
}

// Delete removes a rule by ID and bumps the rule version.
func (r *RuleRepo) Delete(ctx context.Context, id string) error {
	return execTx(ctx, r.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM rules WHERE id = $1`, id)
		if err != nil {
			return fmt.Errorf("delete rule: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrNotFound
		}

		return r.bumpVersion(ctx, tx)
	})
}

// List returns all rules ordered by priority (desc) then creation time.
func (r *RuleRepo) List(ctx context.Context) ([]*model.Rule, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, service, enabled, priority, injection_point,
			match_labels, fault_spec_id, fault_composition_id, mode, created_at, updated_at
		FROM rules ORDER BY priority DESC, created_at`)
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	defer rows.Close()

	return r.scanRules(rows)
}

// ForService returns enabled rules for a specific service, ordered by priority.
func (r *RuleRepo) ForService(ctx context.Context, service string) ([]*model.Rule, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, service, enabled, priority, injection_point,
			match_labels, fault_spec_id, fault_composition_id, mode, created_at, updated_at
		FROM rules WHERE service = $1 AND enabled = true
		ORDER BY priority DESC`, service)
	if err != nil {
		return nil, fmt.Errorf("rules for service: %w", err)
	}
	defer rows.Close()

	return r.scanRules(rows)
}

// Version returns the current rule store version number.
func (r *RuleRepo) Version(ctx context.Context) (uint64, error) {
	var v uint64
	err := r.db.QueryRowContext(ctx,
		`SELECT version FROM rule_version WHERE id = 1`).Scan(&v)
	if err != nil {
		return 0, fmt.Errorf("read rule_version: %w", err)
	}
	return v, nil
}

// bumpVersion increments the rule_version counter within a transaction.
func (r *RuleRepo) bumpVersion(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE rule_version SET version = version + 1 WHERE id = 1`)
	if err != nil {
		return fmt.Errorf("bump rule_version: %w", err)
	}
	return nil
}

// scanRule scans a single rule row.
func (r *RuleRepo) scanRule(row *sql.Row) (*model.Rule, error) {
	var rule model.Rule
	var injPoint, faultSpecID, faultCompID sql.NullString
	var labelsJSON []byte

	err := row.Scan(
		&rule.ID, &rule.Name, &rule.Service, &rule.Enabled, &rule.Priority,
		&injPoint, &labelsJSON, &faultSpecID, &faultCompID,
		&rule.Mode, &rule.CreatedAt, &rule.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan rule: %w", err)
	}

	rule.Match.InjectionPoint = fromNullString(injPoint)
	rule.FaultSpecID = fromNullString(faultSpecID)
	rule.FaultCompositionID = fromNullString(faultCompID)

	if labelsJSON != nil {
		if err := jsonbScan(labelsJSON, &rule.Match.Labels); err != nil {
			return nil, fmt.Errorf("unmarshal match_labels: %w", err)
		}
	}

	return &rule, nil
}

// scanRules scans multiple rule rows.
func (r *RuleRepo) scanRules(rows *sql.Rows) ([]*model.Rule, error) {
	var result []*model.Rule
	for rows.Next() {
		var rule model.Rule
		var injPoint, faultSpecID, faultCompID sql.NullString
		var labelsJSON []byte

		err := rows.Scan(
			&rule.ID, &rule.Name, &rule.Service, &rule.Enabled, &rule.Priority,
			&injPoint, &labelsJSON, &faultSpecID, &faultCompID,
			&rule.Mode, &rule.CreatedAt, &rule.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan rule row: %w", err)
		}

		rule.Match.InjectionPoint = fromNullString(injPoint)
		rule.FaultSpecID = fromNullString(faultSpecID)
		rule.FaultCompositionID = fromNullString(faultCompID)

		if labelsJSON != nil {
			if err := jsonbScan(labelsJSON, &rule.Match.Labels); err != nil {
				return nil, fmt.Errorf("unmarshal match_labels: %w", err)
			}
		}

		result = append(result, &rule)
	}
	return result, rows.Err()
}
