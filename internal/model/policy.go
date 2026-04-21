package model

import (
	"errors"
	"fmt"
	"time"
)

// PolicyRule defines a metric-triggered action. Evaluates conditions on a tick
// interval and launches attacks (or cache-box mode changes) when conditions are met.
// Ownership migrated from zeus-go's Archer to manteion.
type PolicyRule struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Enabled   bool            `json:"enabled"`
	Condition PolicyCondition `json:"condition"`
	Action    PolicyAction    `json:"action"`
	Cooldown  time.Duration   `json:"cooldown"`
	CreatedAt time.Time       `json:"created_at"`
}

func (r *PolicyRule) Validate() error {
	if r.ID == "" {
		return errors.New("policy rule: id required")
	}
	if r.Name == "" {
		return errors.New("policy rule: name required")
	}
	if err := r.Condition.Validate(); err != nil {
		return fmt.Errorf("policy rule: %w", err)
	}
	if err := r.Action.Validate(); err != nil {
		return fmt.Errorf("policy rule: %w", err)
	}
	if r.Cooldown < 0 {
		return errors.New("policy rule: cooldown must be non-negative")
	}
	return nil
}

// PolicyCondition is a threshold check against a named metric.
type PolicyCondition struct {
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator"` // gt, gte, lt, lte, eq
	Threshold float64 `json:"threshold"`
}

var validOperators = map[string]bool{
	"gt": true, "gte": true, "lt": true, "lte": true, "eq": true,
}

func (c *PolicyCondition) Validate() error {
	if c.Metric == "" {
		return errors.New("condition: metric required")
	}
	if !validOperators[c.Operator] {
		return fmt.Errorf("condition: invalid operator %q", c.Operator)
	}
	return nil
}

// PolicyAction describes what happens when a condition fires.
type PolicyAction struct {
	ActionType     string            `json:"action_type"` // "attack" or "cachebox_mode_change"
	AttackTarget   *AttackTargetSpec `json:"attack_target,omitempty"`
	CacheBoxChange *CacheBoxConfig   `json:"cachebox_change,omitempty"`
}

func (a *PolicyAction) Validate() error {
	switch a.ActionType {
	case "attack":
		if a.AttackTarget == nil {
			return errors.New("action: attack_target required for action_type=attack")
		}
		return a.AttackTarget.Validate()
	case "cachebox_mode_change":
		if a.CacheBoxChange == nil {
			return errors.New("action: cachebox_change required for action_type=cachebox_mode_change")
		}
		return a.CacheBoxChange.Validate()
	default:
		return fmt.Errorf("action: invalid action_type %q", a.ActionType)
	}
}

// AttackTargetSpec describes the target for a policy-triggered attack.
type AttackTargetSpec struct {
	URL         string `json:"url"`
	Method      string `json:"method"`
	Rate        int    `json:"rate"`
	DurationMs  int64  `json:"duration_ms"`
	DedupBypass string `json:"dedup_bypass,omitempty"`
}

func (s *AttackTargetSpec) Validate() error {
	if s.URL == "" {
		return errors.New("attack target: url required")
	}
	if s.Method == "" {
		return errors.New("attack target: method required")
	}
	if s.Rate <= 0 {
		return errors.New("attack target: rate must be > 0")
	}
	if s.DurationMs <= 0 {
		return errors.New("attack target: duration_ms must be > 0")
	}
	return nil
}
