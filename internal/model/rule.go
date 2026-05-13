package model

import (
	"errors"
	"fmt"
	"time"
)

// Rule binds an action (fault injection or cache-box) to a service + match
// criteria. The Action field discriminates between fault_spec,
// fault_composition, and cachebox — exactly one must be set.
type Rule struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Service   string        `json:"service"`
	Enabled   bool          `json:"enabled"`
	Priority  int           `json:"priority"`
	Match     MatchCriteria `json:"match"`
	Action    RuleAction    `json:"action"`
	Mode      string        `json:"mode"` // "inline" or "background"
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// RuleAction is a discriminated union: exactly one of FaultSpecID,
// FaultCompID, or CacheBox must be set, matching Type.
type RuleAction struct {
	Type        string              `json:"type"`                           // "fault_spec" | "fault_composition" | "cachebox"
	FaultSpecID string              `json:"fault_spec_id,omitempty"`        // when type=fault_spec
	FaultCompID string              `json:"fault_composition_id,omitempty"` // when type=fault_composition
	CacheBox    *CacheBoxRuleConfig `json:"cachebox,omitempty"`             // when type=cachebox
}

var validRuleActionTypes = map[string]bool{
	"fault_spec": true, "fault_composition": true, "cachebox": true,
}

func (a *RuleAction) Validate() error {
	if !validRuleActionTypes[a.Type] {
		return fmt.Errorf("rule action: invalid type %q", a.Type)
	}
	switch a.Type {
	case "fault_spec":
		if a.FaultSpecID == "" {
			return errors.New("rule action: fault_spec_id required for type=fault_spec")
		}
		if a.FaultCompID != "" || a.CacheBox != nil {
			return errors.New("rule action: only fault_spec_id allowed for type=fault_spec")
		}
	case "fault_composition":
		if a.FaultCompID == "" {
			return errors.New("rule action: fault_composition_id required for type=fault_composition")
		}
		if a.FaultSpecID != "" || a.CacheBox != nil {
			return errors.New("rule action: only fault_composition_id allowed for type=fault_composition")
		}
	case "cachebox":
		if a.CacheBox == nil {
			return errors.New("rule action: cachebox config required for type=cachebox")
		}
		if a.FaultSpecID != "" || a.FaultCompID != "" {
			return errors.New("rule action: only cachebox allowed for type=cachebox")
		}
		return a.CacheBox.Validate()
	}
	return nil
}

// CacheBoxRuleConfig specifies the cache-box mode and key derivation strategy
// for a cache-box rule.
type CacheBoxRuleConfig struct {
	Mode        string `json:"mode"`         // "passthrough" | "replay" | "replay_with_delay"
	KeyStrategy string `json:"key_strategy"` // "exact" | "exact_with_host" | "exact_with_body"
}

var (
	validCacheBoxRuleModes = map[string]bool{
		"passthrough": true, "replay": true, "replay_with_delay": true,
	}
	validCacheBoxKeyStrategies = map[string]bool{
		"exact": true, "exact_with_host": true, "exact_with_body": true,
	}
)

func (c *CacheBoxRuleConfig) Validate() error {
	if !validCacheBoxRuleModes[c.Mode] {
		return fmt.Errorf("cachebox rule: invalid mode %q", c.Mode)
	}
	if !validCacheBoxKeyStrategies[c.KeyStrategy] {
		return fmt.Errorf("cachebox rule: invalid key_strategy %q", c.KeyStrategy)
	}
	return nil
}

func (r *Rule) Validate() error {
	if r.ID == "" {
		return errors.New("rule: id required")
	}
	if r.Name == "" {
		return errors.New("rule: name required")
	}
	if r.Service == "" {
		return errors.New("rule: service required")
	}
	if err := r.Action.Validate(); err != nil {
		return fmt.Errorf("rule: %w", err)
	}
	if r.Action.Type == "cachebox" {
		if r.Mode == "" {
			r.Mode = "inline"
		}
	}
	if r.Mode != "inline" && r.Mode != "background" {
		return fmt.Errorf("rule: invalid mode %q", r.Mode)
	}
	return nil
}

// MatchCriteria defines when a rule applies.
type MatchCriteria struct {
	InjectionPoint string            `json:"injection_point,omitempty"` // ingress/egress/transient/custom, "" = any
	Labels         map[string]string `json:"labels,omitempty"`          // AND semantics
}

var validInjectionPoints = map[string]bool{
	"": true, "ingress": true, "egress": true, "transient": true, "custom": true,
}

func (m *MatchCriteria) Validate() error {
	if !validInjectionPoints[m.InjectionPoint] {
		return fmt.Errorf("match criteria: invalid injection_point %q", m.InjectionPoint)
	}
	return nil
}
