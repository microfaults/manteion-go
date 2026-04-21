package model

import (
	"errors"
	"fmt"
	"time"
)

// Rule binds a fault (atomic or composed) to a service + match criteria.
// Exactly one of FaultSpecID or FaultCompositionID must be set.
type Rule struct {
	ID                 string        `json:"id"`
	Name               string        `json:"name"`
	Service            string        `json:"service"`
	Enabled            bool          `json:"enabled"`
	Priority           int           `json:"priority"`
	Match              MatchCriteria `json:"match"`
	FaultSpecID        string        `json:"fault_spec_id,omitempty"`
	FaultCompositionID string        `json:"fault_composition_id,omitempty"`
	Mode               string        `json:"mode"` // "inline" or "background"
	CreatedAt          time.Time     `json:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
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
	hasFault := r.FaultSpecID != ""
	hasComposition := r.FaultCompositionID != ""
	if hasFault == hasComposition {
		return errors.New("rule: exactly one of fault_spec_id or fault_composition_id must be set")
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
