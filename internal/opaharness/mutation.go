package opaharness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"manteion-go/internal/model"
)

// Mutation ops a policy may emit under data.atropos.mutations.
const (
	OpUpsertRule = "upsert_rule"
	OpSetEnabled = "set_enabled"
	OpSetParams  = "set_params"
	OpDeleteRule = "delete_rule"
)

// Mutation outcomes. OutcomeRefused means the mutation never reached the
// store (out of scope or malformed); OutcomeFailed means the store rejected
// it.
const (
	OutcomeCreated   = "created"
	OutcomeUpdated   = "updated"
	OutcomeDeleted   = "deleted"
	OutcomeUnchanged = "unchanged"
	OutcomeRefused   = "refused"
	OutcomeFailed    = "failed"
)

// Mutation is one rule change a policy asks for. upsert_rule carries the
// whole Rule (its Service/Name are the key; the top-level fields, when set,
// must agree with them); set_enabled, set_params and delete_rule name the
// rule by the top-level Service/Name.
type Mutation struct {
	Op      string          `json:"op"`
	Service string          `json:"service,omitempty"`
	Name    string          `json:"name,omitempty"`
	Rule    *model.Rule     `json:"rule,omitempty"`
	Enabled *bool           `json:"enabled,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Key returns the (service, name) the mutation targets: the rule's own
// Service/Name for upsert_rule, the top-level fields otherwise.
func (m Mutation) Key() (service, name string) {
	if m.Op == OpUpsertRule && m.Rule != nil {
		return m.Rule.Service, m.Rule.Name
	}
	return m.Service, m.Name
}

// Validate checks the mutation's shape for its op. Every error is prefixed
// "mutation: ". An upsert_rule whose non-empty top-level service or name
// disagrees with its rule's is refused, so the key the scope check sees is
// the key the store writes.
func (m Mutation) Validate() error {
	switch m.Op {
	case OpUpsertRule:
		if m.Rule == nil {
			return errors.New("mutation: upsert_rule: rule required")
		}
		if m.Rule.Service == "" || m.Rule.Name == "" {
			return errors.New("mutation: upsert_rule: rule.service and rule.name required")
		}
		if (m.Service != "" && m.Service != m.Rule.Service) || (m.Name != "" && m.Name != m.Rule.Name) {
			return fmt.Errorf("mutation: upsert_rule: service/name disagree with rule (%s/%s vs %s/%s)",
				m.Service, m.Name, m.Rule.Service, m.Rule.Name)
		}
		return nil
	case OpSetEnabled, OpSetParams, OpDeleteRule:
	default:
		return fmt.Errorf("mutation: unknown op %q", m.Op)
	}
	if m.Service == "" {
		return errors.New("mutation: service required")
	}
	if m.Name == "" {
		return errors.New("mutation: name required")
	}
	switch m.Op {
	case OpSetEnabled:
		if m.Enabled == nil {
			return errors.New("mutation: set_enabled: enabled required")
		}
	case OpSetParams:
		if len(m.Params) == 0 {
			return errors.New("mutation: set_params: params required")
		}
		if t := bytes.TrimLeft(m.Params, " \t\r\n"); len(t) == 0 || t[0] != '{' || !json.Valid(m.Params) {
			return errors.New("mutation: set_params: params must be a JSON object")
		}
	}
	return nil
}

// MutationResult is the outcome of one mutation, as recorded on a policy
// decision.
type MutationResult struct {
	Op      string `json:"op"`
	Service string `json:"service"`
	Name    string `json:"name"`
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
	RuleID  string `json:"rule_id,omitempty"`
}

// Changed reports whether the mutation changed the store (created, updated
// or deleted a rule).
func (r MutationResult) Changed() bool {
	switch r.Outcome {
	case OutcomeCreated, OutcomeUpdated, OutcomeDeleted:
		return true
	}
	return false
}

// enforceScope splits ms into the mutations that are well-formed and target
// a service the scope allows, and refusals (OutcomeRefused) for the rest.
// Validate runs before the scope check. Order is preserved within each
// output.
func enforceScope(scope model.PolicyScope, ms []Mutation) (allowed []Mutation, refused []MutationResult) {
	for _, m := range ms {
		service, name := m.Key()
		refuse := func(err string) {
			refused = append(refused, MutationResult{Op: m.Op, Service: service, Name: name, Outcome: OutcomeRefused, Error: err})
		}
		if err := m.Validate(); err != nil {
			refuse(err.Error())
			continue
		}
		if !scope.Allows(service) {
			refuse(fmt.Sprintf("service %q outside policy scope", service))
			continue
		}
		allowed = append(allowed, m)
	}
	return allowed, refused
}
