package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Input sources, input kinds and the default evaluation interval of an
// OPAPolicy.
const (
	InputSourcePrometheus = "prometheus"
	InputSourceLoki       = "loki"

	InputKindScalar = "scalar"
	InputKindVector = "vector"

	// DefaultOPAIntervalS is the evaluation interval Validate applies when
	// IntervalS is 0.
	DefaultOPAIntervalS = 30
)

var (
	validInputSources = setOf(InputSourcePrometheus, InputSourceLoki)
	validInputKinds   = setOf(InputKindScalar, InputKindVector)

	// inputNameRe keeps an input name usable as a Rego identifier
	// (input.<name>).
	inputNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// OPAPolicy is a Rego policy the OPA harness evaluates every IntervalS
// seconds against the inputs it queries. Its outputs are rule mutations
// applied through the desired-state path, confined to the services its
// Scope names; CooldownS spaces the applied decisions of one policy.
type OPAPolicy struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Enabled   bool          `json:"enabled"`
	Rego      string        `json:"rego"`
	IntervalS int           `json:"interval_s"`
	CooldownS int           `json:"cooldown_s"`
	Inputs    []PolicyInput `json:"inputs"`
	Scope     PolicyScope   `json:"scope"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// PolicyInput is one query whose result the harness binds to input.<Name>
// before evaluating the policy. Source is InputSourcePrometheus or
// InputSourceLoki; Kind is InputKindScalar (the default) or InputKindVector.
type PolicyInput struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	Query  string `json:"query"`
	Kind   string `json:"kind"`
}

// PolicyScope names the services a policy may mutate rules for.
type PolicyScope struct {
	Services []string `json:"services"`
}

// Allows reports whether service is one the scope names.
func (s PolicyScope) Allows(service string) bool {
	for _, svc := range s.Services {
		if svc == service {
			return true
		}
	}
	return false
}

// Validate checks the policy and applies its defaults in place: IntervalS 0
// becomes DefaultOPAIntervalS and an empty input Kind becomes
// InputKindScalar. Every error is prefixed "opa policy: ". A policy with no
// inputs is valid; a policy with no scoped service is not.
func (p *OPAPolicy) Validate() error {
	if p.ID == "" {
		return errors.New("opa policy: id required")
	}
	if p.Name == "" {
		return errors.New("opa policy: name required")
	}
	if strings.TrimSpace(p.Rego) == "" {
		return errors.New("opa policy: rego required")
	}
	if p.IntervalS == 0 {
		p.IntervalS = DefaultOPAIntervalS
	}
	if p.IntervalS < 1 {
		return errors.New("opa policy: interval_s must be >= 1")
	}
	if p.CooldownS < 0 {
		return errors.New("opa policy: cooldown_s must be non-negative")
	}
	seen := make(map[string]bool, len(p.Inputs))
	for i := range p.Inputs {
		in := &p.Inputs[i]
		if !inputNameRe.MatchString(in.Name) {
			return fmt.Errorf("opa policy: input[%d]: name %q must match ^[A-Za-z_][A-Za-z0-9_]*$", i, in.Name)
		}
		if seen[in.Name] {
			return fmt.Errorf("opa policy: input[%d]: duplicate name %q", i, in.Name)
		}
		seen[in.Name] = true
		if !validInputSources[in.Source] {
			return fmt.Errorf("opa policy: input[%d]: invalid source %q", i, in.Source)
		}
		if strings.TrimSpace(in.Query) == "" {
			return fmt.Errorf("opa policy: input[%d]: query required", i)
		}
		if in.Kind == "" {
			in.Kind = InputKindScalar
		}
		if !validInputKinds[in.Kind] {
			return fmt.Errorf("opa policy: input[%d]: invalid kind %q", i, in.Kind)
		}
	}
	if len(p.Scope.Services) == 0 {
		return errors.New("opa policy: scope.services must name at least one service")
	}
	for i, svc := range p.Scope.Services {
		if svc == "" {
			return fmt.Errorf("opa policy: scope.services[%d]: blank service", i)
		}
	}
	return nil
}

// PolicyDecision records one evaluation of an OPAPolicy: the digest of the
// inputs it saw, the mutations it produced and whether they were applied.
// Mutations holds a JSON array of the harness's mutation results; it stays
// raw so the model does not import the harness. Error is set when the
// evaluation or its application failed.
type PolicyDecision struct {
	ID          string          `json:"id"`
	PolicyID    string          `json:"policy_id"`
	EvaluatedAt time.Time       `json:"evaluated_at"`
	InputDigest string          `json:"input_digest"`
	Mutations   json.RawMessage `json:"mutations"` // []opaharness.MutationResult {op, service, name, outcome, error?, rule_id?}; a dry run (applied=false) lists one outcome "dry_run" entry per in-scope would-be mutation
	Applied     bool            `json:"applied"`
	Error       string          `json:"error,omitempty"`
}
