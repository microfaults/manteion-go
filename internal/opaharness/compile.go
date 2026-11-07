// Package opaharness compiles Rego v1 policies and evaluates them into rule
// mutations. A policy declares "package atropos" and emits its mutations as
// the set data.atropos.mutations; it runs sandboxed (no network or runtime
// builtins, strict builtin errors) under evalTimeout.
package opaharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/rego"
)

const (
	policyFilename = "policy.rego"
	policyPackage  = "data.atropos"
	mutationsQuery = "data.atropos.mutations"
	evalTimeout    = 5 * time.Second
)

// unsafeBuiltins are refused at compile time: a policy must never reach the
// network or inspect the process from inside the evaluator.
var unsafeBuiltins = map[string]struct{}{"http.send": {}, "net.lookup_ip_addr": {}, "opa.runtime": {}}

// Prepared is a compiled policy, ready to evaluate.
type Prepared struct {
	pq   rego.PreparedEvalQuery
	hash string
}

// Hash returns SourceHash of the source the policy was compiled from.
func (p *Prepared) Hash() string { return p.hash }

// SourceHash returns the sha256 of src as lowercase hex.
func SourceHash(src string) string {
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:])
}

// Compile parses src as a Rego v1 module, requires it to declare
// "package atropos", and prepares data.atropos.mutations in the sandbox. A
// parse error is returned verbatim (it carries rego_parse_error); a call to
// an unsafe builtin fails here as "rego_type_error: unsafe built-in function
// calls ...".
func Compile(src string) (*Prepared, error) {
	if strings.TrimSpace(src) == "" {
		return nil, fmt.Errorf("policy must declare \"package atropos\" (got an empty source)")
	}
	m, err := ast.ParseModule(policyFilename, src)
	if err != nil {
		return nil, err
	}
	if m == nil || m.Package == nil {
		return nil, fmt.Errorf("policy must declare \"package atropos\" (got no package)")
	}
	if got := m.Package.Path.String(); got != policyPackage {
		return nil, fmt.Errorf("policy must declare \"package atropos\" (got %s)", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), evalTimeout)
	defer cancel()
	pq, err := rego.New(
		rego.Query(mutationsQuery),
		rego.Module(policyFilename, src),
		rego.UnsafeBuiltins(unsafeBuiltins),
		rego.StrictBuiltinErrors(true),
	).PrepareForEval(ctx)
	if err != nil {
		return nil, err
	}
	return &Prepared{pq: pq, hash: SourceHash(src)}, nil
}

// Eval evaluates the policy against input under evalTimeout and decodes
// data.atropos.mutations. An undefined result means nothing to do and yields
// an empty, non-nil slice; a member that does not decode as a Mutation (a
// non-object, say) is an error wrapped as "decode mutations: ...", never a
// silent skip.
func (p *Prepared) Eval(ctx context.Context, input any) ([]Mutation, error) {
	ctx, cancel := context.WithTimeout(ctx, evalTimeout)
	defer cancel()
	rs, err := p.pq.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 || len(rs[0].Expressions) == 0 {
		return []Mutation{}, nil
	}
	raw, err := json.Marshal(rs[0].Expressions[0].Value)
	if err != nil {
		return nil, fmt.Errorf("decode mutations: %w", err)
	}
	var ms []Mutation
	if err := json.Unmarshal(raw, &ms); err != nil {
		return nil, fmt.Errorf("decode mutations: %w", err)
	}
	if ms == nil {
		ms = []Mutation{}
	}
	return ms, nil
}
