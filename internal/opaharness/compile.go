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
)

// evalTimeout bounds each compile and each evaluation. It is a variable only
// so a test can shorten it.
var evalTimeout = 5 * time.Second

// unsafeBuiltins are refused at compile time: a policy must never reach the
// network or inspect the process from inside the evaluator. Against OPA
// v1.19.1 this closes every network and host path: the only builtins that
// perform I/O are http.send (topdown/http.go, which also reaches the CA file
// and environment readers of crypto.go through its TLS options) and
// net.lookup_ip_addr (topdown/net.go); print is erased because print
// statements are not enabled. TestSandbox_PinsNondeterministicBuiltins pins
// this classification to the OPA version.
var unsafeBuiltins = map[string]struct{}{"http.send": {}, "net.lookup_ip_addr": {}, "opa.runtime": {}}

// harmlessNondeterministicBuiltins are the builtins OPA flags nondeterministic
// that a policy may still call: they read the clock or a random source and
// touch neither the network nor the host. Every nondeterministic builtin is in
// exactly one of this set and unsafeBuiltins.
var harmlessNondeterministicBuiltins = map[string]struct{}{
	"rand.intn": {}, "uuid.rfc4122": {}, "time.now_ns": {},
	"io.jwt.encode_sign": {}, "io.jwt.encode_sign_raw": {}, "io.jwt.decode_verify": {},
}

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
	if err := requirePackage(m); err != nil {
		return nil, err
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

// requirePackage checks that m declares "package atropos". A nil module or
// one without a package (which ast.ParseModule does not return today) is
// refused as "got no package".
func requirePackage(m *ast.Module) error {
	if m == nil || m.Package == nil {
		return fmt.Errorf("policy must declare \"package atropos\" (got no package)")
	}
	if got := m.Package.Path.String(); got != policyPackage {
		return fmt.Errorf("policy must declare \"package atropos\" (got %s)", got)
	}
	return nil
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
