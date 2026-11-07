package opaharness

import (
	"context"
	"strings"
	"testing"

	"github.com/open-policy-agent/opa/v1/ast"
)

const scalarRego = `package atropos

mutations contains {"op": "set_enabled", "service": "checkout", "name": "r", "enabled": false} if {
	input.inputs.p99 > 1
}
`

const upsertRego = `package atropos

mutations contains {"op": "upsert_rule", "rule": {
	"name": "checkout-egress-latency",
	"service": "checkout",
	"enabled": true,
	"priority": 100,
	"mode": "inline",
	"match": {"injection_point": "egress", "labels": {"atropos.http.path": "/cart/checkout"}},
	"action": {"type": "fault_spec", "fault_spec_id": "spec-egress-latency-200ms"},
}}
`

func TestCompile_Rejects(t *testing.T) {
	tests := []struct{ name, src, wantErr string }{
		{"parse error", "package atropos\nmutations contains x if { x := }", "rego_parse_error"},
		{"unsafe builtin", "package atropos\nmutations contains r if { r := http.send({\"method\": \"get\", \"url\": \"http://x\"}) }", "unsafe built-in"},
		{"wrong package", "package other\nmutations contains 1", "package atropos"},
		{"empty", "   ", "package atropos"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Compile(tt.src); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Compile err = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestCompile_Hash(t *testing.T) {
	p, err := Compile(scalarRego)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if p.Hash() != SourceHash(scalarRego) || len(p.Hash()) != 64 {
		t.Errorf("Hash() = %q, want sha256 hex of the source", p.Hash())
	}
}

func TestPrepared_Eval(t *testing.T) {
	p, err := Compile(scalarRego)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ctx := context.Background()
	hot, err := p.Eval(ctx, map[string]any{"inputs": map[string]any{"p99": 2.0}})
	if err != nil || len(hot) != 1 {
		t.Fatalf("hot eval = %+v, %v; want one mutation", hot, err)
	}
	if hot[0].Op != OpSetEnabled || hot[0].Service != "checkout" || hot[0].Name != "r" || hot[0].Enabled == nil || *hot[0].Enabled {
		t.Errorf("mutation = %+v, want set_enabled checkout/r enabled=false", hot[0])
	}
	for name, in := range map[string]any{
		"cold":       map[string]any{"inputs": map[string]any{"p99": 0.5}},
		"null input": map[string]any{"inputs": map[string]any{"p99": nil}},
		"no inputs":  map[string]any{},
	} {
		got, err := p.Eval(ctx, in)
		if err != nil || got == nil || len(got) != 0 {
			t.Errorf("%s: eval = %+v, %v; want an empty, non-nil slice", name, got, err)
		}
	}
}

func TestPrepared_Eval_DecodesRule(t *testing.T) {
	p, err := Compile(upsertRego)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got, err := p.Eval(context.Background(), map[string]any{})
	if err != nil || len(got) != 1 || got[0].Rule == nil {
		t.Fatalf("eval = %+v, %v; want one upsert with a rule", got, err)
	}
	r := got[0].Rule
	if svc, name := got[0].Key(); svc != "checkout" || name != "checkout-egress-latency" {
		t.Errorf("Key() = %s/%s", svc, name)
	}
	if r.Priority != 100 || r.Mode != "inline" || r.Match.InjectionPoint != "egress" ||
		r.Match.Labels["atropos.http.path"] != "/cart/checkout" || r.Action.FaultSpecID != "spec-egress-latency-200ms" || !r.Enabled {
		t.Errorf("rule = %+v", r)
	}
}

func TestPrepared_Eval_RejectsNonObjectMutation(t *testing.T) {
	p, err := Compile("package atropos\n\nmutations contains \"boom\"\n")
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, err := p.Eval(context.Background(), map[string]any{}); err == nil || !strings.Contains(err.Error(), "decode mutations") {
		t.Fatalf("err = %v, want a decode error, not a panic", err)
	}
}

// TestSandbox_PinsNondeterministicBuiltins pins the sandbox to OPA's builtin
// catalogue: a nondeterministic builtin an OPA upgrade adds must be classified
// before the upgrade lands, and a denied builtin that OPA renames must not
// silently stop being denied.
func TestSandbox_PinsNondeterministicBuiltins(t *testing.T) {
	catalogue := map[string]bool{}
	for _, b := range ast.Builtins {
		catalogue[b.Name] = true
		if !b.Nondeterministic {
			continue
		}
		_, denied := unsafeBuiltins[b.Name]
		_, allowed := harmlessNondeterministicBuiltins[b.Name]
		if !denied && !allowed {
			t.Errorf("new nondeterministic builtin %q: classify it (deny or allow) before upgrading OPA", b.Name)
		}
		if denied && allowed {
			t.Errorf("builtin %q is both denied and allowed", b.Name)
		}
	}
	for name := range unsafeBuiltins {
		if !catalogue[name] {
			t.Errorf("denied builtin %q is not in OPA's catalogue (renamed or removed?): the denylist no longer covers it", name)
		}
	}
	for name := range harmlessNondeterministicBuiltins {
		if !catalogue[name] {
			t.Errorf("allowed builtin %q is not in OPA's catalogue: drop it from harmlessNondeterministicBuiltins", name)
		}
	}
}
