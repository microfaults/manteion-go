package opaharness

import (
	"encoding/json"
	"strings"
	"testing"

	"manteion-go/internal/model"
)

func TestEnforceScope(t *testing.T) {
	scope := model.PolicyScope{Services: []string{"checkout"}}
	on := true
	tests := []struct {
		name        string
		m           Mutation
		wantRefused string // substring of the refused result's error; "" = allowed
	}{
		{"upsert in scope", Mutation{Op: OpUpsertRule, Rule: &model.Rule{Name: "r", Service: "checkout"}}, ""},
		{"set_enabled in scope", Mutation{Op: OpSetEnabled, Service: "checkout", Name: "r", Enabled: &on}, ""},
		{"set_params in scope", Mutation{Op: OpSetParams, Service: "checkout", Name: "r", Params: json.RawMessage(`{"delay":"50ms"}`)}, ""},
		{"delete in scope", Mutation{Op: OpDeleteRule, Service: "checkout", Name: "r"}, ""},
		{"out of scope", Mutation{Op: OpSetEnabled, Service: "frontend", Name: "r", Enabled: &on}, "outside policy scope"},
		{"key mismatch", Mutation{Op: OpUpsertRule, Service: "frontend", Rule: &model.Rule{Name: "r", Service: "checkout"}}, "disagree"},
		{"unknown op", Mutation{Op: "nuke", Service: "checkout", Name: "r"}, "unknown op"},
		{"upsert without rule", Mutation{Op: OpUpsertRule, Service: "checkout", Name: "r"}, "rule required"},
		{"set_enabled without enabled", Mutation{Op: OpSetEnabled, Service: "checkout", Name: "r"}, "enabled required"},
		{"set_params without params", Mutation{Op: OpSetParams, Service: "checkout", Name: "r"}, "params required"},
		{"set_params with non-object params", Mutation{Op: OpSetParams, Service: "checkout", Name: "r", Params: json.RawMessage(`[1]`)}, "JSON object"},
		{"delete without name", Mutation{Op: OpDeleteRule, Service: "checkout"}, "name required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, refused := enforceScope(scope, []Mutation{tt.m})
			if tt.wantRefused == "" {
				if len(allowed) != 1 || len(refused) != 0 {
					t.Fatalf("allowed %d refused %+v, want allowed", len(allowed), refused)
				}
				return
			}
			if len(allowed) != 0 || len(refused) != 1 {
				t.Fatalf("allowed %d refused %+v, want one refusal", len(allowed), refused)
			}
			if refused[0].Outcome != OutcomeRefused || !strings.Contains(refused[0].Error, tt.wantRefused) {
				t.Errorf("refusal = %+v, want outcome refused with %q", refused[0], tt.wantRefused)
			}
		})
	}
}

func TestEnforceScope_KeepsOrderAndSplits(t *testing.T) {
	scope := model.PolicyScope{Services: []string{"checkout"}}
	ms := []Mutation{
		{Op: OpDeleteRule, Service: "frontend", Name: "a"},
		{Op: OpDeleteRule, Service: "checkout", Name: "b"},
		{Op: OpDeleteRule, Service: "checkout", Name: "c"},
	}
	allowed, refused := enforceScope(scope, ms)
	if len(allowed) != 2 || allowed[0].Name != "b" || allowed[1].Name != "c" || len(refused) != 1 || refused[0].Name != "a" {
		t.Errorf("allowed %+v refused %+v", allowed, refused)
	}
}

func TestMutationResult_Changed(t *testing.T) {
	for outcome, want := range map[string]bool{OutcomeCreated: true, OutcomeUpdated: true, OutcomeDeleted: true, OutcomeUnchanged: false, OutcomeRefused: false, OutcomeFailed: false, OutcomeDryRun: false} {
		if got := (MutationResult{Outcome: outcome}).Changed(); got != want {
			t.Errorf("Changed(%s) = %v, want %v", outcome, got, want)
		}
	}
}
