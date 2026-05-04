package model

import (
	"testing"
	"time"
)

func TestPolicyRule_Validate(t *testing.T) {
	base := func() PolicyRule {
		return PolicyRule{
			ID: "p1", Name: "auto-push", Enabled: true,
			Condition: PolicyCondition{Metric: "checkout_p99_us", Operator: "gt", Threshold: 50000},
			Action: PolicyAction{
				ActionType: "push_rules",
				PushRules: &PushRulesAction{
					Service: "productcatalog",
					RuleIDs: []string{"r1"},
				},
			},
			Cooldown:  5 * time.Minute,
			CreatedAt: time.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*PolicyRule)
		wantErr bool
	}{
		{"valid push_rules action", nil, false},
		{"valid clear_rules action", func(r *PolicyRule) {
			r.Action = PolicyAction{
				ActionType: "clear_rules",
				PushRules:  &PushRulesAction{Service: "productcatalog"},
			}
		}, false},
		{"valid attack action", func(r *PolicyRule) {
			r.Action = PolicyAction{
				ActionType: "attack",
				AttackTarget: &AttackTargetSpec{
					URL: "http://productcatalog:3550/products", Method: "GET",
					Rate: 100, DurationMs: 30000,
				},
			}
		}, false},
		{"valid cachebox_mode_change action", func(r *PolicyRule) {
			r.Action = PolicyAction{
				ActionType: "cachebox_mode_change",
				CacheBoxChange: &CacheBoxConfig{
					Service: "productcatalog", Mode: "replay",
					KeyStrategy: "exact", MutationPolicy: "deny",
				},
			}
		}, false},
		{"invalid operator", func(r *PolicyRule) { r.Condition.Operator = "bad" }, true},
		{"missing metric", func(r *PolicyRule) { r.Condition.Metric = "" }, true},
		{"invalid action_type", func(r *PolicyRule) {
			r.Action = PolicyAction{ActionType: "bad"}
		}, true},
		{"attack without target", func(r *PolicyRule) {
			r.Action = PolicyAction{ActionType: "attack"}
		}, true},
		{"cachebox without config", func(r *PolicyRule) {
			r.Action = PolicyAction{ActionType: "cachebox_mode_change"}
		}, true},
		{"attack target missing url", func(r *PolicyRule) {
			r.Action = PolicyAction{
				ActionType: "attack",
				AttackTarget: &AttackTargetSpec{
					Method: "GET", Rate: 100, DurationMs: 30000,
				},
			}
		}, true},
		{"attack target zero rate", func(r *PolicyRule) {
			r.Action = PolicyAction{
				ActionType: "attack",
				AttackTarget: &AttackTargetSpec{
					URL: "http://svc:8080", Method: "GET", Rate: 0, DurationMs: 30000,
				},
			}
		}, true},
		{"push_rules without config", func(r *PolicyRule) {
			r.Action = PolicyAction{ActionType: "push_rules"}
		}, true},
		{"push_rules missing service", func(r *PolicyRule) {
			r.Action.PushRules.Service = ""
		}, true},
		{"push_rules empty rule_ids", func(r *PolicyRule) {
			r.Action.PushRules.RuleIDs = nil
		}, true},
		{"clear_rules without push_rules config", func(r *PolicyRule) {
			r.Action = PolicyAction{ActionType: "clear_rules"}
		}, true},
		{"clear_rules missing service", func(r *PolicyRule) {
			r.Action = PolicyAction{
				ActionType: "clear_rules",
				PushRules:  &PushRulesAction{},
			}
		}, true},
		{"negative cooldown", func(r *PolicyRule) { r.Cooldown = -1 }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := base()
			if tt.modify != nil {
				tt.modify(&r)
			}
			err := r.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPolicyCondition_AllOperators(t *testing.T) {
	for _, op := range []string{"gt", "gte", "lt", "lte", "eq"} {
		c := PolicyCondition{Metric: "test", Operator: op, Threshold: 1.0}
		if err := c.Validate(); err != nil {
			t.Errorf("operator %q should be valid, got: %v", op, err)
		}
	}
}
