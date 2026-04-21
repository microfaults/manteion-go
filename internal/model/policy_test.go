package model

import (
	"testing"
	"time"
)

func TestPolicyRule_Validate(t *testing.T) {
	base := func() PolicyRule {
		return PolicyRule{
			ID: "p1", Name: "auto-attack", Enabled: true,
			Condition: PolicyCondition{Metric: "checkout_p99_us", Operator: "gt", Threshold: 50000},
			Action: PolicyAction{
				ActionType: "attack",
				AttackTarget: &AttackTargetSpec{
					URL: "http://productcatalog:3550/products", Method: "GET",
					Rate: 100, DurationMs: 30000,
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
		{"valid attack action", nil, false},
		{"valid cachebox action", func(r *PolicyRule) {
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
			r.Action.AttackTarget.URL = ""
		}, true},
		{"attack target zero rate", func(r *PolicyRule) {
			r.Action.AttackTarget.Rate = 0
		}, true},
		{"all operators valid", nil, false},
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
