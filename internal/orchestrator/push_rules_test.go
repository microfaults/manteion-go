package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// TestPushPhaseRules_SkipsDisabledRules pins the push channel to the poll
// predicate's enabled conjunct: a disabled-but-attached rule must not fan out
// at phase start. Push is a latency optimization of the poll reconciler and
// must be a strict projection of the same predicate (ForService: enabled AND
// attached-to-running) — before this, push loaded attached rules by id with
// no enabled filter, so a disabled rule fired for up to one poll interval
// until the version-bumped poll wiped it (the "ghost activation").
func TestPushPhaseRules_SkipsDisabledRules(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	ph := mkRunningIsolation(t, exp.ID, "frontend", 0)

	tag := fmt.Sprintf("pushproj-%d", time.Now().UnixNano())
	specID := "spec-" + tag
	spec := &model.FaultSpec{
		ID: specID, Name: "pushproj-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now(),
	}
	if err := testFaultRepo.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed fault spec: %v", err)
	}
	t.Cleanup(func() { _ = testFaultRepo.DeleteSpec(context.Background(), specID) })

	mkRule := func(id string, enabled bool) *model.Rule {
		return &model.Rule{
			ID: id, Name: id, Service: "frontend", Enabled: enabled, Priority: 10,
			Mode: "background", StartPolicy: "always_start",
			Action:    model.RuleAction{Type: "fault_spec", FaultSpecID: specID},
			Match:     model.MatchCriteria{InjectionPoint: "ingress"},
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
	}
	onID, offID := "rule-on-"+tag, "rule-off-"+tag
	for _, r := range []*model.Rule{mkRule(onID, true), mkRule(offID, false)} {
		if err := testRuleRepo.Create(ctx, r); err != nil {
			t.Fatalf("create rule %s: %v", r.ID, err)
		}
		rid := r.ID
		t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rid) })
	}
	if err := testExpRepo.AttachPhaseRules(ctx, ph.ID, []string{onID, offID}); err != nil {
		t.Fatalf("attach phase rules: %v", err)
	}

	sdk := newFakeSDK(t, true)
	registerSDK(t, "frontend", sdk.server.URL)

	if err := o.pushPhaseRules(ctx, ph); err != nil {
		t.Fatalf("pushPhaseRules: %v", err)
	}

	pushed := sdk.rulesPushed()
	for _, name := range pushed {
		if name == offID {
			t.Fatalf("disabled rule %q was pushed (pushed=%v); push must project the poll predicate", offID, pushed)
		}
	}
	if len(pushed) != 1 || pushed[0] != onID {
		t.Fatalf("pushed = %v, want exactly [%s]", pushed, onID)
	}
}
