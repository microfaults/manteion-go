package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
)

// switchFixture is a rule on a service of its own, optionally linked to a
// phase whose experiment and phase are both running, driven through a repo
// that bumps the real generations.
type switchFixture struct {
	t     *testing.T
	repo  *RuleRepo
	gens  *desired.Generations
	svc   string
	rule  *model.Rule
	phase *model.ExperimentPhase
}

func newSwitchFixture(t *testing.T, enabled, attached bool) *switchFixture {
	t.Helper()
	ctx := context.Background()
	tag := fmt.Sprintf("switch-%d", time.Now().UnixNano())
	gens := desired.NewGenerations(NewGenerationRepo(testDB), desired.NewBroker())
	f := &switchFixture{t: t, repo: NewRuleRepo(testDB, gens), gens: gens, svc: "svc-" + tag}

	specID := seedLatencySpec(t, tag)
	f.rule = &model.Rule{ID: "rule-" + tag, Name: "demo-run-" + tag, Service: f.svc, Enabled: enabled, Priority: 100,
		Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: specID},
		CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := testRuleRepo.Create(ctx, f.rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), f.rule.ID) })
	if attached {
		f.phase = seedPhase(t, tag, "switch", "running", nil, f.rule.ID)
	}
	return f
}

func (f *switchFixture) served() bool {
	f.t.Helper()
	rules, err := testRuleRepo.ForService(context.Background(), f.svc)
	if err != nil {
		f.t.Fatalf("ForService: %v", err)
	}
	return slices.Contains(ruleIDs(rules), f.rule.ID)
}

func (f *switchFixture) enabled() bool {
	f.t.Helper()
	r, err := testRuleRepo.Get(context.Background(), f.rule.ID)
	if err != nil {
		f.t.Fatalf("get rule: %v", err)
	}
	return r.Enabled
}

func (f *switchFixture) gen() uint64 {
	f.t.Helper()
	g, err := f.gens.Get(context.Background(), f.svc)
	if err != nil {
		f.t.Fatalf("generation: %v", err)
	}
	return g
}

// TestRuleRepo_SetEnabled pins the unconditional switch: one write that
// always lands, stops a served rule at once and bumps its service's
// generation, whatever the flag was before.
func TestRuleRepo_SetEnabled(t *testing.T) {
	ctx := context.Background()

	t.Run("off stops an attached running rule and bumps", func(t *testing.T) {
		f := newSwitchFixture(t, true, true)
		if !f.served() {
			t.Fatalf("fixture: enabled rule on a running phase of a running experiment is not served")
		}
		before := f.gen()
		if err := f.repo.SetEnabled(ctx, f.rule.ID, false); err != nil {
			t.Fatalf("SetEnabled(false): %v", err)
		}
		if f.served() || f.enabled() {
			t.Errorf("rule still served (%v) or enabled (%v) after SetEnabled(false)", f.served(), f.enabled())
		}
		if after := f.gen(); after <= before {
			t.Errorf("generation %d -> %d; want a bump", before, after)
		}
	})

	t.Run("off on a disabled rule still writes", func(t *testing.T) {
		f := newSwitchFixture(t, false, true)
		before := f.gen()
		if err := f.repo.SetEnabled(ctx, f.rule.ID, false); err != nil {
			t.Fatalf("SetEnabled(false) on a disabled rule: %v", err)
		}
		if f.enabled() {
			t.Errorf("rule enabled")
		}
		if after := f.gen(); after <= before {
			t.Errorf("generation %d -> %d; want a bump (the switch always writes)", before, after)
		}
	})

	t.Run("on writes too", func(t *testing.T) {
		f := newSwitchFixture(t, false, false)
		if err := f.repo.SetEnabled(ctx, f.rule.ID, true); err != nil {
			t.Fatalf("SetEnabled(true): %v", err)
		}
		if !f.enabled() || !f.served() {
			t.Errorf("unattached rule after SetEnabled(true): enabled %v, served %v; want both", f.enabled(), f.served())
		}
	})

	t.Run("missing rule is not found", func(t *testing.T) {
		err := testRuleRepo.SetEnabled(ctx, fmt.Sprintf("rule-missing-%d", time.Now().UnixNano()), false)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("err = %v; want ErrNotFound", err)
		}
	})
}

// TestRuleRepo_EnableIfAttached pins the guarded switch-on: a rule is enabled
// only while a phase links it, in the same statement, so the switch can never
// leave an enabled rule attached to no phase.
func TestRuleRepo_EnableIfAttached(t *testing.T) {
	ctx := context.Background()

	t.Run("attached rule under a running phase becomes served", func(t *testing.T) {
		f := newSwitchFixture(t, false, true)
		if f.served() {
			t.Fatalf("fixture: disabled rule served")
		}
		before := f.gen()
		if err := f.repo.EnableIfAttached(ctx, f.rule.ID); err != nil {
			t.Fatalf("EnableIfAttached: %v", err)
		}
		if !f.enabled() || !f.served() {
			t.Errorf("after EnableIfAttached: enabled %v, served %v; want both", f.enabled(), f.served())
		}
		if after := f.gen(); after <= before {
			t.Errorf("generation %d -> %d; want a bump", before, after)
		}
	})

	t.Run("unattached rule is refused and stays disabled", func(t *testing.T) {
		f := newSwitchFixture(t, false, false)
		before := f.gen()
		err := f.repo.EnableIfAttached(ctx, f.rule.ID)
		if !errors.Is(err, ErrRuleUnattached) || errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v; want ErrRuleUnattached only", err)
		}
		if f.enabled() || f.served() {
			t.Errorf("refused rule: enabled %v, served %v; want neither", f.enabled(), f.served())
		}
		if after := f.gen(); after != before {
			t.Errorf("generation %d -> %d; a refused switch must not bump", before, after)
		}
	})

	t.Run("rule whose experiment was deleted is unattached", func(t *testing.T) {
		f := newSwitchFixture(t, false, true)
		if _, err := testDB.ExecContext(ctx, `DELETE FROM experiments WHERE id = $1`, f.phase.ExperimentID); err != nil {
			t.Fatalf("delete experiment: %v", err)
		}
		if err := f.repo.EnableIfAttached(ctx, f.rule.ID); !errors.Is(err, ErrRuleUnattached) {
			t.Fatalf("err = %v; want ErrRuleUnattached", err)
		}
		if f.enabled() {
			t.Errorf("rule enabled with no phase left")
		}
	})

	t.Run("missing rule is not found", func(t *testing.T) {
		err := testRuleRepo.EnableIfAttached(ctx, fmt.Sprintf("rule-missing-%d", time.Now().UnixNano()))
		if !errors.Is(err, ErrNotFound) || errors.Is(err, ErrRuleUnattached) {
			t.Errorf("err = %v; want ErrNotFound only", err)
		}
	})
}
