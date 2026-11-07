package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// TestOPAPolicyRepo_RoundTripAndDecisions exercises migration 15 end to end:
// policy CRUD incl. the unique-name conflict, the decision log's retention
// prune, LastChangeAt (the cooldown witness), and the ON DELETE CASCADE.
func TestOPAPolicyRepo_RoundTripAndDecisions(t *testing.T) {
	ctx := context.Background()
	policies := NewOPAPolicyRepo(testDB)
	decisions := NewPolicyDecisionRepo(testDB).WithRetention(2)
	tag := fmt.Sprintf("%d", time.Now().UnixNano())

	p := &model.OPAPolicy{
		ID: "opa-" + tag, Name: "rt-" + tag, Enabled: true,
		Rego:      "package atropos\n\nmutations contains m if { false; m := {} }\n",
		IntervalS: 15, CooldownS: 60,
		Inputs:    []model.PolicyInput{{Name: "p99", Source: "prometheus", Query: "up", Kind: "scalar"}},
		Scope:     model.PolicyScope{Services: []string{"checkout"}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := policies.Create(ctx, p); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = policies.Delete(context.Background(), p.ID) })

	dup := *p
	dup.ID = "opa-dup-" + tag
	if err := policies.Create(ctx, &dup); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate name: err = %v, want ErrConflict", err)
	}

	got, err := policies.Get(ctx, p.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != p.Name || got.Rego != p.Rego || got.IntervalS != 15 || got.CooldownS != 60 || !got.Enabled ||
		len(got.Inputs) != 1 || got.Inputs[0].Query != "up" || len(got.Scope.Services) != 1 || got.Scope.Services[0] != "checkout" {
		t.Errorf("round trip = %+v, want %+v", got, p)
	}

	p.Enabled = false
	p.CooldownS = 90
	if err := policies.Update(ctx, p); err != nil {
		t.Fatalf("update: %v", err)
	}
	enabled, err := policies.ListEnabled(ctx)
	if err != nil {
		t.Fatalf("list enabled: %v", err)
	}
	for _, e := range enabled {
		if e.ID == p.ID {
			t.Errorf("disabled policy %s still listed by ListEnabled", p.ID)
		}
	}
	if got, _ = policies.Get(ctx, p.ID); got.CooldownS != 90 {
		t.Errorf("CooldownS after update = %d, want 90", got.CooldownS)
	}

	base := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	for i := 0; i < 3; i++ {
		d := &model.PolicyDecision{
			ID: fmt.Sprintf("pdec-%s-%d", tag, i), PolicyID: p.ID,
			EvaluatedAt: base.Add(time.Duration(i) * time.Second), InputDigest: "abc",
			Applied:   i == 1,
			Mutations: json.RawMessage(`[{"op":"upsert_rule","service":"checkout","name":"r","outcome":"created"}]`),
		}
		if err := decisions.Record(ctx, d); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	list, total, err := decisions.List(ctx, p.ID, Page{Limit: 10})
	if err != nil {
		t.Fatalf("list decisions: %v", err)
	}
	if total != 2 || len(list) != 2 || list[0].ID != fmt.Sprintf("pdec-%s-2", tag) || list[1].ID != fmt.Sprintf("pdec-%s-1", tag) {
		t.Errorf("retention 2: total %d, ids %v; want the two newest, newest first", total, []string{list[0].ID, list[1].ID})
	}
	last, ok, err := decisions.LastChangeAt(ctx, p.ID)
	if err != nil || !ok {
		t.Fatalf("LastChangeAt: ok=%v err=%v", ok, err)
	}
	if want := base.Add(time.Second); !last.Equal(want) {
		t.Errorf("LastChangeAt = %v, want %v (the only applied+changed row)", last, want)
	}

	if err := policies.Delete(ctx, p.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, total, _ = decisions.List(ctx, p.ID, Page{Limit: 10}); total != 0 {
		t.Errorf("decisions after policy delete = %d, want 0 (cascade)", total)
	}
	if _, err := policies.Get(ctx, p.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("get after delete: err = %v, want ErrNotFound", err)
	}
}
