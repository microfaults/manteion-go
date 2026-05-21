// Package store_test contains integration tests for the rule repository.
//
// These tests require a live PostgreSQL instance. They are opt-in: set
// MANTEION_TEST_DB=1 (or supply a DSN via MANTEION_DATABASE_URL) before
// running.
//
// Quick start:
//
//	cd manteion-go
//	docker-compose up -d          # starts Postgres on :5432
//	MANTEION_TEST_DB=1 go test ./internal/store/ -v -count=1
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"manteion-go/internal/db"
	"manteion-go/internal/model"
)

// ----------------------------------------------------------------------------
// Package-level test harness — mirrors internal/orchestrator/orchestrator_test.go.
// ----------------------------------------------------------------------------

var (
	testDB        *sql.DB
	testRuleRepo  *RuleRepo
	testFaultRepo *FaultRepo
)

func TestMain(m *testing.M) {
	if os.Getenv("MANTEION_TEST_DB") == "" {
		fmt.Fprintln(os.Stderr,
			"store integration tests skipped — set MANTEION_TEST_DB=1 to run")
		os.Exit(0)
	}

	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	testDB, err = db.Open(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "store test: cannot connect to DB: %v\n", err)
		os.Exit(1)
	}

	testRuleRepo = NewRuleRepo(testDB)
	testFaultRepo = NewFaultRepo(testDB)

	code := m.Run()

	_ = db.Close(testDB)
	os.Exit(code)
}

// TestRuleRepo_MatchExprRoundTrip inserts a rule with a non-empty match_expr,
// reads it back via Get, and asserts the field round-trips intact. Verifies
// the opaque OPA-rego text storage added by migration 20.
func TestRuleRepo_MatchExprRoundTrip(t *testing.T) {
	ctx := context.Background()

	// Use unique IDs derived from the test name so reruns don't PK-collide
	// and parallel sub-tests stay isolated.
	tag := fmt.Sprintf("mxrt-%d", time.Now().UnixNano())
	specID := "spec-" + tag
	ruleID := "rule-" + tag

	// Seed a fault_spec to satisfy the rules.fault_spec_id FK.
	spec := &model.FaultSpec{
		ID:        specID,
		Name:      "match-expr-rt-spec",
		Category:  "inline",
		FaultType: "latency",
		Host:      "process",
		Params:    json.RawMessage(`{"delay":"100ms"}`),
		CreatedAt: time.Now(),
	}
	if err := testFaultRepo.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed fault spec: %v", err)
	}
	t.Cleanup(func() {
		_ = testFaultRepo.DeleteSpec(context.Background(), specID)
	})

	rule := &model.Rule{
		ID:        ruleID,
		Name:      "match-expr-rt",
		Service:   "svc-" + tag,
		Enabled:   true,
		Priority:  50,
		Mode:      "inline",
		Action:    model.RuleAction{Type: "fault_spec", FaultSpecID: specID},
		Match:     model.MatchCriteria{Labels: map[string]string{"k": "v"}},
		MatchExpr: "package atropos.rules\ndefault allow := false",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := testRuleRepo.Create(ctx, rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() {
		_ = testRuleRepo.Delete(context.Background(), ruleID)
	})

	got, err := testRuleRepo.Get(ctx, ruleID)
	if err != nil {
		t.Fatalf("get rule: %v", err)
	}
	if got.MatchExpr != rule.MatchExpr {
		t.Errorf("MatchExpr roundtrip mismatch:\n got = %q\nwant = %q", got.MatchExpr, rule.MatchExpr)
	}
}
