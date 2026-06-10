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

// TestRuleRepo_GetScansEveryColumn inserts a rule with every optional field
// populated and asserts Get round-trips them. Guards the scan-arity bug
// class: a column added to the SELECT list without a matching Scan
// destination makes single-row Get fail at runtime while List keeps working.
func TestRuleRepo_GetScansEveryColumn(t *testing.T) {
	ctx := context.Background()

	// Use unique IDs derived from the test name so reruns don't PK-collide
	// and parallel sub-tests stay isolated.
	tag := fmt.Sprintf("scanrt-%d", time.Now().UnixNano())
	specID := "spec-" + tag
	ruleID := "rule-" + tag

	// Seed a fault_spec to satisfy the rules.fault_spec_id FK.
	spec := &model.FaultSpec{
		ID:        specID,
		Name:      "scan-rt-spec",
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
		ID:          ruleID,
		Name:        "scan-rt",
		Service:     "svc-" + tag,
		Enabled:     true,
		Priority:    50,
		Mode:        "background",
		StartPolicy: "always_start",
		Action:      model.RuleAction{Type: "fault_spec", FaultSpecID: specID},
		Match: model.MatchCriteria{
			InjectionPoint: "ingress",
			Labels:         map[string]string{"atropos.workflow": "browse"},
		},
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
	if got.StartPolicy != rule.StartPolicy {
		t.Errorf("StartPolicy roundtrip mismatch: got %q want %q", got.StartPolicy, rule.StartPolicy)
	}
	if got.Mode != rule.Mode {
		t.Errorf("Mode roundtrip mismatch: got %q want %q", got.Mode, rule.Mode)
	}
	if got.Match.InjectionPoint != rule.Match.InjectionPoint {
		t.Errorf("InjectionPoint roundtrip mismatch: got %q want %q", got.Match.InjectionPoint, rule.Match.InjectionPoint)
	}
	if got.Match.Labels["atropos.workflow"] != "browse" {
		t.Errorf("Labels roundtrip mismatch: got %v", got.Match.Labels)
	}
	if got.Action.FaultSpecID != specID {
		t.Errorf("FaultSpecID roundtrip mismatch: got %q want %q", got.Action.FaultSpecID, specID)
	}
}
