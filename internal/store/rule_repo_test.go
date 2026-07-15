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

// TestRuleRepo_ForService_PhaseGating pins M4: a rule attached to an experiment
// phase must be served by ForService (the authoritative SDK poll set) ONLY while
// that phase is 'running'. Since the SDK reconciles to EXACTLY the poll array and
// rules.enabled defaults true, a phase-scoped fault rule that leaks into the
// baseline (phase pending) corrupts every downstream delta; one that lingers past
// the measurement window (phase draining/completed) is resurrected after teardown.
// An unattached enabled rule is always present.
func TestRuleRepo_ForService_PhaseGating(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("phasegate-%d", time.Now().UnixNano())
	svc := "svc-" + tag
	specID := "spec-" + tag

	// Seed a fault_spec to satisfy the rules.fault_spec_id FK.
	spec := &model.FaultSpec{
		ID: specID, Name: "phasegate-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now(),
	}
	if err := testFaultRepo.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed fault spec: %v", err)
	}
	// Runs LAST (LIFO): after exp + rules are gone, the spec FK is free.
	t.Cleanup(func() { _ = testFaultRepo.DeleteSpec(context.Background(), specID) })

	mkRule := func(id string, priority int) *model.Rule {
		return &model.Rule{
			ID: id, Name: id, Service: svc, Enabled: true, Priority: priority,
			Mode: "background", StartPolicy: "always_start",
			Action:    model.RuleAction{Type: "fault_spec", FaultSpecID: specID},
			Match:     model.MatchCriteria{InjectionPoint: "ingress"},
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
	}
	freeID := "rule-free-" + tag   // never attached to a phase → always served
	phaseID := "rule-phase-" + tag // attached to one phase → served only while running
	for _, r := range []*model.Rule{mkRule(freeID, 10), mkRule(phaseID, 20)} {
		if err := testRuleRepo.Create(ctx, r); err != nil {
			t.Fatalf("create rule %s: %v", r.ID, err)
		}
		rid := r.ID
		t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rid) })
	}

	// Experiment + one phase; attach the phase-scoped rule.
	expRepo := NewExperimentRepo(testDB)
	exp := &model.Experiment{ID: "exp-" + tag, Name: "phasegate", Status: "planned", CreatedAt: time.Now()}
	if err := expRepo.Create(ctx, exp); err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	// Runs FIRST (LIFO): cascades experiment_phases + phase_rules, releasing the
	// ON DELETE RESTRICT on the phase-attached rule before its own cleanup runs.
	t.Cleanup(func() { _, _ = testDB.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })

	phase := &model.ExperimentPhase{
		ID: "phase-" + tag, ExperimentID: exp.ID, Name: "isolation", Position: 0, Status: "pending",
	}
	if err := expRepo.CreatePhase(ctx, phase); err != nil {
		t.Fatalf("create phase: %v", err)
	}
	if err := expRepo.AttachPhaseRules(ctx, phase.ID, []string{phaseID}); err != nil {
		t.Fatalf("attach phase rule: %v", err)
	}

	setStatus := func(status string) {
		if _, err := testDB.ExecContext(ctx,
			`UPDATE experiment_phases SET status = $1::phase_status WHERE id = $2`, status, phase.ID); err != nil {
			t.Fatalf("set phase status %q: %v", status, err)
		}
	}
	served := func() map[string]bool {
		rules, err := testRuleRepo.ForService(ctx, svc)
		if err != nil {
			t.Fatalf("ForService: %v", err)
		}
		m := map[string]bool{}
		for _, r := range rules {
			m[r.ID] = true
		}
		return m
	}

	// The unattached rule is present in every phase state; the phase-attached
	// rule is present ONLY while the phase is 'running'.
	for _, tc := range []struct {
		status      string
		wantPhaseIn bool
	}{
		{"pending", false},   // baseline window: an isolation fault must NOT be live
		{"running", true},    // measurement window: served
		{"draining", false},  // window closed: faults stop (running only, not draining)
		{"completed", false}, // torn down: not resurrected
	} {
		setStatus(tc.status)
		got := served()
		if !got[freeID] {
			t.Errorf("[phase=%s] unattached rule %s missing; want always served", tc.status, freeID)
		}
		if got[phaseID] != tc.wantPhaseIn {
			t.Errorf("[phase=%s] phase-attached rule %s served=%v, want %v",
				tc.status, phaseID, got[phaseID], tc.wantPhaseIn)
		}
	}
}
