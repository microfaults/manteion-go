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
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"manteion-go/internal/db"
	"manteion-go/internal/desired"
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

// testDSN is the database the store tests run against.
func testDSN() string {
	if dsn := os.Getenv("MANTEION_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
}

func TestMain(m *testing.M) {
	if os.Getenv("MANTEION_TEST_DB") == "" {
		fmt.Fprintln(os.Stderr,
			"store integration tests skipped — set MANTEION_TEST_DB=1 to run")
		os.Exit(0)
	}

	dsn := testDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	testDB, err = db.Open(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "store test: cannot connect to DB: %v\n", err)
		os.Exit(1)
	}

	testRuleRepo = NewRuleRepo(testDB, nil)
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
// that phase is 'running'. Since the SDK reconciles to EXACTLY the poll array, a
// phase-scoped fault rule that leaks into the
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

	// Experiment + one phase; attach the phase-scoped rule. The experiment runs,
	// so the phase's own status is what gates the rule here.
	expRepo := NewExperimentRepo(testDB)
	exp := &model.Experiment{ID: "exp-" + tag, Name: "phasegate", Status: "running", CreatedAt: time.Now()}
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

// seedLatencySpec persists a fault spec for rules to reference and removes it
// at cleanup. Register the rules' own cleanups after calling it: cleanups run
// last-in first-out, so the rules go before the spec their foreign key names.
func seedLatencySpec(t *testing.T, tag string) string {
	t.Helper()
	spec := &model.FaultSpec{ID: "spec-" + tag, Name: "order-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now()}
	if err := testFaultRepo.CreateSpec(context.Background(), spec); err != nil {
		t.Fatalf("seed spec: %v", err)
	}
	t.Cleanup(func() { _ = testFaultRepo.DeleteSpec(context.Background(), spec.ID) })
	return spec.ID
}

// createOrderedRule persists an enabled, phase-free rule with the given
// priority and creation time, and removes it at cleanup.
func createOrderedRule(t *testing.T, specID, svc, id string, priority int, createdAt time.Time) *model.Rule {
	t.Helper()
	rule := &model.Rule{ID: id, Name: id, Service: svc, Enabled: true, Priority: priority, Mode: "background",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: specID}, CreatedAt: createdAt, UpdatedAt: createdAt}
	if err := testRuleRepo.Create(context.Background(), rule); err != nil {
		t.Fatalf("create rule %s: %v", id, err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), id) })
	return rule
}

func ruleIDs(rules []*model.Rule) []string {
	out := make([]string, len(rules))
	for i, r := range rules {
		out[i] = r.ID
	}
	return out
}

// planRepo returns a rule repo on its own single-connection pool whose session
// has run the given SET statements, so a test can pin the plan a query uses.
func planRepo(t *testing.T, sets ...string) *RuleRepo {
	t.Helper()
	conn, err := sql.Open("pgx", testDSN())
	if err != nil {
		t.Fatalf("open pinned pool: %v", err)
	}
	conn.SetMaxOpenConns(1) // one session, so the SETs below stay in force
	t.Cleanup(func() { _ = conn.Close() })
	for _, set := range sets {
		if _, err := conn.Exec(set); err != nil {
			t.Fatalf("%s: %v", set, err)
		}
	}
	return NewRuleRepo(conn, nil)
}

// TestRuleRepo_ForService_EqualPriorityOrderSurvivesRewrites pins the poll set's
// order as a function of the stored rules: priority first, then oldest first,
// whatever plan the query gets and however often a rule is re-saved. A tie-break
// left to the scan follows physical position, and every UPDATE writes a new row
// version, even one that changes nothing: a sequential scan then returns the
// re-saved rule behind its siblings, and so does an index scan once the update
// has to move the row's index entry (here, a rule edited away and back). The
// ETag of the materialized document would flip with no change in content, and so
// would which of two equal-priority rules the SDK, which keeps delivery order
// among equals, evaluates first.
func TestRuleRepo_ForService_EqualPriorityOrderSurvivesRewrites(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("tieorder-%d", time.Now().UnixNano())
	svc := "svc-" + tag
	specID := seedLatencySpec(t, tag)
	t0 := time.Now().Add(-time.Hour).Truncate(time.Microsecond)

	// Created in this order, one second apart. The ids are deliberately not in
	// creation order, so age (not id) is what has to decide among equals; a
	// lower- and a higher-priority rule bracket them, so the tie-breaks are
	// shown to rank below priority.
	low := createOrderedRule(t, specID, svc, "low-"+tag, 1, t0)
	c := createOrderedRule(t, specID, svc, "c-"+tag, 5, t0.Add(1*time.Second))
	a := createOrderedRule(t, specID, svc, "a-"+tag, 5, t0.Add(2*time.Second))
	b := createOrderedRule(t, specID, svc, "b-"+tag, 5, t0.Add(3*time.Second))
	high := createOrderedRule(t, specID, svc, "high-"+tag, 9, t0.Add(4*time.Second))
	want := []string{high.ID, c.ID, a.ID, b.ID, low.ID}

	// The default plan is an index scan on service here; the pinned session is
	// forced to read the heap in physical order. The order must not depend on it.
	repos := []struct {
		plan string
		repo *RuleRepo
	}{
		{"default plan", testRuleRepo},
		{"sequential scan", planRepo(t, "SET enable_indexscan = off", "SET enable_bitmapscan = off")},
	}
	check := func(when string) {
		t.Helper()
		for _, r := range repos {
			got, err := r.repo.ForService(ctx, svc)
			if err != nil {
				t.Fatalf("ForService (%s) %s: %v", r.plan, when, err)
			}
			if !slices.Equal(ruleIDs(got), want) {
				t.Errorf("ForService (%s) %s = %v, want %v", r.plan, when, ruleIDs(got), want)
			}
		}
	}
	check("after creation")

	// Re-save each equal-priority rule unchanged.
	for _, r := range []*model.Rule{c, a, b} {
		if err := testRuleRepo.Update(ctx, r); err != nil {
			t.Fatalf("no-op update of %s: %v", r.ID, err)
		}
		check("after re-saving " + r.ID)
	}

	// Edit the middle rule away and back: the second write changes an indexed
	// column, so the row's index entry moves as well.
	away := *a
	away.Service = "elsewhere-" + tag
	if err := testRuleRepo.Update(ctx, &away); err != nil {
		t.Fatalf("move %s away: %v", a.ID, err)
	}
	if err := testRuleRepo.Update(ctx, a); err != nil {
		t.Fatalf("move %s back: %v", a.ID, err)
	}
	check("after editing " + a.ID + " away and back")
}

// TestRuleRepo_EqualPriorityAndAgeBreakByID covers rules created in the same
// instant (the column is microsecond resolution and callers supply it): with
// priority and creation time equal, id is the last word, in every listing that
// orders rules.
func TestRuleRepo_EqualPriorityAndAgeBreakByID(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("tieid-%d", time.Now().UnixNano())
	svc := "svc-" + tag
	specID := seedLatencySpec(t, tag)
	at := time.Now().Add(-time.Hour).Truncate(time.Microsecond)

	// Inserted z first, so the scan order is the reverse of the id order.
	createOrderedRule(t, specID, svc, "z-"+tag, 5, at)
	createOrderedRule(t, specID, svc, "a-"+tag, 5, at)
	want := []string{"a-" + tag, "z-" + tag}

	forService, err := testRuleRepo.ForService(ctx, svc)
	if err != nil {
		t.Fatalf("ForService: %v", err)
	}
	enabled, err := testRuleRepo.EnabledForService(ctx, svc)
	if err != nil {
		t.Fatalf("EnabledForService: %v", err)
	}
	all, err := testRuleRepo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var listed []*model.Rule
	for _, r := range all { // List spans every service; keep this test's
		if r.Service == svc {
			listed = append(listed, r)
		}
	}
	for name, got := range map[string][]*model.Rule{"ForService": forService, "EnabledForService": enabled, "List": listed} {
		if !slices.Equal(ruleIDs(got), want) {
			t.Errorf("%s = %v, want %v", name, ruleIDs(got), want)
		}
	}
}

// createCacheBoxRule persists an enabled cache-box rule and removes it at
// cleanup. Register an experiment cleanup (seedPhase) after calling it:
// cleanups run last-in first-out, and a rule attached to a phase cannot be
// deleted until its experiment, and with it the phase_rules row, is gone.
func createCacheBoxRule(t *testing.T, svc, id string, priority int, createdAt time.Time) *model.Rule {
	t.Helper()
	rule := &model.Rule{ID: id, Name: id, Service: svc, Enabled: true, Priority: priority, Mode: "inline",
		Action:    model.RuleAction{Type: "cachebox", CacheBox: &model.CacheBoxRuleConfig{Mode: "replay", KeyStrategy: "exact"}},
		CreatedAt: createdAt, UpdatedAt: createdAt}
	if err := testRuleRepo.Create(context.Background(), rule); err != nil {
		t.Fatalf("create rule %s: %v", id, err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), id) })
	return rule
}

// seedPhase persists an experiment with one phase in the given state, attaches
// the rules to the phase, and removes the experiment (the phase and its
// phase_rules go with it) at cleanup.
func seedPhase(t *testing.T, tag, name, status string, startedAt *time.Time, ruleIDs ...string) *model.ExperimentPhase {
	t.Helper()
	ctx := context.Background()
	repo := NewExperimentRepo(testDB)
	exp := &model.Experiment{ID: "exp-" + name + "-" + tag, Name: "exp-" + name + "-" + tag, Status: "running", CreatedAt: time.Now()}
	if err := repo.Create(ctx, exp); err != nil {
		t.Fatalf("create experiment %s: %v", exp.ID, err)
	}
	t.Cleanup(func() { _, _ = testDB.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
	p := &model.ExperimentPhase{ID: "phase-" + name + "-" + tag, ExperimentID: exp.ID, Name: "isolation", Position: 0,
		Status: status, StartedAt: startedAt}
	if err := repo.CreatePhase(ctx, p); err != nil {
		t.Fatalf("create phase %s: %v", p.ID, err)
	}
	if err := repo.AttachPhaseRules(ctx, p.ID, ruleIDs); err != nil {
		t.Fatalf("attach rules to %s: %v", p.ID, err)
	}
	return p
}

// TestRuleRepo_ForServiceCarriesRunningPhase pins the phase a rule is served
// through. A rule attached to a phase is dark until the phase runs, and then the
// poll set carries the (experiment, phase) pair the compiler takes a cache-box
// rule's context from; a rule attached to no phase carries none. Only ForService
// sets the pair: it is the poll set's, not part of what is stored.
func TestRuleRepo_ForServiceCarriesRunningPhase(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("servedphase-%d", time.Now().UnixNano())
	svc := "svc-" + tag
	at := time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)

	free := createCacheBoxRule(t, svc, "free-"+tag, 1, at)         // attached to no phase
	attached := createCacheBoxRule(t, svc, "attached-"+tag, 2, at) // attached to one phase
	phase := seedPhase(t, tag, "iso", "pending", nil, attached.ID)

	served := func() map[string]*model.Rule {
		t.Helper()
		rules, err := testRuleRepo.ForService(ctx, svc)
		if err != nil {
			t.Fatalf("ForService: %v", err)
		}
		byID := map[string]*model.Rule{}
		for _, r := range rules {
			byID[r.ID] = r
		}
		return byID
	}
	wantNoPhase := func(step string, r *model.Rule) {
		t.Helper()
		if r == nil {
			t.Fatalf("%s: the unattached rule %s is not served", step, free.ID)
		}
		if r.ServedPhaseID != "" || r.ServedExperimentID != "" {
			t.Errorf("%s: unattached rule carries phase (%q, %q), want none", step, r.ServedExperimentID, r.ServedPhaseID)
		}
	}

	// Pending: the attached rule is dark, the unattached one names no phase.
	got := served()
	if got[attached.ID] != nil {
		t.Errorf("pending phase: attached rule is served")
	}
	wantNoPhase("pending phase", got[free.ID])

	// Running: the attached rule is served under the phase it is attached to.
	if _, err := testDB.ExecContext(ctx,
		`UPDATE experiment_phases SET status = 'running' WHERE id = $1`, phase.ID); err != nil {
		t.Fatalf("start phase: %v", err)
	}
	got = served()
	a := got[attached.ID]
	if a == nil {
		t.Fatalf("running phase: attached rule is not served")
	}
	if a.ServedPhaseID != phase.ID || a.ServedExperimentID != phase.ExperimentID {
		t.Errorf("running phase: attached rule carries (%q, %q), want (%q, %q)",
			a.ServedExperimentID, a.ServedPhaseID, phase.ExperimentID, phase.ID)
	}
	wantNoPhase("running phase", got[free.ID])

	// No other read names a phase: the rule as stored has none.
	stored, err := testRuleRepo.Get(ctx, attached.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	enabled, err := testRuleRepo.EnabledForService(ctx, svc)
	if err != nil {
		t.Fatalf("EnabledForService: %v", err)
	}
	reads := map[string][]*model.Rule{"Get": {stored}, "EnabledForService": enabled}
	for name, rules := range reads {
		for _, r := range rules {
			if r.ServedPhaseID != "" || r.ServedExperimentID != "" {
				t.Errorf("%s: rule %s carries phase (%q, %q), want none", name, r.ID, r.ServedExperimentID, r.ServedPhaseID)
			}
		}
	}
}

// TestRuleRepo_ForServiceNamesTheFirstRunningPhase pins which phase a rule
// attached to several running phases is served under, and that it is served
// once. Phases of different experiments can run together, and the rule's
// context must name the one the synthesized cache-box rule names: the first in
// ListRunningPhases order (newest-started, a missing start time last, then id),
// the order ActiveCacheBoxPhaseForService and FrozenServiceForService walk.
// A join that kept a row per attached phase would deliver the rule once per
// phase, each under a different pair, so the poll set is checked whole, in
// order, under the default plan and under a sequential scan.
func TestRuleRepo_ForServiceNamesTheFirstRunningPhase(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("servedmany-%d", time.Now().UnixNano())
	svc := "svc-" + tag
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)
	at := func(d time.Duration) *time.Time { ts := t0.Add(d); return &ts }

	// multi is attached to all three phases; high and low bracket it.
	high := createCacheBoxRule(t, svc, "high-"+tag, 9, t0)
	multi := createCacheBoxRule(t, svc, "multi-"+tag, 5, t0)
	low := createCacheBoxRule(t, svc, "low-"+tag, 1, t0)

	// Created in reverse id order, so a tie left to the scan would go the wrong
	// way. a is running without a start time.
	z := seedPhase(t, tag, "z", "running", at(2*time.Minute), multi.ID)
	m := seedPhase(t, tag, "m", "running", at(time.Minute), multi.ID)
	a := seedPhase(t, tag, "a", "running", nil, multi.ID)
	experimentOf := map[string]string{z.ID: z.ExperimentID, m.ID: m.ExperimentID, a.ID: a.ExperimentID}

	setStatus := func(p *model.ExperimentPhase, status string) {
		t.Helper()
		if _, err := testDB.ExecContext(ctx,
			`UPDATE experiment_phases SET status = $2::phase_status WHERE id = $1`, p.ID, status); err != nil {
			t.Fatalf("set %s to %s: %v", p.ID, status, err)
		}
	}
	setStarted := func(p *model.ExperimentPhase, startedAt *time.Time) {
		t.Helper()
		if _, err := testDB.ExecContext(ctx,
			`UPDATE experiment_phases SET started_at = $2 WHERE id = $1`, p.ID, startedAt); err != nil {
			t.Fatalf("set %s started at %v: %v", p.ID, startedAt, err)
		}
	}
	// firstRunning is the answer the cache-box role gives: the first of the
	// rule's phases in the order running phases are considered.
	expRepo := NewExperimentRepo(testDB)
	firstRunning := func() string {
		t.Helper()
		running, err := expRepo.ListRunningPhases(ctx)
		if err != nil {
			t.Fatalf("ListRunningPhases: %v", err)
		}
		for _, p := range running {
			if _, ours := experimentOf[p.ID]; ours {
				return p.ID
			}
		}
		return ""
	}
	repos := []struct {
		plan string
		repo *RuleRepo
	}{
		{"default plan", testRuleRepo},
		{"sequential scan", planRepo(t, "SET enable_indexscan = off", "SET enable_bitmapscan = off")},
	}
	check := func(step, wantPhase string) {
		t.Helper()
		if got := firstRunning(); got != wantPhase {
			t.Fatalf("%s: ListRunningPhases puts %q first among the rule's phases, want %q", step, got, wantPhase)
		}
		want := []string{high.ID, multi.ID, low.ID}
		if wantPhase == "" { // no attached phase runs: the rule is dark
			want = []string{high.ID, low.ID}
		}
		for _, r := range repos {
			got, err := r.repo.ForService(ctx, svc)
			if err != nil {
				t.Fatalf("%s: ForService (%s): %v", step, r.plan, err)
			}
			if !slices.Equal(ruleIDs(got), want) {
				t.Errorf("%s: ForService (%s) = %v, want %v (each rule once, in poll order)", step, r.plan, ruleIDs(got), want)
				continue
			}
			for _, rule := range got {
				wantP, wantE := "", ""
				if rule.ID == multi.ID {
					wantP, wantE = wantPhase, experimentOf[wantPhase]
				}
				if rule.ServedPhaseID != wantP || rule.ServedExperimentID != wantE {
					t.Errorf("%s: ForService (%s): rule %s carries (%q, %q), want (%q, %q)",
						step, r.plan, rule.ID, rule.ServedExperimentID, rule.ServedPhaseID, wantE, wantP)
				}
			}
		}
	}

	check("z started last", z.ID)
	setStatus(z, "completed") // not running: out of the running set, whatever its start time
	check("z completed: m beats a, which has no start time", m.ID)
	setStatus(m, "paused")
	check("m paused: a is the only one running", a.ID)
	// m resumes and a gets m's start time: the lower id, a, comes first. m's
	// phase was created before a's, so a tie left to the scan would pick m.
	setStatus(m, "running")
	setStarted(a, at(time.Minute))
	check("m and a started together", a.ID)
	setStatus(m, "completed")
	setStatus(a, "completed")
	check("nothing running", "")
}

// TestRuleCreate_DefaultsDisabled pins the creation contract behind the
// create→attach→enable experiment lifecycle: a rule whose JSON omits
// "enabled" is created DISABLED, end to end. The zero value governs (the
// handler adds no defaulting and Create writes the column explicitly, so the
// DB column default never applies). Restoring a default-true anywhere in the
// create path would reopen the pre-attachment fault-leak window.
func TestRuleCreate_DefaultsDisabled(t *testing.T) {
	ctx := context.Background()
	tag := fmt.Sprintf("defdis-%d", time.Now().UnixNano())
	specID := "spec-" + tag
	spec := &model.FaultSpec{
		ID: specID, Name: "defdis-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now(),
	}
	if err := testFaultRepo.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed fault spec: %v", err)
	}
	t.Cleanup(func() { _ = testFaultRepo.DeleteSpec(context.Background(), specID) })

	// The exact operator payload shape (runbook §3), minus "enabled".
	body := fmt.Sprintf(`{"id":"rule-%s","name":"defdis","service":"svc-%s","priority":100,
		"match":{},"action":{"type":"fault_spec","fault_spec_id":"%s"},"mode":"background"}`,
		tag, tag, specID)
	var rule model.Rule
	if err := json.Unmarshal([]byte(body), &rule); err != nil {
		t.Fatalf("decode rule payload: %v", err)
	}
	rule.CreatedAt, rule.UpdatedAt = time.Now(), time.Now()

	if err := testRuleRepo.Create(ctx, &rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rule.ID) })

	got, err := testRuleRepo.Get(ctx, rule.ID)
	if err != nil {
		t.Fatalf("get rule: %v", err)
	}
	if got.Enabled {
		t.Fatal("rule created without \"enabled\" came back enabled; creation must default to disabled")
	}
}

// TestRuleRepo_MutationsBumpServiceGeneration pins desired-state v1: every rule
// mutation bumps the target service's generation in the same transaction, an
// update that moves a rule between services bumps both, and a failed write
// (unknown id) bumps nothing.
func TestRuleRepo_MutationsBumpServiceGeneration(t *testing.T) {
	ctx := context.Background()
	gens := desired.NewGenerations(NewGenerationRepo(testDB), desired.NewBroker())
	repo := NewRuleRepo(testDB, gens)
	tag := fmt.Sprintf("gen-%d", time.Now().UnixNano())
	svcA, svcB := "svca-"+tag, "svcb-"+tag

	spec := &model.FaultSpec{ID: "spec-" + tag, Name: "gen-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now()}
	if err := testFaultRepo.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed spec: %v", err)
	}
	t.Cleanup(func() { _ = testFaultRepo.DeleteSpec(context.Background(), spec.ID) })

	gen := func(svc string) uint64 {
		t.Helper()
		g, err := gens.Get(ctx, svc)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	a0, b0 := gen(svcA), gen(svcB)

	rule := &model.Rule{ID: "rule-" + tag, Name: "gen-rule", Service: svcA, Enabled: true, Mode: "background",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: spec.ID}, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := repo.Create(ctx, rule); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = repo.Delete(context.Background(), rule.ID) })
	if gen(svcA) != a0+1 {
		t.Fatalf("create: generation(%s) = %d, want %d", svcA, gen(svcA), a0+1)
	}

	rule.Service = svcB // move: both services change
	if err := repo.Update(ctx, rule); err != nil {
		t.Fatalf("update: %v", err)
	}
	if gen(svcA) != a0+2 || gen(svcB) != b0+1 {
		t.Fatalf("move: generations = (%d, %d), want (%d, %d)", gen(svcA), gen(svcB), a0+2, b0+1)
	}

	if err := repo.Update(ctx, &model.Rule{ID: "rule-missing-" + tag, Name: "x", Service: svcA, Mode: "inline",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: spec.ID}}); err == nil {
		t.Fatal("update of unknown rule succeeded")
	}
	if gen(svcA) != a0+2 {
		t.Fatalf("failed update bumped %s to %d", svcA, gen(svcA))
	}

	if err := repo.Delete(ctx, rule.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if gen(svcB) != b0+2 {
		t.Fatalf("delete: generation(%s) = %d, want %d", svcB, gen(svcB), b0+2)
	}
}

// recordingBumper wraps the real generations and records the repo's calls in
// order ("bump <service>" for BumpTx, "notify <service>" for Notify). failBump
// makes BumpTx of that service fail; onNotify observes the database at the
// moment Notify is called.
type recordingBumper struct {
	inner    GenerationBumper
	calls    []string
	failBump string
	onNotify func(service string, generation uint64)
}

func (b *recordingBumper) BumpTx(ctx context.Context, tx *sql.Tx, service string) (uint64, error) {
	b.calls = append(b.calls, "bump "+service)
	if service == b.failBump {
		return 0, errors.New("injected BumpTx failure")
	}
	return b.inner.BumpTx(ctx, tx, service)
}

func (b *recordingBumper) Notify(service string, generation uint64) {
	b.calls = append(b.calls, "notify "+service)
	if b.onNotify != nil {
		b.onNotify(service, generation)
	}
	b.inner.Notify(service, generation)
}

// genFixture is a rule repo driven through a recordingBumper over the real
// generations, with a seeded fault spec and two unique service names that sort
// svcA < svcB.
type genFixture struct {
	t          *testing.T
	repo       *RuleRepo
	rec        *recordingBumper
	gens       *desired.Generations
	tag        string
	specID     string
	svcA, svcB string
}

func newGenFixture(t *testing.T) *genFixture {
	t.Helper()
	gens := desired.NewGenerations(NewGenerationRepo(testDB), desired.NewBroker())
	rec := &recordingBumper{inner: gens}
	tag := fmt.Sprintf("genrec-%d", time.Now().UnixNano())
	spec := &model.FaultSpec{ID: "spec-" + tag, Name: "genrec-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now()}
	if err := testFaultRepo.CreateSpec(context.Background(), spec); err != nil {
		t.Fatalf("seed spec: %v", err)
	}
	t.Cleanup(func() { _ = testFaultRepo.DeleteSpec(context.Background(), spec.ID) })
	return &genFixture{t: t, repo: NewRuleRepo(testDB, rec), rec: rec, gens: gens, tag: tag,
		specID: spec.ID, svcA: "svca-" + tag, svcB: "svcb-" + tag}
}

func (f *genFixture) rule(id, service string) *model.Rule {
	return &model.Rule{ID: id + "-" + f.tag, Name: "genrec-rule", Service: service, Enabled: true, Mode: "background",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: f.specID}, CreatedAt: time.Now(), UpdatedAt: time.Now()}
}

// seed persists a rule on service as test setup: its bump is not recorded.
func (f *genFixture) seed(id, service string) *model.Rule {
	f.t.Helper()
	rule := f.rule(id, service)
	if err := testRuleRepo.Create(context.Background(), rule); err != nil {
		f.t.Fatalf("seed rule: %v", err)
	}
	f.t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rule.ID) })
	return rule
}

func (f *genFixture) gen(service string) uint64 {
	f.t.Helper()
	g, err := f.gens.Get(context.Background(), service)
	if err != nil {
		f.t.Fatal(err)
	}
	return g
}

// TestRuleRepo_UpdateBumpsEachServiceOnceInSortedOrder pins the lock-ordering
// rule: BumpTx holds the service_generations row lock until commit, so updates
// moving rules in opposite directions must take the two rows in the same order
// or Postgres aborts one with a deadlock (40P01). Whichever way the rule moves
// the lower service name is bumped first, an update that stays put bumps its
// service once, and every notification follows all the bumps (the commit sits
// between them).
func TestRuleRepo_UpdateBumpsEachServiceOnceInSortedOrder(t *testing.T) {
	f := newGenFixture(t)
	rule := f.seed("rule", f.svcB)
	both := []string{"bump " + f.svcA, "bump " + f.svcB, "notify " + f.svcA, "notify " + f.svcB}

	for _, step := range []struct {
		name string
		to   string
		want []string
	}{
		{"move to the lower service", f.svcA, both},
		{"move to the higher service", f.svcB, both},
		{"stay on the same service", f.svcB, []string{"bump " + f.svcB, "notify " + f.svcB}},
	} {
		f.rec.calls = nil
		rule.Service = step.to
		if err := f.repo.Update(context.Background(), rule); err != nil {
			t.Fatalf("%s: update: %v", step.name, err)
		}
		if !slices.Equal(f.rec.calls, step.want) {
			t.Errorf("%s: calls = %q, want %q", step.name, f.rec.calls, step.want)
		}
	}
}

// TestRuleRepo_GenerationBumpIsAtomicWithTheRuleWrite pins "same transaction":
// a bump that fails takes the rule write down with it (and any bump the
// transaction already made), nothing is notified, and a notification only ever
// follows the commit of both the rule write and the bump.
func TestRuleRepo_GenerationBumpIsAtomicWithTheRuleWrite(t *testing.T) {
	ctx := context.Background()

	t.Run("failed bump rolls back create", func(t *testing.T) {
		f := newGenFixture(t)
		rule := f.rule("rule", f.svcA)
		a0 := f.gen(f.svcA)
		f.rec.failBump = f.svcA
		if err := f.repo.Create(ctx, rule); err == nil {
			t.Fatal("create succeeded although its bump failed")
		}
		if _, err := testRuleRepo.Get(ctx, rule.ID); !errors.Is(err, ErrNotFound) {
			t.Errorf("rule after failed create: err = %v, want ErrNotFound", err)
		}
		if got, want := f.rec.calls, []string{"bump " + f.svcA}; !slices.Equal(got, want) {
			t.Errorf("calls = %q, want %q (no notify)", got, want)
		}
		if f.gen(f.svcA) != a0 {
			t.Errorf("generation(%s) moved to %d", f.svcA, f.gen(f.svcA))
		}
	})

	t.Run("failed second bump rolls back update and the first bump", func(t *testing.T) {
		f := newGenFixture(t)
		rule := f.seed("rule", f.svcB)
		a0, b0 := f.gen(f.svcA), f.gen(f.svcB)
		f.rec.failBump = f.svcB // svcA is bumped first and must be rolled back with the rest
		moved := *rule
		moved.Service = f.svcA
		if err := f.repo.Update(ctx, &moved); err == nil {
			t.Fatal("update succeeded although its second bump failed")
		}
		if got, err := testRuleRepo.Get(ctx, rule.ID); err != nil || got.Service != f.svcB {
			t.Errorf("rule after failed update = %+v, %v; want it still on %s", got, err, f.svcB)
		}
		if got, want := f.rec.calls, []string{"bump " + f.svcA, "bump " + f.svcB}; !slices.Equal(got, want) {
			t.Errorf("calls = %q, want %q (no notify)", got, want)
		}
		if f.gen(f.svcA) != a0 || f.gen(f.svcB) != b0 {
			t.Errorf("generations = (%d, %d), want unchanged (%d, %d)", f.gen(f.svcA), f.gen(f.svcB), a0, b0)
		}
	})

	t.Run("failed bump rolls back delete", func(t *testing.T) {
		f := newGenFixture(t)
		rule := f.seed("rule", f.svcA)
		a0 := f.gen(f.svcA)
		f.rec.failBump = f.svcA
		if err := f.repo.Delete(ctx, rule.ID); err == nil {
			t.Fatal("delete succeeded although its bump failed")
		}
		if _, err := testRuleRepo.Get(ctx, rule.ID); err != nil {
			t.Errorf("rule after failed delete: %v, want it still present", err)
		}
		if f.gen(f.svcA) != a0 || !slices.Equal(f.rec.calls, []string{"bump " + f.svcA}) {
			t.Errorf("generation = %d (want %d), calls = %q (want just the bump)", f.gen(f.svcA), a0, f.rec.calls)
		}
	})

	t.Run("notify follows the commit", func(t *testing.T) {
		f := newGenFixture(t)
		rule := f.rule("rule", f.svcA)
		a0, b0 := f.gen(f.svcA), f.gen(f.svcB)

		// Notify runs on the writer's goroutine. Reading through the pool uses
		// another connection, which only sees committed data, so this records what
		// a poller woken by the notification would find.
		type notified struct {
			service     string
			generation  uint64 // the argument Notify received
			committed   uint64 // the service's generation as committed at that moment
			ruleService string // where the rule lives at that moment ("" = gone)
		}
		var seen []notified
		f.rec.onNotify = func(service string, generation uint64) {
			n := notified{service: service, generation: generation, committed: f.gen(service)}
			if got, err := testRuleRepo.Get(ctx, rule.ID); err == nil {
				n.ruleService = got.Service
			}
			seen = append(seen, n)
		}
		check := func(op string, want ...notified) {
			t.Helper()
			if !slices.Equal(seen, want) {
				t.Errorf("%s: notifications = %+v, want %+v", op, seen, want)
			}
			seen = nil
		}

		if err := f.repo.Create(ctx, rule); err != nil {
			t.Fatalf("create: %v", err)
		}
		t.Cleanup(func() { _ = testRuleRepo.Delete(ctx, rule.ID) })
		check("create", notified{f.svcA, a0 + 1, a0 + 1, f.svcA})

		rule.Service = f.svcB
		if err := f.repo.Update(ctx, rule); err != nil {
			t.Fatalf("update: %v", err)
		}
		check("update", notified{f.svcA, a0 + 2, a0 + 2, f.svcB}, notified{f.svcB, b0 + 1, b0 + 1, f.svcB})

		if err := f.repo.Delete(ctx, rule.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		check("delete", notified{f.svcB, b0 + 2, b0 + 2, ""})
	})
}
