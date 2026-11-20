package demo

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
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// openTestDB opens (and migrates) the test database, or skips the test unless
// MANTEION_TEST_DB is set. There is no TestMain, so the package's other tests
// run without a database.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("MANTEION_TEST_DB") == "" {
		t.Skip("set MANTEION_TEST_DB=1 (and MANTEION_DATABASE_URL) to run against Postgres")
	}
	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(conn) })
	return conn
}

// TestFakeLab_ListMatchesStore_DB runs the same experiments through the real
// ExperimentRepo.List and the fake's, page by page.
func TestFakeLab_ListMatchesStore_DB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()
	repo := store.NewExperimentRepo(conn)
	f := newFakeLab(t, testNow)
	tag := fmt.Sprintf("fakecheck-%d", time.Now().UnixNano())
	base := time.Now().UTC().Truncate(time.Millisecond)

	statuses := []string{"planned", "completed", "planned", "failed", "cancelled", "planned", "completed"}
	for i, st := range statuses {
		at := base.Add(time.Duration(i) * time.Millisecond)
		if i == 6 {
			at = base.Add(5 * time.Millisecond) // a tie with #5, broken by id
		}
		exp := &model.Experiment{ID: fmt.Sprintf("exp-%s-%d", tag, i), Name: tag, Status: st, CreatedBy: tag, CreatedAt: at}
		if err := repo.Create(ctx, exp); err != nil {
			t.Fatalf("real create: %v", err)
		}
		t.Cleanup(func() { _, _ = conn.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
		cp := *exp
		if err := (fakeExperiments{f}).Create(ctx, &cp); err != nil {
			t.Fatalf("fake create: %v", err)
		}
	}

	ids := func(exps []*model.Experiment) []string {
		var out []string
		for _, e := range exps {
			out = append(out, e.ID)
		}
		return out
	}
	for _, flt := range []store.ExperimentFilter{{CreatedBy: tag}, {CreatedBy: tag, Status: "planned"}} {
		for _, p := range []store.Page{{Limit: 3}, {Limit: 3, Offset: 3}, {Limit: 3, Offset: 6}, {Limit: 3, Offset: 7},
			{Limit: 3, Offset: 100}, {}, {Limit: -1}, {Limit: 2, Offset: -3}, {Limit: 500}} {
			realExps, realTotal, err := repo.List(ctx, flt, p)
			if err != nil {
				t.Fatalf("real list: %v", err)
			}
			fakeExps, fakeTotal, err := (fakeExperiments{f}).List(ctx, flt, p)
			if err != nil {
				t.Fatalf("fake list: %v", err)
			}
			if !slices.Equal(ids(realExps), ids(fakeExps)) || realTotal != fakeTotal {
				t.Errorf("filter %+v page %+v: store %v total %d; fake %v total %d",
					flt, p, ids(realExps), realTotal, ids(fakeExps), fakeTotal)
			}
		}
	}
}

// TestFakeLab_ServingMatchesStore_DB drives one rule through the same states in
// Postgres and in the fake and compares RuleRepo.ForService with fake.served,
// and the two rule switches' answers.
func TestFakeLab_ServingMatchesStore_DB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()
	rules := store.NewRuleRepo(conn, nil)
	exps := store.NewExperimentRepo(conn)
	faults := store.NewFaultRepo(conn)
	f := newFakeLab(t, testNow)
	f.expectViolations() // the last step enables an unattached demo rule on purpose
	tag := fmt.Sprintf("fakeserve-%d", time.Now().UnixNano())
	svc := "svc-" + tag

	spec := &model.FaultSpec{ID: "spec-" + tag, Name: tag, Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{"delay":"50ms"}`), CreatedAt: time.Now()}
	if err := faults.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("spec: %v", err)
	}
	t.Cleanup(func() { _ = faults.DeleteSpec(context.Background(), spec.ID) })
	f.specs = append(f.specs, spec)

	newRule := func(id string) *model.Rule {
		return &model.Rule{ID: id, Name: RunNamePrefix + id, Service: svc, Priority: 100, Mode: "inline",
			Action: model.RuleAction{Type: "fault_spec", FaultSpecID: spec.ID}, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	}
	both := func(what string, real, fake func() error) {
		t.Helper()
		if err := real(); err != nil {
			t.Fatalf("%s (store): %v", what, err)
		}
		if err := fake(); err != nil {
			t.Fatalf("%s (fake): %v", what, err)
		}
	}
	ruleID, looseID := "rule-"+tag, "rule-loose-"+tag
	for _, id := range []string{ruleID, looseID} {
		both("create rule "+id, func() error { return rules.Create(ctx, newRule(id)) },
			func() error { return (fakeRules{f}).Create(ctx, newRule(id)) })
		t.Cleanup(func() { _ = rules.Delete(context.Background(), id) })
	}
	exp := &model.Experiment{ID: "exp-" + tag, Name: tag, Status: "planned", CreatedBy: tag, CreatedAt: time.Now()}
	phase := &model.ExperimentPhase{ID: "phase-" + tag, ExperimentID: exp.ID, Name: "fault", Position: 0, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{}}
	both("create experiment", func() error { e := *exp; return exps.Create(ctx, &e) },
		func() error { e := *exp; return (fakeExperiments{f}).Create(ctx, &e) })
	t.Cleanup(func() { _, _ = conn.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
	both("create phase", func() error { p := *phase; return exps.CreatePhase(ctx, &p) },
		func() error { p := *phase; return (fakeExperiments{f}).CreatePhase(ctx, &p) })
	both("attach", func() error { return exps.AttachPhaseRules(ctx, phase.ID, []string{ruleID}) },
		func() error { return (fakeExperiments{f}).AttachPhaseRules(ctx, phase.ID, []string{ruleID}) })

	compare := func(what string) {
		t.Helper()
		served, err := rules.ForService(ctx, svc)
		if err != nil {
			t.Fatalf("ForService: %v", err)
		}
		for _, id := range []string{ruleID, looseID} {
			real := slices.ContainsFunc(served, func(r *model.Rule) bool { return r.ID == id })
			if fake := f.served(id); real != fake {
				t.Errorf("%s: rule %s served by the store %v, by the fake %v", what, id, real, fake)
			}
		}
	}
	setExp := func(status string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, `UPDATE experiments SET status = $2::experiment_status WHERE id = $1`, exp.ID, status); err != nil {
			t.Fatal(err)
		}
		f.setExperimentStatus(exp.ID, status)
	}
	setPhase := func(status string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, `UPDATE experiment_phases SET status = $2::phase_status WHERE id = $1`, phase.ID, status); err != nil {
			t.Fatal(err)
		}
		f.setPhaseStatus(phase.ID, status)
	}
	sameErr := func(what string, real, fake error) {
		t.Helper()
		for _, target := range []error{nil, store.ErrNotFound, store.ErrRuleUnattached, store.ErrInUse} {
			if (target == nil && (real == nil) != (fake == nil)) ||
				(target != nil && errors.Is(real, target) != errors.Is(fake, target)) {
				t.Errorf("%s: store %v, fake %v", what, real, fake)
				return
			}
		}
	}

	compare("built: disabled, planned, pending")
	sameErr("enable attached", rules.EnableIfAttached(ctx, ruleID), (fakeRules{f}).EnableIfAttached(ctx, ruleID))
	compare("enabled, planned, pending")
	setExp("running")
	compare("enabled, experiment running, phase pending")
	setPhase("running")
	compare("enabled, experiment and phase running")
	setPhase("draining")
	compare("phase draining")
	setPhase("running")
	setExp("cancelled")
	compare("phase running under a cancelled experiment")
	setExp("running")
	sameErr("disable", rules.SetEnabled(ctx, ruleID, false), (fakeRules{f}).SetEnabled(ctx, ruleID, false))
	compare("disabled while its phase runs")
	sameErr("disable again", rules.SetEnabled(ctx, ruleID, false), (fakeRules{f}).SetEnabled(ctx, ruleID, false))
	sameErr("enable unattached", rules.EnableIfAttached(ctx, looseID), (fakeRules{f}).EnableIfAttached(ctx, looseID))
	compare("unattached rule refused")
	missing := "rule-missing-" + tag
	sameErr("enable missing", rules.EnableIfAttached(ctx, missing), (fakeRules{f}).EnableIfAttached(ctx, missing))
	sameErr("disable missing", rules.SetEnabled(ctx, missing, false), (fakeRules{f}).SetEnabled(ctx, missing, false))
	sameErr("delete attached rule", rules.Delete(ctx, ruleID), (fakeRules{f}).Delete(ctx, ruleID))
	both("delete experiment", func() error { return exps.Delete(ctx, exp.ID) },
		func() error { return (fakeExperiments{f}).Delete(ctx, exp.ID) })
	compare("experiment deleted, rule disabled")
	sameErr("force on unattached", rules.SetEnabled(ctx, ruleID, true), (fakeRules{f}).SetEnabled(ctx, ruleID, true))
	compare("enabled with no phase left: served everywhere")
	if v := f.violations(); len(v) == 0 {
		t.Errorf("the fake did not flag an enabled demo rule attached to no phase")
	}
	sameErr("disable", rules.SetEnabled(ctx, ruleID, false), (fakeRules{f}).SetEnabled(ctx, ruleID, false))
}
