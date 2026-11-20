package demo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// These tests run the sweep and retention against the real store (each skips
// unless MANTEION_TEST_DB is set). Between every step of a switch-off, a
// cancel or a deletion they ask RuleRepo.ForService what productcatalogservice
// is served, so a moment in which a demo rule is served shows up even when the
// end state is clean.

// dbLab is the real store on the test database, a load generator that holds
// one usable demo-products dataset, and an orchestrator whose cancel is the
// experiment's planned|running -> cancelled CAS.
type dbLab struct {
	t     *testing.T
	exps  *store.ExperimentRepo
	rules *store.RuleRepo
	d     Deps
}

func newDBLab(t *testing.T) *dbLab {
	t.Helper()
	conn := openTestDB(t)
	ctx := context.Background()
	clearDemoRows(t, conn)

	wfs := store.NewWorkflowRepo(conn)
	if _, err := wfs.Get(ctx, WorkflowID); errors.Is(err, store.ErrNotFound) {
		wf := &model.Workflow{ID: WorkflowID, Name: fmt.Sprintf("%s (test %d)", WorkflowID, time.Now().UnixNano()),
			DSL: json.RawMessage(`{"name":"demo-browse"}`), CreatedAt: time.Now()}
		if err := wfs.Create(ctx, wf); err != nil {
			t.Fatalf("register workflow %s: %v", WorkflowID, err)
		}
		t.Cleanup(func() { _ = wfs.Delete(context.Background(), WorkflowID) }) // runs after the rows below are gone
	} else if err != nil {
		t.Fatalf("read workflow %s: %v", WorkflowID, err)
	}
	t.Cleanup(func() { clearDemoRows(t, conn) })

	exps := store.NewExperimentRepo(conn)
	rules := store.NewRuleRepo(conn, nil)
	return &dbLab{t: t, exps: exps, rules: rules, d: Deps{
		Orch:        dbOrch{exps},
		Experiments: exps,
		Rules:       rules,
		Specs:       store.NewFaultRepo(conn),
		Workflows:   wfs,
		Zeus: stubZeus{datasets: []zeus.Dataset{{ID: "ds-sweep-db", Name: DatasetName, SizeBytes: 4096,
			CreatedAt: time.Now()}}},
		Clock:  &fakeClock{now: time.Now().UTC().Truncate(time.Millisecond), step: time.Second},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
}

// clearDemoRows removes demo rows an earlier run left: rules off, then the
// experiments (which drops their phase links), then the rules.
func clearDemoRows(t *testing.T, conn *sql.DB) {
	t.Helper()
	for _, q := range []string{
		`UPDATE rules SET enabled = false WHERE name LIKE 'demo-run-%'`,
		`DELETE FROM experiments WHERE created_by = 'manteion-demo'`,
		`DELETE FROM rules WHERE name LIKE 'demo-run-%' AND NOT EXISTS (SELECT 1 FROM phase_rules pr WHERE pr.rule_id = rules.id)`,
	} {
		if _, err := conn.ExecContext(context.Background(), q); err != nil {
			t.Fatalf("clear demo rows: %v", err)
		}
	}
}

// dbOrch cancels with the experiment's CAS alone: planned or running becomes
// cancelled; anything else is *orchestrator.ErrInvalidState, a missing
// experiment store.ErrNotFound.
type dbOrch struct{ exps *store.ExperimentRepo }

func (o dbOrch) StartExperiment(ctx context.Context, id string) error {
	return o.transition(ctx, id, "running", "planned")
}

func (o dbOrch) CancelExperiment(ctx context.Context, id string) error {
	return o.transition(ctx, id, "cancelled", "planned", "running")
}

func (o dbOrch) transition(ctx context.Context, id, to string, from ...string) error {
	ok, err := o.exps.TransitionExperiment(ctx, id, to, from...)
	if err != nil || ok {
		return err
	}
	e, err := o.exps.Get(ctx, id)
	if err != nil {
		return err
	}
	return &orchestrator.ErrInvalidState{Kind: "experiment", ID: id, Current: e.Status, Wanted: strings.Join(from, "|"),
		Msg: "test: experiment " + id + " is " + e.Status}
}

// build builds a run on the real store and returns it.
func (l *dbLab) build() seeded {
	l.t.Helper()
	s := seeded{runID: id.New(RunIDPrefix)}
	built, err := BuildRun(context.Background(), l.d, s.runID, 5*time.Minute)
	if err != nil {
		l.t.Fatalf("BuildRun: %v", err)
	}
	s.Built = built
	return s
}

func (l *dbLab) moveExperiment(expID, to string, from ...string) {
	l.t.Helper()
	if ok, err := l.exps.TransitionExperiment(context.Background(), expID, to, from...); err != nil || !ok {
		l.t.Fatalf("experiment %s -> %s: %v, %v", expID, to, ok, err)
	}
}

func (l *dbLab) movePhase(phaseID, to string, from ...string) {
	l.t.Helper()
	if ok, err := l.exps.TransitionPhase(context.Background(), phaseID, to, from...); err != nil || !ok {
		l.t.Fatalf("phase %s -> %s: %v, %v", phaseID, to, ok, err)
	}
}

// servedDemoRules lists the demo-run-* rules the store serves to
// productcatalogservice right now.
func (l *dbLab) servedDemoRules() []string {
	l.t.Helper()
	served, err := l.rules.ForService(context.Background(), TargetService)
	if err != nil {
		l.t.Fatalf("ForService: %v", err)
	}
	var out []string
	for _, r := range served {
		if strings.HasPrefix(r.Name, RunNamePrefix) {
			out = append(out, r.ID)
		}
	}
	return out
}

// servedLog records, before and after every write the spies wrap, the calls
// made and which demo rules were served at that moment.
type servedLog struct {
	l      *dbLab
	mu     sync.Mutex
	points []servedAt
}

type servedAt struct {
	point  string   // "before rules.SetEnabled <id>", "after experiments.Delete <id>", ...
	served []string // demo rule ids served for productcatalogservice
}

func (s *servedLog) at(point string) {
	served := s.l.servedDemoRules()
	s.mu.Lock()
	s.points = append(s.points, servedAt{point, served})
	s.mu.Unlock()
}

func (s *servedLog) around(what string, fn func() error) error {
	s.at("before " + what)
	err := fn()
	s.at("after " + what)
	return err
}

func (s *servedLog) all() []servedAt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.points)
}

// index is the position of the first point named point, or -1.
func (s *servedLog) index(point string) int {
	return slices.IndexFunc(s.all(), func(p servedAt) bool { return p.point == point })
}

type spyRules struct {
	Rules
	log *servedLog
}

func (r spyRules) SetEnabled(ctx context.Context, id string, enabled bool) error {
	return r.log.around(fmt.Sprintf("rules.SetEnabled %s %v", id, enabled), func() error {
		return r.Rules.SetEnabled(ctx, id, enabled)
	})
}

func (r spyRules) Delete(ctx context.Context, id string) error {
	return r.log.around("rules.Delete "+id, func() error { return r.Rules.Delete(ctx, id) })
}

type spyExperiments struct {
	Experiments
	log *servedLog
}

func (e spyExperiments) Delete(ctx context.Context, id string) error {
	return e.log.around("experiments.Delete "+id, func() error { return e.Experiments.Delete(ctx, id) })
}

type spyOrch struct {
	Orchestrator
	log *servedLog
}

func (o spyOrch) CancelExperiment(ctx context.Context, id string) error {
	return o.log.around("orch.CancelExperiment "+id, func() error { return o.Orchestrator.CancelExperiment(ctx, id) })
}

// spied returns the lab's deps with every write the sweep and retention make
// wrapped by a servedLog.
func (l *dbLab) spied() (Deps, *servedLog) {
	log := &servedLog{l: l}
	d := l.d
	d.Rules = spyRules{l.d.Rules, log}
	d.Experiments = spyExperiments{l.d.Experiments, log}
	d.Orch = spyOrch{l.d.Orch, log}
	return d, log
}

// requireNeverServed fails if a demo rule was served at any recorded point
// from the from-th on.
func requireNeverServed(t *testing.T, log *servedLog, from int) {
	t.Helper()
	pts := log.all()
	if len(pts) == 0 {
		t.Fatal("no point was recorded: the spies were not used")
	}
	for _, p := range pts[from:] {
		if len(p.served) != 0 {
			t.Errorf("%s: demo rules served %v; want none", p.point, p.served)
		}
	}
}

// TestSweep_DB_EnabledRuleOnPlannedExperiment (Review Focus 3): manteion died
// between switching a run's rule on and starting its experiment. At the next
// start the sweep switches the rule off, then cancels the planned experiment,
// and nothing is served at any step.
func TestSweep_DB_EnabledRuleOnPlannedExperiment(t *testing.T) {
	l := newDBLab(t)
	ctx := context.Background()
	run := l.build()
	if err := SetRuleEnabled(ctx, l.d.Rules, run.RuleID, true); err != nil {
		t.Fatalf("switch on: %v", err)
	}
	if r, err := l.rules.Get(ctx, run.RuleID); err != nil || !r.Enabled {
		t.Fatalf("precondition: rule %+v, %v; want enabled", r, err)
	}
	// The crash: nothing else happens to the run.

	d, log := l.spied()
	if err := Sweep(ctx, d, 20); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	r, err := l.rules.Get(ctx, run.RuleID)
	if err != nil || r.Enabled {
		t.Errorf("rule %+v, %v; want disabled", r, err)
	}
	e, err := l.exps.Get(ctx, run.ExperimentID)
	if err != nil || e.Status != "cancelled" {
		t.Errorf("experiment %+v, %v; want cancelled", e, err)
	}
	if served, err := l.rules.ForService(ctx, TargetService); err != nil || len(served) != 0 {
		t.Errorf("ForService(%s) = %d rules, %v; want none", TargetService, len(served), err)
	}
	requireNeverServed(t, log, 0)
	off, cancel := log.index("after rules.SetEnabled "+run.RuleID+" false"), log.index("before orch.CancelExperiment "+run.ExperimentID)
	if off < 0 || cancel < 0 || off > cancel {
		t.Errorf("switch-off at %d, cancel at %d; want the switch-off first", off, cancel)
	}
}

// TestSweep_DB_ServedRuleOnRunningExperiment: manteion died while the fault
// phase ran, its rule served. The sweep's first write switches it off; from
// then on it is never served, and the cancel follows.
func TestSweep_DB_ServedRuleOnRunningExperiment(t *testing.T) {
	l := newDBLab(t)
	ctx := context.Background()
	run := l.build()
	if err := SetRuleEnabled(ctx, l.d.Rules, run.RuleID, true); err != nil {
		t.Fatalf("switch on: %v", err)
	}
	l.moveExperiment(run.ExperimentID, "running", "planned")
	l.movePhase(run.PhaseIDs[0], "running", "pending")
	l.movePhase(run.PhaseIDs[0], "completed", "running")
	l.movePhase(run.PhaseIDs[FaultPhaseIndex], "running", "pending")
	if got := l.servedDemoRules(); !slices.Equal(got, []string{run.RuleID}) {
		t.Fatalf("precondition: served %v; want the run's rule", got)
	}

	d, log := l.spied()
	if err := Sweep(ctx, d, 20); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	off := log.index("after rules.SetEnabled " + run.RuleID + " false")
	cancel := log.index("before orch.CancelExperiment " + run.ExperimentID)
	if off < 0 || cancel < 0 || off > cancel {
		t.Fatalf("switch-off at %d, cancel at %d; want the switch-off first", off, cancel)
	}
	if first := log.all()[0]; first.point != "before rules.SetEnabled "+run.RuleID+" false" {
		t.Errorf("the sweep's first write was %q; want the switch-off", first.point)
	}
	requireNeverServed(t, log, off)
	if e, err := l.exps.Get(ctx, run.ExperimentID); err != nil || e.Status != "cancelled" {
		t.Errorf("experiment %+v, %v; want cancelled", e, err)
	}
	if got := l.servedDemoRules(); len(got) != 0 {
		t.Errorf("served after the sweep: %v", got)
	}
}

// TestRetain_DB_RuleNeverServedDuringDeletion (Review Focus 4): retention
// deletes a finished run whose rule is still enabled (and attached to a phase
// left running under the ended experiment). The rule is switched off, then the
// experiment deleted, then the rule; no demo rule is served before or after
// any of those writes; both rows are gone and the newer run is kept.
func TestRetain_DB_RuleNeverServedDuringDeletion(t *testing.T) {
	l := newDBLab(t)
	ctx := context.Background()
	old := l.build()
	if err := SetRuleEnabled(ctx, l.d.Rules, old.RuleID, true); err != nil {
		t.Fatalf("switch on: %v", err)
	}
	l.moveExperiment(old.ExperimentID, "running", "planned")
	l.movePhase(old.PhaseIDs[0], "running", "pending")
	l.movePhase(old.PhaseIDs[0], "completed", "running")
	l.movePhase(old.PhaseIDs[FaultPhaseIndex], "running", "pending")
	if got := l.servedDemoRules(); !slices.Equal(got, []string{old.RuleID}) {
		t.Fatalf("precondition: served %v; want the old run's rule", got)
	}
	l.moveExperiment(old.ExperimentID, "completed", "running") // its fault phase is left running
	if got := l.servedDemoRules(); len(got) != 0 {
		t.Fatalf("precondition: served %v under a completed experiment; want none", got)
	}
	newer := l.build()
	l.moveExperiment(newer.ExperimentID, "completed", "planned")

	d, log := l.spied()
	if err := Retain(ctx, d, 1); err != nil {
		t.Fatalf("Retain: %v", err)
	}

	requireNeverServed(t, log, 0)
	off := log.index("after rules.SetEnabled " + old.RuleID + " false")
	delExp := log.index("after experiments.Delete " + old.ExperimentID)
	delRule := log.index("after rules.Delete " + old.RuleID)
	if off < 0 || delExp < 0 || delRule < 0 || !(off < delExp && delExp < delRule) {
		t.Errorf("switch-off at %d, experiment delete at %d, rule delete at %d; want them in that order", off, delExp, delRule)
	}
	if _, err := l.rules.Get(ctx, old.RuleID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("old rule: %v; want not found", err)
	}
	if _, err := l.exps.Get(ctx, old.ExperimentID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("old experiment: %v; want not found", err)
	}
	if _, err := l.rules.Get(ctx, newer.RuleID); err != nil {
		t.Errorf("newer rule: %v; want kept", err)
	}
	if e, err := l.exps.Get(ctx, newer.ExperimentID); err != nil || e.Status != "completed" {
		t.Errorf("newer experiment %+v, %v; want kept, completed", e, err)
	}
	for _, p := range log.all() {
		if strings.Contains(p.point, newer.ExperimentID) || strings.Contains(p.point, newer.RuleID) {
			t.Errorf("the newer run was touched: %s", p.point)
		}
	}
}
