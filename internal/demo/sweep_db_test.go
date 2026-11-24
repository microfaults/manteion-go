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

// demoWorkflow is the workflow every phase of a demo run runs: the catalog's
// browse workload's, and build 2's.
const demoWorkflow = "demo-browse"

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
	if _, err := wfs.Get(ctx, demoWorkflow); errors.Is(err, store.ErrNotFound) {
		wf := &model.Workflow{ID: demoWorkflow, Name: fmt.Sprintf("%s (test %d)", demoWorkflow, time.Now().UnixNano()),
			DSL: json.RawMessage(`{"name":"demo-browse"}`), CreatedAt: time.Now()}
		if err := wfs.Create(ctx, wf); err != nil {
			t.Fatalf("register workflow %s: %v", demoWorkflow, err)
		}
		t.Cleanup(func() { _ = wfs.Delete(context.Background(), demoWorkflow) }) // runs after the rows below are gone
	} else if err != nil {
		t.Fatalf("read workflow %s: %v", demoWorkflow, err)
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
		Zeus: stubZeus{datasets: []zeus.Dataset{{ID: "ds-sweep-db", Name: "demo-products", SizeBytes: 4096,
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

// build writes a run of build 2's shape on the real store (buildOldRun) and
// returns it.
func (l *dbLab) build() seeded {
	l.t.Helper()
	s, err := buildOldRun(context.Background(), l.d, id.New(RunIDPrefix))
	if err != nil {
		l.t.Fatalf("build: %v", err)
	}
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
	served, err := l.rules.ForService(context.Background(), testTarget)
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
	watch  map[string]string // more rules to look for: rule id -> its service
	mu     sync.Mutex
	points []servedAt
}

type servedAt struct {
	point  string   // "before rules.SetEnabled <id>", "after experiments.Delete <id>", ...
	served []string // demo rule ids served for productcatalogservice, and watched rules served for theirs
}

func (s *servedLog) at(point string) {
	served := s.l.servedDemoRules()
	for ruleID, svc := range s.watch {
		rules, err := s.l.rules.ForService(context.Background(), svc)
		if err != nil {
			s.l.t.Errorf("ForService(%s): %v", svc, err)
			continue
		}
		if slices.ContainsFunc(rules, func(r *model.Rule) bool { return r.ID == ruleID }) {
			served = append(served, ruleID)
		}
	}
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
// wrapped by a servedLog, which also looks for the watched rules (rule id ->
// service).
func (l *dbLab) spied(watch ...string) (Deps, *servedLog) {
	log := &servedLog{l: l, watch: map[string]string{}}
	for i := 0; i+1 < len(watch); i += 2 {
		log.watch[watch[i]] = watch[i+1]
	}
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
	if served, err := l.rules.ForService(ctx, testTarget); err != nil || len(served) != 0 {
		t.Errorf("ForService(%s) = %d rules, %v; want none", testTarget, len(served), err)
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
	l.movePhase(run.PhaseIDs[oldFaultPhase], "running", "pending")
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
	l.movePhase(old.PhaseIDs[oldFaultPhase], "running", "pending")
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

// TestRetain_DB_ForeignRuleKeepsExperiment: an operator linked a second,
// enabled, differently named rule to a phase of a finished demo experiment.
// Retention with a keep that would delete the experiment keeps it (its run's
// own rule switched off), says the pass is incomplete, and the foreign rule is
// never served, before or after any write: deleting the experiment would have
// left it attached to nothing and served to every instance of its service.
func TestRetain_DB_ForeignRuleKeepsExperiment(t *testing.T) {
	l := newDBLab(t)
	ctx := context.Background()
	old := l.build()
	if err := SetRuleEnabled(ctx, l.d.Rules, old.RuleID, true); err != nil {
		t.Fatalf("switch on: %v", err)
	}
	l.moveExperiment(old.ExperimentID, "completed", "planned")
	own, err := l.rules.Get(ctx, old.RuleID)
	if err != nil {
		t.Fatal(err)
	}
	tag := fmt.Sprintf("%d", time.Now().UnixNano())
	foreign := &model.Rule{ID: "rule-foreign-" + tag, Name: "op-foreign-" + tag, Service: "svc-foreign-" + tag,
		Priority: 10, Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: own.Action.FaultSpecID},
		CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := l.rules.Create(ctx, foreign); err != nil {
		t.Fatalf("create the operator's rule: %v", err)
	}
	t.Cleanup(func() { // before the lab's own clean-up: unlink by deleting the experiment, then the rule
		bg := context.Background()
		_ = l.rules.SetEnabled(bg, foreign.ID, false)
		_ = l.exps.Delete(bg, old.ExperimentID)
		_ = l.rules.Delete(bg, foreign.ID)
	})
	if err := l.exps.AttachPhaseRules(ctx, old.PhaseIDs[0], []string{foreign.ID}); err != nil {
		t.Fatalf("link the operator's rule: %v", err)
	}
	if err := l.rules.SetEnabled(ctx, foreign.ID, true); err != nil {
		t.Fatal(err)
	}
	newer := l.build()
	l.moveExperiment(newer.ExperimentID, "completed", "planned")

	d, log := l.spied(foreign.ID, foreign.Service)
	log.at("before the pass")
	if p := log.all()[0]; len(p.served) != 0 {
		t.Fatalf("precondition: served %v under a completed experiment; want none", p.served)
	}
	err = Retain(ctx, d, 1)
	if !errors.Is(err, ErrRetainIncomplete) || !strings.Contains(err.Error(), "foreign_rule_linked") {
		t.Fatalf("Retain: %v; want incomplete at foreign_rule_linked", err)
	}
	log.at("after the pass")

	requireNeverServed(t, log, 0)
	if e, err := l.exps.Get(ctx, old.ExperimentID); err != nil || e.Status != "completed" {
		t.Errorf("old experiment %+v, %v; want kept", e, err)
	}
	if r, err := l.rules.Get(ctx, old.RuleID); err != nil || r.Enabled {
		t.Errorf("the run's own rule %+v, %v; want kept, off", r, err)
	}
	if r, err := l.rules.Get(ctx, foreign.ID); err != nil || !r.Enabled {
		t.Errorf("the operator's rule %+v, %v; want untouched", r, err)
	}
	if links, err := l.exps.ListPhaseRules(ctx, old.PhaseIDs[0]); err != nil || len(links) != 1 {
		t.Errorf("the operator's link %+v, %v; want kept", links, err)
	}
	if log.index("before experiments.Delete "+old.ExperimentID) >= 0 {
		t.Error("the old experiment's delete was attempted")
	}
	if _, err := l.exps.Get(ctx, newer.ExperimentID); err != nil {
		t.Errorf("newer experiment: %v; want kept", err)
	}
}

// buildPair writes a run of scenario sc on the real store through BuildRun.
func (l *dbLab) buildPair(sc Scenario) pair {
	l.t.Helper()
	runID := id.New(RunIDPrefix)
	built, err := BuildRun(context.Background(), l.d, runID, sc, testRunCap)
	if err != nil {
		l.t.Fatalf("build a run of %s: %v", sc.ID, err)
	}
	return pair{runID: runID, ExperimentID: built.ExperimentID, RuleIDs: built.RuleIDs, PhaseIDs: built.PhaseIDs}
}

// requireDBOrder fails unless, in log, every rule of rules was switched off
// before the experiment was deleted, and each rule deleted after it.
func requireDBOrder(t *testing.T, log *servedLog, expID string, rules ...string) {
	t.Helper()
	delExp := log.index("after experiments.Delete " + expID)
	for _, rule := range rules {
		off := log.index("after rules.SetEnabled " + rule + " false")
		del := log.index("after rules.Delete " + rule)
		if off < 0 || delExp < 0 || del < 0 || !(off < delExp && delExp < del) {
			t.Errorf("experiment %s, rule %s: switch-off at %d, experiment delete at %d, rule delete at %d; want them in that order",
				expID, rule, off, delExp, del)
		}
	}
}

// TestTidy_DB_MixedLab: on the real store, runs of build 2's shape beside
// runs of a scenario, rules of both kinds left on. The start-up switch-off
// switches every one off and deletes nothing; then, with some switched on
// again, retention keeping one removes the three oldest, each run's rules
// off before its experiment and deleted after it, and no demo rule is ever
// served at any write.
func TestTidy_DB_MixedLab(t *testing.T) {
	l := newDBLab(t)
	ctx := context.Background()
	old1 := l.build()
	p1 := l.buildPair(DefaultScenario())
	old2 := l.build()
	p2 := l.buildPair(DefaultScenario())
	all := []string{old1.RuleID, p1.RuleIDs[0], p1.RuleIDs[1], old2.RuleID, p2.RuleIDs[0], p2.RuleIDs[1]}
	on := func(rules ...string) {
		t.Helper()
		for _, rule := range rules {
			if err := SetRuleEnabled(ctx, l.d.Rules, rule, true); err != nil {
				t.Fatalf("switch on %s: %v", rule, err)
			}
		}
	}
	on(all...)

	d, log := l.spied()
	if err := SwitchOffAll(ctx, d); err != nil {
		t.Fatalf("SwitchOffAll: %v", err)
	}
	for _, rule := range all {
		if r, err := l.rules.Get(ctx, rule); err != nil || r.Enabled {
			t.Errorf("rule %s after the start-up switch-off: %+v, %v; want off", rule, r, err)
		}
	}
	for _, p := range log.all() {
		if !strings.Contains(p.point, "rules.SetEnabled ") || strings.HasSuffix(p.point, " true") {
			t.Errorf("the start-up switch-off made %q; want only switch-offs", p.point)
		}
	}

	for _, e := range []string{old1.ExperimentID, p1.ExperimentID, old2.ExperimentID, p2.ExperimentID} {
		l.moveExperiment(e, "completed", "planned")
	}
	on(old1.RuleID, p1.RuleIDs[0], p1.RuleIDs[1])
	d, log = l.spied()
	if err := Retain(ctx, d, 1); err != nil {
		t.Fatalf("Retain: %v", err)
	}
	requireNeverServed(t, log, 0)
	requireDBOrder(t, log, old1.ExperimentID, old1.RuleID)
	requireDBOrder(t, log, p1.ExperimentID, p1.RuleIDs[:]...)
	requireDBOrder(t, log, old2.ExperimentID, old2.RuleID)
	for _, gone := range []string{old1.RuleID, p1.RuleIDs[0], p1.RuleIDs[1], old2.RuleID} {
		if _, err := l.rules.Get(ctx, gone); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("rule %s: %v; want deleted", gone, err)
		}
	}
	for _, kept := range p2.RuleIDs {
		if _, err := l.rules.Get(ctx, kept); err != nil {
			t.Errorf("the newest run's rule %s: %v; want kept", kept, err)
		}
	}
	if _, err := l.exps.Get(ctx, p2.ExperimentID); err != nil {
		t.Errorf("the newest run's experiment: %v; want kept", err)
	}
}

// TestView_DB_StoredRuns (Review Focus 3 and 5): on the real store, a worker
// that has none of them in memory reads a run of a catalog scenario as that
// scenario builds it, a run of a scenario no catalog holds with its stored id
// and its faults from its rules and specs, and a run of build 2's shape as
// build 2 served it.
func TestView_DB_StoredRuns(t *testing.T) {
	l := newDBLab(t)
	ctx := context.Background()
	catalogRun := l.buildPair(DefaultScenario())
	removed := l.buildPair(goneScenario("gone-split", "extreme", 0.15, 0.75))
	old := l.build()
	for _, e := range []string{catalogRun.ExperimentID, removed.ExperimentID, old.ExperimentID} {
		l.moveExperiment(e, "completed", "planned")
	}
	w := NewWorker(l.d, workerConfig(), nil, nil)
	cases := []struct {
		runID, scenario string
		phases          [3][6]any
	}{
		{catalogRun.runID, "catalog-moderate", [3][6]any{{"baseline", nil, nil, nil, nil, nil},
			{"memory", "memory", "productcatalogservice", "moderate", nil, nil}, {"cpu", "cpu", "productcatalogservice", "moderate", nil, nil}}},
		{removed.runID, "gone-split", [3][6]any{{"baseline", nil, nil, nil, nil, nil},
			{"memory", "memory", "productcatalogservice", "extreme", nil, nil}, {"cpu", "cpu", "cartservice", "extreme", nil, nil}}},
		{old.runID, "", [3][6]any{{"plain", nil, nil, nil, nil, nil},
			{"catalog-latency-50ms", "+50 ms latency on productcatalogservice", nil, nil, nil, nil}, {"plain-end", nil, nil, nil, nil, nil}}},
	}
	for _, tc := range cases {
		v, err := w.View(ctx, tc.runID)
		if err != nil {
			t.Errorf("View(%s): %v", tc.runID, err)
			continue
		}
		if v.Scenario != tc.scenario || v.Status != RunCompleted {
			t.Errorf("View(%s): scenario %q, status %s; want %q, completed", tc.runID, v.Scenario, v.Status, tc.scenario)
		}
		if got := phaseFaults(t, v); got != tc.phases {
			t.Errorf("View(%s): phases\n got %v\nwant %v", tc.runID, got, tc.phases)
		}
	}
}
