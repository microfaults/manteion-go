package demo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// fakeLab is an in-memory stand-in for the store repos and zeus, for every
// test of this package. It keeps the store's behaviour that the demo's safety
// depends on:
//   - every create runs the model's Validate, as the repos do, and a rule
//     naming an unknown fault spec is refused (rules.fault_spec_id is a
//     foreign key);
//   - deleting an experiment removes its phases and their workflow and rule
//     links (ON DELETE CASCADE);
//   - deleting a rule that a phase still links is refused (phase_rules.rule_id
//     is RESTRICT);
//   - Rules.SetEnabled always writes; Rules.EnableIfAttached enables only a
//     rule some phase links, else store.ErrRuleUnattached;
//   - Experiments.List pages, orders and counts like ExperimentRepo.List;
//   - served(id) decides serving like RuleRepo.ForService: enabled, and
//     attached to no phase or to a running phase of a running experiment;
//   - the orchestrator (fakeOrch) starts and cancels experiments over these
//     rows as orchestrator.StartExperiment and CancelExperiment do;
//   - every method fails with ctx.Err() once ctx is done, except
//     fakeOrch.StartExperiment, which completes a start whose claim is under
//     way however its context ends (see fakeOrch).
//
// requireDemoRulesOff asserts, at any moment, that no demo-run-* rule is
// enabled or served: worker tests call it after every ending.
//
// After every mutation it checks the demo's invariant: no demo-run-* rule is
// enabled while attached to no phase (such a rule is served to every instance
// of its service). A breach is recorded in violations(); newFakeLab fails the
// test at cleanup if any was recorded, unless expectViolations was called.
//
// Every call is recorded as "<repo>.<Method> <argument>". Injections apply to
// every call of an operation ("experiments.Delete") or to its n-th call only
// ("experiments.CreatePhase#2", 1-based):
//   - fail(op, err): return err, write nothing;
//   - failAfterCommit(op, err): write, then return err (a commit whose answer
//     was lost);
//   - hang(op): block until ctx is done, write nothing, return ctx.Err();
//   - onCall(op, hook): run hook first; hooks added for one op all run, in
//     the order they were added.
//
// Hooks run and hangs wait without f.mu held, so a hook may call back into
// the fake.
type fakeLab struct {
	mu         sync.Mutex
	calls      []string
	injections map[string]injection
	hooks      map[string][]func()
	counts     map[string]int
	viols      []string
	allowViols bool
	watched    map[string]bool // more rules the invariant covers (watchRule)

	clock       *fakeClock
	experiments map[string]*model.Experiment
	phases      map[string]*model.ExperimentPhase
	phaseWFs    map[string][]model.PhaseWorkflow
	phaseRules  map[string][]string
	rules       map[string]*model.Rule
	enabledEver map[string]bool
	specs       []*model.FaultSpec
	workflows   map[string]bool
	datasets    []zeus.Dataset
	instances   []*model.SDKInstance
	results     map[string][]*model.PhaseWorkflowResult // by phase id
	promValues  map[string]float64                      // fakeMetrics.QueryInstant's answers, by query
	// autopilot: each ListPhasesForExperiment of a running demo experiment
	// answers, then moves the run one step on (see advanceLocked).
	autopilot bool
}

type injectKind int

const (
	injectFail injectKind = iota
	injectFailAfterCommit
	injectHang
)

type injection struct {
	kind injectKind
	err  error
}

// newFakeLab returns a lab holding workflow demo-browse and one usable
// demo-products dataset, whose clock starts at now and stands still. It fails
// t at cleanup if the invariant was ever broken.
func newFakeLab(t testing.TB, now time.Time) *fakeLab {
	f := &fakeLab{
		injections:  map[string]injection{},
		hooks:       map[string][]func(){},
		counts:      map[string]int{},
		clock:       &fakeClock{now: now},
		experiments: map[string]*model.Experiment{},
		phases:      map[string]*model.ExperimentPhase{},
		phaseWFs:    map[string][]model.PhaseWorkflow{},
		phaseRules:  map[string][]string{},
		rules:       map[string]*model.Rule{},
		enabledEver: map[string]bool{},
		workflows:   map[string]bool{WorkflowID: true},
		datasets: []zeus.Dataset{{ID: "ds-ok", Name: DatasetName, SizeBytes: 4096, TTLS: 0,
			CreatedAt: now.Add(-time.Hour)}},
		instances:  []*model.SDKInstance{{ID: "inst-1", Service: TargetService, Status: "alive"}},
		results:    map[string][]*model.PhaseWorkflowResult{},
		promValues: map[string]float64{},
	}
	t.Cleanup(func() {
		if !f.allowViols {
			f.requireNoViolations(t)
		}
	})
	return f
}

// fakeClock is a Clock that answers now and then moves on by step (0: it
// stands still); Advance moves it by hand.
type fakeClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.now
	c.now = c.now.Add(c.step)
	return t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *fakeClock) SetStep(d time.Duration) {
	c.mu.Lock()
	c.step = d
	c.mu.Unlock()
}

func (f *fakeLab) deps() Deps {
	return Deps{
		Orch:        fakeOrch{f},
		Experiments: fakeExperiments{f},
		Rules:       fakeRules{f},
		Specs:       fakeSpecs{f},
		Workflows:   fakeWorkflows{f},
		Zeus:        fakeZeus{f},
		Instances:   fakeInstances{f},
		Prom:        fakeMetrics{f},
		DB:          fakePinger{f},
		Clock:       f.clock,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func (f *fakeLab) inject(op string, inj injection) {
	f.mu.Lock()
	f.injections[op] = inj
	f.mu.Unlock()
}

func (f *fakeLab) fail(op string, err error) { f.inject(op, injection{injectFail, err}) }
func (f *fakeLab) failAfterCommit(op string, err error) {
	f.inject(op, injection{injectFailAfterCommit, err})
}
func (f *fakeLab) hang(op string) { f.inject(op, injection{kind: injectHang}) }
func (f *fakeLab) onCall(op string, hook func()) {
	f.mu.Lock()
	f.hooks[op] = append(f.hooks[op], hook)
	f.mu.Unlock()
}

// begin records the call and applies what was injected for it, without f.mu
// held while hooks run or a hang waits. err means: fail now, write nothing.
// after is the error to return once the write is done.
func (f *fakeLab) begin(ctx context.Context, op, arg string) (after, err error) {
	f.mu.Lock()
	f.calls = append(f.calls, op+" "+arg)
	f.counts[op]++
	keys := []string{op + "#" + strconv.Itoa(f.counts[op]), op}
	var hooks []func()
	var inj *injection
	for _, k := range keys {
		hooks = append(hooks, f.hooks[k]...)
		if i, ok := f.injections[k]; ok && inj == nil {
			inj = &i
		}
	}
	f.mu.Unlock()

	for _, h := range hooks {
		h()
	}
	if inj != nil {
		switch inj.kind {
		case injectFail:
			return nil, inj.err
		case injectHang:
			<-ctx.Done()
			return nil, ctx.Err()
		case injectFailAfterCommit:
			after = inj.err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return after, nil
}

// ---- invariant, serving and inspection ----

// checkLocked records a violation for every demo-run-* rule, and every rule a
// test watches (watchRule), that is enabled and attached to no phase. Called
// with f.mu held after each mutation.
func (f *fakeLab) checkLocked(op string) {
	for id, r := range f.rules {
		if r.Enabled && (strings.HasPrefix(r.Name, RunNamePrefix) || f.watched[id]) && !f.attachedLocked(id) {
			f.viols = append(f.viols,
				fmt.Sprintf("after %s: rule %s (%s) is enabled and attached to no phase", op, id, r.Name))
		}
	}
}

func (f *fakeLab) attachedLocked(ruleID string) bool {
	for _, ids := range f.phaseRules {
		if slices.Contains(ids, ruleID) {
			return true
		}
	}
	return false
}

// served reports whether the store would serve the rule now (RuleRepo.ForService).
func (f *fakeLab) served(ruleID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rules[ruleID]
	if !ok || !r.Enabled {
		return false
	}
	attached := false
	for pid, ids := range f.phaseRules {
		if !slices.Contains(ids, ruleID) {
			continue
		}
		attached = true
		if p := f.phases[pid]; p != nil && p.Status == "running" {
			if e := f.experiments[p.ExperimentID]; e != nil && e.Status == "running" {
				return true
			}
		}
	}
	return !attached
}

// violations returns every invariant breach recorded so far.
func (f *fakeLab) violations() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.viols)
}

// requireNoViolations fails t if the invariant was ever broken. It waits at
// most a second for the fake's lock, so a test that deadlocked inside the fake
// fails instead of hanging the whole binary.
func (f *fakeLab) requireNoViolations(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !f.mu.TryLock() {
		if time.Now().After(deadline) {
			t.Errorf("the fake's lock is still held at the end of the test (a deadlock?)")
			return
		}
		time.Sleep(time.Millisecond)
	}
	viols := slices.Clone(f.viols)
	f.mu.Unlock()
	for _, v := range viols {
		t.Errorf("invariant broken: %s", v)
	}
}

// requireDemoRulesOff fails t if any demo-run-* rule is enabled, or would be
// served, right now. Every ending switches its run's rule off before anything
// else, so worker tests call this immediately after each end event.
func (f *fakeLab) requireDemoRulesOff(t testing.TB) {
	t.Helper()
	f.mu.Lock()
	var ids []string
	for id, r := range f.rules {
		if strings.HasPrefix(r.Name, RunNamePrefix) {
			ids = append(ids, id)
			if r.Enabled {
				t.Errorf("demo rule %s (%s) is still enabled", id, r.Name)
			}
		}
	}
	f.mu.Unlock()
	for _, id := range ids {
		if f.served(id) {
			t.Errorf("demo rule %s is served", id)
		}
	}
}

// requireRunRuleOff fails t if the rule of run runID is enabled or served.
// A run whose build never created a rule passes.
func (f *fakeLab) requireRunRuleOff(t testing.TB, runID string) {
	t.Helper()
	id := f.ruleIDOfRun(runID)
	if id == "" {
		return
	}
	if r := f.rule(id); r != nil && r.Enabled {
		t.Errorf("run %s: its rule is still enabled", runID)
	}
	if f.served(id) {
		t.Errorf("run %s: its rule is served", runID)
	}
}

// ruleIDOfRun returns the id of run runID's rule, or "".
func (f *fakeLab) ruleIDOfRun(runID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, r := range f.rules {
		if r.Name == RunNamePrefix+runID {
			return id
		}
	}
	return ""
}

// experimentIDOfRun returns the id of run runID's experiment, or "".
func (f *fakeLab) experimentIDOfRun(runID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, e := range f.experiments {
		if e.Name == RunNamePrefix+runID {
			return id
		}
	}
	return ""
}

// endExperiment ends an experiment as the orchestrator would: status, start
// and end times; phases still pending or in flight are skipped, the others
// keep their status.
func (f *fakeLab) endExperiment(expID, status string, started, ended time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.experiments[expID]
	e.Status, e.StartedAt, e.CompletedAt = status, &started, &ended
	for _, p := range f.phases {
		if p.ExperimentID == expID {
			switch p.Status {
			case "pending", "running", "paused", "draining":
				p.Status = "skipped"
			}
		}
	}
	f.checkLocked("endExperiment")
}

// ruleEnabledEver reports whether the rule was ever switched on.
func (f *fakeLab) ruleEnabledEver(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.enabledEver[id]
}

// experimentStatus returns the experiment's status, or "" when it is gone.
func (f *fakeLab) experimentStatus(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.experiments[id]; ok {
		return e.Status
	}
	return ""
}

// demoRuleIDs lists the demo-run-* rules, sorted.
func (f *fakeLab) demoRuleIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for id, r := range f.rules {
		if strings.HasPrefix(r.Name, RunNamePrefix) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// watchRule makes the invariant cover rule id too, whatever its name: for a
// test whose operator rule must never end up enabled and attached to nothing.
func (f *fakeLab) watchRule(id string) {
	f.mu.Lock()
	if f.watched == nil {
		f.watched = map[string]bool{}
	}
	f.watched[id] = true
	f.mu.Unlock()
}

// expectViolations stops the cleanup check, for a test that breaks the
// invariant on purpose.
func (f *fakeLab) expectViolations() {
	f.mu.Lock()
	f.allowViols = true
	f.mu.Unlock()
}

// setPhaseStatus and setExperimentStatus move rows as the orchestrator would.
func (f *fakeLab) setPhaseStatus(phaseID, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.phases[phaseID].Status = status
	f.checkLocked("setPhaseStatus")
}

func (f *fakeLab) setExperimentStatus(expID, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.experiments[expID].Status = status
	f.checkLocked("setExperimentStatus")
}

// rule returns a copy of the rule, or nil.
func (f *fakeLab) rule(id string) *model.Rule {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rules[id]
	if !ok {
		return nil
	}
	cp := *r
	return &cp
}

// callList returns the recorded calls whose text starts with prefix.
func (f *fakeLab) callList(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

// leftovers lists every per-run row still held: experiments, phases, links and
// rules. Fault specs are shared across runs and not listed.
func (f *fakeLab) leftovers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for id := range f.experiments {
		out = append(out, "experiment "+id)
	}
	for id := range f.phases {
		out = append(out, "phase "+id)
	}
	for id := range f.phaseWFs {
		out = append(out, "phase_workflows "+id)
	}
	for id := range f.phaseRules {
		out = append(out, "phase_rules "+id)
	}
	for id := range f.rules {
		out = append(out, "rule "+id)
	}
	slices.Sort(out)
	return out
}

func (f *fakeLab) phasesOf(expID string) []*model.ExperimentPhase {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.phasesOfLocked(expID)
}

func (f *fakeLab) phasesOfLocked(expID string) []*model.ExperimentPhase {
	var out []*model.ExperimentPhase
	for _, p := range f.phases {
		if p.ExperimentID == expID {
			cp := *p
			out = append(out, &cp)
		}
	}
	slices.SortFunc(out, func(a, b *model.ExperimentPhase) int { return a.Position - b.Position })
	return out
}

// ---- Experiments ----

type fakeExperiments struct{ f *fakeLab }

func (x fakeExperiments) Create(ctx context.Context, exp *model.Experiment) error {
	f := x.f
	after, err := f.begin(ctx, "experiments.Create", exp.Name)
	if err != nil {
		return err
	}
	if err := exp.Validate(); err != nil {
		return fmt.Errorf("validate: %w", store.ErrValidation)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.experiments[exp.ID]; ok {
		return store.ErrConflict
	}
	cp := *exp
	f.experiments[exp.ID] = &cp
	f.checkLocked("experiments.Create")
	return after
}

func (x fakeExperiments) CreatePhase(ctx context.Context, p *model.ExperimentPhase) error {
	f := x.f
	after, err := f.begin(ctx, "experiments.CreatePhase", p.Name)
	if err != nil {
		return err
	}
	if err := p.Validate(); err != nil {
		return fmt.Errorf("validate: %w", store.ErrValidation)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.experiments[p.ExperimentID]; !ok {
		return store.ErrNotFound
	}
	for _, q := range f.phases {
		if q.ExperimentID == p.ExperimentID && q.Position == p.Position {
			return store.ErrConflict
		}
	}
	cp := *p
	f.phases[p.ID] = &cp
	f.checkLocked("experiments.CreatePhase")
	return after
}

func (x fakeExperiments) AttachPhaseWorkflows(ctx context.Context, phaseID string, pws []model.PhaseWorkflow) error {
	f := x.f
	after, err := f.begin(ctx, "experiments.AttachPhaseWorkflows", phaseID)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.phases[phaseID]; !ok {
		return store.ErrNotFound
	}
	rows := make([]model.PhaseWorkflow, 0, len(pws))
	for _, pw := range pws {
		pw.PhaseID = phaseID
		if err := pw.Validate(); err != nil {
			return fmt.Errorf("validate: %w", store.ErrValidation)
		}
		if !f.workflows[pw.WorkflowID] {
			return fmt.Errorf("unknown workflow: %w", store.ErrValidation)
		}
		rows = append(rows, pw)
	}
	f.phaseWFs[phaseID] = rows
	f.checkLocked("experiments.AttachPhaseWorkflows")
	return after
}

func (x fakeExperiments) AttachPhaseRules(ctx context.Context, phaseID string, ruleIDs []string) error {
	f := x.f
	after, err := f.begin(ctx, "experiments.AttachPhaseRules", phaseID+" "+strings.Join(ruleIDs, ","))
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.phases[phaseID]; !ok {
		return store.ErrNotFound
	}
	for _, id := range ruleIDs {
		if _, ok := f.rules[id]; !ok {
			return fmt.Errorf("unknown rule: %w", store.ErrValidation)
		}
	}
	f.phaseRules[phaseID] = slices.Clone(ruleIDs)
	f.checkLocked("experiments.AttachPhaseRules")
	return after
}

func (x fakeExperiments) Get(ctx context.Context, id string) (*model.Experiment, error) {
	f := x.f
	after, err := f.begin(ctx, "experiments.Get", id)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	exp, ok := f.experiments[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *exp
	return &cp, after
}

func (x fakeExperiments) ListPhasesForExperiment(ctx context.Context, experimentID string) ([]*model.ExperimentPhase, error) {
	f := x.f
	after, err := f.begin(ctx, "experiments.ListPhasesForExperiment", experimentID)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.phasesOfLocked(experimentID)
	if f.autopilot {
		f.advanceLocked(experimentID)
	}
	return out, after
}

// advanceLocked moves a running demo experiment one step on, as the
// orchestrator would over a phase's lifetime: the running phase completes
// (with a harvested result row), the next pending one starts, and after the
// last one the experiment completes.
func (f *fakeLab) advanceLocked(expID string) {
	exp := f.experiments[expID]
	if exp == nil || exp.Status != "running" || exp.CreatedBy != CreatedBy {
		return
	}
	now := f.clock.Now()
	var byPos []*model.ExperimentPhase
	for _, p := range f.phases {
		if p.ExperimentID == expID {
			byPos = append(byPos, p)
		}
	}
	slices.SortFunc(byPos, func(a, b *model.ExperimentPhase) int { return a.Position - b.Position })
	for i, p := range byPos {
		if p.Status != "running" {
			continue
		}
		p.Status, p.CompletedAt = "completed", &now
		f.results[p.ID] = []*model.PhaseWorkflowResult{{PhaseID: p.ID, WorkflowID: WorkflowID,
			RequestCount: 290 + int64(i), ErrorCount: int64(i), LatencyP50Us: 110_000, LatencyP95Us: 250_500,
			LatencyP99Us: 359_000, ComputedAt: now}}
		if i+1 < len(byPos) {
			byPos[i+1].Status, byPos[i+1].StartedAt = "running", &now
		} else {
			exp.Status, exp.CompletedAt = "completed", &now
		}
		f.checkLocked("autopilot")
		return
	}
}

func (x fakeExperiments) ListWorkflowResultsForPhase(ctx context.Context, phaseID string) ([]*model.PhaseWorkflowResult, error) {
	f := x.f
	after, err := f.begin(ctx, "experiments.ListWorkflowResultsForPhase", phaseID)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.PhaseWorkflowResult
	for _, r := range f.results[phaseID] {
		cp := *r
		out = append(out, &cp)
	}
	return out, after
}

func (x fakeExperiments) RunningExperimentIDs(ctx context.Context) ([]string, error) {
	f := x.f
	after, err := f.begin(ctx, "experiments.RunningExperimentIDs", "")
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for id, e := range f.experiments {
		if e.Status == "running" {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out, after
}

// List mirrors ExperimentRepo.List: limit <= 0 is 20 and above 200 is 200, a
// negative offset is 0; newest created first, ties by id; the total is a
// window count over the returned rows, so 0 when the offset is past the end.
func (x fakeExperiments) List(ctx context.Context, flt store.ExperimentFilter, p store.Page) ([]*model.Experiment, int, error) {
	f := x.f
	after, err := f.begin(ctx, "experiments.List", flt.CreatedBy+"/"+flt.Status)
	if err != nil {
		return nil, 0, err
	}
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if p.Limit > 200 {
		p.Limit = 200
	}
	if p.Offset < 0 {
		p.Offset = 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var match []*model.Experiment
	for _, e := range f.experiments {
		if (flt.Status == "" || e.Status == flt.Status) && (flt.CreatedBy == "" || e.CreatedBy == flt.CreatedBy) {
			cp := *e
			match = append(match, &cp)
		}
	}
	slices.SortFunc(match, func(a, b *model.Experiment) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	if p.Offset >= len(match) {
		return nil, 0, after
	}
	end := min(p.Offset+p.Limit, len(match))
	return match[p.Offset:end], len(match), after
}

// ListPhaseRules mirrors ExperimentRepo.ListPhaseRules: the phase's links in
// position order; a phase with none, or no such phase, is an empty list.
func (x fakeExperiments) ListPhaseRules(ctx context.Context, phaseID string) ([]model.PhaseRule, error) {
	f := x.f
	after, err := f.begin(ctx, "experiments.ListPhaseRules", phaseID)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []model.PhaseRule
	for i, ruleID := range f.phaseRules[phaseID] {
		out = append(out, model.PhaseRule{PhaseID: phaseID, RuleID: ruleID, Position: i})
	}
	return out, after
}

func (x fakeExperiments) Delete(ctx context.Context, id string) error {
	f := x.f
	after, err := f.begin(ctx, "experiments.Delete", id)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.experiments[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.experiments, id)
	for pid, p := range f.phases {
		if p.ExperimentID == id {
			delete(f.phases, pid)
			delete(f.phaseWFs, pid)
			delete(f.phaseRules, pid)
		}
	}
	f.checkLocked("experiments.Delete")
	return after
}

func (x fakeExperiments) TransitionExperiment(ctx context.Context, id, to string, from ...string) (bool, error) {
	f := x.f
	after, err := f.begin(ctx, "experiments.TransitionExperiment", id+" "+to)
	if err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	exp, ok := f.experiments[id]
	if !ok || !slices.Contains(from, exp.Status) {
		return false, after
	}
	exp.Status = to
	f.checkLocked("experiments.TransitionExperiment")
	return true, after
}

// ---- Rules ----

type fakeRules struct{ f *fakeLab }

func (x fakeRules) Create(ctx context.Context, rule *model.Rule) error {
	f := x.f
	after, err := f.begin(ctx, "rules.Create", rule.Name)
	if err != nil {
		return err
	}
	if err := rule.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rules[rule.ID]; ok {
		return store.ErrConflict
	}
	if id := rule.Action.FaultSpecID; id != "" && !slices.ContainsFunc(f.specs, func(s *model.FaultSpec) bool { return s.ID == id }) {
		return errors.New("insert rule: foreign key violation on rules_fault_spec_id_fkey")
	}
	cp := *rule
	f.rules[rule.ID] = &cp
	f.enabledEver[rule.ID] = f.enabledEver[rule.ID] || rule.Enabled
	f.checkLocked("rules.Create")
	return after
}

func (x fakeRules) Get(ctx context.Context, id string) (*model.Rule, error) {
	f := x.f
	after, err := f.begin(ctx, "rules.Get", id)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rules[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *r
	return &cp, after
}

// SetEnabled always writes, like RuleRepo.SetEnabled.
func (x fakeRules) SetEnabled(ctx context.Context, id string, enabled bool) error {
	f := x.f
	after, err := f.begin(ctx, "rules.SetEnabled", id+" "+strconv.FormatBool(enabled))
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rules[id]
	if !ok {
		return store.ErrNotFound
	}
	r.Enabled = enabled
	f.enabledEver[id] = f.enabledEver[id] || enabled
	f.checkLocked("rules.SetEnabled")
	return after
}

// EnableIfAttached enables only a rule some phase links, like
// RuleRepo.EnableIfAttached.
func (x fakeRules) EnableIfAttached(ctx context.Context, id string) error {
	f := x.f
	after, err := f.begin(ctx, "rules.EnableIfAttached", id)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rules[id]
	if !ok {
		return store.ErrNotFound
	}
	if !f.attachedLocked(id) {
		return store.ErrRuleUnattached
	}
	r.Enabled = true
	f.enabledEver[id] = true
	f.checkLocked("rules.EnableIfAttached")
	return after
}

func (x fakeRules) Delete(ctx context.Context, id string) error {
	f := x.f
	after, err := f.begin(ctx, "rules.Delete", id)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rules[id]; !ok {
		return store.ErrNotFound
	}
	if f.attachedLocked(id) {
		return fmt.Errorf("delete rule %s: a phase still links it: %w", id, store.ErrInUse)
	}
	delete(f.rules, id)
	f.checkLocked("rules.Delete")
	return after
}

func (x fakeRules) List(ctx context.Context) ([]*model.Rule, error) {
	f := x.f
	after, err := f.begin(ctx, "rules.List", "")
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.Rule
	for _, r := range f.rules {
		cp := *r
		out = append(out, &cp)
	}
	slices.SortFunc(out, func(a, b *model.Rule) int {
		if a.Priority != b.Priority {
			return b.Priority - a.Priority
		}
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, after
}

// ---- Specs, workflows, zeus ----

type fakeSpecs struct{ f *fakeLab }

func (x fakeSpecs) CreateSpec(ctx context.Context, spec *model.FaultSpec) error {
	f := x.f
	after, err := f.begin(ctx, "specs.CreateSpec", spec.Name)
	if err != nil {
		return err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *spec
	f.specs = append(f.specs, &cp)
	return after
}

func (x fakeSpecs) ListSpecs(ctx context.Context) ([]*model.FaultSpec, error) {
	f := x.f
	after, err := f.begin(ctx, "specs.ListSpecs", "")
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*model.FaultSpec, 0, len(f.specs))
	for _, s := range f.specs {
		cp := *s
		out = append(out, &cp)
	}
	return out, after
}

type fakeWorkflows struct{ f *fakeLab }

func (x fakeWorkflows) Get(ctx context.Context, id string) (*model.Workflow, error) {
	f := x.f
	after, err := f.begin(ctx, "workflows.Get", id)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.workflows[id] {
		return nil, store.ErrNotFound
	}
	return &model.Workflow{ID: id, Name: id}, after
}

type fakeZeus struct{ f *fakeLab }

func (x fakeZeus) Healthy(ctx context.Context) bool {
	after, err := x.f.begin(ctx, "zeus.Healthy", "")
	return err == nil && after == nil
}

func (x fakeZeus) ListDatasets(ctx context.Context) ([]zeus.Dataset, error) {
	f := x.f
	after, err := f.begin(ctx, "zeus.ListDatasets", "")
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.datasets), after
}

// ---- Orchestrator and instances ----

// fakeOrch stands in for *orchestrator.Orchestrator over the lab's rows.
//
// StartExperiment claims a planned experiment (planned -> running) and starts
// its first phase. Its hooks and injections run as for any call, but it then
// completes the claim whatever its context says: the real start's claim can
// land before it notices a cancelled context, so a worker must never assume a
// cancelled start started nothing. A missing experiment is ErrNotFound and a
// non-planned one *orchestrator.ErrInvalidState.
//
// CancelExperiment is terminateExperiment: a planned or running experiment
// becomes cancelled, its running, paused and draining phases and its pending
// ones skipped; any other status answers *orchestrator.ErrInvalidState, a
// missing experiment ErrNotFound. It honours its context like the store.
type fakeOrch struct{ f *fakeLab }

func (o fakeOrch) StartExperiment(ctx context.Context, id string) error {
	f := o.f
	after, err := f.begin(context.WithoutCancel(ctx), "orch.StartExperiment", id)
	if err != nil {
		return err
	}
	now := f.clock.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	exp, ok := f.experiments[id]
	if !ok {
		return store.ErrNotFound
	}
	if exp.Status != "planned" {
		return &orchestrator.ErrInvalidState{Kind: "experiment", ID: id, Current: exp.Status, Wanted: "planned",
			Msg: "fake: experiment " + id + " is not planned"}
	}
	exp.Status, exp.StartedAt = "running", &now
	for _, p := range f.phases {
		if p.ExperimentID == id && p.Position == 0 && p.Status == "pending" {
			p.Status, p.StartedAt = "running", &now
		}
	}
	f.checkLocked("orch.StartExperiment")
	return after
}

func (o fakeOrch) CancelExperiment(ctx context.Context, id string) error {
	f := o.f
	after, err := f.begin(ctx, "orch.CancelExperiment", id)
	if err != nil {
		return err
	}
	now := f.clock.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	exp, ok := f.experiments[id]
	if !ok {
		return store.ErrNotFound
	}
	if exp.Status != "planned" && exp.Status != "running" {
		return &orchestrator.ErrInvalidState{Kind: "experiment", ID: id, Current: exp.Status,
			Wanted: "planned|running", Msg: "fake: experiment " + id + " is already terminal"}
	}
	exp.Status, exp.CompletedAt = "cancelled", &now
	for _, p := range f.phases {
		if p.ExperimentID != id {
			continue
		}
		switch p.Status {
		case "running", "paused", "draining", "pending":
			p.Status, p.CompletedAt = "skipped", &now
		}
	}
	f.checkLocked("orch.CancelExperiment")
	return after
}

// fakeInstances answers InstancesForService from f.instances.
type fakeInstances struct{ f *fakeLab }

func (x fakeInstances) InstancesForService(ctx context.Context, service string) ([]*model.SDKInstance, error) {
	f := x.f
	after, err := f.begin(ctx, "instances.InstancesForService", service)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.SDKInstance
	for _, in := range f.instances {
		if in.Service == service {
			cp := *in
			out = append(out, &cp)
		}
	}
	return out, after
}

// ---- Prometheus and the database ----

// fakeMetrics stands in for *promql.Client. Healthy is "prom.Healthy" and
// answers false on any injected error; QueryInstant is "prom.QueryInstant
// <query>" and answers f.promValues[query] (0 when unset).
type fakeMetrics struct{ f *fakeLab }

func (x fakeMetrics) Healthy(ctx context.Context) bool {
	after, err := x.f.begin(ctx, "prom.Healthy", "")
	return err == nil && after == nil
}

func (x fakeMetrics) QueryInstant(ctx context.Context, query string) (float64, error) {
	f := x.f
	after, err := f.begin(ctx, "prom.QueryInstant", query)
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.promValues[query], after
}

// fakePinger stands in for *sql.DB: PingContext is "db.PingContext".
type fakePinger struct{ f *fakeLab }

func (x fakePinger) PingContext(ctx context.Context) error {
	after, err := x.f.begin(ctx, "db.PingContext", "")
	if err != nil {
		return err
	}
	return after
}

// errLeaky is an injected cause whose text a client must never see: a driver
// prefix, an address and ids.
var errLeaky = errors.New("pq: dial 192.0.2.10:5432 failed for exp-123 phase-9 rule-7")
