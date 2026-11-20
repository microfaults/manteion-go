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
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// fakeLab is an in-memory stand-in for the store repos and zeus that the
// builder and SetRuleEnabled use. It keeps the constraints of the real store
// that the demo's safety depends on:
//   - every create runs the model's Validate, as the repos do;
//   - deleting an experiment removes its phases and their workflow and rule
//     links (ON DELETE CASCADE);
//   - deleting a rule that a phase still links is refused (phase_rules.rule_id
//     is RESTRICT), so a cleanup that deletes the rule first fails here as it
//     would against Postgres;
//   - every method fails with ctx.Err() once ctx is done.
//
// Every call is recorded as "<repo>.<Method> <id or name>". A failure is
// injected with fail("<repo>.<Method>", err) for every call or
// fail("<repo>.<Method>#n", err) for the n-th call only (1-based); onCall runs a
// hook before the failure is returned (to cancel the caller's context, say).
type fakeLab struct {
	mu       sync.Mutex
	calls    []string
	failures map[string]error
	hooks    map[string]func()
	counts   map[string]int

	experiments map[string]*model.Experiment
	phases      map[string]*model.ExperimentPhase
	phaseWFs    map[string][]model.PhaseWorkflow
	phaseRules  map[string][]string
	rules       map[string]*model.Rule
	enabledEver map[string]bool
	specs       []*model.FaultSpec
	workflows   map[string]bool
	datasets    []zeus.Dataset
	now         time.Time
}

func newFakeLab(now time.Time) *fakeLab {
	return &fakeLab{
		failures:    map[string]error{},
		hooks:       map[string]func(){},
		counts:      map[string]int{},
		experiments: map[string]*model.Experiment{},
		phases:      map[string]*model.ExperimentPhase{},
		phaseWFs:    map[string][]model.PhaseWorkflow{},
		phaseRules:  map[string][]string{},
		rules:       map[string]*model.Rule{},
		enabledEver: map[string]bool{},
		workflows:   map[string]bool{WorkflowID: true},
		datasets: []zeus.Dataset{{ID: "ds-ok", Name: DatasetName, SizeBytes: 4096, TTLS: 0,
			CreatedAt: now.Add(-time.Hour)}},
		now: now,
	}
}

// fixedClock is a Clock that always answers t.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func (f *fakeLab) deps() Deps {
	return Deps{
		Experiments: fakeExperiments{f},
		Rules:       fakeRules{f},
		Specs:       fakeSpecs{f},
		Workflows:   fakeWorkflows{f},
		Zeus:        fakeZeus{f},
		Clock:       fixedClock{f.now},
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func (f *fakeLab) fail(op string, err error) { f.mu.Lock(); f.failures[op] = err; f.mu.Unlock() }
func (f *fakeLab) onCall(op string, hook func()) {
	f.mu.Lock()
	f.hooks[op] = hook
	f.mu.Unlock()
}

// enter records the call and answers the injected failure, if any. The caller
// holds f.mu.
func (f *fakeLab) enter(ctx context.Context, op, arg string) error {
	f.calls = append(f.calls, op+" "+arg)
	f.counts[op]++
	nth := op + "#" + strconv.Itoa(f.counts[op])
	for _, key := range []string{nth, op} {
		if err, ok := f.failures[key]; ok {
			if hook := f.hooks[key]; hook != nil {
				hook()
			}
			return err
		}
	}
	return ctx.Err()
}

// callList returns the recorded calls whose operation starts with prefix.
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.Create", exp.Name); err != nil {
		return err
	}
	if err := exp.Validate(); err != nil {
		return fmt.Errorf("validate: %w", store.ErrValidation)
	}
	if _, ok := f.experiments[exp.ID]; ok {
		return store.ErrConflict
	}
	cp := *exp
	f.experiments[exp.ID] = &cp
	return nil
}

func (x fakeExperiments) CreatePhase(ctx context.Context, p *model.ExperimentPhase) error {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.CreatePhase", p.Name); err != nil {
		return err
	}
	if err := p.Validate(); err != nil {
		return fmt.Errorf("validate: %w", store.ErrValidation)
	}
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
	return nil
}

func (x fakeExperiments) AttachPhaseWorkflows(ctx context.Context, phaseID string, pws []model.PhaseWorkflow) error {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.AttachPhaseWorkflows", phaseID); err != nil {
		return err
	}
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
	return nil
}

func (x fakeExperiments) AttachPhaseRules(ctx context.Context, phaseID string, ruleIDs []string) error {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.AttachPhaseRules", phaseID+" "+strings.Join(ruleIDs, ",")); err != nil {
		return err
	}
	if _, ok := f.phases[phaseID]; !ok {
		return store.ErrNotFound
	}
	for _, id := range ruleIDs {
		if _, ok := f.rules[id]; !ok {
			return fmt.Errorf("unknown rule: %w", store.ErrValidation)
		}
	}
	f.phaseRules[phaseID] = slices.Clone(ruleIDs)
	return nil
}

func (x fakeExperiments) Get(ctx context.Context, id string) (*model.Experiment, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.Get", id); err != nil {
		return nil, err
	}
	exp, ok := f.experiments[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *exp
	return &cp, nil
}

func (x fakeExperiments) ListPhasesForExperiment(ctx context.Context, experimentID string) ([]*model.ExperimentPhase, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.ListPhasesForExperiment", experimentID); err != nil {
		return nil, err
	}
	return f.phasesOf(experimentID), nil
}

func (x fakeExperiments) ListWorkflowResultsForPhase(ctx context.Context, phaseID string) ([]*model.PhaseWorkflowResult, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	return nil, f.enter(ctx, "experiments.ListWorkflowResultsForPhase", phaseID)
}

func (x fakeExperiments) RunningExperimentIDs(ctx context.Context) ([]string, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.RunningExperimentIDs", ""); err != nil {
		return nil, err
	}
	var out []string
	for id, e := range f.experiments {
		if e.Status == "running" {
			out = append(out, id)
		}
	}
	return out, nil
}

func (x fakeExperiments) List(ctx context.Context, flt store.ExperimentFilter, _ store.Page) ([]*model.Experiment, int, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.List", flt.CreatedBy+"/"+flt.Status); err != nil {
		return nil, 0, err
	}
	var out []*model.Experiment
	for _, e := range f.experiments {
		if (flt.Status == "" || e.Status == flt.Status) && (flt.CreatedBy == "" || e.CreatedBy == flt.CreatedBy) {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out, len(out), nil
}

func (x fakeExperiments) Delete(ctx context.Context, id string) error {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.Delete", id); err != nil {
		return err
	}
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
	return nil
}

func (x fakeExperiments) TransitionExperiment(ctx context.Context, id, to string, from ...string) (bool, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "experiments.TransitionExperiment", id+" "+to); err != nil {
		return false, err
	}
	exp, ok := f.experiments[id]
	if !ok || !slices.Contains(from, exp.Status) {
		return false, nil
	}
	exp.Status = to
	return true, nil
}

// ---- Rules ----

type fakeRules struct{ f *fakeLab }

func (x fakeRules) Create(ctx context.Context, rule *model.Rule) error {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "rules.Create", rule.Name); err != nil {
		return err
	}
	if err := rule.Validate(); err != nil {
		return err
	}
	if _, ok := f.rules[rule.ID]; ok {
		return store.ErrConflict
	}
	cp := *rule
	f.rules[rule.ID] = &cp
	f.enabledEver[rule.ID] = f.enabledEver[rule.ID] || rule.Enabled
	return nil
}

func (x fakeRules) Get(ctx context.Context, id string) (*model.Rule, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "rules.Get", id); err != nil {
		return nil, err
	}
	r, ok := f.rules[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *r
	return &cp, nil
}

func (x fakeRules) Update(ctx context.Context, rule *model.Rule) error {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "rules.Update", rule.ID+" enabled="+strconv.FormatBool(rule.Enabled)); err != nil {
		return err
	}
	if err := rule.Validate(); err != nil {
		return err
	}
	if _, ok := f.rules[rule.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *rule
	f.rules[rule.ID] = &cp
	f.enabledEver[rule.ID] = f.enabledEver[rule.ID] || rule.Enabled
	return nil
}

func (x fakeRules) Delete(ctx context.Context, id string) error {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "rules.Delete", id); err != nil {
		return err
	}
	if _, ok := f.rules[id]; !ok {
		return store.ErrNotFound
	}
	for pid, ids := range f.phaseRules {
		if slices.Contains(ids, id) {
			return fmt.Errorf("rule %s still linked by phase %s: restrict violation", id, pid)
		}
	}
	delete(f.rules, id)
	return nil
}

func (x fakeRules) List(ctx context.Context) ([]*model.Rule, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "rules.List", ""); err != nil {
		return nil, err
	}
	var out []*model.Rule
	for _, r := range f.rules {
		cp := *r
		out = append(out, &cp)
	}
	return out, nil
}

// ---- Specs, workflows, zeus ----

type fakeSpecs struct{ f *fakeLab }

func (x fakeSpecs) CreateSpec(ctx context.Context, spec *model.FaultSpec) error {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "specs.CreateSpec", spec.Name); err != nil {
		return err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	cp := *spec
	f.specs = append(f.specs, &cp)
	return nil
}

func (x fakeSpecs) ListSpecs(ctx context.Context) ([]*model.FaultSpec, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "specs.ListSpecs", ""); err != nil {
		return nil, err
	}
	out := make([]*model.FaultSpec, 0, len(f.specs))
	for _, s := range f.specs {
		cp := *s
		out = append(out, &cp)
	}
	return out, nil
}

type fakeWorkflows struct{ f *fakeLab }

func (x fakeWorkflows) Get(ctx context.Context, id string) (*model.Workflow, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "workflows.Get", id); err != nil {
		return nil, err
	}
	if !f.workflows[id] {
		return nil, store.ErrNotFound
	}
	return &model.Workflow{ID: id, Name: id}, nil
}

type fakeZeus struct{ f *fakeLab }

func (x fakeZeus) Healthy(ctx context.Context) bool {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.enter(ctx, "zeus.Healthy", "") == nil
}

func (x fakeZeus) ListDatasets(ctx context.Context) ([]zeus.Dataset, error) {
	f := x.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.enter(ctx, "zeus.ListDatasets", ""); err != nil {
		return nil, err
	}
	return slices.Clone(f.datasets), nil
}

// errLeaky is an injected cause whose text a client must never see: a driver
// prefix, an address and ids.
var errLeaky = errors.New("pq: dial 192.0.2.10:5432 failed for exp-123 phase-9 rule-7")
