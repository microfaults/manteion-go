package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// Names the demo gives the rows it owns.
const (
	// CreatedBy is the created_by of every demo experiment.
	CreatedBy = "manteion-demo"
	// RunNamePrefix + run id names a run's experiment; RuleName adds the
	// fault's kind for each of its rules.
	RunNamePrefix = "demo-run-"
	// RunIDPrefix is the prefix of run ids: id.New(RunIDPrefix).
	RunIDPrefix = "demo"
)

// A run's load, rules and specs.
const (
	loadVUs      = 30
	loadRateRPS  = 3
	rulePriority = 100
	// ruleMode: the SDK runs every non-inline fault in the background anyway.
	ruleMode = "background"
	// specCategory is the atropos category of both fault kinds.
	specCategory = "resource"
	// specDurationMs outlasts a phase: the phase's end withdraws the rule,
	// which stops the fault.
	specDurationMs = 60000
	// datasetSlack is how long past the run cap a dataset must still live.
	datasetSlack = time.Minute
	// cleanupTimeout bounds each delete of a failed build's cleanup, which
	// runs on a context detached from the caller's (that one may be what
	// failed).
	cleanupTimeout = 10 * time.Second
	// switchOffTimeout bounds a rule switch-off, which runs on a context
	// detached from the caller's so that it happens during a shutdown or after
	// a watchdog fired.
	switchOffTimeout = 10 * time.Second
)

// Internal errors. They are for logs and for choosing a Reason; no error, nor
// its text, ever goes into a client's body.
var (
	// ErrNoDataset: the load generator cannot run the preset (no usable
	// demo-products dataset, zeus unreachable, or no demo-browse workflow).
	ErrNoDataset = errors.New("demo: the load generator has no usable dataset or workflow")
	// ErrBuild: the run's rows could not be written.
	ErrBuild = errors.New("demo: the run could not be built")
	// ErrCleanupIncomplete: a failed build could not delete everything it
	// wrote. What is left is a planned experiment and a disabled rule, which
	// serve nothing; the ids are logged.
	ErrCleanupIncomplete = errors.New("demo: the failed build's cleanup did not complete")
	// ErrRuleState: a run's rule could not be read or switched.
	ErrRuleState = errors.New("demo: the run's rule could not be switched")
	// ErrNotDemoRule: SetRuleEnabled refused to enable a rule that is not a
	// demo run's.
	ErrNotDemoRule = errors.New("demo: not a demo run's rule")
)

var (
	// errNoUsableDataset is the cause when zeus answers but holds no usable
	// dataset.
	errNoUsableDataset = errors.New("demo: no usable dataset")
	// errSpecDrifted is the cause when a stored fault spec carries a step's
	// name but no longer says exactly the step's fault.
	errSpecDrifted = errors.New("demo: a fault spec with the demo's name says another fault")
)

// StepError is the demo's internal error: the step that failed, its class and
// the underlying cause. The class is one of the Err* values above, or the
// context's error (context.Canceled, context.DeadlineExceeded) when the caller's
// context was done, so an interrupted build is never mistaken for a missing
// dataset or a database failure.
//
// Error() names the class and the step only, never the cause's text, an id or
// an address, so the string is safe anywhere. errors.Is and errors.As reach the
// class, the cause and, when CleanupIncomplete is set, ErrCleanupIncomplete.
// LogValue carries the cause's full text for manteion's own logs.
type StepError struct {
	Step              string
	Class             error
	Cause             error
	CleanupIncomplete bool
}

func (e *StepError) Error() string {
	s := e.Class.Error() + " (step " + e.Step
	if e.CleanupIncomplete {
		s += "; cleanup incomplete"
	}
	return s + ")"
}

func (e *StepError) Unwrap() []error {
	out := []error{e.Class}
	if e.Cause != nil {
		out = append(out, e.Cause)
	}
	if e.CleanupIncomplete {
		out = append(out, ErrCleanupIncomplete)
	}
	return out
}

// LogValue implements slog.LogValuer: the step, the class, the cause's full
// text and whether the cleanup completed. For logs only.
func (e *StepError) LogValue() slog.Value {
	attrs := []slog.Attr{slog.String("step", e.Step), slog.String("class", e.Class.Error())}
	if e.Cause != nil {
		attrs = append(attrs, slog.String("cause", e.Cause.Error()))
	}
	if e.CleanupIncomplete {
		attrs = append(attrs, slog.Bool("cleanup_incomplete", true))
	}
	return slog.GroupValue(attrs...)
}

// classFor is def, or the context's error once ctx is done.
func classFor(ctx context.Context, def error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return def
}

// Built names the rows BuildRun created for one run: its experiment, its two
// rules (RuleIDs[0] the memory phase's, RuleIDs[1] the CPU phase's) and its
// phases in order.
type Built struct {
	ExperimentID string
	RuleIDs      [2]string
	PhaseIDs     [3]string
}

// RuleName names run runID's rule for the fault of kind k:
// demo-run-<run id>-memory or demo-run-<run id>-cpu.
func RuleName(runID string, k FaultKind) string { return RunNamePrefix + runID + "-" + string(k) }

// SpecNameFor names the shared fault spec of a fault step:
// demo-<kind>-<target>-<intensity>, as demo-memory-productcatalogservice-moderate.
func SpecNameFor(step FaultStep) string {
	return "demo-" + string(step.Kind) + "-" + step.Target + "-" + step.Intensity
}

// RunIDOfRule returns the run id a run's rule is named for: <id> for
// demo-run-<id>-memory and demo-run-<id>-cpu, and for build 2's single rule
// demo-run-<id>. ok is false for any other name, including one whose <id> is
// not of the shape this package mints (validRunID).
//
// The reading is unambiguous: a run id is "demo-" and a lower-case canonical
// UUID (hex digits and hyphens only, of fixed length), and "-memory" and
// "-cpu" hold letters no UUID has, so no run id ends in either and a name
// matches at most one of the three forms.
func RunIDOfRule(name string) (runID string, ok bool) {
	rest, ok := strings.CutPrefix(name, RunNamePrefix)
	if !ok {
		return "", false
	}
	for _, k := range [...]FaultKind{FaultMemory, FaultCPU} {
		if id, cut := strings.CutSuffix(rest, "-"+string(k)); cut && validRunID(id) {
			return id, true
		}
	}
	if validRunID(rest) {
		return rest, true
	}
	return "", false
}

// buildable reports whether sc can be built: a memory step then a CPU step,
// each with a target, an intensity label and a target_load in (0, 1], and a
// workload that names its workflow and dataset.
func buildable(sc Scenario) bool {
	if sc.Memory.Kind != FaultMemory || sc.CPU.Kind != FaultCPU ||
		sc.Workload.WorkflowID == "" || sc.Workload.DatasetName == "" {
		return false
	}
	for _, st := range [...]FaultStep{sc.Memory, sc.CPU} {
		if st.Target == "" || st.Intensity == "" || !(st.TargetLoad > 0 && st.TargetLoad <= 1) {
			return false
		}
	}
	return true
}

// BuildRun writes the rows of one run of scenario sc: two disabled fault
// rules, demo-run-<run id>-memory in the memory step's target and
// demo-run-<run id>-cpu in the CPU step's (mode background, priority 100,
// matching every request), and a planned experiment demo-run-<run id>
// (created_by manteion-demo) with the phases baseline, memory and cpu, each
// running the scenario's workflow at 30 VUs and 3 rps for 40 s on a dataset of
// the scenario's that outlives now + runCap + 1 min. The memory rule is
// attached to the memory phase only and the CPU rule to the cpu phase only.
// The rules are never enabled here (that is SetRuleEnabled's job, just before
// the start).
//
// Each rule runs its step's shared fault spec (SpecNameFor): created by the
// first build that needs it, reused by every later one, and checked before
// use to still say exactly the step's fault (resource, the step's kind,
// params {"target_load": the step's}, duration 60 s). A stored spec with the
// step's name that says anything else is ErrBuild before anything is written:
// it is never used, and none is created beside it.
//
// Nothing is written until the workflow and a dataset are known to exist
// (else ErrNoDataset) and both specs are known not to have drifted. The writes
// are not one transaction, so a failure at any later step deletes what this
// call created, experiment first (which drops its phase links) and then each
// rule, each delete on its own context detached from ctx with its own timeout;
// the shared fault specs stay for the next run. A write failure is ErrBuild,
// plus ErrCleanupIncomplete when a delete failed. When ctx is done the class
// is ctx's error instead (see StepError). A scenario that is not buildable is
// ErrBuild with nothing read or written.
func BuildRun(ctx context.Context, d Deps, runID string, sc Scenario, runCap time.Duration) (Built, error) {
	return buildRun(ctx, d, runID, sc, runCap, cleanupTimeout)
}

// buildRun is BuildRun with the timeout of each cleanup delete as a parameter.
func buildRun(ctx context.Context, d Deps, runID string, sc Scenario, runCap, cleanupTimeout time.Duration) (Built, error) {
	log := d.logger().With("run_id", runID, "scenario", sc.ID)
	if runID == "" || runCap <= 0 || d.Experiments == nil || d.Rules == nil || d.Specs == nil ||
		d.Workflows == nil || d.Zeus == nil || d.Clock == nil || !buildable(sc) {
		return Built{}, &StepError{Step: "check_input", Class: ErrBuild}
	}
	now := d.Clock.Now()

	refuse := func(step string, class, cause error) (Built, error) {
		err := &StepError{Step: step, Class: classFor(ctx, class), Cause: cause}
		log.Warn("demo: run not built", "error", err)
		return Built{}, err
	}

	// Preconditions: nothing is written before they hold.
	if _, err := d.Workflows.Get(ctx, sc.Workload.WorkflowID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return refuse("get_workflow", ErrNoDataset, err)
		}
		return refuse("get_workflow", ErrBuild, err)
	}
	datasetID, err := pickDataset(ctx, d.Zeus, sc.Workload.DatasetName, now, runCap)
	if err != nil {
		return refuse("pick_dataset", ErrNoDataset, err)
	}
	steps := [2]FaultStep{sc.Memory, sc.CPU}
	specIDs, step, err := ensureSpecs(ctx, d.Specs, steps, now, log)
	if err != nil {
		return refuse(step, ErrBuild, err)
	}

	// The run's own rows. made records each row before its write is
	// attempted: a write that reports an error may still have committed.
	var made created
	fail := func(step string, cause error) (Built, error) {
		complete := made.undo(ctx, d, log, cleanupTimeout)
		err := &StepError{Step: step, Class: classFor(ctx, ErrBuild), Cause: cause, CleanupIncomplete: !complete}
		log.Warn("demo: run not built", "error", err, "experiment_id", made.experimentID, "rule_ids", made.ruleIDs)
		return Built{}, err
	}

	var built Built
	for i, st := range steps {
		rule := &model.Rule{
			ID:        id.New("rule"),
			Name:      RuleName(runID, st.Kind),
			Service:   st.Target,
			Enabled:   false,
			Priority:  rulePriority,
			Mode:      ruleMode,
			Action:    model.RuleAction{Type: "fault_spec", FaultSpecID: specIDs[i]},
			CreatedAt: now,
			UpdatedAt: now,
		}
		made.ruleIDs = append(made.ruleIDs, rule.ID)
		if err := d.Rules.Create(ctx, rule); err != nil {
			return fail("create_rule_"+string(st.Kind), err)
		}
		built.RuleIDs[i] = rule.ID
	}

	exp := &model.Experiment{
		ID:   id.New("exp"),
		Name: RunNamePrefix + runID,
		Description: "Public demo run of scenario " + sc.ID + ": no fault, then the " + sc.Memory.Intensity +
			" memory fault in " + sc.Memory.Target + ", then the " + sc.CPU.Intensity + " CPU fault in " + sc.CPU.Target + ".",
		Status:    "planned",
		CreatedBy: CreatedBy,
		CreatedAt: now,
	}
	made.experimentID = exp.ID
	if err := d.Experiments.Create(ctx, exp); err != nil {
		return fail("create_experiment", err)
	}
	built.ExperimentID = exp.ID

	// The rule each phase carries: none, the memory rule, the CPU rule.
	phaseRule := [3]string{"", built.RuleIDs[0], built.RuleIDs[1]}
	for i, name := range PhaseNames {
		phase := &model.ExperimentPhase{
			ID:             id.New("phase"),
			ExperimentID:   exp.ID,
			Name:           name,
			Position:       i,
			Status:         "pending",
			FrozenServices: []model.CacheBoxConfig{},
		}
		if err := d.Experiments.CreatePhase(ctx, phase); err != nil {
			return fail(fmt.Sprintf("create_phase_%d", i), err)
		}
		built.PhaseIDs[i] = phase.ID
		load := []model.PhaseWorkflow{{
			PhaseID:     phase.ID,
			WorkflowID:  sc.Workload.WorkflowID,
			VUs:         loadVUs,
			RateRPS:     loadRateRPS,
			DurationSec: PhaseDurationS,
			DatasetID:   datasetID,
		}}
		if err := d.Experiments.AttachPhaseWorkflows(ctx, phase.ID, load); err != nil {
			return fail(fmt.Sprintf("attach_workflows_%d", i), err)
		}
		if ruleID := phaseRule[i]; ruleID != "" {
			if err := d.Experiments.AttachPhaseRules(ctx, phase.ID, []string{ruleID}); err != nil {
				return fail(fmt.Sprintf("attach_rule_%d", i), err)
			}
		}
	}
	log.Info("demo: run built", "experiment_id", exp.ID, "rule_ids", built.RuleIDs[:])
	return built, nil
}

// created is what a build wrote so far.
type created struct {
	experimentID string
	ruleIDs      []string
}

// undo deletes a failed build's rows: the experiment first (its phases and
// their workflow and rule links go with it), then each rule, which the store
// refuses to delete while a phase still links it. No rule was ever enabled,
// so none is served at any point in between. Each delete runs on its own
// context, detached from ctx (which may be why the build failed) and bounded
// by timeout, so a hung delete cannot starve the next. A row that cannot be
// deleted is logged by id and left: a planned experiment and a disabled rule
// serve nothing, and the start-up sweep removes them. undo reports whether
// everything it tried to delete is gone.
func (c created) undo(ctx context.Context, d Deps, log *slog.Logger, timeout time.Duration) bool {
	del := func(fn func(context.Context, string) error, id string) error {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if err := fn(cctx, id); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		return nil
	}
	complete := true
	if c.experimentID != "" {
		if err := del(d.Experiments.Delete, c.experimentID); err != nil {
			complete = false
			log.Error("demo: cleanup left the run's planned experiment behind",
				"experiment_id", c.experimentID, "rule_ids", c.ruleIDs, "error", err)
		}
	}
	for _, ruleID := range c.ruleIDs {
		if err := del(d.Rules.Delete, ruleID); err != nil {
			complete = false
			log.Error("demo: cleanup left a disabled rule of the run behind",
				"rule_id", ruleID, "experiment_id", c.experimentID, "error", err)
		}
	}
	return complete
}

// pickDataset returns the id of the dataset named name the run's load reads:
// see usableDataset. A zeus that cannot be asked, or that holds no usable
// dataset, is an error; when ctx is done the error is ctx's own, never "no
// dataset".
func pickDataset(ctx context.Context, z LoadGen, name string, now time.Time, runCap time.Duration) (string, error) {
	all, err := z.ListDatasets(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", ctxErr
	}
	if err != nil {
		return "", err
	}
	ds, ok := usableDataset(all, name, now, runCap)
	if !ok {
		return "", errNoUsableDataset
	}
	return ds.ID, nil
}

// usableDataset picks, among zeus's datasets, one named name that has rows
// and outlives the run: ExpiresAt is zero (zeus's ttl_s <= 0, which zeus
// stores without expiry) or strictly after now + runCap + 1 min. Of several,
// the newest wins (then the lowest id), so the choice does not depend on the
// order zeus lists them in.
func usableDataset(all []zeus.Dataset, name string, now time.Time, runCap time.Duration) (zeus.Dataset, bool) {
	need := now.Add(runCap + datasetSlack)
	var best zeus.Dataset
	found := false
	for _, ds := range all {
		if ds.Name != name || ds.SizeBytes <= 0 || ds.ID == "" {
			continue
		}
		if exp := ds.ExpiresAt(); !exp.IsZero() && !exp.After(need) {
			continue
		}
		if !found || ds.CreatedAt.After(best.CreatedAt) || (ds.CreatedAt.Equal(best.CreatedAt) && ds.ID < best.ID) {
			best, found = ds, true
		}
	}
	return best, found
}

// specParams is a step's fault params: target_load and nothing else (no
// mechanism: atropos picks its own).
type specParams struct {
	TargetLoad float64 `json:"target_load"`
}

// ensureSpecs returns, for each step, the id of its shared fault spec
// (SpecNameFor): the stored one when it says exactly the step's fault, else a
// new one. Both steps are checked before anything is created: a stored spec
// with a step's name that says anything else (edited by hand, say) is
// errSpecDrifted, logged with its id, and nothing is created. On error it
// also returns the step that failed.
func ensureSpecs(ctx context.Context, specs Specs, steps [2]FaultStep, now time.Time, log *slog.Logger) ([2]string, string, error) {
	var ids [2]string
	all, err := specs.ListSpecs(ctx)
	if err != nil {
		return ids, "list_specs", err
	}
	for i, st := range steps {
		name := SpecNameFor(st)
		for _, s := range all {
			if s == nil || s.Name != name {
				continue
			}
			if !specMatches(s, st) {
				log.Warn("demo: a fault spec carries the demo's name but not the catalog's fault; not building the run",
					"spec_id", s.ID, "spec", name)
				return ids, "check_spec", errSpecDrifted
			}
			if ids[i] == "" {
				ids[i] = s.ID
			}
		}
	}
	for i, st := range steps {
		if ids[i] != "" {
			continue
		}
		params, err := json.Marshal(specParams{TargetLoad: st.TargetLoad})
		if err != nil {
			return ids, "create_spec", err
		}
		spec := &model.FaultSpec{
			ID:          id.New("spec"),
			Name:        SpecNameFor(st),
			Category:    specCategory,
			FaultType:   string(st.Kind),
			Params:      params,
			Description: "Public demo: the " + st.Intensity + " " + string(st.Kind) + " fault in " + st.Target + ".",
			DurationMs:  specDurationMs,
			CreatedAt:   now,
		}
		if err := specs.CreateSpec(ctx, spec); err != nil {
			return ids, "create_spec", err
		}
		ids[i] = spec.ID
	}
	return ids, "", nil
}

// specMatches reports whether s says exactly step st's fault: category
// resource, the step's kind, no host, network envelope or ramp, duration_ms
// 60000, and params that decode to exactly {"target_load": the step's} (no
// mechanism, no other key). Values are compared decoded, not as bytes: the
// store hands params back as they were sent.
func specMatches(s *model.FaultSpec, st FaultStep) bool {
	if s.Category != specCategory || s.FaultType != string(st.Kind) || s.Host != "" || s.Network != nil ||
		s.DurationMs != specDurationMs || s.RampUpMs != 0 || s.RampDownMs != 0 {
		return false
	}
	var params map[string]any
	if err := json.Unmarshal(s.Params, &params); err != nil || len(params) != 1 {
		return false
	}
	load, ok := params["target_load"].(float64)
	return ok && load == st.TargetLoad
}

// SetRuleEnabled switches a run's rule on or off.
//
// Off is one unconditional write (Rules.SetEnabled(false)): no read first, no
// "already off" shortcut, never refused whatever the rule's name, and run on a
// context detached from ctx with its own 10 s timeout, so a switch-off issued
// during a shutdown or after a watchdog still lands. A rule that no longer
// exists is success: nothing is left to serve. Off is safe to call at any
// time, any number of times.
//
// On is refused (ErrNotDemoRule) for a rule whose name is not demo-run-*, so
// the demo never switches on an operator's rule; it then calls
// Rules.EnableIfAttached, which enables the rule only while a phase links it,
// in one statement. An unattached rule stays disabled and the error satisfies
// errors.Is(err, store.ErrRuleUnattached). On uses ctx as given: a cancelled
// caller does not enable anything.
//
// The store bumps the service's generation in the same transaction as each
// switch, so held polls wake at once. Callers must serialise switches of one
// rule (one guarded switch per run, closed once the run has ended), so that a
// late switch-on can never follow the switch-off. Failures are logged through
// slog.Default with their full cause.
func SetRuleEnabled(ctx context.Context, r Rules, ruleID string, on bool) error {
	err := setRuleEnabled(ctx, r, ruleID, on)
	if err != nil {
		slog.Default().Error("demo: switching a run's rule failed", "rule_id", ruleID, "on", on, "error", err)
	}
	return err
}

// setRuleEnabled is SetRuleEnabled without its log line, for callers that log
// the failure themselves.
func setRuleEnabled(ctx context.Context, r Rules, ruleID string, on bool) error {
	if !on {
		_, err := switchOffRule(ctx, r, ruleID, switchOffTimeout)
		return err
	}
	rule, err := r.Get(ctx, ruleID)
	if err != nil {
		return &StepError{Step: "get_rule", Class: classFor(ctx, ErrRuleState), Cause: err}
	}
	if !strings.HasPrefix(rule.Name, RunNamePrefix) {
		return &StepError{Step: "check_rule", Class: ErrNotDemoRule}
	}
	if err := r.EnableIfAttached(ctx, ruleID); err != nil {
		return &StepError{Step: "enable_rule", Class: classFor(ctx, ErrRuleState), Cause: err}
	}
	return nil
}

// switchOffRule is SetRuleEnabled's switch-off with timeout in place of its
// 10 s, and no log line: one unconditional Rules.SetEnabled(false) on a
// context detached from ctx and bounded by timeout; a rule that no longer
// exists is success, reported as gone.
func switchOffRule(ctx context.Context, r Rules, ruleID string, timeout time.Duration) (gone bool, err error) {
	octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if err := r.SetEnabled(octx, ruleID, false); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return true, nil
		}
		return false, &StepError{Step: "disable_rule", Class: ErrRuleState, Cause: err}
	}
	return false, nil
}
