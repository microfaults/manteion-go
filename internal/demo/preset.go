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
	// RunNamePrefix + run id names a run's experiment and its rule.
	RunNamePrefix = "demo-run-"
	// RunIDPrefix is the prefix of run ids: id.New(RunIDPrefix).
	RunIDPrefix = "demo"
	// SpecName names the one fault spec every run's rule uses.
	SpecName = "demo-catalog-latency-50ms"
	// TargetService is the service the fault phase slows down.
	TargetService = "productcatalogservice"
	// WorkflowID is the manteion workflow every phase runs.
	WorkflowID = "demo-browse"
	// DatasetName is the zeus dataset the workflow reads, looked up by name
	// at build time because its id changes whenever zeus restarts.
	DatasetName = "demo-products"
)

// The preset's load, rule and spec.
const (
	loadVUs        = 30
	loadRateRPS    = 3
	rulePriority   = 100
	specDelay      = "50ms"
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

// errNoUsableDataset is the cause when zeus answers but holds no usable dataset.
var errNoUsableDataset = errors.New("demo: no usable dataset")

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

// Built names the rows BuildRun created for one run.
type Built struct {
	ExperimentID, RuleID string
	PhaseIDs             [3]string
}

// BuildRun writes one run's preset: a disabled fault rule on
// productcatalogservice, and a planned experiment (created_by manteion-demo)
// with the phases plain, catalog-latency-50ms and plain-end, each running
// demo-browse at 30 VUs and 3 rps for 40 s on a demo-products dataset that
// outlives now + runCap + 1 min; the rule is attached to the middle phase only.
// The rule is never enabled here (that is SetRuleEnabled's job, just before the
// start).
//
// Nothing is written until the workflow and a dataset are known to exist
// (else ErrNoDataset). The writes are not one transaction, so a failure at any
// later step deletes what this call created, experiment first (which drops its
// phase links) and then the rule, each delete on its own context detached
// from ctx with its own timeout; the shared fault spec stays for the next run.
// A write failure is ErrBuild, plus ErrCleanupIncomplete when a delete failed.
// When ctx is done the class is ctx's error instead (see StepError).
func BuildRun(ctx context.Context, d Deps, runID string, runCap time.Duration) (Built, error) {
	return buildRun(ctx, d, runID, runCap, cleanupTimeout)
}

// buildRun is BuildRun with the timeout of each cleanup delete as a parameter.
func buildRun(ctx context.Context, d Deps, runID string, runCap, cleanupTimeout time.Duration) (Built, error) {
	log := d.logger().With("run_id", runID)
	if runID == "" || runCap <= 0 || d.Experiments == nil || d.Rules == nil || d.Specs == nil ||
		d.Workflows == nil || d.Zeus == nil || d.Clock == nil {
		return Built{}, &StepError{Step: "check_input", Class: ErrBuild}
	}
	now := d.Clock.Now()

	refuse := func(step string, class, cause error) (Built, error) {
		err := &StepError{Step: step, Class: classFor(ctx, class), Cause: cause}
		log.Warn("demo: run not built", "error", err)
		return Built{}, err
	}

	// Preconditions: nothing is written before they hold.
	if _, err := d.Workflows.Get(ctx, WorkflowID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return refuse("get_workflow", ErrNoDataset, err)
		}
		return refuse("get_workflow", ErrBuild, err)
	}
	datasetID, err := pickDataset(ctx, d.Zeus, now, runCap)
	if err != nil {
		return refuse("pick_dataset", ErrNoDataset, err)
	}
	specID, step, err := ensureSpec(ctx, d.Specs, now, log)
	if err != nil {
		return refuse(step, ErrBuild, err)
	}

	// The run's own rows. made records each row before its write is
	// attempted: a write that reports an error may still have committed.
	var made created
	fail := func(step string, cause error) (Built, error) {
		complete := made.undo(ctx, d, log, cleanupTimeout)
		err := &StepError{Step: step, Class: classFor(ctx, ErrBuild), Cause: cause, CleanupIncomplete: !complete}
		log.Warn("demo: run not built", "error", err, "experiment_id", made.experimentID, "rule_id", made.ruleID)
		return Built{}, err
	}

	rule := &model.Rule{
		ID:        id.New("rule"),
		Name:      RunNamePrefix + runID,
		Service:   TargetService,
		Enabled:   false,
		Priority:  rulePriority,
		Mode:      "inline",
		Action:    model.RuleAction{Type: "fault_spec", FaultSpecID: specID},
		CreatedAt: now,
		UpdatedAt: now,
	}
	made.ruleID = rule.ID
	if err := d.Rules.Create(ctx, rule); err != nil {
		return fail("create_rule", err)
	}

	exp := &model.Experiment{
		ID:          id.New("exp"),
		Name:        RunNamePrefix + runID,
		Description: "Public demo run: plain load, then " + FaultLabel + ", then plain load again.",
		Status:      "planned",
		CreatedBy:   CreatedBy,
		CreatedAt:   now,
	}
	made.experimentID = exp.ID
	if err := d.Experiments.Create(ctx, exp); err != nil {
		return fail("create_experiment", err)
	}

	built := Built{ExperimentID: exp.ID, RuleID: rule.ID}
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
			WorkflowID:  WorkflowID,
			VUs:         loadVUs,
			RateRPS:     loadRateRPS,
			DurationSec: PhaseDurationS,
			DatasetID:   datasetID,
		}}
		if err := d.Experiments.AttachPhaseWorkflows(ctx, phase.ID, load); err != nil {
			return fail(fmt.Sprintf("attach_workflows_%d", i), err)
		}
		if i == FaultPhaseIndex {
			if err := d.Experiments.AttachPhaseRules(ctx, phase.ID, []string{rule.ID}); err != nil {
				return fail("attach_rule", err)
			}
		}
	}
	log.Info("demo: run built", "experiment_id", exp.ID, "rule_id", rule.ID)
	return built, nil
}

// created is what a build wrote so far.
type created struct {
	experimentID, ruleID string
}

// undo deletes a failed build's rows: the experiment first (its phases and
// their workflow and rule links go with it), then the rule, which the store
// refuses to delete while a phase still links it. The rule was never enabled,
// so it is not served at any point in between. Each delete runs on its own
// context, detached from ctx (which may be why the build failed) and bounded
// by timeout, so a hung first delete cannot starve the second. A row that
// cannot be deleted is logged by id and left: a planned experiment and a
// disabled rule serve nothing, and the start-up sweep removes them. undo
// reports whether everything it tried to delete is gone.
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
				"experiment_id", c.experimentID, "rule_id", c.ruleID, "error", err)
		}
	}
	if c.ruleID != "" {
		if err := del(d.Rules.Delete, c.ruleID); err != nil {
			complete = false
			log.Error("demo: cleanup left the run's disabled rule behind",
				"rule_id", c.ruleID, "experiment_id", c.experimentID, "error", err)
		}
	}
	return complete
}

// pickDataset returns the id of the dataset the run's load reads: see
// usableDataset. A zeus that cannot be asked, or that holds no usable dataset,
// is an error; when ctx is done the error is ctx's own, never "no dataset".
func pickDataset(ctx context.Context, z LoadGen, now time.Time, runCap time.Duration) (string, error) {
	all, err := z.ListDatasets(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", ctxErr
	}
	if err != nil {
		return "", err
	}
	ds, ok := usableDataset(all, now, runCap)
	if !ok {
		return "", errNoUsableDataset
	}
	return ds.ID, nil
}

// usableDataset picks, among zeus's datasets, one named demo-products that has
// rows and outlives the run: ExpiresAt is zero (zeus's ttl_s <= 0, which zeus
// stores without expiry) or strictly after now + runCap + 1 min. Of several,
// the newest wins (then the lowest id), so the choice does not depend on the
// order zeus lists them in.
func usableDataset(all []zeus.Dataset, now time.Time, runCap time.Duration) (zeus.Dataset, bool) {
	need := now.Add(runCap + datasetSlack)
	var best zeus.Dataset
	found := false
	for _, ds := range all {
		if ds.Name != DatasetName || ds.SizeBytes <= 0 || ds.ID == "" {
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

// ensureSpec returns the id of the shared fault spec, creating it when no spec
// named demo-catalog-latency-50ms still says exactly +50 ms inline latency. A
// spec with that name but other content (edited by hand, say) is never used.
// On error it also returns the step that failed.
func ensureSpec(ctx context.Context, specs Specs, now time.Time, log *slog.Logger) (string, string, error) {
	all, err := specs.ListSpecs(ctx)
	if err != nil {
		return "", "list_specs", err
	}
	for _, s := range all {
		if s.Name != SpecName {
			continue
		}
		if presetSpecMatches(s) {
			return s.ID, "", nil
		}
		log.Warn("demo: a fault spec carries the demo's name but not its fault; not using it", "spec_id", s.ID)
	}
	spec := &model.FaultSpec{
		ID:          id.New("spec"),
		Name:        SpecName,
		Category:    "inline",
		FaultType:   "latency",
		Params:      json.RawMessage(`{"delay":"` + specDelay + `"}`),
		Description: "Public demo: " + FaultLabel + ".",
		DurationMs:  specDurationMs,
		CreatedAt:   now,
	}
	if err := specs.CreateSpec(ctx, spec); err != nil {
		return "", "create_spec", err
	}
	return spec.ID, "", nil
}

// presetSpecMatches reports whether s is exactly the preset's fault.
func presetSpecMatches(s *model.FaultSpec) bool {
	if s.Category != "inline" || s.FaultType != "latency" || s.Host != "" || s.Network != nil ||
		s.DurationMs != specDurationMs || s.RampUpMs != 0 || s.RampDownMs != 0 {
		return false
	}
	var params map[string]any
	if err := json.Unmarshal(s.Params, &params); err != nil || len(params) != 1 {
		return false
	}
	return params["delay"] == specDelay
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

func setRuleEnabled(ctx context.Context, r Rules, ruleID string, on bool) error {
	if !on {
		octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), switchOffTimeout)
		defer cancel()
		if err := r.SetEnabled(octx, ruleID, false); err != nil && !errors.Is(err, store.ErrNotFound) {
			return &StepError{Step: "disable_rule", Class: ErrRuleState, Cause: err}
		}
		return nil
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
