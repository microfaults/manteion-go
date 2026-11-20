package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
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
	// cleanupTimeout bounds the cleanup of a failed build, which runs on a
	// context detached from the caller's (that one may be what failed).
	cleanupTimeout = 10 * time.Second
)

// Internal errors. They are for logs and for choosing a Reason; no error, nor
// its text, ever goes into a client's body.
var (
	// ErrNoDataset: the load generator cannot run the preset (no usable
	// demo-products dataset, zeus unreachable, or no demo-browse workflow).
	ErrNoDataset = errors.New("demo: the load generator has no usable dataset or workflow")
	// ErrBuild: the run's rows could not be written; nothing was left behind
	// unless a cleanup failure was logged.
	ErrBuild = errors.New("demo: the run could not be built")
	// ErrRuleState: a run's rule could not be read or written.
	ErrRuleState = errors.New("demo: the run's rule could not be switched")
	// ErrNotDemoRule: SetRuleEnabled refused to enable a rule that is not a
	// demo run's.
	ErrNotDemoRule = errors.New("demo: not a demo run's rule")
)

// errNoUsableDataset is the cause when zeus answers but holds no usable dataset.
var errNoUsableDataset = errors.New("demo: no usable dataset")

// StepError is the demo's internal error: the step that failed, its class (one
// of the Err* values above) and the underlying cause. Error() names the class
// and the step only, never the cause's text, an id or an address, so the
// string is safe anywhere; errors.Is and errors.As reach both the class and the
// cause.
type StepError struct {
	Step  string
	Class error
	Cause error
}

func (e *StepError) Error() string { return e.Class.Error() + " (step " + e.Step + ")" }

func (e *StepError) Unwrap() []error {
	if e.Cause == nil {
		return []error{e.Class}
	}
	return []error{e.Class, e.Cause}
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
// phase links) and then the rule, on a context detached from ctx; the shared
// fault spec stays for the next run. Any write failure is ErrBuild.
func BuildRun(ctx context.Context, d Deps, runID string, runCap time.Duration) (Built, error) {
	log := d.logger().With("run_id", runID)
	if runID == "" || runCap <= 0 || d.Experiments == nil || d.Rules == nil || d.Specs == nil ||
		d.Workflows == nil || d.Zeus == nil || d.Clock == nil {
		return Built{}, &StepError{Step: "check_input", Class: ErrBuild}
	}
	now := d.Clock.Now()

	refuse := func(step string, class, cause error) (Built, error) {
		log.Warn("demo: run not built", "step", step, "cause", causeClass(cause))
		return Built{}, &StepError{Step: step, Class: class, Cause: cause}
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

	// The run's own rows. made records each row as soon as its write is
	// attempted: a write that reports an error may still have committed.
	var made created
	fail := func(step string, cause error) (Built, error) {
		log.Warn("demo: run not built, removing what was written", "step", step, "cause", causeClass(cause),
			"experiment_id", made.experimentID, "rule_id", made.ruleID)
		made.undo(ctx, d, log)
		return Built{}, &StepError{Step: step, Class: ErrBuild, Cause: cause}
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
// so it is not served at any point in between. It runs on a context detached
// from ctx, which may be the reason the build failed. A row that cannot be
// deleted is logged by id and left: a planned experiment and a disabled rule
// serve nothing, and the start-up sweep removes them.
func (c created) undo(ctx context.Context, d Deps, log *slog.Logger) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if c.experimentID != "" {
		if err := d.Experiments.Delete(cctx, c.experimentID); err != nil && !errors.Is(err, store.ErrNotFound) {
			log.Error("demo: cleanup left the run's planned experiment behind",
				"experiment_id", c.experimentID, "rule_id", c.ruleID, "cause", causeClass(err))
		}
	}
	if c.ruleID != "" {
		if err := d.Rules.Delete(cctx, c.ruleID); err != nil && !errors.Is(err, store.ErrNotFound) {
			log.Error("demo: cleanup left the run's disabled rule behind",
				"rule_id", c.ruleID, "experiment_id", c.experimentID, "cause", causeClass(err))
		}
	}
}

// pickDataset returns the id of the dataset the run's load reads: see
// usableDataset. A zeus that cannot be asked, or that holds no usable dataset,
// is an error.
func pickDataset(ctx context.Context, z LoadGen, now time.Time, runCap time.Duration) (string, error) {
	all, err := z.ListDatasets(ctx)
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

// SetRuleEnabled switches a run's rule on or off; a rule already in that state
// is not written. Enabling is refused (ErrNotDemoRule) for a rule whose name is
// not demo-run-*, so the demo can never switch on an operator's rule.
// Disabling is never refused: it can only stop a fault. RuleRepo.Update bumps
// the service's generation in the same transaction, so held polls wake at once.
//
// Enable a rule only once it is attached to its phase: an enabled rule
// attached to no phase is served to every instance of its service.
func SetRuleEnabled(ctx context.Context, r Rules, ruleID string, on bool) error {
	rule, err := r.Get(ctx, ruleID)
	if err != nil {
		return &StepError{Step: "get_rule", Class: ErrRuleState, Cause: err}
	}
	if on && !strings.HasPrefix(rule.Name, RunNamePrefix) {
		return &StepError{Step: "check_rule", Class: ErrNotDemoRule}
	}
	if rule.Enabled == on {
		return nil
	}
	rule.Enabled = on
	if err := r.Update(ctx, rule); err != nil {
		return &StepError{Step: "update_rule", Class: ErrRuleState, Cause: err}
	}
	return nil
}

// causeClass names the kind of an error for a log line without its text,
// which can carry addresses and database detail.
func causeClass(err error) string {
	var (
		zerr *zeus.ResponseError
		nerr net.Error
	)
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, store.ErrNotFound):
		return "not_found"
	case errors.Is(err, store.ErrConflict):
		return "conflict"
	case errors.Is(err, store.ErrValidation):
		return "validation"
	case errors.Is(err, errNoUsableDataset):
		return "no_usable_dataset"
	case errors.As(err, &zerr):
		return "zeus_status_" + strconv.Itoa(zerr.Status)
	case errors.As(err, &nerr):
		return "network"
	}
	return "error"
}
