package demo

import (
	"context"
	"io"
	"log/slog"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// The interfaces below are the only ways the demo touches the rest of
// manteion. Each lists methods of an existing type with that type's exact
// signature, so the real value is passed in as it is (deps_test.go asserts
// it): *orchestrator.Orchestrator, *store.ExperimentRepo, *store.RuleRepo,
// *store.FaultRepo, *store.WorkflowRepo, *zeus.Client, *atrocontrol.Controller,
// *promql.Client and *sql.DB.
//
// Deliberately absent: phase-level start, pause, resume and stop, experiment
// pause, resume and stop, the non-CAS status writers, RuleRepo.Update, fault
// configs and the kill switch. The demo drives an experiment only through StartExperiment and
// CancelExperiment, and owns its rule's enabled flag.

// Clock tells the time. Package code never calls time.Now itself, except
// for a socket's read or write deadline, which the network poller compares
// with the wall clock (events.go, wallDeadline).
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock: ClockFunc(time.Now) in production.
type ClockFunc func() time.Time

// Now calls f.
func (f ClockFunc) Now() time.Time { return f() }

// Orchestrator starts and cancels experiments (*orchestrator.Orchestrator).
// Both may block for tens of seconds; call them from the worker only, never
// from a request's goroutine.
type Orchestrator interface {
	StartExperiment(ctx context.Context, experimentID string) error
	CancelExperiment(ctx context.Context, experimentID string) error
}

// Experiments is the slice of *store.ExperimentRepo the demo uses. Delete is
// for a terminal experiment whose rule is already disabled: deleting an
// experiment removes its phase links, and an enabled rule attached to no phase
// is served to every instance of its service.
type Experiments interface {
	Create(ctx context.Context, exp *model.Experiment) error
	CreatePhase(ctx context.Context, p *model.ExperimentPhase) error
	AttachPhaseWorkflows(ctx context.Context, phaseID string, pws []model.PhaseWorkflow) error
	AttachPhaseRules(ctx context.Context, phaseID string, ruleIDs []string) error
	Get(ctx context.Context, id string) (*model.Experiment, error)
	ListPhasesForExperiment(ctx context.Context, experimentID string) ([]*model.ExperimentPhase, error)
	ListWorkflowResultsForPhase(ctx context.Context, phaseID string) ([]*model.PhaseWorkflowResult, error)
	RunningExperimentIDs(ctx context.Context) ([]string, error)
	List(ctx context.Context, f store.ExperimentFilter, p store.Page) ([]*model.Experiment, int, error)
	// ListPhaseRules lists the rules a phase links, so retention can refuse to
	// delete an experiment that carries a rule other than its run's own.
	ListPhaseRules(ctx context.Context, phaseID string) ([]model.PhaseRule, error)
	Delete(ctx context.Context, id string) error
	TransitionExperiment(ctx context.Context, id, to string, from ...string) (bool, error)
}

// Rules is the slice of *store.RuleRepo the demo uses. Every write bumps the
// rule's service generation in the same transaction. The demo switches a rule
// only through SetEnabled (unconditional, one statement) and EnableIfAttached
// (enables only while a phase links the rule, one statement), never through a
// read-modify-write Update.
type Rules interface {
	Create(ctx context.Context, rule *model.Rule) error
	Get(ctx context.Context, id string) (*model.Rule, error)
	SetEnabled(ctx context.Context, id string, enabled bool) error
	EnableIfAttached(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]*model.Rule, error)
}

// Specs is the slice of *store.FaultRepo the demo uses.
type Specs interface {
	CreateSpec(ctx context.Context, spec *model.FaultSpec) error
	ListSpecs(ctx context.Context) ([]*model.FaultSpec, error)
}

// Workflows is the slice of *store.WorkflowRepo the demo uses.
type Workflows interface {
	Get(ctx context.Context, id string) (*model.Workflow, error)
}

// LoadGen is the slice of *zeus.Client the demo uses.
type LoadGen interface {
	Healthy(ctx context.Context) bool
	ListDatasets(ctx context.Context) ([]zeus.Dataset, error)
}

// Instances is the slice of *atrocontrol.Controller the demo uses: the live
// (alive or suspect) instances of a service.
type Instances interface {
	InstancesForService(ctx context.Context, service string) ([]*model.SDKInstance, error)
}

// Metrics is the slice of *promql.Client the demo uses.
type Metrics interface {
	QueryInstant(ctx context.Context, query string) (float64, error)
	Healthy(ctx context.Context) bool
}

// Pinger is the slice of *sql.DB the demo uses.
type Pinger interface {
	PingContext(ctx context.Context) error
}

// Deps is everything the demo needs from the rest of manteion.
type Deps struct {
	Orch        Orchestrator
	Experiments Experiments
	Rules       Rules
	Specs       Specs
	Workflows   Workflows
	Zeus        LoadGen
	Instances   Instances
	Prom        Metrics
	DB          Pinger
	Clock       Clock
	Logger      *slog.Logger
}

// logger returns d.Logger, or a logger that discards everything.
func (d Deps) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
