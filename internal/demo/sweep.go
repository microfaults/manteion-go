package demo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
)

// The start-up sweep, retention and the worker's shutdown step.
//
// What they may touch:
//   - An experiment is the demo's when its created_by is manteion-demo and its
//     name starts with demo-run-. Only the demo's experiments are ever
//     cancelled or deleted.
//   - Any rule named demo-run-* that is enabled is switched off when the sweep
//     finds it: switching off is safe whoever made the rule, and the name is
//     the demo's.
//   - A rule is deleted only when its name is demo-run-<a run id this package
//     mints> (validRunID). Rules record no creator, so the name is all that
//     ties a rule to a run: a demo-run-x rule an operator made is switched off
//     and kept, and so is any rule a phase of an operator's experiment still
//     links (the store refuses that delete).
//   - A run's experiment and its rule share the name demo-run-<run id>
//     (BuildRun); that is how a rule's experiment is found, and the other way.
//
// The ordering rule. Deleting an experiment removes its phase links, and an
// enabled rule attached to no phase is served to every instance of its
// service, for ever. So an experiment is deleted only after every rule named
// like it was switched off, each with SetRuleEnabled's one unconditional
// write, which no caller's cancellation stops; and a rule is deleted only once
// its experiment is gone (the store refuses the other order while a phase
// links the rule). A failed switch-off keeps the experiment; a failed
// experiment delete keeps the rule (off); the next pass tries again.

// tidyTimeouts bound one pass's calls; the caller's context bounds them too.
type tidyTimeouts struct {
	// call bounds each store call: a page of experiments, the rule list, a
	// delete. (A switch-off has SetRuleEnabled's own 10 s, detached from the
	// caller's context.)
	call time.Duration
	// cancel bounds the wait for each CancelExperiment, which runs in its own
	// goroutine: it takes the experiment's lock, which no context interrupts.
	cancel time.Duration
}

var defaultTidyTimeouts = tidyTimeouts{call: 10 * time.Second, cancel: 30 * time.Second}

// maxListPages bounds one listing of the demo's experiments (listPage each).
const maxListPages = 500

// Errors of an incomplete pass. The error a pass returns satisfies errors.Is
// with its class and with the cause of every step that failed.
var (
	// ErrSweepIncomplete: some step of the start-up sweep failed; manteion
	// starts anyway, and what is left is in the logs, by id.
	ErrSweepIncomplete = errors.New("demo: the start-up sweep did not complete")
	// ErrRetainIncomplete: some step of a retention pass failed; the next pass
	// tries again.
	ErrRetainIncomplete = errors.New("demo: retention did not complete")
)

var (
	errBadKeep             = errors.New("demo: keep must be at least 1")
	errTooManyPages        = errors.New("demo: too many demo experiments to list")
	errNotWired            = errors.New("demo: the experiment or rule store is not wired")
	errSwitchOffUnfinished = errors.New("demo: the switch-off did not finish in time")
)

// incompleteError is what a pass returns when a step failed. Error() names the
// class and the failed steps only, never an id or a cause's text; errors.Is
// and errors.As reach the class and every cause; LogValue carries the causes'
// text for manteion's own logs.
type incompleteError struct {
	class  error
	steps  []string // the failed steps, in the order they first failed
	causes []error
}

func (e *incompleteError) Error() string {
	return e.class.Error() + " (failed: " + strings.Join(e.steps, ", ") + ")"
}

func (e *incompleteError) Unwrap() []error { return append([]error{e.class}, e.causes...) }

// LogValue implements slog.LogValuer. For logs only.
func (e *incompleteError) LogValue() slog.Value {
	causes := make([]string, len(e.causes))
	for i, c := range e.causes {
		causes[i] = c.Error()
	}
	return slog.GroupValue(slog.String("class", e.class.Error()), slog.Any("steps", e.steps), slog.Any("causes", causes))
}

// Sweep cleans up after an earlier manteion process, at start-up: after
// orchestrator.Recover and before the worker runs (it would take the run in
// progress for a leftover). In this order, each step going on past individual
// failures:
//
//  1. every enabled demo-run-* rule is switched off, before anything else; the
//     rule listing for this step is detached from ctx, as the switch-offs are,
//     so a caller that gives up does not leave a rule on;
//  2. every planned or running demo experiment is cancelled through the
//     orchestrator (one that has already ended counts as done);
//  3. the rules are read again: one still on (step 1 could not read or switch
//     it) is switched off now; a run's rule whose experiment no longer exists
//     is switched off, again, and deleted;
//  4. Retain(keep).
//
// Every call has a deadline. A failed step is logged with its ids and full
// cause, and the sweep then returns an error satisfying
// errors.Is(err, ErrSweepIncomplete); the caller should log it and start.
func Sweep(ctx context.Context, d Deps, keep int) error {
	return newTidier(d, ErrSweepIncomplete, defaultTidyTimeouts).sweep(ctx, keep)
}

// Retain keeps the newest keep finished demo experiments (completed, failed or
// cancelled; newest by creation) and deletes the rest, each as: every rule
// named like it switched off, the experiment deleted, then the run's rule
// deleted. A run's rule whose experiment no longer exists is switched off and
// deleted too. Retain never touches a planned or running experiment and never
// cancels anything; it pages through all the demo's experiments.
//
// It knows no run in progress, so call it only where no run is being built:
// from the worker's OnRunEnded, or with no worker running (Sweep does). The
// worker's own Retain method is safe at any time and also collects stale
// planned leftovers; prefer it for OnRunEnded.
//
// A failed step is logged and skipped, and the pass returns an error
// satisfying errors.Is(err, ErrRetainIncomplete); the next pass tries again.
// keep must be at least 1.
func Retain(ctx context.Context, d Deps, keep int) error {
	t := newTidier(d, ErrRetainIncomplete, defaultTidyTimeouts)
	if t.wired() {
		t.retain(ctx, keep)
	}
	return t.result()
}

// Retain is the retention pass for the worker's OnRunEnded:
//
//	w.OnRunEnded = func(ctx context.Context) { _ = w.Retain(ctx) }
//
// It is the package's Retain with keep = Config.Retain, and it also cancels,
// through the orchestrator, every planned demo experiment created more than
// twice Config.RunCap ago (by Deps.Clock) that is not the run in progress's.
// The worker leaves such an experiment when its watchdog or a shutdown ends a
// run before its start was launched; a cancelled one is terminal and is
// deleted by a later pass like any other. (A running leftover, whose cancel
// failed, needs nothing: its phases end on their own.)
//
// The run in progress is never touched: its experiment, known by id once
// built and by name before, is neither cancelled nor deleted, and its rule,
// written before its experiment, is never taken for an orphan. The run in
// progress is read again just before each cancel and delete, so this is safe
// at any time, not only from OnRunEnded.
func (w *Worker) Retain(ctx context.Context) error {
	t := newTidier(w.d, ErrRetainIncomplete, defaultTidyTimeouts)
	t.stale = 2 * w.cfg.RunCap
	t.current = w.currentRun
	if t.wired() {
		t.retain(ctx, w.cfg.Retain)
	}
	return t.result()
}

// currentRun names the run in progress: its experiment's name (known from
// pickup, before the build) and its experiment's id (once built).
func (w *Worker) currentRun() currentRun {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == nil {
		return currentRun{}
	}
	return currentRun{name: RunNamePrefix + w.cur.runID, experimentID: w.cur.expID}
}

// Shutdown stops the worker as manteion shuts down. No run is picked up any
// more; the run in progress has its rule switched off now, from this
// goroutine, without waiting for the worker's; its experiment is not
// cancelled (the next start's Sweep does that); it and every waiting run end
// cancelled/interrupted, and Run returns. Shutdown waits for Run to return, or
// for ctx to be done, whichever is first, so it returns by ctx's deadline even
// when the worker goroutine is stuck.
//
// It returns nil, or the switch-off's failure (errors.Is ErrRuleState), and
// ctx's error when the switch-off or Run had not finished by then; a
// switch-off still in flight goes on, with its own 10 s deadline. A rule that
// stays on is the next start's Sweep's first job.
//
// Safe to call any number of times, from any goroutine, while Run works, after
// Run returned, and before Run: Run then returns at once, and the runs waiting
// in line are ended here.
func (w *Worker) Shutdown(ctx context.Context) error {
	before := w.currentActive()
	w.halt()
	if w.running.CompareAndSwap(false, true) {
		// Run was never called, and now returns at once if it is: end what
		// waits, as Run would on its way out.
		for _, id := range w.q.waiting() {
			w.safely(id, "end a waiting run", func() { w.end(id, RunCancelled, ReasonInterrupted) })
		}
		close(w.done)
		w.log.Info("demo: worker shut down before it ran")
		return nil
	}
	// The run current before halt, one the worker may have picked up as halt
	// landed, and an ended run whose switch-off is still being retried (the
	// retry stops with the worker, so this is its last try before the next
	// start's sweep). Each switch is idempotent; writes to one rule never
	// overlap.
	runs := []*active{before}
	if after := w.currentActive(); after != before {
		runs = append(runs, after)
	}
	if p := w.pendingActive(); p != nil && !slices.Contains(runs, p) {
		runs = append(runs, p)
	}

	switched := make(chan error, 1)
	go func() {
		var failed []error
		for _, a := range runs {
			if a == nil {
				continue
			}
			if err := a.sw.off(ctx); err != nil {
				failed = append(failed, err)
			}
		}
		switched <- errors.Join(failed...)
	}()
	var errs []error
	if err, ok := recvOrDone(ctx, switched); !ok {
		errs = append(errs, &StepError{Step: "disable_rule", Class: ErrRuleState,
			Cause: errors.Join(errSwitchOffUnfinished, ctx.Err())})
	} else if err != nil {
		errs = append(errs, err)
	}
	if _, ok := recvOrDone(ctx, w.done); !ok {
		errs = append(errs, &StepError{Step: "wait_for_worker", Class: ctx.Err()})
	}
	for _, err := range errs {
		w.log.Error("demo: the worker's shutdown did not complete", "error", err)
	}
	if len(errs) == 0 {
		w.log.Info("demo: worker shut down")
	}
	return errors.Join(errs...)
}

// currentActive is the run in progress, or nil.
func (w *Worker) currentActive() *active {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cur
}

// recvOrDone waits for ch's value or for ctx, preferring the value when both are
// ready; ok is false when ctx ended first.
func recvOrDone[T any](ctx context.Context, ch <-chan T) (v T, ok bool) {
	select {
	case v = <-ch:
		return v, true
	case <-ctx.Done():
		select {
		case v = <-ch:
			return v, true
		default:
			return v, false
		}
	}
}

// ---- one pass ----

// currentRun names the run in progress, for the worker's retention.
type currentRun struct{ name, experimentID string }

func (c currentRun) owns(e *model.Experiment) bool {
	return (c.name != "" && e.Name == c.name) || (c.experimentID != "" && e.ID == c.experimentID)
}

func (c currentRun) ownsRule(r *model.Rule) bool { return c.name != "" && r.Name == c.name }

// tidier carries one sweep or retention pass and what failed in it.
type tidier struct {
	d     Deps
	log   *slog.Logger
	limit tidyTimeouts
	class error // ErrSweepIncomplete or ErrRetainIncomplete

	// stale > 0 (the worker's Retain): a planned demo experiment created more
	// than stale ago, not the run in progress's, is cancelled.
	stale time.Duration
	// current, when set, names the run in progress.
	current func() currentRun

	steps  []string
	causes []error
	done   struct{ off, cancelled, deletedExps, deletedRules int }
}

func newTidier(d Deps, class error, limit tidyTimeouts) *tidier {
	return &tidier{d: d, log: d.logger(), limit: limit, class: class}
}

func (t *tidier) what() string {
	if t.class == ErrSweepIncomplete {
		return "start-up sweep"
	}
	return "retention"
}

// wired reports whether the pass can run at all, recording a failure if not.
func (t *tidier) wired() bool {
	if t.d.Rules == nil || t.d.Experiments == nil || (t.stale > 0 && t.d.Clock == nil) {
		t.fail("check_input", errNotWired)
		return false
	}
	return true
}

// fail logs a failed step with its ids and full cause, and records it.
func (t *tidier) fail(step string, cause error, attrs ...any) {
	if !slices.Contains(t.steps, step) {
		t.steps = append(t.steps, step)
	}
	t.causes = append(t.causes, cause)
	t.log.Error("demo: "+t.what()+": a step failed", append([]any{"step", step, "error", cause}, attrs...)...)
}

// result is nil, or the pass's incompleteError.
func (t *tidier) result() error {
	t.log.Info("demo: "+t.what()+" done", "rules_switched_off", t.done.off, "experiments_cancelled", t.done.cancelled,
		"experiments_deleted", t.done.deletedExps, "rules_deleted", t.done.deletedRules, "failed_steps", t.steps)
	if len(t.steps) == 0 {
		return nil
	}
	return &incompleteError{class: t.class, steps: slices.Clone(t.steps), causes: slices.Clone(t.causes)}
}

func (t *tidier) currentRun() currentRun {
	if t.current == nil {
		return currentRun{}
	}
	return t.current()
}

// sweep is Sweep's body.
func (t *tidier) sweep(ctx context.Context, keep int) error {
	if !t.wired() {
		return t.result()
	}
	t.log.Info("demo: start-up sweep begins")

	// 1. Every enabled demo-run-* rule off, first.
	if rules, ok := t.listRules(context.WithoutCancel(ctx)); ok {
		t.switchOffEnabled(ctx, rules)
	}

	// 2. Every planned or running demo experiment cancelled.
	exps, complete := t.listExperiments(ctx)
	for _, e := range exps {
		if ownExperiment(e) && (e.Status == "planned" || e.Status == "running") {
			t.cancel(ctx, e)
		}
	}

	// 3. What step 1 missed goes off; runs' rules whose experiment is gone go.
	if rules, ok := t.listRules(ctx); ok {
		t.switchOffEnabled(ctx, rules)
		if complete {
			t.removeOrphanRules(ctx, rules, namesOf(exps))
		}
	}

	// 4. Retention.
	t.retain(ctx, keep)
	return t.result()
}

// retain is Retain's body: see Retain and Worker.Retain.
func (t *tidier) retain(ctx context.Context, keep int) {
	if keep < 1 {
		t.fail("check_keep", errBadKeep, "keep", keep)
		return
	}
	exps, complete := t.listExperiments(ctx)
	if t.stale > 0 {
		t.cancelStalePlanned(ctx, exps)
	}

	var doomed []*model.Experiment // newest first
	finished := 0
	for _, e := range exps {
		if !ownExperiment(e) || !terminalStatus(e.Status) {
			continue
		}
		if finished++; finished > keep {
			doomed = append(doomed, e)
		}
	}
	if len(doomed) == 0 && !complete {
		return
	}
	// Read after the experiments: a run's rule is written before its
	// experiment, so every listed experiment's rule is in this list.
	rules, ok := t.listRules(ctx)
	if !ok {
		return // no rule can be switched off by name, so nothing is deleted
	}
	byName := map[string][]*model.Rule{}
	for _, r := range rules {
		if r != nil && demoRuleName(r.Name) {
			byName[r.Name] = append(byName[r.Name], r)
		}
	}
	for i := len(doomed) - 1; i >= 0; i-- { // oldest first
		t.remove(ctx, doomed[i], byName[doomed[i].Name])
	}
	if complete {
		t.removeOrphanRules(ctx, rules, namesOf(exps))
	}
}

// cancelStalePlanned cancels the demo's planned experiments created more than
// t.stale ago, except the run in progress's.
func (t *tidier) cancelStalePlanned(ctx context.Context, exps []*model.Experiment) {
	now := t.d.Clock.Now()
	for _, e := range exps {
		if !ownExperiment(e) || e.Status != "planned" || now.Sub(e.CreatedAt) <= t.stale {
			continue
		}
		if t.currentRun().owns(e) {
			continue
		}
		t.log.Warn("demo: a run's planned experiment outlived twice the run cap; cancelling it",
			"experiment_id", e.ID, "experiment", e.Name, "created_at", e.CreatedAt)
		t.cancel(ctx, e)
	}
}

// remove deletes one finished demo experiment: every rule named like it off,
// then the experiment, then the run's rule (a rule with the name that is not a
// run's stays, off). A step that fails ends the sequence for this experiment.
func (t *tidier) remove(ctx context.Context, e *model.Experiment, rules []*model.Rule) {
	if t.currentRun().owns(e) {
		return
	}
	for _, r := range rules {
		if !t.switchOff(ctx, r) {
			t.log.Warn("demo: keeping a finished experiment whose rule could not be switched off",
				"experiment_id", e.ID, "rule_id", r.ID)
			return
		}
	}
	if !t.deleteExperiment(ctx, e) {
		return
	}
	for _, r := range rules {
		if ownRuleName(r.Name) {
			t.deleteRule(ctx, r)
		} else {
			t.log.Warn("demo: keeping a rule named like a deleted run that is not a run's own; it is off",
				"rule_id", r.ID, "rule", r.Name)
		}
	}
}

// removeOrphanRules switches off and deletes every run's rule whose
// experiment is not in names (all the demo's experiments, read before the
// rules), except the run in progress's: its rule is written before its
// experiment.
func (t *tidier) removeOrphanRules(ctx context.Context, rules []*model.Rule, names map[string]bool) {
	for _, r := range rules {
		if r == nil || !ownRuleName(r.Name) || names[r.Name] || t.currentRun().ownsRule(r) {
			continue
		}
		t.log.Info("demo: a run's rule has no experiment; removing it", "rule_id", r.ID, "rule", r.Name)
		if t.switchOff(ctx, r) {
			t.deleteRule(ctx, r)
		}
	}
}

// switchOffEnabled switches off every enabled demo-run-* rule in rules.
func (t *tidier) switchOffEnabled(ctx context.Context, rules []*model.Rule) {
	for _, r := range rules {
		if r != nil && r.Enabled && demoRuleName(r.Name) {
			t.switchOff(ctx, r)
		}
	}
}

// ---- single calls ----

func (t *tidier) listRules(ctx context.Context) ([]*model.Rule, bool) {
	cctx, cancel := context.WithTimeout(ctx, t.limit.call)
	defer cancel()
	rules, err := t.d.Rules.List(cctx)
	if err != nil {
		t.fail("list_rules", err)
		return nil, false
	}
	return rules, true
}

// listExperiments returns every experiment created by manteion-demo, newest
// first, page by page, each page with its own deadline. complete is false when
// a page could not be read: what was read is then the newest part.
func (t *tidier) listExperiments(ctx context.Context) (exps []*model.Experiment, complete bool) {
	seen := map[string]bool{}
	for page := 0; page < maxListPages; page++ {
		cctx, cancel := context.WithTimeout(ctx, t.limit.call)
		got, total, err := t.d.Experiments.List(cctx, store.ExperimentFilter{CreatedBy: CreatedBy},
			store.Page{Limit: listPage, Offset: page * listPage})
		cancel()
		if err != nil {
			t.fail("list_experiments", err, "offset", page*listPage)
			return exps, false
		}
		for _, e := range got {
			if e != nil && !seen[e.ID] { // rows inserted while paging repeat one
				seen[e.ID] = true
				exps = append(exps, e)
			}
		}
		if len(got) < listPage || (page+1)*listPage >= total {
			return exps, true
		}
	}
	t.fail("list_experiments", errTooManyPages, "pages", maxListPages)
	return exps, false
}

// switchOff switches r off with SetRuleEnabled (detached from ctx, never
// refused, not-found is success).
func (t *tidier) switchOff(ctx context.Context, r *model.Rule) bool {
	if err := SetRuleEnabled(ctx, t.d.Rules, r.ID, false); err != nil {
		t.fail("disable_rule", err, "rule_id", r.ID, "rule", r.Name)
		return false
	}
	t.done.off++
	t.log.Info("demo: rule switched off", "rule_id", r.ID, "rule", r.Name)
	return true
}

// cancel cancels e through the orchestrator. An experiment that has already
// ended, or is gone, is done.
func (t *tidier) cancel(ctx context.Context, e *model.Experiment) {
	if t.d.Orch == nil {
		t.fail("cancel_experiment", errNoOrchestrator, "experiment_id", e.ID)
		return
	}
	err := callWithin(ctx, t.limit.cancel, func(cctx context.Context) error {
		return t.d.Orch.CancelExperiment(cctx, e.ID)
	})
	var ended *orchestrator.ErrInvalidState
	switch {
	case err == nil:
		t.done.cancelled++
		t.log.Info("demo: experiment cancelled", "experiment_id", e.ID, "experiment", e.Name, "was", e.Status)
	case errors.As(err, &ended):
		t.log.Info("demo: experiment had already ended", "experiment_id", e.ID, "status", ended.Current)
	case errors.Is(err, store.ErrNotFound):
		t.log.Info("demo: experiment is gone", "experiment_id", e.ID)
	default:
		t.fail("cancel_experiment", err, "experiment_id", e.ID, "experiment", e.Name)
	}
}

func (t *tidier) deleteExperiment(ctx context.Context, e *model.Experiment) bool {
	cctx, cancel := context.WithTimeout(ctx, t.limit.call)
	defer cancel()
	if err := t.d.Experiments.Delete(cctx, e.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
		t.fail("delete_experiment", err, "experiment_id", e.ID, "experiment", e.Name)
		return false
	}
	t.done.deletedExps++
	t.log.Info("demo: experiment deleted", "experiment_id", e.ID, "experiment", e.Name, "status", e.Status)
	return true
}

func (t *tidier) deleteRule(ctx context.Context, r *model.Rule) {
	cctx, cancel := context.WithTimeout(ctx, t.limit.call)
	defer cancel()
	if err := t.d.Rules.Delete(cctx, r.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
		// Most likely a phase of another experiment still links it. It is off.
		t.fail("delete_rule", err, "rule_id", r.ID, "rule", r.Name)
		return
	}
	t.done.deletedRules++
	t.log.Info("demo: rule deleted", "rule_id", r.ID, "rule", r.Name)
}

// callWithin runs fn in its own goroutine on a context bounded by timeout, and
// waits for it at most that long. When the wait ends first, fn goes on in the
// background until it returns, and the context's error is returned. A panic
// in fn comes back as an error.
func callWithin(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	done := make(chan error, 1)
	go func() {
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("demo: panic: %v", r)
			}
		}()
		done <- fn(cctx)
	}()
	if err, ok := recvOrDone(cctx, done); ok {
		return err
	}
	return cctx.Err()
}

// ---- ownership ----

// demoRuleName: a name in the demo's namespace. Such a rule is switched off
// whenever a pass finds it on.
func demoRuleName(name string) bool { return strings.HasPrefix(name, RunNamePrefix) }

// ownRuleName: a run's rule as this package names it, demo-run-<run id>. Only
// such rules are ever deleted.
func ownRuleName(name string) bool {
	runID, ok := strings.CutPrefix(name, RunNamePrefix)
	return ok && validRunID(runID)
}

// ownExperiment: an experiment the demo created and named.
func ownExperiment(e *model.Experiment) bool {
	return e != nil && e.CreatedBy == CreatedBy && strings.HasPrefix(e.Name, RunNamePrefix)
}

func terminalStatus(status string) bool {
	return status == "completed" || status == "failed" || status == "cancelled"
}

// namesOf is the set of the experiments' names.
func namesOf(exps []*model.Experiment) map[string]bool {
	names := make(map[string]bool, len(exps))
	for _, e := range exps {
		names[e.Name] = true
	}
	return names
}
