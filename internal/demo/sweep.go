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

// The start-up switch-off and sweep, retention and the worker's shutdown step.
//
// What they may touch:
//   - An experiment is the demo's when its created_by is manteion-demo and its
//     name starts with demo-run-. Only the demo's experiments are ever
//     cancelled or deleted.
//   - Any rule named demo-run-* that is enabled is switched off when the
//     start-up switch-off finds it: switching off is safe whoever made the
//     rule, and the name is the demo's.
//   - A rule is deleted only when RunIDOfRule reads its name as a run's rule:
//     demo-run-<run id>-memory or demo-run-<run id>-cpu (BuildRun), or build
//     2's demo-run-<run id>, the run id one this package mints (validRunID).
//     Rules record no creator, so the name is all that ties a rule to a run: a
//     demo-run-x rule an operator made is switched off and kept, and so is any
//     rule a phase of an operator's experiment still links (the store refuses
//     that delete with store.ErrInUse).
//   - A run's experiment is named demo-run-<run id>, and each of its rules is
//     grouped under that name (runKey): that is how a run's rules are found
//     for its experiment, and the other way.
//
// The ordering rule. Deleting an experiment removes its phases' rule links,
// and an enabled rule attached to no phase is served to every instance of its
// service, for ever. So:
//   - an experiment is deleted only after every rule grouped under its name
//     (runKey) was switched off (one unconditional write each, detached from
//     the caller's cancellation);
//   - and only when every rule its phases link is one of those: an operator
//     can link any rule to any phase, a finished demo experiment's included,
//     and deleting the experiment would leave that rule, if it is linked
//     nowhere else, served for ever. Such an experiment is kept, and the pass
//     is incomplete. The links are read just before the delete; a read that
//     fails keeps the experiment too;
//   - a rule is deleted only once its experiment is gone (the store refuses the
//     other order while a phase links the rule).
//
// A failed step keeps what it guards, and the next pass tries again: a failed
// switch-off keeps the experiment; a failed experiment delete keeps the rule,
// off.
//
// Start-up order in main. The clean-up is split in two because a cancel waits
// for the service fleet to acknowledge its new desired state, and the fleet
// reaches manteion only through its HTTP listener (about 30 s per running demo
// experiment if the cancel ran first):
//
//  1. orch.Recover;
//  2. SwitchOffAll: database only, before the listener starts, so no
//     instance that polls once it is up is served a demo fault;
//  3. start the HTTP listener;
//  4. Sweep, with a deadline (2 minutes);
//  5. Worker.Run.

// tidyTimeouts bound one pass's calls; the caller's deadline bounds every
// call after the start-up switch-off too.
type tidyTimeouts struct {
	// call bounds each store call: a page of experiments, the rule list, a
	// phase's links, a delete.
	call time.Duration
	// off bounds each switch-off write (SetRuleEnabled's 10 s).
	off time.Duration
	// offAll bounds the start-up switch-off as a whole: listing and writes.
	offAll time.Duration
	// cancel bounds the wait for each CancelExperiment (see callWithin).
	cancel time.Duration
}

var defaultTidyTimeouts = tidyTimeouts{
	call:   10 * time.Second,
	off:    switchOffTimeout,
	offAll: 30 * time.Second,
	cancel: 30 * time.Second,
}

// maxListPages bounds one listing of the demo's experiments (listPage each).
const maxListPages = 500

// Errors of an incomplete pass. The error a pass returns satisfies errors.Is
// with its class and with the cause of every step that failed.
var (
	// ErrSwitchOffIncomplete: the start-up switch-off could not list the rules
	// or switch one off; the rules it missed are in the logs, by id.
	ErrSwitchOffIncomplete = errors.New("demo: the start-up switch-off did not complete")
	// ErrSweepIncomplete: some step of the start-up sweep failed, or its
	// deadline cut it short; manteion starts anyway, and what is left is in
	// the logs, by id.
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
	errForeignRule         = errors.New("demo: a phase of the experiment links a rule that is not its run's")
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

// SwitchOffAll switches off every enabled demo-run-* rule: it lists the rules
// and writes each switch-off, and touches nothing else (no cancel, no delete,
// no orchestrator call), so it needs only the database. Call it at start-up
// after orch.Recover and before the HTTP listener starts (see the start-up
// order above); Sweep does the same again as its first step, which is then
// cheap.
//
// It is detached from ctx's cancellation (a caller that gives up still gets
// its rules switched off) and bounded on its own: each call 10 s, the whole 30
// s. It returns nil, or an error satisfying errors.Is(err,
// ErrSwitchOffIncomplete) when the listing or a switch-off failed or ran out
// of time; each is logged with its ids.
func SwitchOffAll(ctx context.Context, d Deps) error {
	t := newTidier(d, ErrSwitchOffIncomplete, defaultTidyTimeouts)
	if t.d.Rules == nil {
		t.fail("check_input", errNotWired)
	} else {
		t.switchOffAll(ctx)
	}
	return t.result()
}

// Sweep cleans up after an earlier manteion process: after orch.Recover, once
// the HTTP listener is up (a cancel waits for the fleet), and before the
// worker runs (it would take the run in progress for a leftover). In this
// order, each step going on past individual failures:
//
//  1. SwitchOffAll's work: every enabled demo-run-* rule switched off, before
//     anything else, detached from ctx and bounded by its own 30 s;
//  2. every planned or running demo experiment cancelled through the
//     orchestrator (one that has already ended counts as done);
//  3. the rules read again: one still on (step 1 could not read or switch it)
//     is switched off now; a run's rule whose experiment no longer exists is
//     switched off, again, and deleted;
//  4. Retain(keep).
//
// Pass a context with a deadline (the wiring passes 2 minutes). Every call
// after step 1 is bounded by that deadline as well as by its own (10 s per
// store call or switch-off, 30 s per cancel), and once ctx is done no new step,
// cancel, switch-off or delete is started: Sweep returns at once, incomplete.
// So with N planned or running demo experiments, each cancel waiting up to
// 30 s for the fleet, the sweep still ends by the deadline, not after N x 30
// s; the cancels it did not reach are logged, their rules are already off,
// and the next start's sweep cancels them. The worst case is max(30 s, the
// deadline), the 30 s being step 1's own bound.
//
// A failed step is logged with its ids and full cause, and the sweep then
// returns an error satisfying errors.Is(err, ErrSweepIncomplete); the caller
// should log it and start.
func Sweep(ctx context.Context, d Deps, keep int) error {
	return newTidier(d, ErrSweepIncomplete, defaultTidyTimeouts).sweep(ctx, keep)
}

// Retain keeps the newest keep finished demo experiments (completed, failed or
// cancelled; newest by creation) and deletes the rest, each as: every rule
// grouped under its name (runKey: the run's rules) switched off; its phases'
// rule links read, and the experiment kept if any links a rule that is not one
// of those; the experiment deleted; then the run's rules deleted. A run's rule
// whose experiment no longer exists is switched off and deleted too. Retain never touches a planned or running
// experiment and never cancels anything; it pages through all the demo's
// experiments. Once ctx is done it starts nothing new.
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
// twice Config.RunCap ago (by Deps.Clock) that is not spared (below).
// The worker leaves such an experiment when its watchdog or a shutdown ends a
// run before its start was launched; a cancelled one is terminal and is
// deleted by a later pass like any other. (A running leftover, whose cancel
// failed, needs nothing: its phases end on their own.)
//
// It spares the run in progress and the ended run whose switch-off is still
// being retried (OnRunEnded runs before the worker waits for that switch-off):
// neither's experiment, known by id once built and by name before, is
// cancelled or deleted, and neither's rule is taken for an orphan. Both are
// read again just before each cancel and delete, so this is safe at any time,
// not only from OnRunEnded.
//
// Its listing and its deletes keep the worker's run index up to date
// (lookups.go).
func (w *Worker) Retain(ctx context.Context) error {
	t := newTidier(w.d, ErrRetainIncomplete, defaultTidyTimeouts)
	t.stale = 2 * w.cfg.RunCap
	t.spare = w.spared
	t.runs = &w.runs
	if t.wired() {
		t.retain(ctx, w.cfg.Retain)
	}
	return t.result()
}

// spared names the runs retention must not touch: the run in progress and the
// ended run whose switch-off is pending.
func (w *Worker) spared() spared {
	w.mu.Lock()
	defer w.mu.Unlock()
	var s spared
	for _, a := range []*active{w.cur, w.pending} {
		if a != nil {
			s.names = append(s.names, RunNamePrefix+a.runID)
			if a.expID != "" {
				s.experimentIDs = append(s.experimentIDs, a.expID)
			}
		}
	}
	return s
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
// stays on is the next start's SwitchOffAll's job.
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
	// start's switch-off). Each switch is idempotent; writes to one rule never
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
			if err := a.sw.off(ctx, offShutdown); err != nil {
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

// spared names runs a pass must not cancel, delete or take for orphans.
type spared struct{ names, experimentIDs []string }

func (s spared) owns(e *model.Experiment) bool {
	return slices.Contains(s.names, e.Name) || slices.Contains(s.experimentIDs, e.ID)
}

func (s spared) ownsRule(r *model.Rule) bool { return slices.Contains(s.names, runKey(r.Name)) }

// tidier carries one pass and what failed in it.
type tidier struct {
	d     Deps
	log   *slog.Logger
	limit tidyTimeouts
	class error // ErrSwitchOffIncomplete, ErrSweepIncomplete or ErrRetainIncomplete

	// stale > 0 (the worker's Retain): a planned demo experiment created more
	// than stale ago, and not spared, is cancelled.
	stale time.Duration
	// spare, when set, names the runs the pass must not touch.
	spare func() spared
	// runs, when set, is told of every listing of the demo's experiments and
	// of every experiment deleted (the worker's run index, lookups.go).
	runs *runIndex

	steps   []string
	causes  []error
	outHit  bool     // the caller's context ended the pass
	inUse   []string // runs' rules kept because a phase of another experiment still links them
	counted struct{ off, cancelled, deletedExps, deletedRules int }
}

func newTidier(d Deps, class error, limit tidyTimeouts) *tidier {
	return &tidier{d: d, log: d.logger(), limit: limit, class: class}
}

func (t *tidier) what() string {
	switch t.class {
	case ErrSwitchOffIncomplete:
		return "start-up switch-off"
	case ErrSweepIncomplete:
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

// stopped reports whether ctx is done; the first time, the pass records it.
// Nothing new is started once it is.
func (t *tidier) stopped(ctx context.Context) bool {
	err := expired(ctx)
	if err == nil {
		return false
	}
	if !t.outHit {
		t.outHit = true
		t.fail("out_of_time", err)
	}
	return true
}

// result is nil, or the pass's incompleteError.
func (t *tidier) result() error {
	if len(t.inUse) > 0 {
		t.log.Warn("demo: "+t.what()+": runs' rules kept, switched off: a phase of another experiment still links them",
			"rule_ids", t.inUse)
	}
	t.log.Info("demo: "+t.what()+" done", "rules_switched_off", t.counted.off, "experiments_cancelled", t.counted.cancelled,
		"experiments_deleted", t.counted.deletedExps, "rules_deleted", t.counted.deletedRules, "failed_steps", t.steps)
	if len(t.steps) == 0 {
		return nil
	}
	return &incompleteError{class: t.class, steps: slices.Clone(t.steps), causes: slices.Clone(t.causes)}
}

func (t *tidier) spared() spared {
	if t.spare == nil {
		return spared{}
	}
	return t.spare()
}

// expired is ctx's error, or context.DeadlineExceeded once ctx's deadline has
// passed by the clock: a context's own timer marks it done a moment after its
// deadline, and nothing should start in that moment.
func expired(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// offWithin is how long a switch-off may take now: t.limit.off, or less when
// ctx's deadline is nearer.
func (t *tidier) offWithin(ctx context.Context) time.Duration {
	d := t.limit.off
	if deadline, ok := ctx.Deadline(); ok {
		d = min(d, time.Until(deadline))
	}
	return d
}

// switchOffAll is step 1 of the sweep and all of SwitchOffAll: every enabled
// demo-run-* rule off. It runs on a context detached from ctx's cancellation
// and bounded by t.limit.offAll; a rule it has no time left for is a failure.
func (t *tidier) switchOffAll(ctx context.Context) {
	octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), t.limit.offAll)
	defer cancel()
	rules, ok := t.listRules(octx)
	if !ok {
		return
	}
	var missed []string
	for _, r := range rules {
		if r == nil || !r.Enabled || !demoRuleName(r.Name) {
			continue
		}
		if expired(octx) != nil {
			missed = append(missed, r.ID)
			continue
		}
		t.switchOff(octx, r)
	}
	if len(missed) > 0 {
		t.fail("disable_rule", context.DeadlineExceeded, "rule_ids", missed, "note", "no time left to switch them off")
	}
}

// sweep is Sweep's body.
func (t *tidier) sweep(ctx context.Context, keep int) error {
	if !t.wired() {
		return t.result()
	}
	t.log.Info("demo: start-up sweep begins")

	// 1. Every enabled demo-run-* rule off, first.
	t.switchOffAll(ctx)

	// 2. Every planned or running demo experiment cancelled. If step 1 could
	// not list or switch a rule, this cancels with that rule possibly still on.
	// That is still safe: a cancel can only stop a rule being served (the
	// store serves a phase's rule only while the phase and its experiment run),
	// never start serving one; step 3 then tries the switch-off again.
	if t.stopped(ctx) {
		return t.result()
	}
	exps, complete := t.listExperiments(ctx)
	for _, e := range exps {
		if !ownExperiment(e) || (e.Status != "planned" && e.Status != "running") {
			continue
		}
		if t.stopped(ctx) {
			return t.result()
		}
		t.cancel(ctx, e)
	}

	// 3. What step 1 missed goes off; runs' rules whose experiment is gone go.
	if t.stopped(ctx) {
		return t.result()
	}
	if rules, ok := t.listRules(ctx); ok {
		for _, r := range rules {
			if r == nil || !r.Enabled || !demoRuleName(r.Name) {
				continue
			}
			if t.stopped(ctx) {
				return t.result()
			}
			t.switchOff(ctx, r)
		}
		if complete {
			t.removeOrphanRules(ctx, rules, namesOf(exps))
		}
	}

	// 4. Retention.
	if t.stopped(ctx) {
		return t.result()
	}
	t.retain(ctx, keep)
	return t.result()
}

// retain is Retain's body: see Retain and Worker.Retain.
func (t *tidier) retain(ctx context.Context, keep int) {
	if keep < 1 {
		t.fail("check_keep", errBadKeep, "keep", keep)
		return
	}
	if t.stopped(ctx) {
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
	if (len(doomed) == 0 && !complete) || t.stopped(ctx) {
		return
	}
	// Read after the experiments: a run's rules are written before its
	// experiment, so every listed experiment's rules are in this list.
	rules, ok := t.listRules(ctx)
	if !ok {
		return // no rule can be switched off by name, so nothing is deleted
	}
	byName := map[string][]*model.Rule{} // by runKey: a run's rules under its experiment's name
	for _, r := range rules {
		if r != nil && demoRuleName(r.Name) {
			byName[runKey(r.Name)] = append(byName[runKey(r.Name)], r)
		}
	}
	for i := len(doomed) - 1; i >= 0; i-- { // oldest first
		if t.stopped(ctx) {
			return
		}
		t.remove(ctx, doomed[i], byName[doomed[i].Name])
	}
	if complete {
		t.removeOrphanRules(ctx, rules, namesOf(exps))
	}
}

// cancelStalePlanned cancels the demo's planned experiments created more than
// t.stale ago, except the spared runs'.
func (t *tidier) cancelStalePlanned(ctx context.Context, exps []*model.Experiment) {
	now := t.d.Clock.Now()
	for _, e := range exps {
		if !ownExperiment(e) || e.Status != "planned" || now.Sub(e.CreatedAt) <= t.stale {
			continue
		}
		if t.stopped(ctx) {
			return
		}
		if t.spared().owns(e) {
			continue
		}
		t.log.Warn("demo: a run's planned experiment outlived twice the run cap; cancelling it",
			"experiment_id", e.ID, "experiment", e.Name, "created_at", e.CreatedAt)
		t.cancel(ctx, e)
	}
}

// remove deletes one finished demo experiment: every rule grouped under its
// name (runKey) off, then, if its phases link no other rule, the experiment,
// then each of the run's rules (a rule with the name that is not a run's
// stays, off). A step that fails, or
// a foreign link, ends the sequence for this experiment.
func (t *tidier) remove(ctx context.Context, e *model.Experiment, rules []*model.Rule) {
	if t.spared().owns(e) {
		return
	}
	for _, r := range rules {
		if !t.switchOff(ctx, r) {
			t.log.Warn("demo: keeping a finished experiment whose rule could not be switched off",
				"experiment_id", e.ID, "rule_id", r.ID)
			return
		}
	}
	if !t.linksOnly(ctx, e, rules) {
		return
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

// linksOnly reports whether e's phases link no rule but own, the rules found
// for its run by name: deleting e removes its phases' links, and a rule of
// someone else's that thereby loses its last link while enabled is served to
// every instance of its service for ever. It reads every phase and each
// phase's links; a read that fails is false (the experiment is kept), and so
// is a foreign link, which is logged with its ids and makes the pass
// incomplete.
func (t *tidier) linksOnly(ctx context.Context, e *model.Experiment, own []*model.Rule) bool {
	cctx, cancel := context.WithTimeout(ctx, t.limit.call)
	phases, err := t.d.Experiments.ListPhasesForExperiment(cctx, e.ID)
	cancel()
	if err != nil {
		t.fail("read_phase_links", err, "experiment_id", e.ID, "experiment", e.Name, "note", "experiment kept")
		return false
	}
	var foreign, where []string
	for _, p := range phases {
		if p == nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, t.limit.call)
		links, err := t.d.Experiments.ListPhaseRules(cctx, p.ID)
		cancel()
		if err != nil {
			t.fail("read_phase_links", err, "experiment_id", e.ID, "experiment", e.Name, "phase_id", p.ID,
				"note", "experiment kept")
			return false
		}
		for _, l := range links {
			if !slices.ContainsFunc(own, func(r *model.Rule) bool { return r.ID == l.RuleID }) {
				foreign, where = append(foreign, l.RuleID), append(where, p.ID)
			}
		}
	}
	if len(foreign) > 0 {
		t.fail("foreign_rule_linked", errForeignRule, "experiment_id", e.ID, "experiment", e.Name,
			"rule_ids", foreign, "phase_ids", where,
			"note", "experiment kept, its run's rule off: deleting it would leave these rules linked to nothing")
		return false
	}
	return true
}

// removeOrphanRules switches off and deletes every run's rule whose
// experiment is not in names (all the demo's experiments, read before the
// rules), except the spared runs': a run's rule is written before its
// experiment.
func (t *tidier) removeOrphanRules(ctx context.Context, rules []*model.Rule, names map[string]bool) {
	for _, r := range rules {
		if r == nil || !ownRuleName(r.Name) || names[runKey(r.Name)] {
			continue
		}
		if t.stopped(ctx) {
			return
		}
		if t.spared().ownsRule(r) {
			continue
		}
		t.log.Info("demo: a run's rule has no experiment; removing it", "rule_id", r.ID, "rule", r.Name)
		if t.switchOff(ctx, r) {
			t.deleteRule(ctx, r)
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
// a page could not be read: what was read is then the newest part. The run
// index, when the pass has one, learns the result.
func (t *tidier) listExperiments(ctx context.Context) (exps []*model.Experiment, complete bool) {
	if t.runs != nil {
		epoch := t.runs.beginListing()
		defer func() { t.runs.listed(epoch, exps, complete) }()
	}
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

// switchOff switches r off: one unconditional write, detached from ctx's
// cancellation, bounded by t.limit.off or ctx's nearer deadline; a rule that
// no longer exists is success. A failure is logged once, here. A rule the
// listing showed enabled is logged at Info, "demo: rule switched off" with the
// pass's reason, as the worker's switch logs it; one that was already off, or
// is gone, at Debug.
func (t *tidier) switchOff(ctx context.Context, r *model.Rule) bool {
	gone, err := switchOffRule(ctx, t.d.Rules, r.ID, t.offWithin(ctx))
	if err != nil {
		t.fail("disable_rule", err, "rule_id", r.ID, "rule", r.Name)
		return false
	}
	attrs := []any{"rule_id", r.ID, "rule", r.Name, "reason", t.offReason()}
	if runID, ok := RunIDOfRule(r.Name); ok {
		attrs = append([]any{"run_id", runID}, attrs...)
	}
	t.counted.off++
	switch {
	case gone:
		t.log.Debug("demo: rule already gone; nothing to switch off", attrs...)
	case !r.Enabled:
		t.log.Debug("demo: rule already off", attrs...)
	default:
		t.log.Info("demo: rule switched off", attrs...)
	}
	return true
}

// offReason is why the pass switches rules off: the start-up switch-off and
// sweep, or retention.
func (t *tidier) offReason() string {
	if t.class == ErrRetainIncomplete {
		return offRetention
	}
	return offStartUp
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
		t.counted.cancelled++
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
	if runID, ok := runIDOf(e); ok && t.runs != nil {
		t.runs.drop(runID)
	}
	t.counted.deletedExps++
	t.log.Info("demo: experiment deleted", "experiment_id", e.ID, "experiment", e.Name, "status", e.Status)
	return true
}

// deleteRule deletes r. A refusal because a phase of another experiment still
// links it (store.ErrInUse) is no failure: the rule is off, it is reported
// once per pass at Warn, and a later pass deletes it once unlinked.
func (t *tidier) deleteRule(ctx context.Context, r *model.Rule) {
	cctx, cancel := context.WithTimeout(ctx, t.limit.call)
	defer cancel()
	err := t.d.Rules.Delete(cctx, r.ID)
	switch {
	case err == nil || errors.Is(err, store.ErrNotFound):
		t.counted.deletedRules++
		t.log.Info("demo: rule deleted", "rule_id", r.ID, "rule", r.Name)
	case errors.Is(err, store.ErrInUse):
		t.inUse = append(t.inUse, r.ID)
	default:
		t.fail("delete_rule", err, "rule_id", r.ID, "rule", r.Name)
	}
}

// callWithin runs fn in its own goroutine on a context bounded by timeout and
// by ctx, and waits for it at most that long: CancelExperiment first takes the
// experiment's lock, which no context interrupts. When the wait ends first,
// the context fn runs on is already done, so the cancel does not carry on
// regardless: if it was still waiting for the lock, its status change fails
// once it gets it; if it was past the status change, the phases' teardown is
// cut short and the next start's recovery finishes it. Either way the rule is
// already off. The goroutine ends when fn returns, never later; the context's
// error is returned. A panic in fn comes back as an error.
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
// whenever a start-up pass finds it on.
func demoRuleName(name string) bool { return strings.HasPrefix(name, RunNamePrefix) }

// ownRuleName: a run's rule as this package names it (RunIDOfRule):
// demo-run-<run id>-memory, demo-run-<run id>-cpu, or build 2's
// demo-run-<run id>. Only such rules are ever deleted.
func ownRuleName(name string) bool {
	_, ok := RunIDOfRule(name)
	return ok
}

// runKey is the name a rule is grouped under: for a run's rule (RunIDOfRule),
// its run's experiment name demo-run-<run id>, so a run's rules are found
// together for its experiment; any other name stands for itself.
func runKey(name string) string {
	if runID, ok := RunIDOfRule(name); ok {
		return RunNamePrefix + runID
	}
	return name
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
