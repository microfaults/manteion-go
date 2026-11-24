package demo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
)

// View's answers besides a run. Routes map them to error codes; their text
// never reaches a client.
var (
	// ErrRunNotFound: no run has this id (404 not_found). An id not of the
	// shape this package mints is never looked up, nor, while the run index is
	// complete, an id outside it (lookups.go).
	ErrRunNotFound = errors.New("demo: no such run")
	// ErrRunUnavailable: the run could not be read now (the database failed,
	// or every lookup slot is taken; 503 unavailable). A failure's cause is in
	// manteion's logs.
	ErrRunUnavailable = errors.New("demo: the run could not be read")
)

// The worker's internal errors: for logs only.
var (
	errSwitchClosed   = errors.New("demo: the run's rule switch is closed")
	errNoRule         = errors.New("demo: the run has no rule yet")
	errOffNotLanded   = errors.New("demo: a rule's switch-off has not landed")
	errStartPanicked  = errors.New("demo: starting the experiment panicked")
	errNoOrchestrator = errors.New("demo: no orchestrator")
	errNoExperiments  = errors.New("demo: no experiment store")
)

// listPage is the page size of the worker's experiment listings (the store's
// maximum).
const listPage = 200

// A switch-off that fails at a run's ending is retried after offRetryFirst,
// then after twice as long each time, at most offRetryMax apart, until it
// succeeds or the worker stops. Tests shrink Worker.offRetry.
const (
	offRetryFirst = time.Second
	offRetryMax   = 30 * time.Second
)

// backoff is a retry schedule: first, then doubling, capped at max.
type backoff struct{ first, max time.Duration }

// next is the wait after one of d.
func (b backoff) next(d time.Duration) time.Duration { return min(2*d, b.max) }

// Deadlines of the worker's calls, all derived from Config.
//
//   - readTimeout bounds each single read: a status, the phases, a phase's
//     results, a page of experiments.
//   - preflightTimeout bounds the whole preflight, four probes in a row.
//   - switchOnTimeout bounds the switch-on (a read and a write).
//   - teardownTimeout bounds how long an ending waits for the start to return
//     and the cancel to finish, each cancel call, and OnRunEnded.
//
// Preflight, the build and the switch-on also end when the run reaches its cap.
func (c Config) readTimeout() time.Duration      { return c.QueryTimeout }
func (c Config) preflightTimeout() time.Duration { return 4 * c.QueryTimeout }
func (c Config) switchOnTimeout() time.Duration  { return 4 * c.QueryTimeout }
func (c Config) teardownTimeout() time.Duration  { return c.RunCap }

// Worker runs the queue's runs one at a time, and is the only caller of
// StartExperiment and CancelExperiment, always from goroutines it owns.
//
// For each run, from the moment it picks the run up with the scenario it was
// queued with:
//
//  1. a watchdog fires Config.RunCap later unless the run has ended: it
//     switches the run's rules off at once, from its own goroutine, whatever
//     the worker is blocked on, and cuts the run's calls short;
//  2. preflight of the run's scenario; the run is running from here
//     (started_at);
//  3. BuildRun of the run's scenario: two disabled rules, each attached to its
//     fault phase (memory, cpu), and a planned experiment;
//  4. the rules are switched on, one after the other (each only while a
//     phase links it); if one cannot be, every rule is switched off again at
//     once and the run fails;
//  5. StartExperiment, in a goroutine of the worker's, on a context that no
//     request and no shutdown cancels, only the watchdog;
//  6. every Config.StatusPoll the experiment and its phases are read; a phase
//     that completed has its proof's figures read (observe.go: at most
//     Config.QueryTimeout per read, never failing the run), and a phase
//     whose public status changed is published;
//  7. the run ends. Every ending, before anything else, switches the rules
//     off, all at once.
//     A watchdog, a start error, a panic, an unknown status or a vanished
//     experiment then cancel the experiment, but only once its start has
//     returned; a completed, failed or externally cancelled experiment needs
//     no cancel, nor does a shutdown. Then a last read of the phases, the run's
//     final view is kept, its slot freed, its end event published (last), the
//     runs behind it told their new positions, and OnRunEnded called;
//  8. if the ending's switch-off did not land on every rule, it is retried
//     with backoff from a goroutine of the worker's, each retry writing only
//     the rules whose switch-off has not landed (SwitchOffPending reports it
//     meanwhile), and the next run is not picked up until the switch-off of
//     every rule of this run has landed, or the worker stops.
//
// Every rule switch of a run goes through one ruleSwitch, which refuses a
// switch-on once the run's rules have been switched off: a late switch-on can
// never follow the switch-off. A panic in the run's code is recovered: the run
// ends failed/internal_error with its rules off and the worker goes on.
type Worker struct {
	// OnRunEnded, if set, is called after each run has ended (rule off, end
	// event published, slot freed), on the worker's goroutine, with a context
	// bounded by Config.RunCap that it must honour. It is not called for runs
	// ended by a shutdown. A panic in it is recovered. Set it before Run.
	OnRunEnded func(ctx context.Context)

	d   Deps
	cfg Config
	q   *Queue
	h   *Hub
	log *slog.Logger

	mu       sync.Mutex
	cur      *active // the run being worked on, nil between runs
	pending  *active // the ended run whose switch-off is being retried, if any
	finished []Run   // ended runs, oldest first, at most cfg.Retain

	// View's lookups in the database (lookups.go): the runs that may exist
	// and the ended views kept, and the slots of the lookups in flight.
	runs    runIndex
	lookups chan struct{}

	// proofSlots holds one token per proof query that has not returned
	// (observe.go, maxProofQueries).
	proofSlots chan struct{}

	offRetry backoff // the switch-off retry schedule; set before Run

	running atomic.Bool
	ended   atomic.Uint64   // runs ended in this process (endedCount)
	halted  context.Context // done once halt was called
	haltFn  context.CancelFunc
	done    chan struct{}  // closed when Run returns
	bg      sync.WaitGroup // the worker's goroutines that may outlive a run: starts, late cancels, switch-off retries
}

// active is the run the worker is on.
type active struct {
	runID string
	sc    Scenario // the scenario the run was queued with
	sw    *ruleSwitch

	// ctx carries the run's own calls (preflight, build, switch-on, reads):
	// cancelled when the watchdog fires and when the worker stops.
	ctx    context.Context
	cancel context.CancelFunc
	// startCtx carries StartExperiment: cancelled only by the watchdog, and
	// when the start returns.
	startCtx    context.Context
	cancelStart context.CancelFunc

	timedOut atomic.Bool   // set first thing when the watchdog fires
	watchdog chan struct{} // closed when the watchdog has cut the run short
	timer    *time.Timer
	ending   atomic.Bool // the worker is ending the run: the watchdog stands down

	// Owned by the worker goroutine.
	phaseIDs   [3]string
	last       [3]PhaseStatus // last published status of each phase
	haveResult [3]bool
	started    bool          // StartExperiment was launched
	startDone  chan struct{} // closed when the start goroutine returns
	startErr   error         // StartExperiment's answer, valid once startDone is closed
	// The run's proof (observe.go): the phases whose figures were asked
	// for, and the baseline phase's figures (the memory target's memory, the
	// CPU target's CPU), nil when not measured.
	proved              [3]bool
	baseMemory, baseCPU *float64

	// Written by the worker goroutine under Worker.mu.
	expID        string
	phase        string
	phaseRunning bool
}

// outcome is how a run ends.
type outcome struct {
	status RunStatus
	reason Reason
	// cancel: the experiment may still be running; cancel it, if it was
	// started, once its start has returned.
	cancel bool
	// shutdown: the worker is stopping; no cancel, no last read, no OnRunEnded.
	shutdown bool
}

// Why a rule is switched off, as the "rule switched off" log line says.
const (
	offCompleted = "completed"
	offFailed    = "failed"
	offCancelled = "cancelled"
	offTimedOut  = "timed out"
	offShutdown  = "shutdown"
	offStartUp   = "start-up"
	offRetention = "retention"
)

// offReason is why the ending switches the run's rules off.
func (o outcome) offReason() string {
	switch {
	case o.shutdown:
		return offShutdown
	case o.reason == ReasonTimedOut:
		return offTimedOut
	case o.status == RunCompleted:
		return offCompleted
	case o.status == RunFailed:
		return offFailed
	}
	return offCancelled
}

// NewWorker returns a worker over q and h (new ones when nil). d.Clock is
// required. A non-positive RunCap, StatusPoll, QueryTimeout or Retain takes its
// DefaultConfig value.
func NewWorker(d Deps, cfg Config, q *Queue, h *Hub) *Worker {
	if d.Clock == nil {
		panic("demo: NewWorker needs Deps.Clock")
	}
	def := DefaultConfig()
	if cfg.RunCap <= 0 {
		cfg.RunCap = def.RunCap
	}
	if cfg.StatusPoll <= 0 {
		cfg.StatusPoll = def.StatusPoll
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = def.QueryTimeout
	}
	if cfg.Retain <= 0 {
		cfg.Retain = def.Retain
	}
	if q == nil {
		q = NewQueue(cfg.Capacity)
	}
	if h == nil {
		h = NewHub()
	}
	halted, haltFn := context.WithCancel(context.Background())
	return &Worker{d: d, cfg: cfg, q: q, h: h, log: d.logger(), halted: halted, haltFn: haltFn,
		done: make(chan struct{}), offRetry: backoff{first: offRetryFirst, max: offRetryMax},
		lookups: make(chan struct{}, maxDBLookups), proofSlots: make(chan struct{}, maxProofQueries)}
}

// Run works the queue, one run at a time, until ctx is done or halt is called.
// The run in progress then ends cancelled/interrupted with its rules switched
// off and its experiment left as it is (the start-up sweep cancels it), the
// queue is closed (Enqueue answers ErrQueueClosed), every run still waiting
// ends the same way, and Run returns. Call it once; a second call returns at
// once.
func (w *Worker) Run(ctx context.Context) {
	if !w.running.CompareAndSwap(false, true) {
		return
	}
	defer close(w.done)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopOnHalt := context.AfterFunc(w.halted, cancel)
	defer stopOnHalt()

	for {
		runID, sc, ok := w.q.next(ctx)
		if !ok {
			break
		}
		w.runOne(ctx, runID, sc)
	}
	// Close first, then read what waits: a run enqueued before the close is
	// read here and ended, one after it is refused.
	w.q.closeLine()
	for _, id := range w.q.waiting() {
		w.safely(id, "end a waiting run", func() { w.end(id, RunCancelled, ReasonInterrupted) })
	}
}

// halt makes Run stop as when its context ends, and closes the queue at once:
// Enqueue answers ErrQueueClosed from now on. Safe to call any number of
// times, from any goroutine, before or during Run.
func (w *Worker) halt() {
	w.q.closeLine()
	w.haltFn()
}

// SwitchOffPending reports whether an ended run's rules are not yet confirmed
// off: the switch-off of one of them failed and is being retried, and no run
// is picked up until it has landed on every rule. The health route reports
// the lab as not accepting runs meanwhile. Enqueue still works: the run waits
// in line.
func (w *Worker) SwitchOffPending() bool {
	w.mu.Lock()
	p := w.pending
	w.mu.Unlock()
	return p != nil && !p.sw.isOff()
}

// pendingActive is the ended run whose switch-off is being retried, or nil.
func (w *Worker) pendingActive() *active {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pending
}

// switchOffCurrent switches the current run's rules off and closes its switch,
// so no switch-on of that run lands afterwards. The run goes on until it ends
// (halt ends it). With no current run it does nothing. Safe to call any number
// of times, from any goroutine. A shutdown calls halt, then this.
func (w *Worker) switchOffCurrent(ctx context.Context) error {
	w.mu.Lock()
	a := w.cur
	w.mu.Unlock()
	if a == nil {
		return nil
	}
	return a.sw.off(ctx, offShutdown)
}

// Current reports the run the worker is on and where it is: the running
// phase's name and true, or, while no phase runs, the next phase's name (the
// last one's once all have ended) and false. Between runs it is "", "", false.
func (w *Worker) Current() (runID, phase string, phaseRunning bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == nil {
		return "", "", false
	}
	return w.cur.runID, w.cur.phase, w.cur.phaseRunning
}

// sampling is Current with the service whose own CPU and memory a live sample
// of that phase carries (sampleTarget); "" between runs.
func (w *Worker) sampling() (runID, phase string, phaseRunning bool, target string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == nil {
		return "", "", false, ""
	}
	return w.cur.runID, w.cur.phase, w.cur.phaseRunning, sampleTarget(w.cur.sc, w.cur.phase)
}

// currentExperimentID is the current run's experiment id once built, else "":
// the "own" experiment a health check passes to Preflight.
func (w *Worker) currentExperimentID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == nil {
		return ""
	}
	return w.cur.expID
}

// ---- one run ----

func (w *Worker) runOne(parent context.Context, runID string, sc Scenario) {
	a := w.pickUp(parent, runID, sc)
	defer a.timer.Stop()
	defer a.cancel()
	out := w.guardedDrive(a)
	if !a.started {
		a.cancelStart()
	}
	w.finish(parent, a, out)
	w.awaitOff(parent, a)
}

// awaitOff holds the worker until the switch-off of every rule of the run has
// landed, or the worker stops: no next run is picked up while an ended run's
// rule may still be on. It returns at once for a run with no rule or whose
// first switch-off landed on every rule.
func (w *Worker) awaitOff(parent context.Context, a *active) {
	if len(a.sw.attached()) == 0 || a.sw.isOff() {
		return
	}
	for _, ruleID := range a.sw.notLanded() {
		w.log.Warn("demo: the next run waits until the ended run's rule is switched off", "run_id", a.runID, "rule_id", ruleID)
	}
	select {
	case <-a.sw.confirmed:
	case <-parent.Done():
	}
}

// pickUp makes runID, a run of scenario sc, the current run and starts its
// watchdog.
func (w *Worker) pickUp(parent context.Context, runID string, sc Scenario) *active {
	a := &active{runID: runID, sc: sc, sw: newRuleSwitch(runID, w.d.Rules, w.log, w.d.Clock), watchdog: make(chan struct{}),
		phase: PhaseNames[0]}
	for i := range a.last {
		a.last[i] = PhasePending
	}
	a.ctx, a.cancel = context.WithCancel(parent)
	a.startCtx, a.cancelStart = context.WithCancel(context.WithoutCancel(parent))
	w.mu.Lock()
	w.cur = a
	w.mu.Unlock()
	a.timer = time.AfterFunc(w.cfg.RunCap, func() { w.fireWatchdog(a) })
	w.log.Info("demo: run picked up", "run_id", runID, "scenario", sc.ID)
	return a
}

// fireWatchdog runs on the timer's goroutine at the cap, without waiting for
// the worker. In this order: it marks the run timed out (so a call cut short
// below is never read as a shutdown); closes the rule switch, which refuses
// any later switch-on and cuts one in flight short; cancels the run's calls
// and its start; tells the worker; and switches the rules off, after any
// switch-on still in flight.
func (w *Worker) fireWatchdog(a *active) {
	if a.ending.Load() {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("demo: the watchdog panicked", "run_id", a.runID, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	a.timedOut.Store(true)
	a.sw.shut(offTimedOut)
	a.cancel()
	a.cancelStart()
	close(a.watchdog)
	w.log.Warn("demo: the run reached its cap; switching its rule off", "run_id", a.runID, "cap", w.cfg.RunCap)
	if err := a.sw.off(context.Background(), offTimedOut); err != nil {
		w.logOffFailure(a, err, "the watchdog could not switch the run's rule off; the ending tries again")
	}
}

// interrupted reports whether the run was cut short: by its watchdog
// (timed_out) or by the worker stopping (interrupted).
func (a *active) interrupted() (outcome, bool) {
	if a.timedOut.Load() {
		return outcome{status: RunCancelled, reason: ReasonTimedOut, cancel: true}, true
	}
	if a.ctx.Err() != nil {
		return outcome{status: RunCancelled, reason: ReasonInterrupted, shutdown: true}, true
	}
	return outcome{}, false
}

// guardedDrive is drive with a panic turned into a failed run.
func (w *Worker) guardedDrive(a *active) (out outcome) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("demo: the run's work panicked; ending it", "run_id", a.runID,
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			out = outcome{status: RunFailed, reason: ReasonInternalError, cancel: true}
		}
	}()
	return w.drive(a)
}

// drive takes the run from preflight to the moment its ending is known.
func (w *Worker) drive(a *active) outcome {
	pctx, cancel := context.WithTimeout(a.ctx, w.cfg.preflightTimeout())
	reason, ok := preflight(pctx, w.d, a.sc, "", w.cfg.RunCap)
	cancel()
	if !ok {
		if o, stop := a.interrupted(); stop {
			return o
		}
		w.log.Info("demo: run refused by preflight", "run_id", a.runID, "reason", reason.String())
		return outcome{status: RunFailed, reason: reason}
	}
	started := w.d.Clock.Now()
	w.q.update(a.runID, func(r *Run) { r.Status, r.StartedAt = RunRunning, &started })

	built, err := BuildRun(a.ctx, w.d, a.runID, a.sc, w.cfg.RunCap)
	if err != nil {
		if o, stop := a.interrupted(); stop {
			return o
		}
		w.log.Warn("demo: run not built", "run_id", a.runID, "error", err,
			"cleanup_incomplete", errors.Is(err, ErrCleanupIncomplete))
		return buildOutcome(err)
	}
	w.mu.Lock()
	a.expID = built.ExperimentID
	w.mu.Unlock()
	a.phaseIDs = built.PhaseIDs
	a.sw.attach(a.ctx, built.RuleIDs[:]...)

	octx, cancel := context.WithTimeout(a.ctx, w.cfg.switchOnTimeout())
	err = a.sw.on(octx)
	cancel()
	if err != nil {
		if o, stop := a.interrupted(); stop {
			return o
		}
		// Unattached, closed or failed: nothing started, and the switch-on
		// itself switched off every rule again (a failed one of those
		// switch-offs is logged here, and the ending tries it again). The
		// build's rows stay for the start-up sweep.
		var f *offFailure
		if errors.As(err, &f) {
			w.logOffFailure(a, f, "switching the run's rules off after a failed switch-on failed; the ending tries again")
		}
		w.log.Error("demo: the run's rules could not be switched on; not starting", "run_id", a.runID,
			"experiment_id", built.ExperimentID, "rule_ids", built.RuleIDs[:], "error", err)
		return outcome{status: RunFailed, reason: ReasonInternalError}
	}
	if o, stop := a.interrupted(); stop {
		return o
	}
	w.launchStart(a)
	return w.watch(a)
}

// buildOutcome maps a BuildRun error to the run's ending.
func buildOutcome(err error) outcome {
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return outcome{status: RunCancelled, reason: ReasonInterrupted}
	case errors.Is(err, ErrNoDataset):
		return outcome{status: RunFailed, reason: ReasonLoadGeneratorUnavailable}
	}
	return outcome{status: RunFailed, reason: ReasonInternalError}
}

// startReason maps a StartExperiment error to a public reason.
func startReason(err error) Reason {
	var overlap *orchestrator.ErrServiceOverlap
	if errors.As(err, &overlap) {
		return ReasonLabBusy
	}
	return ReasonInternalError
}

// launchStart runs StartExperiment in a goroutine of the worker's: it can
// block for a minute or more.
func (w *Worker) launchStart(a *active) {
	a.started = true
	a.startDone = make(chan struct{})
	w.bg.Add(1)
	go func() {
		defer w.bg.Done()
		defer close(a.startDone)
		defer a.cancelStart()
		defer func() {
			if r := recover(); r != nil {
				w.log.Error("demo: starting the run's experiment panicked", "run_id", a.runID,
					"experiment_id", a.expID, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				a.startErr = errStartPanicked
			}
		}()
		if w.d.Orch == nil {
			a.startErr = errNoOrchestrator
			return
		}
		a.startErr = w.d.Orch.StartExperiment(a.startCtx, a.expID)
	}()
	w.log.Info("demo: starting the run's experiment", "run_id", a.runID, "experiment_id", a.expID)
}

// watch follows the started run until its ending is known.
func (w *Worker) watch(a *active) outcome {
	tick := time.NewTicker(w.cfg.StatusPoll)
	defer tick.Stop()
	startDone := a.startDone
	for {
		select {
		case <-a.watchdog:
			return outcome{status: RunCancelled, reason: ReasonTimedOut, cancel: true}
		case <-a.ctx.Done():
			if o, stop := a.interrupted(); stop {
				return o
			}
		case <-startDone:
			startDone = nil
			if err := a.startErr; err != nil {
				if o, stop := a.interrupted(); stop {
					return o
				}
				w.log.Error("demo: the run's experiment did not start", "run_id", a.runID,
					"experiment_id", a.expID, "error", err)
				return outcome{status: RunFailed, reason: startReason(err), cancel: true}
			}
		case <-tick.C:
			if o, ended := w.poll(a); ended {
				return o
			}
		}
	}
}

// poll reads the experiment once. A terminal or unknown status ends the run at
// once, before anything else is read or published; otherwise the phases are
// read and their changes published.
func (w *Worker) poll(a *active) (outcome, bool) {
	rctx, cancel := context.WithTimeout(a.ctx, w.cfg.readTimeout())
	exp, err := w.d.Experiments.Get(rctx, a.expID)
	cancel()
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			w.log.Error("demo: the run's experiment is gone", "run_id", a.runID, "experiment_id", a.expID)
			return outcome{status: RunFailed, reason: ReasonInternalError, cancel: true}, true
		}
		if a.ctx.Err() == nil {
			w.log.Warn("demo: reading the run's experiment failed", "run_id", a.runID, "experiment_id", a.expID, "error", err)
		}
		return outcome{}, false
	}
	switch exp.Status {
	case "planned", "running":
	case "completed":
		return outcome{status: RunCompleted}, true
	case "failed":
		return outcome{status: RunFailed, reason: ReasonPhaseFailed}, true
	case "cancelled":
		return outcome{status: RunCancelled, reason: ReasonInterrupted}, true
	default:
		w.log.Error("demo: the run's experiment has a status the demo does not know", "run_id", a.runID,
			"experiment_id", a.expID, "status", exp.Status)
		return outcome{status: RunFailed, reason: ReasonInternalError, cancel: true}, true
	}
	unknown, err := w.refresh(a.ctx, a)
	if err != nil {
		if a.ctx.Err() == nil {
			w.log.Warn("demo: reading the run's phases failed", "run_id", a.runID, "experiment_id", a.expID, "error", err)
		}
		return outcome{}, false
	}
	if unknown {
		return outcome{status: RunFailed, reason: ReasonInternalError, cancel: true}, true
	}
	return outcome{}, false
}

// refresh reads the run's phases, the proof figures of those that completed
// since the last read (prove: at most QueryTimeout) and the results of
// completed ones, updates the run's view and Current, then publishes each
// phase whose public status changed, in phase order. A phase whose core
// status the contract does not know keeps its last public status; refresh
// then reports unknown.
func (w *Worker) refresh(ctx context.Context, a *active) (unknown bool, err error) {
	rctx, cancel := context.WithTimeout(ctx, w.cfg.readTimeout())
	phases, err := w.d.Experiments.ListPhasesForExperiment(rctx, a.expID)
	cancel()
	if err != nil {
		return false, err
	}
	now := w.d.Clock.Now()
	var (
		seen    [3]*model.ExperimentPhase
		changed []PhaseEvent
	)
	for _, p := range phases {
		if p == nil {
			continue
		}
		i := slices.Index(a.phaseIDs[:], p.ID)
		if i < 0 {
			continue
		}
		st, ok := mapPhaseStatus(p.Status)
		if !ok {
			unknown = true
			w.log.Error("demo: a phase of the run has a status the demo does not know", "run_id", a.runID,
				"phase_id", p.ID, "status", p.Status)
			continue
		}
		seen[i] = p
		if st != a.last[i] {
			a.last[i] = st
			changed = append(changed, PhaseEvent{RunID: a.runID, Phase: PhaseNames[i], PhaseIndex: i, Status: st, At: phaseAt(p, st, now)})
		}
	}
	// First, so each figure is read as close to its phase's end as can be.
	proofs := w.prove(ctx, a, seen)
	var results [3]*Result
	for i := range a.last {
		if a.last[i] != PhaseCompleted || a.haveResult[i] || seen[i] == nil {
			continue
		}
		res, err := w.readResult(ctx, seen[i].ID, a.sc.Workload.WorkflowID)
		if err != nil {
			w.log.Warn("demo: reading a phase's results failed", "run_id", a.runID, "phase_id", seen[i].ID, "error", err)
			continue
		}
		if res != nil {
			results[i], a.haveResult[i] = res, true
		}
	}
	w.q.update(a.runID, func(r *Run) {
		for i, p := range seen {
			if p == nil {
				continue
			}
			pv := &r.Phases[i]
			pv.Status, pv.StartedAt, pv.EndedAt = a.last[i], clonePtr(p.StartedAt), clonePtr(p.CompletedAt)
			if results[i] != nil {
				pv.Result = results[i]
			}
			if pr := proofs[i]; pr != nil {
				m := pr.measure
				pv.Measure, pv.FaultObserved = &m, pr.observed
			}
		}
	})
	phase, running := currentPhase(a.last)
	w.mu.Lock()
	a.phase, a.phaseRunning = phase, running
	w.mu.Unlock()
	for _, ev := range changed {
		w.h.Publish(a.runID, Event{Type: EventPhase, Data: ev})
	}
	return unknown, nil
}

// readResult returns a phase's load result, or nil while it has none: the row
// of workflowID (the run's scenario's workflow), else the first row.
func (w *Worker) readResult(ctx context.Context, phaseID, workflowID string) (*Result, error) {
	rctx, cancel := context.WithTimeout(ctx, w.cfg.readTimeout())
	rows, err := w.d.Experiments.ListWorkflowResultsForPhase(rctx, phaseID)
	cancel()
	if err != nil {
		return nil, err
	}
	var row *model.PhaseWorkflowResult
	for _, r := range rows {
		if r != nil && (row == nil || r.WorkflowID == workflowID) {
			row = r
			if r.WorkflowID == workflowID {
				break
			}
		}
	}
	if row == nil {
		return nil, nil
	}
	return &Result{
		Requests:   row.RequestCount,
		HTTPErrors: row.ErrorCount,
		P50Ms:      float64(row.LatencyP50Us) / 1000,
		P95Ms:      float64(row.LatencyP95Us) / 1000,
		P99Ms:      float64(row.LatencyP99Us) / 1000,
	}, nil
}

// mapPhaseStatus maps a core phase status to the public one: paused and
// draining read as running. false for a status the contract does not know.
func mapPhaseStatus(core string) (PhaseStatus, bool) {
	switch core {
	case "pending":
		return PhasePending, true
	case "running", "paused", "draining":
		return PhaseRunning, true
	case "completed":
		return PhaseCompleted, true
	case "failed":
		return PhaseFailed, true
	case "skipped":
		return PhaseSkipped, true
	}
	return "", false
}

// phaseAt is when a phase reached st: its start or end time when the store has
// it, else now.
func phaseAt(p *model.ExperimentPhase, st PhaseStatus, now time.Time) time.Time {
	switch {
	case st == PhaseRunning && p.StartedAt != nil:
		return *p.StartedAt
	case st != PhaseRunning && st != PhasePending && p.CompletedAt != nil:
		return *p.CompletedAt
	}
	return now
}

// currentPhase is the running phase, else the next pending one, else the last.
func currentPhase(last [3]PhaseStatus) (string, bool) {
	for i, st := range last {
		if st == PhaseRunning {
			return PhaseNames[i], true
		}
	}
	for i, st := range last {
		if st == PhasePending {
			return PhaseNames[i], false
		}
	}
	return PhaseNames[len(PhaseNames)-1], false
}

// ---- endings ----

// finish ends the run. The rule goes off first, whatever else happens; if that
// switch-off fails, it is retried in the background (see retryOff) while the
// ending goes on, and runOne holds the next run until it lands.
func (w *Worker) finish(parent context.Context, a *active, out outcome) {
	a.ending.Store(true)
	why := out.offReason()
	if err := w.tryOff(context.WithoutCancel(parent), a, why); err != nil {
		w.logOffFailure(a, err, "switching the run's rule off failed; retrying until it succeeds")
		w.retryOff(parent, a, why)
	}
	if out.cancel && a.started {
		a.cancelStart()
		w.teardown(parent, a)
	}
	if a.expID != "" && !out.shutdown {
		w.safely(a.runID, "read the phases", func() {
			if _, err := w.refresh(parent, a); err != nil {
				w.log.Warn("demo: the last read of the run's phases failed", "run_id", a.runID, "error", err)
			}
		})
	}
	w.safely(a.runID, "end the run", func() { w.end(a.runID, out.status, out.reason) })
	if !out.shutdown {
		w.callOnRunEnded(parent)
	}
}

// teardown cancels the run's experiment once its start has returned (a cancel
// issued first would wait behind it anyway, and could precede a start that
// then succeeds). It waits at most teardownTimeout, and no longer once the
// worker is stopping; past that the run ends and the cancel still follows,
// from a goroutine of the worker's, when the start returns. The cancel is
// issued once.
func (w *Worker) teardown(parent context.Context, a *active) {
	done := make(chan struct{})
	w.bg.Add(1)
	go func() {
		defer w.bg.Done()
		defer close(done)
		<-a.startDone
		w.cancelExperiment(parent, a)
	}()
	t := time.NewTimer(w.cfg.teardownTimeout())
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		w.log.Warn("demo: the run's start or cancel has not returned; ending the run now, the cancel follows",
			"run_id", a.runID, "experiment_id", a.expID)
	case <-parent.Done():
		w.log.Warn("demo: stopping while the run's start or cancel is in flight; the cancel follows",
			"run_id", a.runID, "experiment_id", a.expID)
	}
}

// cancelExperiment cancels the run's experiment; one that already ended is
// fine. Its context is detached from the worker's, with its own deadline.
func (w *Worker) cancelExperiment(parent context.Context, a *active) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("demo: cancelling the run's experiment panicked", "run_id", a.runID,
				"experiment_id", a.expID, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	if w.d.Orch == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), w.cfg.teardownTimeout())
	defer cancel()
	err := w.d.Orch.CancelExperiment(ctx, a.expID)
	var ended *orchestrator.ErrInvalidState
	switch {
	case err == nil:
		w.log.Info("demo: the run's experiment was cancelled", "run_id", a.runID, "experiment_id", a.expID)
	case errors.As(err, &ended):
		w.log.Info("demo: the run's experiment had already ended", "run_id", a.runID,
			"experiment_id", a.expID, "status", ended.Current)
	default:
		w.log.Error("demo: cancelling the run's experiment failed; its rule is off", "run_id", a.runID,
			"experiment_id", a.expID, "error", err)
	}
}

// tryOff is one switch-off of the run's rules, for the reason why, with a
// panic turned into an error.
func (w *Worker) tryOff(ctx context.Context, a *active, why string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("demo: switching the run's rule off panicked", "run_id", a.runID,
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("demo: switching the rule off panicked: %v", r)
		}
	}()
	return a.sw.off(ctx, why)
}

// offWarnEvery is how often a switch-off that keeps failing is logged: its
// first failure, then at most one line this often, then its success.
const offWarnEvery = time.Minute

// logOffFailure logs, at Warn, a switch-off of a's rules that failed and is
// tried again (by the ending, or by retryOff), when the switch marked that
// failure due a line (offFailure: the first, then at most one every
// offWarnEvery): one line per rule whose switch-off has not landed, with its
// rule_id and the attempt number. Each rule's success is the switch's own
// Info line, with the attempt that landed. A failure that is not a write's (a
// panic) is always logged, once, with the run's rule ids.
func (w *Worker) logOffFailure(a *active, err error, msg string, attrs ...any) {
	var f *offFailure
	if !errors.As(err, &f) {
		head := []any{"run_id", a.runID, "rule_ids", a.sw.attached()}
		w.log.Warn("demo: "+msg, append(append(head, "error", err), attrs...)...)
		return
	}
	if !f.warn {
		return
	}
	for _, ruleID := range f.rules {
		head := []any{"run_id", a.runID, "rule_id", ruleID, "attempt", f.attempt}
		w.log.Warn("demo: "+msg, append(append(head, "error", err), attrs...)...)
	}
}

// retryOff marks the run's switch-off pending and retries it from a goroutine
// of the worker's: after w.offRetry.first, then doubling up to w.offRetry.max,
// until one succeeds (then pending clears and the worker picks up the next
// run) or the worker stops (the rule's next chance is Shutdown's own switch-off,
// then the next start's sweep). A switch-off that lands from elsewhere
// (Shutdown's) ends it too: every switch-off goes through the run's switch and
// is idempotent. Failures are logged through logOffFailure.
func (w *Worker) retryOff(parent context.Context, a *active, why string) {
	w.mu.Lock()
	w.pending = a
	w.mu.Unlock()
	w.bg.Add(1)
	go func() {
		defer w.bg.Done()
		delay := w.offRetry.first
		for {
			t := time.NewTimer(delay)
			select {
			case <-a.sw.confirmed:
				t.Stop()
				w.clearPending(a)
				return
			case <-parent.Done():
				t.Stop()
				w.log.Error("demo: stopping while the ended run's rule could not be switched off; the next start's sweep switches it off",
					"run_id", a.runID, "rule_ids", a.sw.notLanded(), "attempts", a.sw.failed())
				return
			case <-t.C:
			}
			if err := w.tryOff(context.WithoutCancel(parent), a, why); err != nil {
				delay = w.offRetry.next(delay)
				w.logOffFailure(a, err, "switching the run's rule off failed again", "next_attempt_in", delay)
				continue
			}
			w.clearPending(a)
			return
		}
	}()
}

// clearPending forgets a's pending switch-off.
func (w *Worker) clearPending(a *active) {
	w.mu.Lock()
	if w.pending == a {
		w.pending = nil
	}
	w.mu.Unlock()
}

// end ends the run's public life: its final view is recorded and the run taken
// out of the line (its slot freed) in one step under w.mu, so no reader ever
// sees the run ended and still in line; then its end event is published, and
// the new positions of the runs behind it. Called once per run. The removal is
// deferred, so it happens even if anything before it panics: the line never
// stays blocked by a run whose ending failed.
func (w *Worker) end(runID string, status RunStatus, reason Reason) {
	final, ok := w.q.get(runID)
	if !ok {
		w.log.Error("demo: ending a run that is not in the line", "run_id", runID)
		return
	}
	final.Status, final.Reason, final.Position = status, reason, nil
	defer w.retire(runID, &final)
	now := w.d.Clock.Now()
	final.EndedAt = &now
}

// retire records final, takes the run out of the line, and publishes its end
// and the runs' new positions. The run enters the run index as it leaves the
// line, so View finds it once it has left the ended runs in memory too.
func (w *Worker) retire(runID string, final *Run) {
	w.mu.Lock()
	moved, removed := w.q.remove(runID)
	if removed {
		w.finished = append(w.finished, cloneRun(*final))
		if extra := len(w.finished) - w.cfg.Retain; extra > 0 {
			w.finished = slices.Clone(w.finished[extra:])
		}
		w.runs.ended(runID)
		w.ended.Add(1)
	}
	if w.cur != nil && w.cur.runID == runID {
		w.cur = nil
	}
	w.mu.Unlock()
	if !removed {
		w.log.Error("demo: ending a run that already left the line", "run_id", runID)
		return
	}
	w.h.Publish(runID, Event{Type: EventEnd, Data: cloneRun(*final)})
	for _, m := range moved {
		w.h.Publish(m.RunID, Event{Type: EventQueue, Data: m})
	}
	w.log.Info("demo: run ended", "run_id", runID, "status", string(final.Status), "reason", final.Reason.String())
}

// endedCount is how many runs have ended in this process. It counts each run
// as its ended view is recorded, before its end event is published, so a
// reader that sees the new count finds the run in View and Recent.
func (w *Worker) endedCount() uint64 { return w.ended.Load() }

func (w *Worker) callOnRunEnded(parent context.Context) {
	if w.OnRunEnded == nil {
		return
	}
	w.safely("", "OnRunEnded", func() {
		ctx, cancel := context.WithTimeout(parent, w.cfg.teardownTimeout())
		defer cancel()
		w.OnRunEnded(ctx)
	})
}

// safely runs f, logging and swallowing a panic.
func (w *Worker) safely(runID, step string, f func()) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("demo: recovered a panic", "run_id", runID, "step", step,
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	f()
}

// ---- views ----

// View returns a run's public view: from the line, else from the runs that
// ended in this process, else, for an id of the demo's shape, from the
// database with the restart mapping (completed -> no reason, failed ->
// phase_failed, anything else -> cancelled/interrupted). It is for the routes
// only: its database lookups are bounded and indexed (see lookups.go).
//
// An id of the demo's shape that is not in memory is ErrRunNotFound without a
// query while the run index is complete and does not hold it; an ended run's
// view read before is answered from memory; otherwise the database is read if
// one of the maxDBLookups slots is free, and ErrRunUnavailable is the answer
// at once if none is.
//
// The error is ErrRunNotFound for an id that names no run, or
// ErrRunUnavailable when the run could not be read now; a database failure's
// cause is logged, never returned. A run that has ended is never shown in
// line: the worker records its ended view and takes it out of the line in one
// step, and View looks in the line first.
func (w *Worker) View(ctx context.Context, runID string) (Run, error) {
	if r, ok := w.q.get(runID); ok {
		return r, nil
	}
	w.mu.Lock()
	for i := len(w.finished) - 1; i >= 0; i-- {
		if w.finished[i].RunID == runID {
			r := cloneRun(w.finished[i])
			w.mu.Unlock()
			return r, nil
		}
	}
	w.mu.Unlock()
	if !validRunID(runID) {
		return Run{}, ErrRunNotFound
	}
	r, kept, absent := w.runs.lookup(runID)
	switch {
	case absent:
		return Run{}, ErrRunNotFound
	case kept:
		return r, nil
	}
	select {
	case w.lookups <- struct{}{}:
		defer func() { <-w.lookups }()
	default:
		w.log.Debug("demo: every run lookup slot is taken; answering unavailable", "run_id", runID)
		return Run{}, ErrRunUnavailable
	}
	return w.viewFromDB(ctx, runID)
}

// viewFromDB reads run runID from the database, holding one of View's slots.
// An id with no experiment leaves the run index; a settled view is kept.
func (w *Worker) viewFromDB(ctx context.Context, runID string) (Run, error) {
	mark := w.runs.mark()
	exp, err := w.findExperiment(ctx, runID)
	if errors.Is(err, ErrRunNotFound) {
		w.runs.drop(runID)
		return Run{}, ErrRunNotFound
	}
	if err != nil {
		w.log.Warn("demo: reading a run from the database failed", "run_id", runID, "error", err)
		return Run{}, ErrRunUnavailable
	}
	r, err := w.runFromDB(ctx, exp)
	if err != nil {
		w.log.Warn("demo: reading a run from the database failed", "run_id", runID, "error", err)
		return Run{}, ErrRunUnavailable
	}
	if settled(exp, r) {
		w.runs.keep(r, mark)
	}
	return r, nil
}

// Recent returns up to n ended runs, newest ended first: those that ended in
// this process, then, to fill up, ended demo experiments from the database
// (restart mapping, as View). complete is false when the database could not be
// read, or one run in it could not: runs then holds what was found (at least
// what is in memory), and the cause is logged.
func (w *Worker) Recent(ctx context.Context, n int) (runs []Run, complete bool) {
	if n <= 0 {
		return nil, true
	}
	seen := map[string]bool{}
	for _, r := range w.q.Snapshot() {
		seen[r.RunID] = true
	}
	w.mu.Lock()
	out := make([]Run, 0, n)
	for i := len(w.finished) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, cloneRun(w.finished[i]))
	}
	for _, r := range w.finished {
		seen[r.RunID] = true
	}
	w.mu.Unlock()
	if len(out) >= n {
		return out, true
	}
	rctx, cancel := context.WithTimeout(ctx, w.cfg.readTimeout())
	exps, _, err := w.d.Experiments.List(rctx, store.ExperimentFilter{CreatedBy: CreatedBy}, store.Page{Limit: listPage})
	cancel()
	if err != nil {
		w.log.Warn("demo: listing past runs failed", "error", err)
		return out, false
	}
	complete = true
	var ended []*model.Experiment
	for _, e := range exps {
		runID, ok := strings.CutPrefix(e.Name, RunNamePrefix)
		if !ok || !validRunID(runID) || seen[runID] {
			continue
		}
		switch e.Status {
		case "completed", "failed", "cancelled":
			ended = append(ended, e)
		}
	}
	endedAt := func(e *model.Experiment) time.Time {
		if e.CompletedAt != nil {
			return *e.CompletedAt
		}
		return e.CreatedAt
	}
	slices.SortStableFunc(ended, func(a, b *model.Experiment) int { return endedAt(b).Compare(endedAt(a)) })
	for _, e := range ended {
		if len(out) >= n {
			break
		}
		r, err := w.runFromDB(ctx, e)
		if err != nil {
			w.log.Warn("demo: reading a past run failed", "experiment_id", e.ID, "error", err)
			complete = false
			continue
		}
		out = append(out, r)
	}
	return out, complete
}

// findExperiment finds run runID's experiment among the demo's.
func (w *Worker) findExperiment(ctx context.Context, runID string) (*model.Experiment, error) {
	name := RunNamePrefix + runID
	for page := 0; page < 10; page++ {
		rctx, cancel := context.WithTimeout(ctx, w.cfg.readTimeout())
		exps, total, err := w.d.Experiments.List(rctx, store.ExperimentFilter{CreatedBy: CreatedBy},
			store.Page{Limit: listPage, Offset: page * listPage})
		cancel()
		if err != nil {
			return nil, err
		}
		for _, e := range exps {
			if e.Name == name {
				return e, nil
			}
		}
		if len(exps) == 0 || (page+1)*listPage >= total {
			break
		}
	}
	return nil, ErrRunNotFound
}

// runFromDB builds a run's view from its experiment, phases and results: its
// scenario and phases as storedView tells them, then each phase's status,
// times and result. No proof figure is stored, so a completed fault phase
// shows its measure's metric with both figures null, and fault_observed null
// (storedMeasure); Prometheus is never asked.
func (w *Worker) runFromDB(ctx context.Context, exp *model.Experiment) (Run, error) {
	rctx, cancel := context.WithTimeout(ctx, w.cfg.readTimeout())
	phases, err := w.d.Experiments.ListPhasesForExperiment(rctx, exp.ID)
	cancel()
	if err != nil {
		return Run{}, err
	}
	r, workflowID, err := w.storedView(ctx, strings.TrimPrefix(exp.Name, RunNamePrefix), exp, phases)
	if err != nil {
		return Run{}, err
	}
	r.StartedAt, r.EndedAt = clonePtr(exp.StartedAt), clonePtr(exp.CompletedAt)
	switch exp.Status {
	case "completed":
		r.Status = RunCompleted
	case "failed":
		r.Status, r.Reason = RunFailed, ReasonPhaseFailed
	default: // cancelled, or left planned or running by an earlier process
		r.Status, r.Reason = RunCancelled, ReasonInterrupted
	}
	for _, p := range phases {
		if p == nil || p.Position < 0 || p.Position >= len(r.Phases) {
			continue
		}
		pv := &r.Phases[p.Position]
		if st, ok := mapPhaseStatus(p.Status); ok {
			pv.Status = st
		}
		pv.StartedAt, pv.EndedAt = clonePtr(p.StartedAt), clonePtr(p.CompletedAt)
		if p.Status == "completed" {
			res, err := w.readResult(ctx, p.ID, workflowID)
			if err != nil {
				return Run{}, err
			}
			pv.Result = res
			pv.Measure = storedMeasure(pv.Fault)
		}
	}
	return r, nil
}

// ---- the rule switch ----

// ruleSwitch is one run's rule switch; every switch of the run's rules (its
// memory rule and its CPU rule) goes through it.
//
//   - on enables the rules, one after the other (SetRuleEnabled, so each only
//     while a phase links it). It is refused once the switch is closed. If it
//     cannot enable a rule, it closes the switch and switches every rule off
//     at once, as off does, before it returns: a rule it enabled is never
//     left on for the run's ending to find.
//   - off closes the switch, cuts a switch-on in flight short, and switches
//     off every rule whose switch-off has not landed yet, all at once: one
//     write each, each in its own goroutine on its own 10 s. It may be called
//     any number of times, from any goroutine.
//
// Writes never overlap a switch-on: each on and each off holds write across
// all its writes. An off issued while an on is in flight therefore writes
// after it, and an on that gets write after an off sees the switch closed and
// writes nothing. So when off returns, each rule's last write was a
// switch-off, and no switch-on follows it.
//
// A rule's switch-off has landed once one of its switch-off writes succeeded
// (or found the rule gone). Every such write follows the switch's closing, so
// nothing of the demo's switches that rule on again: no later off writes it
// again, and a retry writes only the rules still pending. confirmed is closed
// once every rule's switch-off has landed, by the off that lands the last
// one. Until then the switch is not confirmed, whichever rules are off.
//
// Its log lines mark each rule in the log, with its rule_id: "demo: rule
// switched on" at Info once its switch-on is written; "demo: rule switched
// off" at Info, with its reason, once, for the write that landed its
// switch-off; a switch-off that found the rule gone is a Debug line. A failed
// switch-off is its caller's to log (Worker.logOffFailure), as the switch
// marks it (offFailure).
type ruleSwitch struct {
	runID string
	rules Rules
	log   *slog.Logger

	write     sync.Mutex // held across each on's and each off's store writes
	confirmed chan struct{}
	confirm   sync.Once

	clock Clock // times the failed switch-offs' Warn lines

	mu        sync.Mutex // guards the fields below; never held across a write
	ruleIDs   []string
	landed    map[string]bool // rules whose switch-off has landed
	closed    bool
	closedFor string // why the switch was first closed: an off reason
	cancelOn  context.CancelFunc
	failures  int       // switch-offs that left a rule not landed
	warnedAt  time.Time // when the last failure due a Warn line failed
}

func newRuleSwitch(runID string, r Rules, log *slog.Logger, clock Clock) *ruleSwitch {
	return &ruleSwitch{runID: runID, rules: r, log: log, clock: clock, confirmed: make(chan struct{}),
		landed: map[string]bool{}}
}

// offFailure is a switch-off of a switch that left rules not landed: those
// rules, its attempt number (1 for the switch's first failed switch-off) and
// whether it is due a Warn line (the first failure, then at most one every
// offWarnEvery by the switch's clock). Both are decided with the count, under
// the switch's lock, so of two writers failing at once exactly one holds
// attempt 1 and the first line. It wraps the store's errors, and logs as
// them.
type offFailure struct {
	rules   []string
	attempt int
	warn    bool
	err     error
}

func (e *offFailure) Error() string        { return e.err.Error() }
func (e *offFailure) Unwrap() error        { return e.err }
func (e *offFailure) LogValue() slog.Value { return slog.AnyValue(e.err) }

// attached is the switch's rule ids, none before attach.
func (s *ruleSwitch) attached() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ruleIDs)
}

// notLanded is the switch's rules whose switch-off has not landed yet.
func (s *ruleSwitch) notLanded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, id := range s.ruleIDs {
		if !s.landed[id] {
			out = append(out, id)
		}
	}
	return out
}

// isOff reports whether every rule's switch-off has landed.
func (s *ruleSwitch) isOff() bool {
	select {
	case <-s.confirmed:
		return true
	default:
		return false
	}
}

// isClosed reports whether the switch was closed: no switch-on is written any
// more.
func (s *ruleSwitch) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// failed is how many switch-offs have left a rule not landed.
func (s *ruleSwitch) failed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}

// failure counts a switch-off that left the rules pending not landed, and
// marks whether it is due a Warn line (see offFailure).
func (s *ruleSwitch) failure(pending []string, err error) error {
	now := s.clock.Now() // read outside the lock: a clock that panics must not leave it held
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures++
	f := &offFailure{rules: pending, attempt: s.failures, err: err}
	switch {
	case f.attempt == 1 || now.Sub(s.warnedAt) >= offWarnEvery:
		f.warn, s.warnedAt = true, now
	case now.Before(s.warnedAt): // the clock went back: count the interval from now
		s.warnedAt = now
	}
	return f
}

// attach gives the switch its rules. If the switch was already closed, they
// are switched off at once.
func (s *ruleSwitch) attach(ctx context.Context, ruleIDs ...string) {
	s.mu.Lock()
	s.ruleIDs = slices.Clone(ruleIDs)
	closed, why := s.closed, s.closedFor
	s.mu.Unlock()
	if closed {
		if err := s.off(ctx, why); err != nil {
			s.log.Error("demo: switching off rules attached after their run ended failed", "run_id", s.runID,
				"rule_ids", ruleIDs, "error", err)
		}
	}
}

// on switches the rules on, one after the other, unless the switch is closed
// or has no rule. If it cannot switch one on, it closes the switch (for
// offFailed) and, still holding write, switches every rule off at once (see
// off); it returns the switch-on's error, joined with that switch-off's
// offFailure when one of those writes failed too (its caller logs it; the
// run's ending tries again). A switch-on cut short because the switch was
// closed meanwhile leaves the rules to the off that closed it, which writes
// as soon as on lets go of write.
func (s *ruleSwitch) on(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errSwitchClosed
	}
	if len(s.ruleIDs) == 0 {
		s.mu.Unlock()
		return errNoRule
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.cancelOn = cancel
	ruleIDs := slices.Clone(s.ruleIDs)
	s.mu.Unlock()

	s.write.Lock()
	defer s.write.Unlock()
	for _, ruleID := range ruleIDs {
		if s.isClosed() {
			return errSwitchClosed
		}
		if err := SetRuleEnabled(ctx, s.rules, ruleID, true); err != nil {
			if s.isClosed() {
				return err
			}
			s.shut(offFailed)
			return errors.Join(err, s.offLocked(ctx, offFailed))
		}
		s.log.Info("demo: rule switched on", "run_id", s.runID, "rule_id", ruleID)
	}
	return nil
}

// shut closes the switch for the reason why (the first reason stays) and cuts
// a switch-on in flight short, without writing or waiting; it returns the
// rules' ids (none before attach).
func (s *ruleSwitch) shut(why string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed, s.closedFor = true, why
	}
	if s.cancelOn != nil {
		s.cancelOn()
	}
	return slices.Clone(s.ruleIDs)
}

// off shuts the switch and switches off, for the reason why and after any
// switch-on still in flight, every rule whose switch-off has not landed, all
// at once (offLocked). Once every rule's switch-off has landed the switch is
// confirmed off and off returns nil; otherwise it returns an offFailure
// naming the rules whose switch-off has still not landed.
func (s *ruleSwitch) off(ctx context.Context, why string) error {
	if len(s.shut(why)) == 0 {
		return nil
	}
	s.write.Lock()
	defer s.write.Unlock()
	return s.offLocked(ctx, why)
}

// offLocked is off's writes, with write held and the switch closed: one
// unconditional switch-off of each rule not landed, each in its own goroutine
// on a context detached from ctx with its own 10 s (switchOffRule), all
// started at once, so k rules take as long as the slowest write, not k
// writes. A panic in a write is that rule's failure. It returns when every
// write has returned. The write that lands a rule's switch-off is logged at
// Info (Debug for a rule found gone), in the rules' order.
func (s *ruleSwitch) offLocked(ctx context.Context, why string) error {
	pending := s.notLanded()
	if len(pending) > 0 {
		type result struct {
			gone bool
			err  error
		}
		results := make([]result, len(pending))
		var wg sync.WaitGroup
		for i, ruleID := range pending {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						s.log.Error("demo: switching a rule off panicked", "run_id", s.runID, "rule_id", ruleID,
							"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
						results[i].err = &StepError{Step: "disable_rule", Class: ErrRuleState,
							Cause: fmt.Errorf("demo: switching the rule off panicked: %v", r)}
					}
				}()
				// Not SetRuleEnabled: every caller of off logs its failure.
				gone, err := switchOffRule(ctx, s.rules, ruleID, switchOffTimeout)
				results[i] = result{gone: gone, err: err}
			}()
		}
		wg.Wait()
		attempt := s.failed() + 1
		var errs []error
		for i, ruleID := range pending {
			if err := results[i].err; err != nil {
				errs = append(errs, err)
				continue
			}
			// Logged before the switch is confirmed, so whoever waits on
			// confirmed finds the line written.
			if results[i].gone {
				s.log.Debug("demo: rule already gone; nothing to switch off", "run_id", s.runID, "rule_id", ruleID, "reason", why)
			} else {
				attrs := []any{"run_id", s.runID, "rule_id", ruleID, "reason", why}
				if attempt > 1 {
					attrs = append(attrs, "attempt", attempt)
				}
				s.log.Info("demo: rule switched off", attrs...)
			}
			// A landed switch-off is final: no later off writes this rule
			// again (each writes only the rules not landed), so none would
			// undo a switch-on that commits after this write, such as an
			// EnableIfAttached whose commit came late after its call had
			// failed. Such a rule is served only while its own phase runs
			// under a running experiment; what switches it off is retention's
			// unconditional switch-off of every rule of the run before it
			// deletes the run's experiment, or the next start-up's switch-off
			// of every enabled demo rule, whichever comes first.
			s.mu.Lock()
			s.landed[ruleID] = true
			s.mu.Unlock()
		}
		if left := s.notLanded(); len(left) > 0 {
			err := errors.Join(errs...)
			if err == nil {
				err = errOffNotLanded
			}
			return s.failure(left, err)
		}
	}
	s.confirm.Do(func() { close(s.confirmed) })
	return nil
}
