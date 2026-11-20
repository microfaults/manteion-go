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
	// ErrRunNotFound: no run has this id (404 not_found). An id this package
	// never minted is never looked up.
	ErrRunNotFound = errors.New("demo: no such run")
	// ErrRunUnavailable: the run could not be read now (the database failed;
	// 503 unavailable). The cause is in manteion's logs.
	ErrRunUnavailable = errors.New("demo: the run could not be read")
)

// The worker's internal errors: for logs only.
var (
	errSwitchClosed   = errors.New("demo: the run's rule switch is closed")
	errNoRule         = errors.New("demo: the run has no rule yet")
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
// For each run, from the moment it picks the run up:
//
//  1. a watchdog fires Config.RunCap later unless the run has ended: it
//     switches the run's rule off at once, from its own goroutine, whatever
//     the worker is blocked on, and cuts the run's calls short;
//  2. preflight; the run is running from here (started_at);
//  3. BuildRun: a disabled rule attached to the middle phase, a planned
//     experiment;
//  4. the rule is switched on (only while a phase links it);
//  5. StartExperiment, in a goroutine of the worker's, on a context that no
//     request and no shutdown cancels, only the watchdog;
//  6. every Config.StatusPoll the experiment and its phases are read; a phase
//     whose public status changed is published;
//  7. the run ends. Every ending, before anything else, switches the rule off.
//     A watchdog, a start error, a panic, an unknown status or a vanished
//     experiment then cancel the experiment, but only once its start has
//     returned; a completed, failed or externally cancelled experiment needs
//     no cancel, nor does a shutdown. Then a last read of the phases, the run's
//     final view is kept, its slot freed, its end event published (last), the
//     runs behind it told their new positions, and OnRunEnded called;
//  8. if the ending's switch-off failed, it is retried with backoff from a
//     goroutine of the worker's (SwitchOffPending reports it meanwhile), and
//     the next run is not picked up until a switch-off of this run's rule has
//     succeeded, or the worker stops.
//
// Every rule switch of a run goes through one ruleSwitch, which refuses a
// switch-on once the run's rule has been switched off: a late switch-on can
// never follow the switch-off. A panic in the run's code is recovered: the run
// ends failed/internal_error with its rule off and the worker goes on.
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
		done: make(chan struct{}), offRetry: backoff{first: offRetryFirst, max: offRetryMax}}
}

// Run works the queue, one run at a time, until ctx is done or halt is called.
// The run in progress then ends cancelled/interrupted with its rule switched
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
		runID, ok := w.q.next(ctx)
		if !ok {
			break
		}
		w.runOne(ctx, runID)
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

// SwitchOffPending reports whether an ended run's rule is not yet confirmed
// off: its switch-off failed and is being retried, and no run is picked up
// until it succeeds. The health route reports the lab as not accepting runs
// meanwhile. Enqueue still works: the run waits in line.
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

// switchOffCurrent switches the current run's rule off and closes its switch,
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
	return a.sw.off(ctx)
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

func (w *Worker) runOne(parent context.Context, runID string) {
	a := w.pickUp(parent, runID)
	defer a.timer.Stop()
	defer a.cancel()
	out := w.guardedDrive(a)
	if !a.started {
		a.cancelStart()
	}
	w.finish(parent, a, out)
	w.awaitOff(parent, a)
}

// awaitOff holds the worker until a switch-off of the run's rule has
// succeeded, or the worker stops: no next run is picked up while an ended
// run's rule may still be on. It returns at once for a run with no rule or
// whose first switch-off succeeded.
func (w *Worker) awaitOff(parent context.Context, a *active) {
	if a.sw.rule() == "" || a.sw.isOff() {
		return
	}
	w.log.Warn("demo: the next run waits until the ended run's rule is switched off", "run_id", a.runID, "rule_id", a.sw.rule())
	select {
	case <-a.sw.confirmed:
	case <-parent.Done():
	}
}

// pickUp makes runID the current run and starts its watchdog.
func (w *Worker) pickUp(parent context.Context, runID string) *active {
	a := &active{runID: runID, sw: newRuleSwitch(w.d.Rules, w.log), watchdog: make(chan struct{}), phase: PhaseNames[0]}
	for i := range a.last {
		a.last[i] = PhasePending
	}
	a.ctx, a.cancel = context.WithCancel(parent)
	a.startCtx, a.cancelStart = context.WithCancel(context.WithoutCancel(parent))
	w.mu.Lock()
	w.cur = a
	w.mu.Unlock()
	a.timer = time.AfterFunc(w.cfg.RunCap, func() { w.fireWatchdog(a) })
	w.log.Info("demo: run picked up", "run_id", runID)
	return a
}

// fireWatchdog runs on the timer's goroutine at the cap, without waiting for
// the worker. In this order: it marks the run timed out (so a call cut short
// below is never read as a shutdown); closes the rule switch, which refuses
// any later switch-on and cuts one in flight short; cancels the run's calls
// and its start; tells the worker; and switches the rule off, after any
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
	a.sw.shut()
	a.cancel()
	a.cancelStart()
	close(a.watchdog)
	w.log.Warn("demo: the run reached its cap; switching its rule off", "run_id", a.runID, "cap", w.cfg.RunCap)
	if err := a.sw.off(context.Background()); err != nil {
		w.log.Error("demo: the watchdog could not switch the run's rule off; the ending tries again",
			"run_id", a.runID, "rule_id", a.sw.rule(), "error", err)
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
	reason, ok := preflight(pctx, w.d, "", w.cfg.RunCap)
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

	built, err := BuildRun(a.ctx, w.d, a.runID, w.cfg.RunCap)
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
	a.sw.attach(a.ctx, built.RuleID)

	octx, cancel := context.WithTimeout(a.ctx, w.cfg.switchOnTimeout())
	err = a.sw.on(octx)
	cancel()
	if err != nil {
		if o, stop := a.interrupted(); stop {
			return o
		}
		// Unattached, closed or failed: the rule is off and nothing started.
		// The build's rows stay for the start-up sweep.
		w.log.Error("demo: the run's rule could not be switched on; not starting", "run_id", a.runID,
			"experiment_id", built.ExperimentID, "rule_id", built.RuleID, "error", err)
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

// refresh reads the run's phases and the results of completed ones, updates
// the run's view and Current, then publishes each phase whose public status
// changed, in phase order. A phase whose core status the contract does not
// know keeps its last public status; refresh then reports unknown.
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
	var results [3]*Result
	for i := range a.last {
		if a.last[i] != PhaseCompleted || a.haveResult[i] || seen[i] == nil {
			continue
		}
		res, err := w.readResult(ctx, seen[i].ID)
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

// readResult returns a phase's load result, or nil while it has none.
func (w *Worker) readResult(ctx context.Context, phaseID string) (*Result, error) {
	rctx, cancel := context.WithTimeout(ctx, w.cfg.readTimeout())
	rows, err := w.d.Experiments.ListWorkflowResultsForPhase(rctx, phaseID)
	cancel()
	if err != nil {
		return nil, err
	}
	var row *model.PhaseWorkflowResult
	for _, r := range rows {
		if r != nil && (row == nil || r.WorkflowID == WorkflowID) {
			row = r
			if r.WorkflowID == WorkflowID {
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
	if err := w.tryOff(context.WithoutCancel(parent), a); err != nil {
		w.log.Error("demo: switching the run's rule off failed; retrying until it succeeds",
			"run_id", a.runID, "rule_id", a.sw.rule(), "attempt", 1, "error", err)
		w.retryOff(parent, a)
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

// tryOff is one switch-off of the run's rule, with a panic turned into an
// error.
func (w *Worker) tryOff(ctx context.Context, a *active) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("demo: switching the run's rule off panicked", "run_id", a.runID,
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("demo: switching the rule off panicked: %v", r)
		}
	}()
	return a.sw.off(ctx)
}

// retryOff marks the run's switch-off pending and retries it from a goroutine
// of the worker's: after w.offRetry.first, then doubling up to w.offRetry.max,
// until one succeeds (then pending clears and the worker picks up the next
// run) or the worker stops (the rule's next chance is Shutdown's own switch-off,
// then the next start's sweep). A switch-off that lands from elsewhere
// (Shutdown's) ends it too: every switch-off goes through the run's switch and
// is idempotent.
func (w *Worker) retryOff(parent context.Context, a *active) {
	w.mu.Lock()
	w.pending = a
	w.mu.Unlock()
	w.bg.Add(1)
	go func() {
		defer w.bg.Done()
		delay := w.offRetry.first
		for attempt := 2; ; attempt++ {
			t := time.NewTimer(delay)
			select {
			case <-a.sw.confirmed:
				t.Stop()
				w.clearPending(a)
				return
			case <-parent.Done():
				t.Stop()
				w.log.Error("demo: stopping while the ended run's rule could not be switched off; the next start's sweep switches it off",
					"run_id", a.runID, "rule_id", a.sw.rule(), "attempts", attempt-1)
				return
			case <-t.C:
			}
			if err := w.tryOff(context.WithoutCancel(parent), a); err != nil {
				delay = w.offRetry.next(delay)
				w.log.Error("demo: switching the run's rule off failed again", "run_id", a.runID,
					"rule_id", a.sw.rule(), "attempt", attempt, "next_attempt_in", delay, "error", err)
				continue
			}
			w.log.Info("demo: the ended run's rule is off", "run_id", a.runID, "rule_id", a.sw.rule(), "attempt", attempt)
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
// and the runs' new positions.
func (w *Worker) retire(runID string, final *Run) {
	w.mu.Lock()
	moved, removed := w.q.remove(runID)
	if removed {
		w.finished = append(w.finished, cloneRun(*final))
		if extra := len(w.finished) - w.cfg.Retain; extra > 0 {
			w.finished = slices.Clone(w.finished[extra:])
		}
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
// phase_failed, anything else -> cancelled/interrupted).
//
// The error is ErrRunNotFound for an id that names no run (an id never minted
// here is not looked up), or ErrRunUnavailable when the database could not be
// read; the cause is logged, never returned. A run that has ended is never
// shown in line: the worker records its ended view and takes it out of the
// line in one step, and View looks in the line first.
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
	exp, err := w.findExperiment(ctx, runID)
	if errors.Is(err, ErrRunNotFound) {
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

// runFromDB builds a run's view from its experiment, phases and results.
func (w *Worker) runFromDB(ctx context.Context, exp *model.Experiment) (Run, error) {
	r := newRun(strings.TrimPrefix(exp.Name, RunNamePrefix), exp.CreatedAt)
	r.StartedAt, r.EndedAt = clonePtr(exp.StartedAt), clonePtr(exp.CompletedAt)
	switch exp.Status {
	case "completed":
		r.Status = RunCompleted
	case "failed":
		r.Status, r.Reason = RunFailed, ReasonPhaseFailed
	default: // cancelled, or left planned or running by an earlier process
		r.Status, r.Reason = RunCancelled, ReasonInterrupted
	}
	rctx, cancel := context.WithTimeout(ctx, w.cfg.readTimeout())
	phases, err := w.d.Experiments.ListPhasesForExperiment(rctx, exp.ID)
	cancel()
	if err != nil {
		return Run{}, err
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
			res, err := w.readResult(ctx, p.ID)
			if err != nil {
				return Run{}, err
			}
			pv.Result = res
		}
	}
	return r, nil
}

// ---- the rule switch ----

// ruleSwitch is one run's rule switch; every switch of the run's rule goes
// through it.
//
//   - on enables the rule (SetRuleEnabled, so only while a phase links it). It
//     is refused once the switch is closed.
//   - off closes the switch, cuts a switch-on in flight short, and disables
//     the rule (one unconditional write). It may be called any number of
//     times, from any goroutine.
//
// Writes never overlap: each holds write. An off issued while an on is in
// flight therefore writes after it, and an on that gets write after an off
// sees the switch closed and writes nothing. So when off returns, the rule's
// last write was a switch-off, and no switch-on follows it.
//
// confirmed is closed by the first switch-off write that succeeds. From then
// on the rule is off for good, as far as the demo is concerned: every later
// write is a switch-off.
type ruleSwitch struct {
	rules Rules
	log   *slog.Logger

	write     sync.Mutex // held across each store write
	confirmed chan struct{}
	confirm   sync.Once

	mu       sync.Mutex // guards the fields below; never held across a write
	ruleID   string
	closed   bool
	cancelOn context.CancelFunc
}

func newRuleSwitch(r Rules, log *slog.Logger) *ruleSwitch {
	return &ruleSwitch{rules: r, log: log, confirmed: make(chan struct{})}
}

// rule is the switch's rule id, "" before attach.
func (s *ruleSwitch) rule() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ruleID
}

// isOff reports whether a switch-off of the rule has succeeded.
func (s *ruleSwitch) isOff() bool {
	select {
	case <-s.confirmed:
		return true
	default:
		return false
	}
}

// attach gives the switch its rule. If the switch was already closed, the rule
// is switched off at once.
func (s *ruleSwitch) attach(ctx context.Context, ruleID string) {
	s.mu.Lock()
	s.ruleID = ruleID
	closed := s.closed
	s.mu.Unlock()
	if closed {
		if err := s.off(ctx); err != nil {
			s.log.Error("demo: switching off a rule attached after its run ended failed", "rule_id", ruleID, "error", err)
		}
	}
}

// on switches the rule on, unless the switch is closed or has no rule.
func (s *ruleSwitch) on(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errSwitchClosed
	}
	if s.ruleID == "" {
		s.mu.Unlock()
		return errNoRule
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.cancelOn = cancel
	ruleID := s.ruleID
	s.mu.Unlock()

	s.write.Lock()
	defer s.write.Unlock()
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errSwitchClosed
	}
	return SetRuleEnabled(ctx, s.rules, ruleID, true)
}

// shut closes the switch and cuts a switch-on in flight short, without
// writing or waiting; it returns the rule's id ("" before attach).
func (s *ruleSwitch) shut() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.cancelOn != nil {
		s.cancelOn()
	}
	return s.ruleID
}

// off shuts the switch and switches the rule off, after any switch-on still
// in flight. A success confirms the switch off.
func (s *ruleSwitch) off(ctx context.Context) error {
	ruleID := s.shut()
	if ruleID == "" {
		return nil
	}
	s.write.Lock()
	defer s.write.Unlock()
	// Not SetRuleEnabled: every caller of off logs its failure, once.
	if err := setRuleEnabled(ctx, s.rules, ruleID, false); err != nil {
		return err
	}
	s.confirm.Do(func() { close(s.confirmed) })
	return nil
}
