package demo

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// The live metrics, in the order of a Sample's fields.
const (
	metricRPS = iota
	metricP95
	metricErrorRate
	metricTargetCPU
	metricTargetMemory
	numMetrics
)

var metricNames = [numMetrics]string{"rps", "p95_ms", "error_rate", MetricTargetCPUCores, MetricTargetMemoryBytes}

// sampleQueries are a round's queries, by metric: the contract's three, then
// target's own CPU and memory (liveTargetQuery). The target's two are ""
// (not asked, null) when target is not a catalog target.
func sampleQueries(target string) [numMetrics]string {
	q := [numMetrics]string{QueryRPS, QueryP95Ms, QueryErrorRate}
	if isCatalogTarget(target) {
		q[metricTargetCPU], q[metricTargetMemory] = liveTargetQuery(FaultCPU, target), liveTargetQuery(FaultMemory, target)
	}
	return q
}

// sampleTarget is the service whose own CPU and memory the live samples of a
// run of sc carry in phase: the CPU fault's target during cpu, the memory
// fault's target during baseline and memory.
func sampleTarget(sc Scenario, phase string) string {
	if phase == PhaseNames[2] {
		return sc.CPU.Target
	}
	return sc.Memory.Target
}

// RunSampler publishes the live metrics of the run in progress as sample
// events until ctx is done. Every Config.SampleEvery it asks w what runs, and
// only while a phase is running does it make a round: the five queries (the
// frontend's three of the contract, and the phase's target's own CPU and
// memory, sampleTarget) at once, each on its own context with
// Config.QueryTimeout, so that one hung query delays neither the others nor
// the next round.
//
//   - A failed, empty, NaN or infinite answer is null in the sample; the error
//     rate is clamped into 0..1. A sample whose five values are all null is
//     not published.
//   - A sample belongs to the run and phase that were current when its round
//     started, stamped with that moment, and is dropped if that run is no
//     longer current when the round ends (its end must stay its last event).
//   - Rounds never overlap: a tick that finds a round still running is
//     skipped. A round waits at most QueryTimeout for its answers; a query
//     that ignores its deadline is abandoned, and its metric is not queried
//     again (it is null) until that query returns, so a hung Prometheus costs
//     one goroutine per metric, not one per round.
//   - A metric's problem is logged at most once per run, phase and metric,
//     with its detail, in manteion's log only; a client only ever sees null.
//
// It returns once ctx is done and its round has stopped (at once with a nil w,
// m or h). One sampler runs per demo: (*Demo).Run starts it.
func RunSampler(ctx context.Context, w *Worker, m Metrics, h *Hub, cfg Config) {
	if w == nil || m == nil || h == nil {
		return
	}
	newSampler(w, m, h, cfg).run(ctx)
}

// sampler is RunSampler's state.
type sampler struct {
	w   *Worker
	m   Metrics
	h   *Hub
	cfg Config
	log *slog.Logger

	busy     atomic.Bool             // a round is running
	inflight [numMetrics]atomic.Bool // a query of the metric has not returned

	mu     sync.Mutex
	logged [numMetrics]string // per metric, the run and phase whose problem was last logged

	rounds  atomic.Int64 // rounds started
	skipped atomic.Int64 // ticks skipped because a round was still running
}

func newSampler(w *Worker, m Metrics, h *Hub, cfg Config) *sampler {
	return &sampler{w: w, m: m, h: h, cfg: cfg.withDefaults(), log: w.log}
}

// answer is one query's result.
type answer struct {
	metric int
	v      float64
	err    error
}

func (s *sampler) run(ctx context.Context) {
	var rounds sync.WaitGroup
	defer rounds.Wait()
	tick := time.NewTicker(s.cfg.SampleEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if ctx.Err() != nil {
			return
		}
		runID, phase, running, target := s.w.sampling()
		if runID == "" || !running {
			continue
		}
		if !s.busy.CompareAndSwap(false, true) {
			s.skipped.Add(1)
			continue
		}
		s.rounds.Add(1)
		rounds.Add(1)
		go func() {
			defer rounds.Done()
			defer s.busy.Store(false)
			s.round(ctx, runID, phase, target)
		}()
	}
}

// round queries the five metrics for runID's phase, whose target is target,
// and publishes the sample.
func (s *sampler) round(ctx context.Context, runID, phase, target string) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("demo: a sampling round panicked", "run_id", runID, "phase", phase,
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	at := s.w.d.Clock.Now()
	answers := make(chan answer, numMetrics) // never blocks a query, even one abandoned
	queries := sampleQueries(target)
	var asked, answered [numMetrics]bool
	n := 0
	for i := range numMetrics {
		if queries[i] == "" {
			continue
		}
		if !s.inflight[i].CompareAndSwap(false, true) {
			s.note(runID, phase, i, "its previous query has not returned", nil)
			continue
		}
		asked[i] = true
		n++
		go s.query(ctx, i, queries[i], answers)
	}
	var vals [numMetrics]*float64
	timer := time.NewTimer(s.cfg.QueryTimeout)
	defer timer.Stop()
wait:
	for got := 0; got < n; got++ {
		select {
		case a := <-answers:
			answered[a.metric] = true
			vals[a.metric] = s.value(runID, phase, a)
		case <-timer.C:
			for i := range numMetrics {
				if asked[i] && !answered[i] {
					s.note(runID, phase, i, "no answer within the query deadline", nil)
				}
			}
			break wait
		case <-ctx.Done():
			return
		}
	}
	sample := Sample{RunID: runID, Phase: phase, At: at, RPS: vals[metricRPS], P95Ms: vals[metricP95], ErrorRate: vals[metricErrorRate],
		TargetCPUCores: vals[metricTargetCPU], TargetMemoryBytes: vals[metricTargetMemory]}
	if sample.Empty() {
		return
	}
	// The hub drops anything published after a run's end; this does not lean
	// on that alone.
	if cur, _, _ := s.w.Current(); cur != runID {
		return
	}
	s.h.Publish(runID, Event{Type: EventSample, Data: sample})
}

// query asks metric i's query q on its own QueryTimeout context and hands the
// answer over; a panic is an error.
func (s *sampler) query(ctx context.Context, i int, q string, answers chan<- answer) {
	qctx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	a := answer{metric: i}
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("demo: a metrics query panicked", "metric", metricNames[i], "panic", fmt.Sprint(r),
				"stack", string(debug.Stack()))
			a.err = fmt.Errorf("the query panicked: %v", r)
		}
		// The mark clears before the answer is handed over: a round that
		// starts as soon as this one ends must find the metric free.
		s.inflight[i].Store(false)
		answers <- a
		cancel()
	}()
	a.v, a.err = s.m.QueryInstant(qctx, q)
}

// value is a's value as a sample holds it: nil for a failure, NaN or ±Inf;
// the error rate clamped into 0..1.
func (s *sampler) value(runID, phase string, a answer) *float64 {
	switch {
	case a.err != nil:
		s.note(runID, phase, a.metric, "the query failed", a.err)
		return nil
	case math.IsNaN(a.v) || math.IsInf(a.v, 0):
		s.note(runID, phase, a.metric, "not a finite number", nil)
		return nil
	}
	v := a.v
	if a.metric == metricErrorRate && (v < 0 || v > 1) {
		s.note(runID, phase, a.metric, fmt.Sprintf("%g is outside 0..1; clamped", v), nil)
		v = min(max(v, 0), 1)
	}
	return &v
}

// note logs a metric's problem, once per run, phase and metric.
func (s *sampler) note(runID, phase string, i int, problem string, err error) {
	key := runID + " " + phase
	s.mu.Lock()
	seen := s.logged[i] == key
	s.logged[i] = key
	s.mu.Unlock()
	if seen {
		return
	}
	attrs := []any{"run_id", runID, "phase", phase, "metric", metricNames[i], "problem", problem}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	s.log.Warn("demo: a live metric is null or clamped in samples (logged once per run, phase and metric)", attrs...)
}
