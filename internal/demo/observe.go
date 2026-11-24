package demo

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"manteion-go/internal/model"
)

// A fault phase's proof that its fault ran: the target's own memory (memory
// phase) or CPU (cpu phase) over the phase, against the same service's figure
// over the baseline phase, from the lab's Prometheus with fixed queries. The
// series are the target process's own (the Prometheus Go client's process
// collector, scraped every 10 s with the label service=<service>); the lab has
// no container series.
//
// When a figure is read. Metrics.QueryInstant evaluates a query at the moment
// it is asked, and has no evaluation time. So each figure is read as the
// phase it covers completes, over "the last W seconds, now", W the phase's
// length: the baseline's two figures (the memory target's memory and the CPU
// target's CPU) when baseline completes, kept on the run; each fault phase's
// figure when that phase completes, compared with the baseline's figure of
// the same service and kind. A figure whose phase's end is seen more than
// proofMaxLag away from the read is not read (its window would have moved),
// and is null.
//
// A proof never fails a run and never delays its end by more than
// Config.QueryTimeout per read of the phases: the figures a read needs are
// asked at once and waited for at most that long. A figure that could not be
// read is null, logged at Warn in manteion's log only; fault_observed is then
// null, never true or false. The figures live in the run's view only: a run
// read back from the database shows each completed fault phase's metric with
// both figures null.

// The rise over the baseline phase at which a fault phase's fault counts as
// observed. Both come from the box trial of 2026-11-23: half the smallest rise
// seen at the moderate intensity (memory: cartservice at target_load 0.1,
// 13,389,005 bytes; CPU: productcatalogservice at 0.5, 0.1007 cores).
const (
	observedMemoryBytes = 6694502
	observedCPUCores    = 0.0503
)

// proofMaxLag is how far from its phase's end a figure may be read. Normally
// the worker reads a phase's end within a status poll (1 s) of it; 5 s, half
// the scrape interval, moves a 40 s window by at most one sample, which the
// thresholds (half the smallest rise) absorb.
const proofMaxLag = 5 * time.Second

// maxProofQueries bounds the worker's proof queries that have not returned: a
// query that ignores its deadline holds its slot until it returns, and a
// figure that finds no slot free is not asked (null). A run asks four.
const maxProofQueries = 4

// liveTargetRate is the rate window of a live sample's target CPU.
const liveTargetRate = "20s"

// Problems a figure of a proof can have, as its Warn line says.
const (
	problemNoMetrics = "no metrics client"
	problemNoWindow  = "the phase's start or end is not known"
	problemNotTarget = "the service is not one of the catalog's targets"
	problemNotKind   = "the fault kind is not one the proof measures"
	problemLate      = "the phase's end was read too far from its time; its window would have moved"
	problemBusy      = "earlier proof queries have not returned"
	problemFailed    = "the query failed"
	problemNotFinite = "not a finite number"
	problemNoAnswer  = "no answer within the query deadline"
	problemCutShort  = "the run's reads were cut short"
)

// ---- queries ----

// isCatalogTarget reports whether service is one of CatalogTargets().
func isCatalogTarget(service string) bool { return slices.Contains(CatalogTargets(), service) }

// targetSelector is {service="<service>"} for a catalog target. It panics for
// any other service, without naming it.
func targetSelector(service string) string {
	if !isCatalogTarget(service) {
		panic("demo: a target query for a service outside the catalog")
	}
	return `{service="` + service + `"}`
}

// targetQuery is the PromQL of a proof figure: target service's k figure over
// the last span, in whole seconds and at least 1, as the box trial ran it:
//
//	memory: avg_over_time(process_resident_memory_bytes{service="<service>"}[<W>s])
//	cpu:    rate(process_cpu_seconds_total{service="<service>"}[<W>s])
//
// It is built only from constants, a catalog target and a number: it panics
// for a service that is not one of CatalogTargets() and for a kind outside
// the list, so nothing from a request or the database can reach a query.
func targetQuery(k FaultKind, service string, span time.Duration) string {
	sel := targetSelector(service)
	w := strconv.FormatInt(max(int64(span/time.Second), 1), 10)
	switch k {
	case FaultMemory:
		return "avg_over_time(process_resident_memory_bytes" + sel + "[" + w + "s])"
	case FaultCPU:
		return "rate(process_cpu_seconds_total" + sel + "[" + w + "s])"
	}
	panic("demo: a target query for a fault kind outside the list")
}

// liveTargetQuery is a live sample's query of target service's k figure: its
// CPU over the last 20 s, or its latest resident memory. It panics as
// targetQuery does.
func liveTargetQuery(k FaultKind, service string) string {
	sel := targetSelector(service)
	switch k {
	case FaultMemory:
		return "process_resident_memory_bytes" + sel
	case FaultCPU:
		return "rate(process_cpu_seconds_total" + sel + "[" + liveTargetRate + "])"
	}
	panic("demo: a target query for a fault kind outside the list")
}

// measureMetric is the Measure metric of a k proof; false for a kind outside
// the list.
func measureMetric(k FaultKind) (string, bool) {
	switch k {
	case FaultMemory:
		return MetricTargetMemoryBytes, true
	case FaultCPU:
		return MetricTargetCPUCores, true
	}
	return "", false
}

// ---- windows and figures ----

// window is a phase's span: when it started and when it ended.
type window struct{ start, end time.Time }

// windowOf is phase p's window from the store's started_at and completed_at;
// the zero window when either is unknown.
func windowOf(p *model.ExperimentPhase) window {
	if p == nil || p.StartedAt == nil || p.CompletedAt == nil {
		return window{}
	}
	return window{start: *p.StartedAt, end: *p.CompletedAt}
}

// known reports whether both ends of the window are known.
func (w window) known() bool { return !w.start.IsZero() && !w.end.IsZero() }

// span is the window's length (targetQuery rounds it down to whole seconds,
// at least 1).
func (w window) span() time.Duration { return w.end.Sub(w.start) }

// figureAsk is one figure a proof needs: service's k figure over win, the
// window of the run's phase at index phase. figures fills v, or leaves it nil
// and says why in problem (with err when a query failed).
type figureAsk struct {
	phase   int
	k       FaultKind
	service string
	win     window
	v       *float64
	problem string
	err     error
}

// figures reads every figure of asks at now, the moment their phases' ends
// were read: each query asked at once, on its own context with timeout, and
// all of them waited for at most timeout, or until ctx is done. A query that
// ignores its deadline is abandoned, and holds its slot in slots until it
// returns. A figure is not asked when m is nil, its window is unknown or
// further than proofMaxLag from now, its service is not a catalog target, its
// kind is not a proof's, or no slot is free. A failed, NaN or infinite answer
// is null. It never panics on an ask, and never puts an error in a figure.
func figures(ctx context.Context, m Metrics, slots chan struct{}, now time.Time, timeout time.Duration, asks []*figureAsk) {
	type answer struct {
		i   int
		v   float64
		err error
	}
	answers := make(chan answer, len(asks)) // never blocks a query, even an abandoned one
	pending := map[int]bool{}
	for i, q := range asks {
		_, kind := measureMetric(q.k)
		switch lag := now.Sub(q.win.end); {
		case m == nil:
			q.problem = problemNoMetrics
		case !kind:
			q.problem = problemNotKind
		case !isCatalogTarget(q.service):
			q.problem = problemNotTarget
		case !q.win.known():
			q.problem = problemNoWindow
		case lag > proofMaxLag || lag < -proofMaxLag:
			q.problem = problemLate
		}
		if q.problem != "" {
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			q.problem = problemBusy
			continue
		}
		pending[i] = true
		k, service, span := q.k, q.service, q.win.span()
		go func() {
			qctx, cancel := context.WithTimeout(ctx, timeout)
			a := answer{i: i}
			defer func() {
				if r := recover(); r != nil {
					a.err = fmt.Errorf("the query panicked: %v", r)
				}
				<-slots // the slot frees when the query returns, not when it is abandoned
				answers <- a
				cancel()
			}()
			a.v, a.err = m.QueryInstant(qctx, targetQuery(k, service, span))
		}()
	}
	if len(pending) == 0 {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for len(pending) > 0 {
		select {
		case a := <-answers:
			delete(pending, a.i)
			q := asks[a.i]
			switch {
			case a.err != nil:
				q.problem, q.err = problemFailed, a.err
			case math.IsNaN(a.v) || math.IsInf(a.v, 0):
				q.problem = problemNotFinite
			default:
				v := a.v
				q.v = &v
			}
		case <-timer.C:
			for i := range pending {
				asks[i].problem = problemNoAnswer
			}
			return
		case <-ctx.Done():
			for i := range pending {
				asks[i].problem = problemCutShort
			}
			return
		}
	}
}

// ---- the proof ----

// observe is a k fault phase's proof from its two figures, the phase's
// (value) and the baseline phase's (baseline), each nil when it was not
// measured: a Measure of k's metric holding both, and fault_observed. That is
// true when the rise value - baseline reaches k's threshold, false when it
// does not, and nil when either figure is missing or not a finite number: a
// rise cannot be told without both. (Whatever a view holds, PhaseView
// marshals fault_observed as null unless its measure holds both figures.) k
// must be FaultMemory or FaultCPU.
func observe(k FaultKind, value, baseline *float64) (Measure, *bool) {
	metric, _ := measureMetric(k)
	m := Measure{Metric: metric, Value: clonePtr(finite(value)), Baseline: clonePtr(finite(baseline))}
	if m.Value == nil || m.Baseline == nil {
		return m, nil
	}
	threshold := observedCPUCores
	if k == FaultMemory {
		threshold = observedMemoryBytes
	}
	seen := *m.Value-*m.Baseline >= threshold
	return m, &seen
}

// phaseProof is a fault phase's proof as its view shows it.
type phaseProof struct {
	measure  Measure
	observed *bool
}

// prove reads the figures the run's proof needs from the phases that
// completed since the last read (seen holds this read's phases, a.last their
// public statuses), all at once and within one QueryTimeout (figures):
//
//   - baseline: the memory target's memory and the CPU target's CPU over its
//     window, kept on the run;
//   - memory, cpu: the phase's target's figure over its window, compared with
//     the baseline's figure of the same service and kind (observe).
//
// It returns the proof of each fault phase that completed since the last
// read; a phase's figures are asked for once. A figure that could not be read
// is null and logged at Warn with the run, the phase, the metric and a fixed
// problem; nothing of it reaches the run's view but the null.
func (w *Worker) prove(ctx context.Context, a *active, seen [3]*model.ExperimentPhase) [3]*phaseProof {
	var (
		out  [3]*phaseProof
		asks []*figureAsk
	)
	steps := faultSteps(&a.sc)
	for i, p := range seen {
		if p == nil || a.last[i] != PhaseCompleted || a.proved[i] {
			continue
		}
		a.proved[i] = true
		win := windowOf(p)
		if st := steps[i]; st != nil {
			asks = append(asks, &figureAsk{phase: i, k: st.Kind, service: st.Target, win: win})
			continue
		}
		asks = append(asks,
			&figureAsk{phase: i, k: FaultMemory, service: a.sc.Memory.Target, win: win},
			&figureAsk{phase: i, k: FaultCPU, service: a.sc.CPU.Target, win: win})
	}
	if len(asks) == 0 {
		return out
	}
	figures(ctx, w.d.Prom, w.proofSlots, w.d.Clock.Now(), w.cfg.QueryTimeout, asks)
	for _, q := range asks {
		if q.problem != "" {
			metric, _ := measureMetric(q.k)
			attrs := []any{"run_id", a.runID, "phase", PhaseNames[q.phase], "metric", metric, "problem", q.problem}
			if q.err != nil {
				attrs = append(attrs, "error", q.err)
			}
			w.log.Warn("demo: a figure of a fault phase's proof is null", attrs...)
		}
		if q.phase == 0 {
			if q.k == FaultMemory {
				a.baseMemory = q.v
			} else {
				a.baseCPU = q.v
			}
		}
	}
	for _, q := range asks {
		if q.phase == 0 {
			continue
		}
		if _, ok := measureMetric(q.k); !ok {
			continue
		}
		base := a.baseCPU
		if q.k == FaultMemory {
			base = a.baseMemory
		}
		m, observed := observe(q.k, q.v, base)
		out[q.phase] = &phaseProof{measure: m, observed: observed}
		attrs := []any{"run_id", a.runID, "phase", PhaseNames[q.phase], "metric", m.Metric}
		if m.Value != nil {
			attrs = append(attrs, "value", *m.Value)
		}
		if m.Baseline != nil {
			attrs = append(attrs, "baseline", *m.Baseline)
		}
		verdict := "unknown"
		if observed != nil {
			verdict = strconv.FormatBool(*observed)
		}
		w.log.Info("demo: fault phase measured", append(attrs, "fault_observed", verdict)...)
	}
	return out
}

// storedMeasure is the measure of a fault phase read back from the database:
// its metric, with no figure (none is stored). nil for a phase whose fault is
// not a proof's kind (baseline's none, build 2's latency).
func storedMeasure(fault *string) *Measure {
	if fault == nil {
		return nil
	}
	metric, ok := measureMetric(FaultKind(*fault))
	if !ok {
		return nil
	}
	return &Measure{Metric: metric}
}
