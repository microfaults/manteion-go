package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// ---- the queries ----

// catchPanic calls f and returns its result, or what it panicked with.
func catchPanic(f func() string) (s string, r any) {
	defer func() { r = recover() }()
	return f(), nil
}

// TestTargetQuery: each kind's proof query for each catalog target is the box
// trial's expression, its window in whole seconds and at least 1; the live
// samples' two queries are the trial's with a 20 s rate. A service outside
// the catalog, or a kind outside the list, panics, and the panic carries
// nothing of the name: no hostile character can reach a query, because no
// such service can be passed.
func TestTargetQuery(t *testing.T) {
	t.Parallel()
	if got := CatalogTargets(); !slices.Equal(got, []string{"cartservice", "productcatalogservice"}) {
		t.Fatalf("catalog targets %v; this test covers cartservice and productcatalogservice", got)
	}
	var built []string
	for _, svc := range CatalogTargets() {
		for _, tc := range []struct {
			span time.Duration
			w    string
		}{
			{40 * time.Second, "40"}, {44900 * time.Millisecond, "44"}, {time.Second, "1"},
			{999 * time.Millisecond, "1"}, {0, "1"}, {-5 * time.Second, "1"},
		} {
			want := map[FaultKind]string{
				FaultMemory: `avg_over_time(process_resident_memory_bytes{service="` + svc + `"}[` + tc.w + `s])`,
				FaultCPU:    `rate(process_cpu_seconds_total{service="` + svc + `"}[` + tc.w + `s])`,
			}
			for k, w := range want {
				if got := targetQuery(k, svc, tc.span); got != w {
					t.Errorf("targetQuery(%s, %s, %v) = %s; want %s", k, svc, tc.span, got, w)
				}
				built = append(built, w)
			}
		}
		live := map[FaultKind]string{
			FaultMemory: `process_resident_memory_bytes{service="` + svc + `"}`,
			FaultCPU:    `rate(process_cpu_seconds_total{service="` + svc + `"}[20s])`,
		}
		for k, w := range live {
			if got := liveTargetQuery(k, svc); got != w {
				t.Errorf("liveTargetQuery(%s, %s) = %s; want %s", k, svc, got, w)
			}
			built = append(built, w)
		}
	}
	for _, q := range built {
		for _, c := range q {
			if !strings.ContainsRune(`abcdefghijklmnopqrstuvwxyz0123456789_(){}[]="`, c) {
				t.Errorf("query %s holds %q", q, c)
			}
		}
	}

	hostile := []string{
		"", "frontend", "Cartservice", " cartservice", "cartservice ", "cartservice\n", "cart-service",
		`cartservice"} or vector(1) or {service="`, "productcatalogservice}[1h])#", "192.0.2.10", "productcatalogservice\x00",
	}
	builders := map[string]func(svc string) string{
		"targetQuery memory":     func(svc string) string { return targetQuery(FaultMemory, svc, 40*time.Second) },
		"targetQuery cpu":        func(svc string) string { return targetQuery(FaultCPU, svc, 40*time.Second) },
		"liveTargetQuery memory": func(svc string) string { return liveTargetQuery(FaultMemory, svc) },
		"liveTargetQuery cpu":    func(svc string) string { return liveTargetQuery(FaultCPU, svc) },
	}
	for name, build := range builders {
		for _, svc := range hostile {
			q, r := catchPanic(func() string { return build(svc) })
			switch {
			case r == nil:
				t.Errorf("%s(%q) built %q; want a panic", name, svc, q)
			case svc != "" && strings.Contains(fmt.Sprint(r), svc):
				t.Errorf("%s(%q) panicked with %q, which carries the name", name, svc, r)
			}
		}
	}
	for _, k := range []FaultKind{"", "disk", "Memory"} {
		if q, r := catchPanic(func() string { return targetQuery(k, targetCart, 40*time.Second) }); r == nil {
			t.Errorf("targetQuery(%q, ...) built %q; want a panic", k, q)
		}
		if q, r := catchPanic(func() string { return liveTargetQuery(k, targetCart) }); r == nil {
			t.Errorf("liveTargetQuery(%q, ...) built %q; want a panic", k, q)
		}
	}
}

// ---- reading figures and the proof ----

// readOne reads one figure as the worker does when the phase it covers
// completes: service's k figure over win, asked at win's end.
func readOne(m Metrics, k FaultKind, service string, win window, timeout time.Duration) *figureAsk {
	q := &figureAsk{k: k, service: service, win: win}
	figures(context.Background(), m, make(chan struct{}, maxProofQueries), win.end, timeout, []*figureAsk{q})
	return q
}

// ignoreDeadline answers only when release is closed, whatever its context.
func ignoreDeadline(release <-chan struct{}) func(context.Context) (float64, error) {
	return func(context.Context) (float64, error) {
		<-release
		return 1, nil
	}
}

// TestObserve (Review Focus 4): a fault phase's proof from the phase's figure
// and the baseline's, each read when its phase completes. A rise that reaches
// the threshold is true, one below it false; a figure whose query fails,
// answers nothing, NaN or ±Inf, or does not answer within QueryTimeout (even
// one that ignores its deadline) is null, and then fault_observed is null:
// never true, never false. The proof never carries the error, and a hung
// query costs at most QueryTimeout.
func TestObserve(t *testing.T) {
	t.Parallel()
	const timeout = 100 * time.Millisecond
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	baseWin := window{start: testNow, end: testNow.Add(40500 * time.Millisecond)}
	phaseWin := window{start: baseWin.end, end: baseWin.end.Add(44900 * time.Millisecond)}
	val := func(v float64) func(context.Context) (float64, error) {
		return func(context.Context) (float64, error) { return v, nil }
	}
	fail := func(context.Context) (float64, error) { return 0, errLeaky }
	empty := func(context.Context) (float64, error) {
		return 0, errors.New(`prometheus query "x" returned no data`)
	}
	yes, no := true, false
	cases := []struct {
		name        string
		k           FaultKind
		base, phase func(context.Context) (float64, error)
		value, bval *float64
		want        *bool
	}{
		{"memory rise above", FaultMemory, val(45e6), val(61_456_499), f64p(61_456_499), f64p(45e6), &yes},
		{"memory rise exactly at", FaultMemory, val(45e6), val(45e6 + observedMemoryBytes), f64p(45e6 + observedMemoryBytes), f64p(45e6), &yes},
		{"memory rise a byte below", FaultMemory, val(45e6), val(45e6 + observedMemoryBytes - 1), f64p(45e6 + observedMemoryBytes - 1), f64p(45e6), &no},
		{"cpu rise above", FaultCPU, val(0.0293), val(0.13), f64p(0.13), f64p(0.0293), &yes},
		{"cpu rise below", FaultCPU, val(0.0293), val(0.0693), f64p(0.0693), f64p(0.0293), &no},
		{"no rise", FaultCPU, val(0.03), val(0.03), f64p(0.03), f64p(0.03), &no},
		{"a fall", FaultMemory, val(61e6), val(45e6), f64p(45e6), f64p(61e6), &no},
		{"the phase's query fails", FaultMemory, val(45e6), fail, nil, f64p(45e6), nil},
		{"the baseline's query fails", FaultCPU, fail, val(0.13), f64p(0.13), nil, nil},
		{"an empty answer", FaultCPU, val(0.0293), empty, nil, f64p(0.0293), nil},
		{"NaN", FaultMemory, val(45e6), val(math.NaN()), nil, f64p(45e6), nil},
		{"+Inf", FaultCPU, val(0.0293), val(math.Inf(1)), nil, f64p(0.0293), nil},
		{"-Inf baseline", FaultMemory, val(math.Inf(-1)), val(61e6), f64p(61e6), nil, nil},
		{"a query that ignores its deadline", FaultCPU, val(0.0293), ignoreDeadline(release), nil, f64p(0.0293), nil},
		{"a query that never answers", FaultMemory, hang, val(61e6), f64p(61e6), nil, nil},
		{"both fail", FaultMemory, fail, fail, nil, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newScriptedMetrics()
			m.set(targetQuery(tc.k, targetCart, baseWin.span()), tc.base)
			m.set(targetQuery(tc.k, targetCart, phaseWin.span()), tc.phase)
			read := func(win window) *figureAsk {
				began := time.Now()
				q := readOne(m, tc.k, targetCart, win, timeout)
				if d := time.Since(began); d > timeout+100*time.Millisecond {
					t.Errorf("a figure took %v; want it within QueryTimeout %v", d, timeout)
				}
				return q
			}
			b, p := read(baseWin), read(phaseWin)
			measure, observed := observe(tc.k, p.v, b.v)
			metric := map[FaultKind]string{FaultMemory: "target_memory_bytes", FaultCPU: "target_cpu_cores"}[tc.k]
			if measure.Metric != metric || !sameFigure(measure.Value, tc.value) || !sameFigure(measure.Baseline, tc.bval) {
				t.Errorf("measure %s; want metric %s, value %v, baseline %v", measureJSON(t, measure), metric, show(tc.value), show(tc.bval))
			}
			if !sameBool(observed, tc.want) {
				t.Errorf("fault_observed = %v; want %v", showBool(observed), showBool(tc.want))
			}
			for _, q := range []*figureAsk{b, p} {
				if (q.v == nil) != (q.problem != "") {
					t.Errorf("figure %v with problem %q; want a problem exactly when the figure is null", show(q.v), q.problem)
				}
			}
			assertNoLeak(t, measureJSON(t, measure))
		})
	}
}

// TestObserve_QueriesHaveTheirDeadline: every figure's query runs on a
// context whose deadline is at most QueryTimeout away.
func TestObserve_QueriesHaveTheirDeadline(t *testing.T) {
	t.Parallel()
	m := newScriptedMetrics()
	win := window{start: testNow, end: testNow.Add(40 * time.Second)}
	for _, k := range []FaultKind{FaultMemory, FaultCPU} {
		m.value(targetQuery(k, targetCatalog, win.span()), 1)
		if q := readOne(m, k, targetCatalog, win, 300*time.Millisecond); q.v == nil || *q.v != 1 {
			t.Errorf("%s figure %v (%s); want 1", k, show(q.v), q.problem)
		}
	}
	_, _, deadlines, noDeadline := m.snapshot()
	if noDeadline != 0 || len(deadlines) != 2 {
		t.Fatalf("%d queries with a deadline, %d without; want 2 and 0", len(deadlines), noDeadline)
	}
	for _, d := range deadlines {
		if d > 300*time.Millisecond {
			t.Errorf("a query with %v left; want at most QueryTimeout", d)
		}
	}
}

// TestObserve_WhatIsNotAsked: a figure is not asked, and is null with its
// problem, when its phase's end was seen more than proofMaxLag before or after
// the read (its window would have moved), when its phase's start or end is
// unknown, when its service is not a catalog target (no panic), and when
// there is no metrics client. At exactly proofMaxLag it is asked.
func TestObserve_WhatIsNotAsked(t *testing.T) {
	t.Parallel()
	win := window{start: testNow, end: testNow.Add(40 * time.Second)}
	m := newScriptedMetrics()
	m.value(targetQuery(FaultCPU, targetCart, win.span()), 0.2)
	slots := make(chan struct{}, maxProofQueries)
	ask := func(m Metrics, q *figureAsk, now time.Time) {
		t.Helper()
		figures(context.Background(), m, slots, now, 100*time.Millisecond, []*figureAsk{q})
	}
	for _, tc := range []struct {
		name  string
		q     figureAsk
		now   time.Time
		asked bool
	}{
		{"seen at the end", figureAsk{k: FaultCPU, service: targetCart, win: win}, win.end, true},
		{"seen proofMaxLag late", figureAsk{k: FaultCPU, service: targetCart, win: win}, win.end.Add(proofMaxLag), true},
		{"seen just over proofMaxLag late", figureAsk{k: FaultCPU, service: targetCart, win: win}, win.end.Add(proofMaxLag + time.Millisecond), false},
		{"seen a phase later", figureAsk{k: FaultCPU, service: targetCart, win: win}, win.end.Add(40 * time.Second), false},
		{"a clock behind the store's", figureAsk{k: FaultCPU, service: targetCart, win: win}, win.end.Add(-proofMaxLag - time.Second), false},
		{"no start", figureAsk{k: FaultCPU, service: targetCart, win: window{end: win.end}}, win.end, false},
		{"no end", figureAsk{k: FaultCPU, service: targetCart, win: window{start: win.start}}, win.end, false},
		{"not a catalog target", figureAsk{k: FaultCPU, service: "frontend", win: win}, win.end, false},
		{"a hostile service", figureAsk{k: FaultCPU, service: `cartservice"} or vector(1) or {x="`, win: win}, win.end, false},
		{"not a kind", figureAsk{k: "disk", service: targetCart, win: win}, win.end, false},
	} {
		before := len(callsOf(m))
		q := tc.q
		ask(m, &q, tc.now)
		asked := len(callsOf(m)) > before
		if asked != tc.asked || (q.v != nil) != tc.asked || (q.problem == "") != tc.asked {
			t.Errorf("%s: asked %v, figure %v, problem %q; want asked %v", tc.name, asked, show(q.v), q.problem, tc.asked)
		}
	}
	q := figureAsk{k: FaultCPU, service: targetCart, win: win}
	ask(nil, &q, win.end)
	if q.v != nil || q.problem == "" {
		t.Errorf("no metrics client: figure %v, problem %q; want null with a problem", show(q.v), q.problem)
	}
}

// callsOf lists every query m was asked, in no order.
func callsOf(m *scriptedMetrics) []string {
	calls, _, _, _ := m.snapshot()
	var out []string
	for q, n := range calls {
		for range n {
			out = append(out, q)
		}
	}
	return out
}

// TestObserve_QueriesInFlightAreBounded: the figures' queries that have not
// returned are at most maxProofQueries, over every read: with that many hung
// (ignoring their deadline), a further figure is not asked and is null; once
// they return, it is asked again. Each read still returns within its
// QueryTimeout. Not parallel: it counts goroutines.
func TestObserve_QueriesInFlightAreBounded(t *testing.T) {
	const timeout = 30 * time.Millisecond
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	m := newScriptedMetrics()
	win := window{start: testNow, end: testNow.Add(40 * time.Second)}
	hung := targetQuery(FaultMemory, targetCatalog, win.span())
	m.set(hung, ignoreDeadline(release))
	slots := make(chan struct{}, maxProofQueries)
	before := runtime.NumGoroutine()
	for i := 0; i < 3*maxProofQueries; i++ {
		q := &figureAsk{k: FaultMemory, service: targetCatalog, win: win}
		began := time.Now()
		figures(context.Background(), m, slots, win.end, timeout, []*figureAsk{q})
		if d := time.Since(began); d > timeout+150*time.Millisecond {
			t.Errorf("read %d took %v; want at most QueryTimeout %v", i, d, timeout)
		}
		if q.v != nil || q.problem == "" {
			t.Errorf("read %d: figure %v, problem %q; want null", i, show(q.v), q.problem)
		}
	}
	if n := m.count(hung); n != maxProofQueries {
		t.Errorf("%d queries asked while they hung; want %d, the bound", n, maxProofQueries)
	}
	if n := runtime.NumGoroutine() - before; n > maxProofQueries+2 {
		t.Errorf("%d more goroutines with every query hung; want at most %d", n, maxProofQueries)
	}
	free()
	waitFor(t, "the hung queries to return", func() bool { return len(slots) == 0 })
	m.value(hung, 5)
	q := &figureAsk{k: FaultMemory, service: targetCatalog, win: win}
	figures(context.Background(), m, slots, win.end, timeout, []*figureAsk{q})
	if q.v == nil || *q.v != 5 {
		t.Errorf("figure %v (%s) once the hung queries returned; want 5", show(q.v), q.problem)
	}
}

// ---- fault_observed on the wire ----

// TestPhaseViewJSON_ObservedNeedsBothFigures: fault_observed marshals as
// null unless the phase's measure holds both figures as finite numbers,
// whatever the view holds; with both, it marshals as held.
func TestPhaseViewJSON_ObservedNeedsBothFigures(t *testing.T) {
	t.Parallel()
	yes, no := true, false
	for _, tc := range []struct {
		name     string
		observed *bool
		m        *Measure
		want     any
	}{
		{"true, no measure", &yes, nil, nil},
		{"false, no measure", &no, nil, nil},
		{"true, no value", &yes, &Measure{Metric: MetricTargetCPUCores, Baseline: f64p(0.02)}, nil},
		{"false, no baseline", &no, &Measure{Metric: MetricTargetCPUCores, Value: f64p(0.2)}, nil},
		{"true, neither", &yes, &Measure{Metric: MetricTargetMemoryBytes}, nil},
		{"true, NaN value", &yes, &Measure{Metric: MetricTargetMemoryBytes, Value: f64p(math.NaN()), Baseline: f64p(1)}, nil},
		{"false, infinite baseline", &no, &Measure{Metric: MetricTargetMemoryBytes, Value: f64p(1), Baseline: f64p(math.Inf(1))}, nil},
		{"true, both", &yes, &Measure{Metric: MetricTargetCPUCores, Value: f64p(0.2), Baseline: f64p(0.02)}, true},
		{"false, both", &no, &Measure{Metric: MetricTargetCPUCores, Value: f64p(0.02), Baseline: f64p(0.02)}, false},
		{"null, both", nil, &Measure{Metric: MetricTargetCPUCores, Value: f64p(0.2), Baseline: f64p(0.02)}, nil},
	} {
		p := decodeObject(t, PhaseView{Name: "cpu", Fault: strp("cpu"), Target: strp(targetCart), Intensity: strp("heavy"),
			Status: PhaseCompleted, DurationS: PhaseDurationS, FaultObserved: tc.observed, Measure: tc.m})
		if v, ok := p["fault_observed"]; !ok || v != tc.want {
			t.Errorf("%s: fault_observed = %v (present %v); want %v", tc.name, v, ok, tc.want)
		}
	}
}

// ---- the worker ----

// paceRun drives the demo's running experiment as the orchestrator does, with
// phases lengths long, instead of the fake's autopilot: every second read of
// the phases first moves the clock to the running phase's end, then ends it
// there (completed) and starts the next one at that moment, or completes the
// experiment after the last. The read that ends a phase shows it ended, so the
// worker sees each end as it happens, as it does on the lab within a status
// poll. completedAt, if not nil, is set to the wall time of the experiment's
// completion.
func paceRun(f *fakeLab, lengths [3]time.Duration, completedAt *atomic.Int64) {
	var reads atomic.Int32
	f.onCall("experiments.ListPhasesForExperiment", func() {
		if reads.Add(1)%2 == 1 {
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, exp := range f.experiments {
			if exp.CreatedBy != CreatedBy || exp.Status != "running" {
				continue
			}
			var byPos []*model.ExperimentPhase
			for _, p := range f.phases {
				if p.ExperimentID == exp.ID {
					byPos = append(byPos, p)
				}
			}
			slices.SortFunc(byPos, func(a, b *model.ExperimentPhase) int { return a.Position - b.Position })
			for i, p := range byPos {
				if p.Status != "running" || p.StartedAt == nil {
					continue
				}
				end := p.StartedAt.Add(lengths[i])
				f.clock.Advance(end.Sub(f.clock.Now()))
				p.Status, p.CompletedAt = "completed", &end
				if i+1 < len(byPos) {
					byPos[i+1].Status, byPos[i+1].StartedAt = "running", &end
				} else {
					exp.Status, exp.CompletedAt = "completed", &end
					if completedAt != nil {
						completedAt.Store(time.Now().UnixNano())
					}
				}
				f.checkLocked("paceRun")
				return
			}
		}
	})
}

// phaseLengths are the paced phases' lengths in TestWorker_*: baseline 40.5 s,
// memory 44.9 s, cpu 41.2 s, so their windows are 40, 44 and 41 whole seconds.
var phaseLengths = [3]time.Duration{40500 * time.Millisecond, 44900 * time.Millisecond, 41200 * time.Millisecond}

// TestWorker_PhaseCarriesMeasure: a completed run's memory and cpu phases
// carry their measure and fault_observed, baseline carries null for both.
// The figures are read as each phase completes: when baseline completes, the
// memory target's memory and the CPU target's CPU over baseline's 40 s; when
// memory completes, the memory target's memory over its 44 s; when cpu
// completes, the CPU target's CPU over its 41 s; nothing else is asked. A
// rise that reaches the threshold is true, one below it false, and a
// Prometheus that fails gives null figures and fault_observed null on a run
// that completes and shows nothing of the error. The ended run's kept view
// says the same.
func TestWorker_PhaseCarriesMeasure(t *testing.T) {
	t.Parallel()
	yes, no := true, false
	type figs struct{ baseMem, mem, baseCPU, cpu float64 }
	for _, tc := range []struct {
		name     string
		scenario string
		figs     figs
		fail     bool
		want     [2]*bool // memory, cpu
	}{
		{"observed", "catalog-moderate", figs{45e6, 61_456_499, 0.0293, 0.13}, false, [2]*bool{&yes, &yes}},
		{"not observed", "cart-heavy", figs{37e6, 38e6, 0.02, 0.05}, false, [2]*bool{&no, &no}},
		{"Prometheus fails", "cart-moderate", figs{}, true, [2]*bool{nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newHarness(t, noCap)
			sc := scenario(t, tc.scenario)
			memQ := func(s int) string { return targetQuery(FaultMemory, sc.Memory.Target, time.Duration(s)*time.Second) }
			cpuQ := func(s int) string { return targetQuery(FaultCPU, sc.CPU.Target, time.Duration(s)*time.Second) }
			x.f.mu.Lock()
			x.f.promValues[memQ(40)], x.f.promValues[cpuQ(40)] = tc.figs.baseMem, tc.figs.baseCPU
			x.f.promValues[memQ(44)], x.f.promValues[cpuQ(41)] = tc.figs.mem, tc.figs.cpu
			x.f.mu.Unlock()
			if tc.fail {
				x.f.fail("prom.QueryInstant", errLeaky)
			}
			paceRun(x.f, phaseLengths, nil)
			r, ch := x.enqueueOf("a", sc)
			x.start()
			_, end := awaitEnd(t, ch)
			requireEnded(t, end, RunCompleted, Reason{})

			asked := x.f.callList("prom.QueryInstant ")
			for i := range asked {
				asked[i] = strings.TrimPrefix(asked[i], "prom.QueryInstant ")
			}
			if len(asked) != 4 || !slices.Equal(slices.Sorted(slices.Values(asked[:2])), slices.Sorted(slices.Values([]string{memQ(40), cpuQ(40)}))) ||
				asked[2] != memQ(44) || asked[3] != cpuQ(41) {
				t.Errorf("queries asked %q; want baseline's two (%s, %s), then %s, then %s", asked, memQ(40), cpuQ(40), memQ(44), cpuQ(41))
			}

			check := func(what string, run Run) {
				t.Helper()
				b := run.Phases[0]
				if b.Measure != nil || b.FaultObserved != nil {
					t.Errorf("%s: baseline carries measure %v, fault_observed %v; want both null", what, b.Measure, showBool(b.FaultObserved))
				}
				wants := [2]struct {
					metric      string
					value, base float64
				}{
					{MetricTargetMemoryBytes, tc.figs.mem, tc.figs.baseMem},
					{MetricTargetCPUCores, tc.figs.cpu, tc.figs.baseCPU},
				}
				for j, w := range wants {
					p := run.Phases[j+1]
					switch {
					case p.Measure == nil:
						t.Errorf("%s: phase %s has no measure", what, p.Name)
					case p.Measure.Metric != w.metric:
						t.Errorf("%s: phase %s measures %s; want %s", what, p.Name, p.Measure.Metric, w.metric)
					case tc.fail && (p.Measure.Value != nil || p.Measure.Baseline != nil):
						t.Errorf("%s: phase %s figures %v, %v; want both null", what, p.Name, show(p.Measure.Value), show(p.Measure.Baseline))
					case !tc.fail && !(sameFigure(p.Measure.Value, &w.value) && sameFigure(p.Measure.Baseline, &w.base)):
						t.Errorf("%s: phase %s figures %v, %v; want %v, %v", what, p.Name, show(p.Measure.Value), show(p.Measure.Baseline), w.value, w.base)
					}
					if !sameBool(p.FaultObserved, tc.want[j]) {
						t.Errorf("%s: phase %s fault_observed %v; want %v", what, p.Name, showBool(p.FaultObserved), showBool(tc.want[j]))
					}
				}
				body, err := json.Marshal(run)
				if err != nil {
					t.Fatalf("%s: the run does not marshal: %v", what, err)
				}
				assertNoLeak(t, string(body))
			}
			check("the end event", end)
			v, err := x.w.View(context.Background(), r.RunID)
			if err != nil {
				t.Fatalf("View: %v", err)
			}
			check("the kept view", v)
		})
	}
}

// TestWorker_ProofDelaysTheEndAtMostQueryTimeout (Review Focus 4): with every
// proof query ignoring its deadline, each phase's proof waits at most
// QueryTimeout: the run completes, its end comes within QueryTimeout (and a
// little scheduling) of the experiment's completion, both fault phases carry
// a measure with null figures and fault_observed null, and nothing of the
// error reaches the run. Not parallel: it times the end.
func TestWorker_ProofDelaysTheEndAtMostQueryTimeout(t *testing.T) {
	const timeout = 250 * time.Millisecond
	x := newHarness(t, noCap)
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	m := newScriptedMetrics()
	for _, svc := range CatalogTargets() {
		for _, k := range []FaultKind{FaultMemory, FaultCPU} {
			for _, s := range []int{40, 41, 44} {
				m.set(targetQuery(k, svc, time.Duration(s)*time.Second), ignoreDeadline(release))
			}
		}
	}
	x.cfg.QueryTimeout = timeout
	x.rebuild(func(d *Deps) { d.Prom = m })
	var completed atomic.Int64
	paceRun(x.f, phaseLengths, &completed)
	_, ch := x.enqueue("a")
	x.start()
	_, end := awaitEnd(t, ch)
	got := time.Now()
	requireEnded(t, end, RunCompleted, Reason{})
	if d := got.Sub(time.Unix(0, completed.Load())); d > timeout+150*time.Millisecond {
		t.Errorf("the end came %v after the experiment completed; want within QueryTimeout %v", d, timeout)
	}
	for _, p := range end.Phases[1:] {
		if p.Measure == nil || p.Measure.Value != nil || p.Measure.Baseline != nil || p.FaultObserved != nil {
			t.Errorf("phase %s: measure %+v, fault_observed %v; want a measure with null figures, null", p.Name, p.Measure, showBool(p.FaultObserved))
		}
	}
	calls, _, _, _ := m.snapshot()
	if n := len(callsOf(m)); n != 4 {
		t.Errorf("%d proof queries %v; want 4", n, calls)
	}
	free()
}

// TestView_StoredRunHasNoProof: a run read back from the database (after a
// restart) has no stored proof: each completed fault phase shows its metric
// with both figures null and fault_observed null, baseline shows null, and
// nothing is asked of Prometheus.
func TestView_StoredRunHasNoProof(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFakeLab(t, testNow)
	for _, id := range []string{"catalog-moderate", "cart-heavy"} {
		p := seedPair(t, f, scenario(t, id), "completed", false)
		for _, ph := range p.PhaseIDs {
			f.setPhaseStatus(ph, "completed")
		}
		w := NewWorker(f.deps(), workerConfig(), nil, nil)
		v, err := w.View(ctx, p.runID)
		if err != nil {
			t.Fatalf("%s: View: %v", id, err)
		}
		got := phaseFaults(t, v)
		want := [3][2]any{
			{nil, nil},
			{nil, map[string]any{"metric": "target_memory_bytes", "value": nil, "baseline": nil}},
			{nil, map[string]any{"metric": "target_cpu_cores", "value": nil, "baseline": nil}},
		}
		for i := range got {
			if fmt.Sprint(got[i][4], got[i][5]) != fmt.Sprint(want[i][0], want[i][1]) {
				t.Errorf("%s: phase %v: fault_observed %v, measure %v; want %v, %v", id, got[i][0], got[i][4], got[i][5], want[i][0], want[i][1])
			}
		}
	}
	if calls := f.callList("prom."); len(calls) != 0 {
		t.Errorf("reading stored runs asked Prometheus: %v", calls)
	}
}

// ---- helpers ----

func sameFigure(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameBool(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func show(v *float64) string {
	if v == nil {
		return "null"
	}
	return fmt.Sprint(*v)
}

func showBool(v *bool) string {
	if v == nil {
		return "null"
	}
	return fmt.Sprint(*v)
}

func measureJSON(t *testing.T, m Measure) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("the measure does not marshal: %v", err)
	}
	return string(b)
}
