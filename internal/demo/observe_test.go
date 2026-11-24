package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"slices"
	"strconv"
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

// TestTargetQuery (G1, G2): each proof query for each catalog target reads
// the target's own series over its phase's window, W whole seconds and at
// least 1, shifted back L seconds with PromQL's offset (no offset when L is
// 0): the memory phase's memory is its peak (max_over_time), the baseline's
// memory its floor (min_over_time), and CPU, of either phase, its rate. The
// live samples' two queries are the trial's with a 20 s rate. A service
// outside the catalog, a kind or figure outside the list, or an offset
// outside 0..300 panics, and the panic carries nothing of the name: no hostile
// character can reach a query, because no such service can be passed.
func TestTargetQuery(t *testing.T) {
	t.Parallel()
	if got := CatalogTargets(); !slices.Equal(got, []string{"cartservice", "productcatalogservice"}) {
		t.Fatalf("catalog targets %v; this test covers cartservice and productcatalogservice", got)
	}
	// The exact strings, for L = 0, 1, 7 and 300.
	for _, tc := range []struct {
		k      FaultKind
		of     figureOf
		offset int64
		want   string
	}{
		{FaultMemory, ofPhase, 0, `max_over_time(process_resident_memory_bytes{service="cartservice"}[40s])`},
		{FaultMemory, ofPhase, 1, `max_over_time(process_resident_memory_bytes{service="cartservice"}[40s] offset 1s)`},
		{FaultMemory, ofPhase, 7, `max_over_time(process_resident_memory_bytes{service="cartservice"}[40s] offset 7s)`},
		{FaultMemory, ofPhase, 300, `max_over_time(process_resident_memory_bytes{service="cartservice"}[40s] offset 300s)`},
		{FaultMemory, ofBaseline, 0, `min_over_time(process_resident_memory_bytes{service="cartservice"}[40s])`},
		{FaultMemory, ofBaseline, 1, `min_over_time(process_resident_memory_bytes{service="cartservice"}[40s] offset 1s)`},
		{FaultMemory, ofBaseline, 7, `min_over_time(process_resident_memory_bytes{service="cartservice"}[40s] offset 7s)`},
		{FaultMemory, ofBaseline, 300, `min_over_time(process_resident_memory_bytes{service="cartservice"}[40s] offset 300s)`},
		{FaultCPU, ofPhase, 0, `rate(process_cpu_seconds_total{service="cartservice"}[40s])`},
		{FaultCPU, ofPhase, 1, `rate(process_cpu_seconds_total{service="cartservice"}[40s] offset 1s)`},
		{FaultCPU, ofPhase, 7, `rate(process_cpu_seconds_total{service="cartservice"}[40s] offset 7s)`},
		{FaultCPU, ofPhase, 300, `rate(process_cpu_seconds_total{service="cartservice"}[40s] offset 300s)`},
		{FaultCPU, ofBaseline, 0, `rate(process_cpu_seconds_total{service="cartservice"}[40s])`},
		{FaultCPU, ofBaseline, 7, `rate(process_cpu_seconds_total{service="cartservice"}[40s] offset 7s)`},
	} {
		if got := targetQuery(tc.k, tc.of, targetCart, 40*time.Second, tc.offset); got != tc.want {
			t.Errorf("targetQuery(%s, %s, %s, 40s, %d) = %s; want %s", tc.k, tc.of, targetCart, tc.offset, got, tc.want)
		}
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
			for _, off := range []struct {
				l int64
				s string
			}{{0, ""}, {1, " offset 1s"}, {7, " offset 7s"}, {300, " offset 300s"}} {
				rng := `{service="` + svc + `"}[` + tc.w + `s]` + off.s + `)`
				for _, w := range []struct {
					k  FaultKind
					of figureOf
					q  string
				}{
					{FaultMemory, ofPhase, `max_over_time(process_resident_memory_bytes` + rng},
					{FaultMemory, ofBaseline, `min_over_time(process_resident_memory_bytes` + rng},
					{FaultCPU, ofPhase, `rate(process_cpu_seconds_total` + rng},
					{FaultCPU, ofBaseline, `rate(process_cpu_seconds_total` + rng},
				} {
					if got := targetQuery(w.k, w.of, svc, tc.span, off.l); got != w.q {
						t.Errorf("targetQuery(%s, %s, %s, %v, %d) = %s; want %s", w.k, w.of, svc, tc.span, off.l, got, w.q)
					}
					built = append(built, w.q)
				}
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
			if !strings.ContainsRune(`abcdefghijklmnopqrstuvwxyz0123456789_(){}[]=" `, c) {
				t.Errorf("query %s holds %q", q, c)
			}
		}
	}

	hostile := []string{
		"", "frontend", "Cartservice", " cartservice", "cartservice ", "cartservice\n", "cart-service",
		`cartservice"} or vector(1) or {service="`, "productcatalogservice}[1h])#", "192.0.2.10", "productcatalogservice\x00",
	}
	builders := map[string]func(svc string) string{
		"targetQuery memory":          func(svc string) string { return targetQuery(FaultMemory, ofPhase, svc, 40*time.Second, 0) },
		"targetQuery memory baseline": func(svc string) string { return targetQuery(FaultMemory, ofBaseline, svc, 40*time.Second, 7) },
		"targetQuery cpu":             func(svc string) string { return targetQuery(FaultCPU, ofPhase, svc, 40*time.Second, 0) },
		"liveTargetQuery memory":      func(svc string) string { return liveTargetQuery(FaultMemory, svc) },
		"liveTargetQuery cpu":         func(svc string) string { return liveTargetQuery(FaultCPU, svc) },
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
		if q, r := catchPanic(func() string { return targetQuery(k, ofPhase, targetCart, 40*time.Second, 0) }); r == nil {
			t.Errorf("targetQuery(%q, ...) built %q; want a panic", k, q)
		}
		if q, r := catchPanic(func() string { return liveTargetQuery(k, targetCart) }); r == nil {
			t.Errorf("liveTargetQuery(%q, ...) built %q; want a panic", k, q)
		}
	}
	for _, of := range []figureOf{-1, 2} {
		for _, k := range []FaultKind{FaultMemory, FaultCPU} {
			if q, r := catchPanic(func() string { return targetQuery(k, of, targetCart, 40*time.Second, 0) }); r == nil {
				t.Errorf("targetQuery(%s, figure %d, ...) built %q; want a panic", k, of, q)
			}
		}
	}
	for _, offset := range []int64{-1, 301, 3600} {
		if q, r := catchPanic(func() string { return targetQuery(FaultCPU, ofPhase, targetCart, 40*time.Second, offset) }); r == nil {
			t.Errorf("targetQuery with offset %d built %q; want a panic", offset, q)
		}
	}
}

// ---- reading figures and the proof ----

// readOne reads one figure as the worker does when the phase it covers
// completes: service's k figure of, over win, asked at win's end. It returns
// once figures has; the query's goroutine is counted in bg (nil: a group of
// its own).
func readOne(m Metrics, k FaultKind, of figureOf, service string, win window, timeout time.Duration, bg *sync.WaitGroup) *figureAsk {
	if bg == nil {
		bg = &sync.WaitGroup{}
	}
	q := &figureAsk{k: k, of: of, service: service, win: win}
	figures(context.Background(), m, make(chan struct{}, maxProofQueries), bg, win.end, timeout, []*figureAsk{q})
	return q
}

// groupDone is a channel closed once wg's Wait returns.
func groupDone(wg *sync.WaitGroup) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

// requireNotDone fails t if done closes within d: what done waits for is
// still counted. (It can only pass wrongly, never fail wrongly: done cannot
// close while the work is held.)
func requireNotDone(t *testing.T, done <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	select {
	case <-done:
		t.Errorf("%s ended while it was held; want it counted until it returns", what)
	case <-time.After(d):
	}
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
// query costs at most QueryTimeout. Its cases run one after another: each
// times its reads.
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
			m := newScriptedMetrics()
			m.set(targetQuery(tc.k, ofBaseline, targetCart, baseWin.span(), 0), tc.base)
			m.set(targetQuery(tc.k, ofPhase, targetCart, phaseWin.span(), 0), tc.phase)
			read := func(of figureOf, win window) *figureAsk {
				began := time.Now()
				q := readOne(m, tc.k, of, targetCart, win, timeout, nil)
				if d := time.Since(began); d > timeout+150*time.Millisecond {
					t.Errorf("a figure took %v; want it within QueryTimeout %v", d, timeout)
				}
				return q
			}
			b, p := read(ofBaseline, baseWin), read(ofPhase, phaseWin)
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
		m.value(targetQuery(k, ofPhase, targetCatalog, win.span(), 0), 1)
		if q := readOne(m, k, ofPhase, targetCatalog, win, 300*time.Millisecond, nil); q.v == nil || *q.v != 1 {
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
// problem, when its phase's end was read more than 300 s after its time, when
// its phase's start or end is unknown, when its service is not a catalog
// target (no panic), when its kind or figure is outside the list, and when
// there is no metrics client.
func TestObserve_WhatIsNotAsked(t *testing.T) {
	t.Parallel()
	win := window{start: testNow, end: testNow.Add(40 * time.Second)}
	m := newScriptedMetrics()
	m.value(targetQuery(FaultCPU, ofPhase, targetCart, win.span(), 0), 0.2)
	slots := make(chan struct{}, maxProofQueries)
	var bg sync.WaitGroup
	ask := func(m Metrics, q *figureAsk, now time.Time) {
		t.Helper()
		figures(context.Background(), m, slots, &bg, now, 100*time.Millisecond, []*figureAsk{q})
	}
	for _, tc := range []struct {
		name  string
		q     figureAsk
		now   time.Time
		asked bool
	}{
		{"seen at the end", figureAsk{k: FaultCPU, service: targetCart, win: win}, win.end, true},
		{"seen 301 s late", figureAsk{k: FaultCPU, service: targetCart, win: win}, win.end.Add(301 * time.Second), false},
		{"seen an hour late", figureAsk{k: FaultCPU, service: targetCart, win: win}, win.end.Add(time.Hour), false},
		{"no start", figureAsk{k: FaultCPU, service: targetCart, win: window{end: win.end}}, win.end, false},
		{"no end", figureAsk{k: FaultCPU, service: targetCart, win: window{start: win.start}}, win.end, false},
		{"not a catalog target", figureAsk{k: FaultCPU, service: "frontend", win: win}, win.end, false},
		{"a hostile service", figureAsk{k: FaultCPU, service: `cartservice"} or vector(1) or {x="`, win: win}, win.end, false},
		{"not a kind", figureAsk{k: "disk", service: targetCart, win: win}, win.end, false},
		{"not a figure", figureAsk{k: FaultCPU, of: 2, service: targetCart, win: win}, win.end, false},
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

// TestObserve_ReadOverItsOwnWindow (G1): however late a figure is read, its
// query covers its phase's own window: the window is shifted back by L, the
// whole seconds from the phase's end to the read (rounded down), with
// PromQL's offset, and L 0 has none. A read before the phase's end (a clock
// behind the store's) is L 0. Up to L 300 the figure is asked; from 301 on it
// is not, and is null.
func TestObserve_ReadOverItsOwnWindow(t *testing.T) {
	t.Parallel()
	win := window{start: testNow, end: testNow.Add(40500 * time.Millisecond)}
	const cpu = `rate(process_cpu_seconds_total{service="cartservice"}[40s]`
	const floor = `min_over_time(process_resident_memory_bytes{service="productcatalogservice"}[40s]`
	for _, tc := range []struct {
		name string
		lag  time.Duration
		k    FaultKind
		of   figureOf
		svc  string
		want string // "": not asked
	}{
		{"at the end", 0, FaultCPU, ofPhase, targetCart, cpu + `)`},
		{"1 s late", time.Second, FaultCPU, ofPhase, targetCart, cpu + ` offset 1s)`},
		{"1.999 s late", 1999 * time.Millisecond, FaultCPU, ofPhase, targetCart, cpu + ` offset 1s)`},
		{"7 s late", 7 * time.Second, FaultCPU, ofPhase, targetCart, cpu + ` offset 7s)`},
		{"7 s late, the baseline's memory", 7 * time.Second, FaultMemory, ofBaseline, targetCatalog, floor + ` offset 7s)`},
		{"300 s late", 300 * time.Second, FaultCPU, ofPhase, targetCart, cpu + ` offset 300s)`},
		{"300.9 s late", 300900 * time.Millisecond, FaultCPU, ofPhase, targetCart, cpu + ` offset 300s)`},
		{"301 s late", 301 * time.Second, FaultCPU, ofPhase, targetCart, ""},
		{"301 s late, the baseline's memory", 301 * time.Second, FaultMemory, ofBaseline, targetCatalog, ""},
		{"a clock half a second behind", -500 * time.Millisecond, FaultCPU, ofPhase, targetCart, cpu + `)`},
		{"a clock 7 s behind", -7 * time.Second, FaultCPU, ofPhase, targetCart, cpu + `)`},
		{"a clock an hour behind", -time.Hour, FaultMemory, ofBaseline, targetCatalog, floor + `)`},
	} {
		m := newScriptedMetrics()
		if tc.want != "" {
			m.value(tc.want, 0.25)
		}
		q := &figureAsk{k: tc.k, of: tc.of, service: tc.svc, win: win}
		figures(context.Background(), m, make(chan struct{}, maxProofQueries), &sync.WaitGroup{}, win.end.Add(tc.lag),
			100*time.Millisecond, []*figureAsk{q})
		asked := callsOf(m)
		switch {
		case tc.want == "" && (len(asked) != 0 || q.v != nil || q.problem == ""):
			t.Errorf("%s: asked %q, figure %v, problem %q; want nothing asked and null with a problem", tc.name, asked, show(q.v), q.problem)
		case tc.want != "" && (!slices.Equal(asked, []string{tc.want}) || q.v == nil || *q.v != 0.25):
			t.Errorf("%s: asked %q, figure %v (%s); want %s asked, 0.25", tc.name, asked, show(q.v), q.problem, tc.want)
		}
	}
}

// TestObserve_CutShort (G3): a read cut short asks nothing more. With its
// context already done, no figure's query reaches the client, even from a
// goroutine figures started: once figures' group is done, nothing was asked,
// and every figure is null with its problem. A query already in the client
// when the read is cut short is counted in the group until it returns, and
// figures does not wait for it.
func TestObserve_CutShort(t *testing.T) {
	t.Parallel()
	win := window{start: testNow, end: testNow.Add(40 * time.Second)}
	mem, cpu := targetQuery(FaultMemory, ofBaseline, targetCatalog, win.span(), 0), targetQuery(FaultCPU, ofBaseline, targetCatalog, win.span(), 0)
	asks := func() []*figureAsk {
		return []*figureAsk{
			{k: FaultMemory, of: ofBaseline, service: targetCatalog, win: win},
			{k: FaultCPU, of: ofBaseline, service: targetCatalog, win: win},
		}
	}

	t.Run("before the queries start", func(t *testing.T) {
		t.Parallel()
		m := newScriptedMetrics()
		m.value(mem, 45e6)
		m.value(cpu, 0.03)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var bg sync.WaitGroup
		qs := asks()
		figures(ctx, m, make(chan struct{}, maxProofQueries), &bg, win.end, time.Second, qs)
		waitClosed(t, groupDone(&bg), "figures' goroutines to end")
		if asked := callsOf(m); len(asked) != 0 {
			t.Errorf("a read cut short before its queries started asked %q; want nothing", asked)
		}
		for _, q := range qs {
			if q.v != nil || q.problem == "" {
				t.Errorf("%s figure %v, problem %q; want null with a problem", q.k, show(q.v), q.problem)
			}
		}
	})

	t.Run("in flight", func(t *testing.T) {
		t.Parallel()
		release := make(chan struct{})
		var once sync.Once
		free := func() { once.Do(func() { close(release) }) }
		t.Cleanup(free)
		m := newScriptedMetrics()
		m.set(mem, ignoreDeadline(release))
		m.set(cpu, ignoreDeadline(release))
		ctx, cancel := context.WithCancel(context.Background())
		var bg sync.WaitGroup
		returned := make(chan struct{})
		go func() {
			defer close(returned)
			figures(ctx, m, make(chan struct{}, maxProofQueries), &bg, win.end, 10*time.Second, asks())
		}()
		waitFor(t, "both queries in the client", func() bool { return m.count(mem) == 1 && m.count(cpu) == 1 })
		cancel()
		waitClosed(t, returned, "figures to return once cut short")
		done := groupDone(&bg)
		requireNotDone(t, done, 30*time.Millisecond, "figures' group, with both queries held in the client,")
		free()
		waitClosed(t, done, "figures' goroutines to end once their queries returned")
	})
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
	hung := targetQuery(FaultMemory, ofPhase, targetCatalog, win.span(), 0)
	m.set(hung, ignoreDeadline(release))
	slots := make(chan struct{}, maxProofQueries)
	var bg sync.WaitGroup
	before := runtime.NumGoroutine()
	for i := 0; i < 3*maxProofQueries; i++ {
		q := &figureAsk{k: FaultMemory, service: targetCatalog, win: win}
		began := time.Now()
		figures(context.Background(), m, slots, &bg, win.end, timeout, []*figureAsk{q})
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
	waitClosed(t, groupDone(&bg), "the hung queries to return")
	if n := len(slots); n != 0 {
		t.Errorf("%d slots held once every query returned; want 0", n)
	}
	m.value(hung, 5)
	q := &figureAsk{k: FaultMemory, service: targetCatalog, win: win}
	figures(context.Background(), m, slots, &bg, win.end, timeout, []*figureAsk{q})
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
	paceRunLate(f, lengths, [3]time.Duration{}, completedAt)
}

// paceRunLate is paceRun with each phase's end read lags[i] after it: the read
// that ends phase i moves the clock to its end plus lags[i] (the next phase
// still starts at the end), as when the worker sees an end late, or reads the
// last phase after the run's switch-off. Each lag must be shorter than the
// next phase's length plus its lag.
func paceRunLate(f *fakeLab, lengths, lags [3]time.Duration, completedAt *atomic.Int64) {
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
				f.clock.Advance(end.Add(lags[i]).Sub(f.clock.Now()))
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
// memory target's lowest memory and the CPU target's CPU over baseline's
// 40 s; when memory completes, the memory target's highest memory over its
// 44 s; when cpu completes, the CPU target's CPU over its 41 s; nothing else
// is asked. A rise that reaches the threshold is true, one below it false,
// and a Prometheus that fails gives null figures and fault_observed null on a
// run that completes and shows nothing of the error. The ended run's kept
// view says the same.
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
			memQ := func(of figureOf, s int) string {
				return targetQuery(FaultMemory, of, sc.Memory.Target, time.Duration(s)*time.Second, 0)
			}
			cpuQ := func(of figureOf, s int) string {
				return targetQuery(FaultCPU, of, sc.CPU.Target, time.Duration(s)*time.Second, 0)
			}
			x.f.mu.Lock()
			x.f.promValues[memQ(ofBaseline, 40)], x.f.promValues[cpuQ(ofBaseline, 40)] = tc.figs.baseMem, tc.figs.baseCPU
			x.f.promValues[memQ(ofPhase, 44)], x.f.promValues[cpuQ(ofPhase, 41)] = tc.figs.mem, tc.figs.cpu
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
			baseline := []string{memQ(ofBaseline, 40), cpuQ(ofBaseline, 40)}
			if len(asked) != 4 || !slices.Equal(slices.Sorted(slices.Values(asked[:2])), slices.Sorted(slices.Values(baseline))) ||
				asked[2] != memQ(ofPhase, 44) || asked[3] != cpuQ(ofPhase, 41) {
				t.Errorf("queries asked %q; want baseline's two %q, then %s, then %s", asked, baseline, memQ(ofPhase, 44), cpuQ(ofPhase, 41))
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

// seriesMetrics is a Prometheus over synthetic series, for the proof's
// queries: each series is a function of time, scraped every 10 s from origin
// on. It evaluates targetQuery's forms, and avg_over_time (so that a proof
// comparing averages would get its verdict here, not an error), as Prometheus
// does at its own now, which is clock's: over the samples in (t-W, t], t
// being now less the query's offset; max, min and average of them, and rate
// as the increase from the first sample to the last over their interval
// (Prometheus extrapolates a little; these series need none). Any other
// query, or a range with fewer than two samples, is an error. It lists the
// queries asked.
type seriesMetrics struct {
	clock  Clock
	origin time.Time
	series map[string]func(time.Time) float64 // by selector: metric{service="x"}

	mu    sync.Mutex
	asked []string
}

var seriesQuery = regexp.MustCompile(`^(max_over_time|min_over_time|avg_over_time|rate)\(([a-z_]+\{service="[a-z]+"\})\[([0-9]+)s\](?: offset ([0-9]+)s)?\)$`)

func (m *seriesMetrics) QueryInstant(_ context.Context, query string) (float64, error) {
	m.mu.Lock()
	m.asked = append(m.asked, query)
	m.mu.Unlock()
	g := seriesQuery.FindStringSubmatch(query)
	if g == nil {
		return 0, errors.New("series: not a proof query")
	}
	f := m.series[g[2]]
	if f == nil {
		return 0, errors.New("series: no such series")
	}
	w, _ := strconv.Atoi(g[3])
	offset := 0
	if g[4] != "" {
		offset, _ = strconv.Atoi(g[4])
	}
	const scrape = 10 * time.Second
	end := m.clock.Now().Add(-time.Duration(offset) * time.Second)
	start := end.Add(-time.Duration(w) * time.Second)
	var at []time.Time
	for s := m.origin.Add(start.Sub(m.origin).Truncate(scrape) - scrape); !s.After(end); s = s.Add(scrape) {
		if s.After(start) {
			at = append(at, s)
		}
	}
	if len(at) < 2 {
		return 0, errors.New("series: too few samples")
	}
	vs := make([]float64, len(at))
	sum := 0.0
	for i, s := range at {
		vs[i] = f(s)
		sum += vs[i]
	}
	switch g[1] {
	case "max_over_time":
		return slices.Max(vs), nil
	case "min_over_time":
		return slices.Min(vs), nil
	case "avg_over_time":
		return sum / float64(len(vs)), nil
	}
	return (vs[len(vs)-1] - vs[0]) / at[len(at)-1].Sub(at[0]).Seconds(), nil
}

func (m *seriesMetrics) Healthy(context.Context) bool { return true }

func (m *seriesMetrics) askedList() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.asked)
}

// TestWorker_MemoryProofSurvivesARelease (G2, G1): the previous run's memory
// is still held when this run's baseline starts, and is released 20 s into
// it (100 MiB, then 45 MiB); the memory phase rises from that floor to
// 61 MiB, which is still held through the cpu phase; the CPU burns 0.03 cores
// but 0.2 during the cpu phase. The memory proof compares the memory phase's
// peak (61 MiB) with the baseline's floor (45 MiB) and reads true, where the
// two phases' averages (56.2 and 72.5 MiB) would read false; the CPU proof
// compares the two phases' rates. Whether each phase's end is read at once
// or late (7 s, 7 s, and the cpu phase's 300 s, as after a slow switch-off),
// the figures are the same: each query is shifted back by its read's lag.
// Prometheus here evaluates the series as it would, at the lab's clock.
func TestWorker_MemoryProofSurvivesARelease(t *testing.T) {
	t.Parallel()
	const mib = 1 << 20
	sec := func(at time.Time) float64 { return at.Sub(testNow).Seconds() }
	// The phases (phaseLengths): baseline 0 to 40.5 s, memory to 85.4 s, cpu
	// to 126.6 s; scrapes at 5 s, 15 s, ...
	memory := func(at time.Time) float64 {
		switch s := sec(at); {
		case s < 20:
			return 100 * mib
		case s < 40.5:
			return 45 * mib
		case s < 85.4:
			return (45 + min(16, 4*math.Ceil((s-40.5)/10))) * mib
		}
		return 61 * mib
	}
	cpuSeconds := func(at time.Time) float64 {
		switch s := sec(at); {
		case s <= 85.4:
			return 0.03 * s
		case s <= 126.6:
			return 0.03*85.4 + 0.2*(s-85.4)
		default:
			return 0.03*85.4 + 0.2*(126.6-85.4) + 0.03*(s-126.6)
		}
	}
	const (
		memSel = `process_resident_memory_bytes{service="productcatalogservice"}`
		cpuSel = `process_cpu_seconds_total{service="productcatalogservice"}`
	)
	for _, tc := range []struct {
		name    string
		lags    [3]time.Duration
		offsets [3]string
	}{
		{"read at each end", [3]time.Duration{}, [3]string{"", "", ""}},
		{"read late", [3]time.Duration{7 * time.Second, 7 * time.Second, 300 * time.Second},
			[3]string{" offset 7s", " offset 7s", " offset 300s"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newHarness(t, noCap)
			sc := scenario(t, "catalog-moderate")
			if sc.Memory.Target != targetCatalog || sc.CPU.Target != targetCatalog {
				t.Fatalf("scenario %s targets %s and %s; the series are productcatalogservice's", sc.ID, sc.Memory.Target, sc.CPU.Target)
			}
			m := &seriesMetrics{clock: x.f.clock, origin: testNow.Add(5 * time.Second),
				series: map[string]func(time.Time) float64{memSel: memory, cpuSel: cpuSeconds}}
			x.rebuild(func(d *Deps) { d.Prom = m })
			paceRunLate(x.f, phaseLengths, tc.lags, nil)
			_, ch := x.enqueueOf("a", sc)
			x.start()
			_, end := awaitEnd(t, ch)
			requireEnded(t, end, RunCompleted, Reason{})
			if s := end.Phases[0].StartedAt; s == nil || !s.Equal(testNow) {
				t.Fatalf("baseline started at %v; the series assume %v", s, testNow)
			}

			asked := m.askedList()
			baseline := []string{
				"min_over_time(" + memSel + "[40s]" + tc.offsets[0] + ")",
				"rate(" + cpuSel + "[40s]" + tc.offsets[0] + ")",
			}
			slices.Sort(baseline)
			if len(asked) != 4 || !slices.Equal(slices.Sorted(slices.Values(asked[:2])), baseline) ||
				asked[2] != "max_over_time("+memSel+"[44s]"+tc.offsets[1]+")" || asked[3] != "rate("+cpuSel+"[41s]"+tc.offsets[2]+")" {
				t.Errorf("queries asked %q; want baseline's %q, then the memory phase's peak and the cpu phase's rate, offsets %q", asked, baseline, tc.offsets)
			}
			near := func(got *float64, want float64) bool { return got != nil && math.Abs(*got-want) <= 1e-9*max(1, want) }
			for i, w := range []struct {
				metric      string
				value, base float64
			}{
				{MetricTargetMemoryBytes, 61 * mib, 45 * mib},
				{MetricTargetCPUCores, 0.2, 0.03},
			} {
				p := end.Phases[i+1]
				switch {
				case p.Measure == nil:
					t.Errorf("phase %s: no measure; want %s %v over a baseline of %v", p.Name, w.metric, w.value, w.base)
				case p.Measure.Metric != w.metric || !near(p.Measure.Value, w.value) || !near(p.Measure.Baseline, w.base):
					t.Errorf("phase %s: measure %s %v over a baseline of %v; want %s %v over %v", p.Name, p.Measure.Metric,
						show(p.Measure.Value), show(p.Measure.Baseline), w.metric, w.value, w.base)
				case p.FaultObserved == nil || !*p.FaultObserved:
					t.Errorf("phase %s: fault_observed %v; want true (value %v, baseline %v)", p.Name, showBool(p.FaultObserved),
						show(p.Measure.Value), show(p.Measure.Baseline))
				}
			}
		})
	}
}

// TestWorker_ProofDelaysTheEndAtMostQueryTimeout (Review Focus 4): with every
// proof query ignoring its deadline, each phase's proof waits at most
// QueryTimeout: the run completes, its end comes within QueryTimeout (and a
// little scheduling) of the experiment's completion, both fault phases carry
// a measure with null figures and fault_observed null, and nothing of the
// error reaches the run. The queries still held are counted in the worker's
// group until they return (G3). Not parallel: it times the end.
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
			for _, of := range []figureOf{ofPhase, ofBaseline} {
				for _, s := range []int{40, 41, 44} {
					m.set(targetQuery(k, of, svc, time.Duration(s)*time.Second, 0), ignoreDeadline(release))
				}
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
	// The run has ended and no run waits: only the four held queries keep
	// the worker's group from being done.
	done := groupDone(&x.w.bg)
	requireNotDone(t, done, 30*time.Millisecond, "the worker's group, with four proof queries held in the client,")
	free()
	waitClosed(t, done, "the proof queries to return once released")
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
