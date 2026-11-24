package demo

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/id"
)

// ---- harness ----

// scriptedMetrics answers each query with its own function, and counts the
// calls, the calls in flight at once, and the time each call's context had.
type scriptedMetrics struct {
	mu         sync.Mutex
	answers    map[string]func(ctx context.Context) (float64, error)
	calls      map[string]int
	active     map[string]int
	maxActive  map[string]int
	deadlines  []time.Duration
	noDeadline int
}

func newScriptedMetrics() *scriptedMetrics {
	return &scriptedMetrics{
		answers:   map[string]func(context.Context) (float64, error){},
		calls:     map[string]int{},
		active:    map[string]int{},
		maxActive: map[string]int{},
	}
}

func (m *scriptedMetrics) set(query string, fn func(context.Context) (float64, error)) {
	m.mu.Lock()
	m.answers[query] = fn
	m.mu.Unlock()
}

func (m *scriptedMetrics) value(query string, v float64) {
	m.set(query, func(context.Context) (float64, error) { return v, nil })
}

func (m *scriptedMetrics) all(fn func(context.Context) (float64, error)) {
	for _, q := range []string{QueryRPS, QueryP95Ms, QueryErrorRate} {
		m.set(q, fn)
	}
}

func (m *scriptedMetrics) QueryInstant(ctx context.Context, query string) (float64, error) {
	m.mu.Lock()
	m.calls[query]++
	m.active[query]++
	m.maxActive[query] = max(m.maxActive[query], m.active[query])
	if dl, ok := ctx.Deadline(); ok {
		m.deadlines = append(m.deadlines, time.Until(dl))
	} else {
		m.noDeadline++
	}
	fn := m.answers[query]
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.active[query]--
		m.mu.Unlock()
	}()
	if fn == nil {
		return 0, errors.New("scripted: no answer for this query")
	}
	return fn(ctx)
}

func (m *scriptedMetrics) Healthy(context.Context) bool { return true }

func (m *scriptedMetrics) count(query string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[query]
}

func (m *scriptedMetrics) snapshot() (calls, maxActive map[string]int, deadlines []time.Duration, noDeadline int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.calls), maps.Clone(m.maxActive), slices.Clone(m.deadlines), m.noDeadline
}

// hang answers when ctx is done, as promql.Client does when its deadline
// passes.
func hang(ctx context.Context) (float64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// samplerHarness is a sampler over a worker that runs nothing; the test sets
// what Current reports.
type samplerHarness struct {
	t    *testing.T
	w    *Worker
	h    *Hub
	m    *scriptedMetrics
	s    *sampler
	logs *syncBuffer

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func newSamplerHarness(t *testing.T, every, timeout time.Duration) *samplerHarness {
	t.Helper()
	f := newFakeLab(t, testNow)
	deps := f.deps()
	logs := &syncBuffer{}
	deps.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	cfg := DefaultConfig()
	cfg.SampleEvery, cfg.QueryTimeout = every, timeout
	h := NewHub()
	w := NewWorker(deps, cfg, nil, h)
	m := newScriptedMetrics()
	return &samplerHarness{t: t, w: w, h: h, m: m, s: newSampler(w, m, h, cfg), logs: logs}
}

// setCurrent makes the worker report runID in phase (running or not); an
// empty runID means between runs.
func (x *samplerHarness) setCurrent(runID, phase string, running bool) {
	x.w.mu.Lock()
	defer x.w.mu.Unlock()
	if runID == "" {
		x.w.cur = nil
		return
	}
	x.w.cur = &active{runID: runID, phase: phase, phaseRunning: running}
}

// run starts the sampler; the test's cleanup stops it.
func (x *samplerHarness) run() {
	ctx, cancel := context.WithCancel(context.Background())
	x.cancel, x.done = cancel, make(chan struct{})
	go func() {
		defer close(x.done)
		x.s.run(ctx)
	}()
	x.t.Cleanup(x.stop)
}

func (x *samplerHarness) stop() {
	x.once.Do(func() {
		x.cancel()
		waitClosed(x.t, x.done, "the sampler to stop")
	})
}

// samples reads ch for d (d 0: only what is buffered) and returns the samples
// published, failing on any other event.
func samples(t *testing.T, ch <-chan Event, d time.Duration) []Sample {
	t.Helper()
	var out []Sample
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		var (
			ev Event
			ok bool
		)
		if d <= 0 {
			select {
			case ev, ok = <-ch:
			default:
				return out
			}
		} else {
			select {
			case ev, ok = <-ch:
			case <-timer.C:
				return out
			}
		}
		if !ok {
			return out
		}
		s, isSample := ev.Data.(Sample)
		if ev.Type != EventSample || !isSample {
			t.Errorf("event %s %T; want only samples", ev.Type, ev.Data)
			continue
		}
		out = append(out, s)
	}
}

// firstSample waits up to 3 s for a sample.
func firstSample(t *testing.T, ch <-chan Event) Sample {
	t.Helper()
	evs := readUntil(t, ch, "a sample", func(ev Event) bool { return ev.Type == EventSample })
	return evs[len(evs)-1].Data.(Sample)
}

func sampleJSON(t *testing.T, s Sample) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("the sample does not marshal: %v", err)
	}
	return string(b)
}

// ---- the brief's tests ----

// TestSampler_HangingPrometheus: every query hangs until its deadline
// (QueryTimeout 20 ms, SampleEvery 30 ms). Over 200 ms at least three rounds
// are made, each query on its own deadline, no sample is published, and the
// run goes on as if nothing happened: the worker keeps polling and the run
// completes.
func TestSampler_HangingPrometheus(t *testing.T) {
	t.Parallel()
	x := newHarness(t, noCap)
	_, ch := x.enqueue("a")
	x.start()
	waitFor(t, "a phase to run", func() bool { _, _, running := x.w.Current(); return running })

	m := newScriptedMetrics()
	m.all(hang)
	cfg := x.cfg
	cfg.QueryTimeout, cfg.SampleEvery = 20*time.Millisecond, 30*time.Millisecond
	s := newSampler(x.w, m, x.h, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.run(ctx)
	}()
	polls := len(x.f.callList("experiments.Get "))
	time.Sleep(200 * time.Millisecond)
	cancel()
	waitClosed(t, done, "the sampler to stop")

	calls, _, deadlines, noDeadline := m.snapshot()
	for _, q := range []string{QueryRPS, QueryP95Ms, QueryErrorRate} {
		if calls[q] < 3 {
			t.Errorf("%d calls of %s in 200 ms; want at least 3 rounds", calls[q], q)
		}
	}
	if noDeadline != 0 {
		t.Errorf("%d queries without a deadline", noDeadline)
	}
	for _, d := range deadlines {
		if d > cfg.QueryTimeout {
			t.Errorf("a query with %v left; want at most QueryTimeout %v", d, cfg.QueryTimeout)
		}
	}
	if n := len(x.f.callList("experiments.Get ")) - polls; n < 10 {
		t.Errorf("the worker polled %d times in 200 ms of hung queries; want it unaffected", n)
	}

	x.f.mu.Lock()
	x.f.autopilot = true
	x.f.mu.Unlock()
	evs, end := awaitEnd(t, ch)
	requireEnded(t, end, RunCompleted, Reason{})
	for _, ev := range evs {
		if ev.Type == EventSample {
			t.Errorf("a sample was published: %+v", ev.Data)
		}
	}
}

// TestSampler_NaNIsNull: rps 5, p95 NaN, error rate 0 -> rps 5, p95_ms null,
// error_rate 0; the three queries are the contract's, verbatim.
func TestSampler_NaNIsNull(t *testing.T) {
	t.Parallel()
	x := newSamplerHarness(t, 10*time.Millisecond, 500*time.Millisecond)
	x.m.value(QueryRPS, 5)
	x.m.value(QueryP95Ms, math.NaN())
	x.m.value(QueryErrorRate, 0)
	runID := id.New(RunIDPrefix)
	x.setCurrent(runID, PhaseNames[0], true)
	ch, unsub := x.h.Subscribe(runID)
	defer unsub()
	x.run()
	s := firstSample(t, ch)
	if got, want := sampleJSON(t, s), `{"run_id":"`+runID+`","phase":"plain","at":"2026-11-20T12:00:00.000Z","rps":5,"p95_ms":null,"error_rate":0,"target_cpu_cores":null,"target_memory_bytes":null}`; got != want {
		t.Errorf("sample\n %s\nwant\n %s", got, want)
	}
	x.stop()
	calls, _, _, _ := x.m.snapshot()
	if got := slices.Sorted(maps.Keys(calls)); !slices.Equal(got, slices.Sorted(slices.Values([]string{QueryRPS, QueryP95Ms, QueryErrorRate}))) {
		t.Errorf("queries %q; want exactly the contract's three", got)
	}
}

// TestSampler_OnlyWhilePhaseRunning: nothing is queried between runs or
// between phases; while a phase runs the sampler queries and publishes for
// that run and phase; once the run is gone it stops querying.
func TestSampler_OnlyWhilePhaseRunning(t *testing.T) {
	t.Parallel()
	x := newSamplerHarness(t, 10*time.Millisecond, 500*time.Millisecond)
	x.m.all(func(context.Context) (float64, error) { return 1, nil })
	runID := id.New(RunIDPrefix)
	ch, unsub := x.h.Subscribe(runID)
	defer unsub()
	x.run()

	time.Sleep(60 * time.Millisecond)
	x.setCurrent(runID, PhaseNames[1], false) // between phases
	time.Sleep(60 * time.Millisecond)
	if n := x.m.count(QueryRPS); n != 0 {
		t.Fatalf("%d queries while no phase ran; want none", n)
	}
	if got := samples(t, ch, 0); len(got) != 0 {
		t.Fatalf("%d samples while no phase ran", len(got))
	}

	x.setCurrent(runID, PhaseNames[1], true)
	s := firstSample(t, ch)
	if s.RunID != runID || s.Phase != PhaseNames[1] {
		t.Errorf("sample for %s/%s; want %s/%s", s.RunID, s.Phase, runID, PhaseNames[1])
	}

	x.setCurrent("", "", false)
	time.Sleep(30 * time.Millisecond) // a round in flight finishes
	n := x.m.count(QueryRPS)
	time.Sleep(60 * time.Millisecond)
	if m := x.m.count(QueryRPS); m != n {
		t.Errorf("%d queries after the run was gone; want none", m-n)
	}
}

// ---- beyond the brief ----

// TestSampler_NullsAndClamp: a failed or infinite value is null; the error
// rate is clamped into 0..1; a sample whose three values are all null is not
// published.
func TestSampler_NullsAndClamp(t *testing.T) {
	t.Parallel()
	x := newSamplerHarness(t, 10*time.Millisecond, 500*time.Millisecond)
	x.m.set(QueryRPS, func(context.Context) (float64, error) { return 0, errLeaky })
	x.m.value(QueryP95Ms, math.Inf(1))
	x.m.value(QueryErrorRate, 1.7)
	runID := id.New(RunIDPrefix)
	x.setCurrent(runID, PhaseNames[0], true)
	ch, unsub := x.h.Subscribe(runID)
	defer unsub()
	x.run()
	if got := sampleJSON(t, firstSample(t, ch)); !strings.HasSuffix(got, `"rps":null,"p95_ms":null,"error_rate":1,"target_cpu_cores":null,"target_memory_bytes":null}`) {
		t.Errorf("sample %s; want rps and p95 null, error_rate clamped to 1", got)
	}
	x.m.value(QueryErrorRate, -0.25)
	for i := 0; ; i++ {
		s := firstSample(t, ch)
		if s.ErrorRate != nil && *s.ErrorRate == 0 {
			break
		}
		if i == 20 {
			t.Fatalf("sample %s after the error rate went to -0.25; want error_rate clamped to 0", sampleJSON(t, s))
		}
	}
	x.m.set(QueryErrorRate, func(context.Context) (float64, error) { return math.NaN(), nil })
	samples(t, ch, 30*time.Millisecond) // what was in flight
	n := x.m.count(QueryRPS)
	if got := samples(t, ch, 80*time.Millisecond); len(got) != 0 {
		t.Errorf("%d samples with every value null; want none (first: %s)", len(got), sampleJSON(t, got[0]))
	}
	if x.m.count(QueryRPS) < n+3 {
		t.Errorf("the sampler stopped querying")
	}
}

// TestSampler_OneHungQueryDoesNotHoldTheOthers: a p95 query that ignores its
// deadline does not delay the other two or the next round: samples keep
// coming with p95_ms null, and no second p95 query is issued while the first
// hangs. Once it returns, p95 is sampled again.
func TestSampler_OneHungQueryDoesNotHoldTheOthers(t *testing.T) {
	t.Parallel()
	x := newSamplerHarness(t, 40*time.Millisecond, 30*time.Millisecond)
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	x.m.value(QueryRPS, 3)
	x.m.value(QueryErrorRate, 0.5)
	x.m.set(QueryP95Ms, func(context.Context) (float64, error) {
		<-release // ignores its context
		return 120, nil
	})
	runID := id.New(RunIDPrefix)
	x.setCurrent(runID, PhaseNames[0], true)
	ch, unsub := x.h.Subscribe(runID)
	defer unsub()
	x.run()
	got := samples(t, ch, 300*time.Millisecond)
	if len(got) < 4 {
		t.Errorf("%d samples in 300 ms at 40 ms with one query hung; want at least 4", len(got))
	}
	for _, s := range got {
		if s.RPS == nil || *s.RPS != 3 || s.ErrorRate == nil || *s.ErrorRate != 0.5 || s.P95Ms != nil {
			t.Errorf("sample %s; want rps 3, error_rate 0.5, p95_ms null", sampleJSON(t, s))
		}
	}
	if n := x.m.count(QueryP95Ms); n != 1 {
		t.Errorf("%d p95 queries while the first hung; want 1", n)
	}
	free()
	for {
		if s := firstSample(t, ch); s.P95Ms != nil {
			if *s.P95Ms != 120 {
				t.Errorf("p95 %v; want 120", *s.P95Ms)
			}
			break
		}
	}
}

// TestSampler_SkipsTicksWhileARoundRuns: with rounds that outlast the tick
// (QueryTimeout 45 ms over SampleEvery 10 ms; Config forbids it, the sampler
// must still cope), a tick that finds a round running is skipped: rounds
// never overlap and never pile up.
func TestSampler_SkipsTicksWhileARoundRuns(t *testing.T) {
	t.Parallel()
	x := newSamplerHarness(t, 10*time.Millisecond, 45*time.Millisecond)
	x.m.all(hang)
	x.setCurrent(id.New(RunIDPrefix), PhaseNames[0], true)
	x.run()
	time.Sleep(300 * time.Millisecond)
	x.stop()
	rounds, skipped := x.s.rounds.Load(), x.s.skipped.Load()
	if rounds > 300/45+2 {
		t.Errorf("%d rounds in 300 ms of 45 ms rounds; want at most %d: ticks piled up", rounds, 300/45+2)
	}
	if skipped < 10 {
		t.Errorf("%d ticks skipped; want most of the 30", skipped)
	}
	_, maxActive, _, _ := x.m.snapshot()
	for q, n := range maxActive {
		if n != 1 {
			t.Errorf("%d calls of %s at once; want 1", n, q)
		}
	}
}

// TestSampler_RoundStartDecides: a sample carries the run and phase current
// when its round started; it is dropped if that run ended (or another took
// its place) before the round finished.
func TestSampler_RoundStartDecides(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		meantime func(x *samplerHarness, runID string)
		want     string // the sample's phase, or "" for none
	}{
		{"phase moved on", func(x *samplerHarness, runID string) { x.setCurrent(runID, PhaseNames[1], true) }, PhaseNames[0]},
		{"run ended", func(x *samplerHarness, _ string) { x.setCurrent("", "", false) }, ""},
		{"another run", func(x *samplerHarness, _ string) { x.setCurrent(id.New(RunIDPrefix), PhaseNames[0], true) }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newSamplerHarness(t, 10*time.Millisecond, time.Second)
			gate := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(gate) }) })
			x.m.set(QueryRPS, func(ctx context.Context) (float64, error) {
				select {
				case <-gate:
					return 7, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			})
			runID := id.New(RunIDPrefix)
			x.setCurrent(runID, PhaseNames[0], true)
			ch, unsub := x.h.Subscribe(runID)
			defer unsub()
			x.run()
			waitFor(t, "a round to start", func() bool { return x.m.count(QueryRPS) >= 1 })
			tc.meantime(x, runID)
			once.Do(func() { close(gate) })
			got := samples(t, ch, 60*time.Millisecond)
			switch {
			case tc.want == "" && len(got) != 0:
				t.Errorf("%d samples for a run that is no longer current (first %s)", len(got), sampleJSON(t, got[0]))
			case tc.want != "" && (len(got) == 0 || got[0].Phase != tc.want || got[0].RunID != runID):
				t.Errorf("samples %v; want the first for %s/%s", got, runID, tc.want)
			}
		})
	}
}

// TestSampler_StopsWithItsContext: the sampler returns at once when its
// context ends, well within one tick, even with a round waiting on a query
// that ignores its context; once that query returns, no goroutine is left.
// Not parallel: it counts goroutines.
func TestSampler_StopsWithItsContext(t *testing.T) {
	x := newSamplerHarness(t, 200*time.Millisecond, 150*time.Millisecond)
	x.m.all(hang)
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	x.m.set(QueryRPS, func(context.Context) (float64, error) {
		<-release // ignores its context
		return 1, nil
	})
	x.setCurrent(id.New(RunIDPrefix), PhaseNames[0], true)
	before := runtime.NumGoroutine()
	x.run()
	waitFor(t, "queries in flight", func() bool { return x.m.count(QueryRPS) >= 1 && x.m.count(QueryP95Ms) >= 1 })
	began := time.Now()
	x.cancel()
	waitClosed(t, x.done, "the sampler to stop")
	if d := time.Since(began); d > 75*time.Millisecond {
		t.Errorf("the sampler took %v to stop; want at once, well within its 200 ms tick", d)
	}
	free()
	waitGoroutines(t, before, time.Second)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		RunSampler(context.Background(), x.w, nil, x.h, DefaultConfig())
	}()
	waitClosed(t, stopped, "RunSampler without Prometheus to return")
}

// TestSampler_LogsOncePerPhaseAndMetric: a failing query is logged with its
// detail once per phase and metric, not once a round; no sample carries any
// of it.
func TestSampler_LogsOncePerPhaseAndMetric(t *testing.T) {
	t.Parallel()
	x := newSamplerHarness(t, 5*time.Millisecond, 500*time.Millisecond)
	x.m.value(QueryRPS, 2)
	x.m.value(QueryErrorRate, 0)
	x.m.set(QueryP95Ms, func(context.Context) (float64, error) { return 0, errLeaky })
	runID := id.New(RunIDPrefix)
	x.setCurrent(runID, PhaseNames[0], true)
	ch, unsub := x.h.Subscribe(runID)
	defer unsub()
	x.run()
	waitFor(t, "eight rounds", func() bool { return x.m.count(QueryP95Ms) >= 8 })
	x.setCurrent(runID, PhaseNames[1], true)
	n := x.m.count(QueryP95Ms)
	waitFor(t, "eight more rounds", func() bool { return x.m.count(QueryP95Ms) >= n+8 })
	x.stop()

	var p95, others int
	for _, line := range strings.Split(x.logs.String(), "\n") {
		if !strings.Contains(line, "live metric") {
			continue
		}
		if strings.Contains(line, `"metric":"p95_ms"`) {
			p95++
			if !strings.Contains(line, "192.0.2.10") {
				t.Errorf("the log line lacks the error's detail: %s", line)
			}
		} else {
			others++
		}
	}
	if p95 != 2 || others != 0 {
		t.Errorf("%d p95 lines and %d others over two phases; want 2 and 0:\n%s", p95, others, x.logs)
	}
	for _, s := range samples(t, ch, 0) {
		assertNoLeak(t, sampleJSON(t, s))
	}
}
