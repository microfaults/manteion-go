package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// MANTEION_CONVERGE_TIMEOUT bounds the convergence barrier; zero or a
// negative value would fail every phase entry at its first look, so startup
// refuses it, and refuses a value that is not a duration rather than silently
// running on the default.
func TestPositiveDurationEnv(t *testing.T) {
	const key = "MANTEION_TEST_POSITIVE_DURATION"
	for _, tc := range []struct {
		val     string
		want    time.Duration
		wantErr string
	}{
		{val: "", want: 30 * time.Second}, // unset: the fallback
		{val: "45s", want: 45 * time.Second},
		{val: "2m", want: 2 * time.Minute},
		{val: "0s", wantErr: "must be positive"},
		{val: "-5s", wantErr: "must be positive"},
		{val: "soon", wantErr: "not a duration"},
	} {
		t.Setenv(key, tc.val)
		got, err := positiveDurationEnv(key, 30*time.Second)
		switch {
		case tc.wantErr != "":
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), key) {
				t.Errorf("%q: err = %v, want one naming %s and saying %q", tc.val, err, key, tc.wantErr)
			}
		case err != nil || got != tc.want:
			t.Errorf("%q: = %v, %v; want %v", tc.val, got, err, tc.want)
		}
	}
}

// Both barrier bounds are read through positiveDurationEnv, capped at
// maxBarrierTimeout. The drain bound sizes the HTTP write timeout next to the
// converge bound, and a negative one would shrink that deadline (far enough
// below zero, to none at all); zero would end every drain before an instance
// could report; and a huge value of either would overflow the deadline to a
// negative duration. Startup refuses all of them, naming the key.
func TestBarrierTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		converge, drain         string
		wantConverge, wantDrain time.Duration
		wantErr                 string
	}{
		{name: "unset: the defaults", wantConverge: 30 * time.Second, wantDrain: 30 * time.Second},
		{name: "both set", converge: "45s", drain: "2m", wantConverge: 45 * time.Second, wantDrain: 2 * time.Minute},
		{name: "converge zero", converge: "0s", wantErr: "MANTEION_CONVERGE_TIMEOUT"},
		{name: "drain negative", drain: "-90s", wantErr: "MANTEION_DRAIN_TIMEOUT=\"-90s\" must be positive"},
		{name: "drain zero", drain: "0s", wantErr: "MANTEION_DRAIN_TIMEOUT=\"0s\" must be positive"},
		{name: "drain not a duration", drain: "soon", wantErr: "MANTEION_DRAIN_TIMEOUT=\"soon\" is not a duration"},
		{name: "both at the bound", converge: "1h", drain: "60m", wantConverge: time.Hour, wantDrain: time.Hour},
		{name: "converge above the bound", converge: "61m", wantErr: "MANTEION_CONVERGE_TIMEOUT=\"61m\" must be at most 1h0m0s"},
		{name: "drain near the largest duration", drain: "2562047h", wantErr: "MANTEION_DRAIN_TIMEOUT=\"2562047h\" must be at most 1h0m0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MANTEION_CONVERGE_TIMEOUT", tc.converge)
			t.Setenv("MANTEION_DRAIN_TIMEOUT", tc.drain)
			converge, drain, err := barrierTimeouts()
			switch {
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("err = %v, want one containing %q", err, tc.wantErr)
				}
			case err != nil || converge != tc.wantConverge || drain != tc.wantDrain:
				t.Errorf("= %v, %v, %v; want %v, %v", converge, drain, err, tc.wantConverge, tc.wantDrain)
			}
		})
	}
}

// MANTEION_POLL_INTERVAL sets how often the orchestrator asks zeus whether a
// phase's load has ended. Unset keeps the poller's own 15 s; zero would spin
// (and panics time.NewTicker), a typo would silently run on the default, and a
// value above a minute would leave a phase running long after its load ended.
func TestPollIntervalEnv(t *testing.T) {
	for _, tc := range []struct {
		name, val string
		want      time.Duration
		wantErr   string
	}{
		{name: "unset: the poller's default", want: 15 * time.Second},
		{name: "the lab's 2s", val: "2s", want: 2 * time.Second},
		{name: "at the bound", val: "1m", want: time.Minute},
		{name: "zero", val: "0s", wantErr: "MANTEION_POLL_INTERVAL=\"0s\" must be positive"},
		{name: "negative", val: "-2s", wantErr: "MANTEION_POLL_INTERVAL=\"-2s\" must be positive"},
		{name: "not a duration", val: "abc", wantErr: "MANTEION_POLL_INTERVAL=\"abc\" is not a duration"},
		{name: "above the bound", val: "2m", wantErr: "MANTEION_POLL_INTERVAL=\"2m\" must be at most 1m0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MANTEION_POLL_INTERVAL", tc.val)
			got, err := pollIntervalEnv()
			switch {
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("err = %v, want one containing %q", err, tc.wantErr)
				}
			case err != nil || got != tc.want:
				t.Errorf("= %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

// The HTTP server's write deadline must outlast what a stop can wait out
// before it answers: the drain barrier's convergence wait and report gate,
// then the exit bump's convergence wait. Otherwise a stop that succeeded has
// its 202 cut off.
func TestHTTPWriteTimeoutOutlastsAStop(t *testing.T) {
	for _, tc := range []struct{ converge, drain time.Duration }{
		{30 * time.Second, 30 * time.Second},
		{2 * time.Minute, 30 * time.Second},
		{10 * time.Second, 5 * time.Minute},
		{maxBarrierTimeout, maxBarrierTimeout}, // the largest values startup accepts
	} {
		got := httpWriteTimeout(tc.converge, tc.drain)
		if stop := tc.converge + tc.drain + tc.converge; got <= stop {
			t.Errorf("httpWriteTimeout(%v, %v) = %v, want more than a stop's %v of waits", tc.converge, tc.drain, got, stop)
		}
		if floor := 2*tc.converge + 30*time.Second; got < floor {
			t.Errorf("httpWriteTimeout(%v, %v) = %v, want at least 2 × the converge timeout + 30s (%v)", tc.converge, tc.drain, got, floor)
		}
	}
}

// ---- demo mode ----

// Demo mode refuses a policy engine: the OPA harness ("rego" or "on") and the
// threshold engine both switch rules on and off by themselves, and the demo
// owns its run's rule. Without demo mode, any engine is allowed.
func TestDemoModeCheck(t *testing.T) {
	for _, tc := range []struct {
		demo    bool
		mode    string
		wantErr bool
	}{
		{true, "rego", true},
		{true, "on", true},
		{true, "threshold", true},
		{true, "off", false},
		{false, "rego", false},
	} {
		err := demoModeCheck(tc.demo, tc.mode)
		switch {
		case tc.wantErr && err == nil:
			t.Errorf("demoModeCheck(%v, %q) = nil, want an error", tc.demo, tc.mode)
		case !tc.wantErr && err != nil:
			t.Errorf("demoModeCheck(%v, %q) = %v, want nil", tc.demo, tc.mode, err)
		case err != nil && (!strings.Contains(err.Error(), "MANTEION_DEMO") || !strings.Contains(err.Error(), "MANTEION_POLICY_ENGINE")):
			t.Errorf("demoModeCheck(%v, %q) = %v, want a message naming MANTEION_DEMO and MANTEION_POLICY_ENGINE", tc.demo, tc.mode, err)
		}
	}
}

// The demo listener's server: its handler is the demo's own, unwrapped (the
// event stream and the unread-body guard reach the connection through it),
// its timeouts bound slow clients, net/http does not answer OPTIONS * before
// the handler, and it speaks HTTP/1.1 only.
func TestNewDemoServer(t *testing.T) {
	h := http.NewServeMux()
	s := newDemoServer("127.0.0.1:8090", h)
	if s.Addr != "127.0.0.1:8090" {
		t.Errorf("Addr = %q, want %q", s.Addr, "127.0.0.1:8090")
	}
	if s.Handler != http.Handler(h) {
		t.Errorf("Handler = %T, want the demo's handler itself, with nothing around it", s.Handler)
	}
	for _, d := range []struct {
		name      string
		got, want time.Duration
	}{
		{"ReadHeaderTimeout", s.ReadHeaderTimeout, 5 * time.Second},
		{"ReadTimeout", s.ReadTimeout, 10 * time.Second},
		{"WriteTimeout", s.WriteTimeout, 30 * time.Second},
		{"IdleTimeout", s.IdleTimeout, 60 * time.Second},
	} {
		if d.got != d.want {
			t.Errorf("%s = %v, want %v", d.name, d.got, d.want)
		}
	}
	if s.MaxHeaderBytes != 16<<10 {
		t.Errorf("MaxHeaderBytes = %d, want %d", s.MaxHeaderBytes, 16<<10)
	}
	if !s.DisableGeneralOptionsHandler {
		t.Error("DisableGeneralOptionsHandler = false: net/http would answer OPTIONS * itself, before the demo's handler")
	}
	if p := s.Protocols; p == nil || !p.HTTP1() || p.HTTP2() || p.UnencryptedHTTP2() {
		t.Errorf("Protocols = %v, want HTTP/1 only", p)
	}
}

// callLog records calls in the order they happen, from any goroutine.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, s)
}

func (l *callLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

// untilDeadline is how long ctx has left, or -1 when it has no deadline.
func untilDeadline(ctx context.Context) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok {
		return -1
	}
	return time.Until(dl)
}

// fakeDemo is a demoLifecycle that records its calls and what each was given.
type fakeDemo struct {
	log *callLog

	mu            sync.Mutex
	switchOffErrs []error         // returned in turn, then nil
	switchOffLeft []time.Duration // each attempt's time to its deadline
	sweepErr      error
	sweepGate     chan struct{} // when not nil, Sweep waits for it to close
	sweepLeft     time.Duration
	ran           chan struct{} // closed by Run

	shutdownErr      error
	shutdownTook     time.Duration
	shutdownCtxErr   error // the context's Err when Shutdown was called
	shutdownLeft     time.Duration
	shutdownReturned time.Time
}

func newFakeDemo(log *callLog) *fakeDemo { return &fakeDemo{log: log, ran: make(chan struct{})} }

func (f *fakeDemo) SwitchOffAll(ctx context.Context) error {
	f.log.add("SwitchOffAll")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.switchOffLeft = append(f.switchOffLeft, untilDeadline(ctx))
	if len(f.switchOffErrs) == 0 {
		return nil
	}
	err := f.switchOffErrs[0]
	f.switchOffErrs = f.switchOffErrs[1:]
	return err
}

func (f *fakeDemo) Sweep(ctx context.Context) error {
	f.log.add("Sweep")
	left := untilDeadline(ctx)
	if f.sweepGate != nil {
		<-f.sweepGate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweepLeft = left
	return f.sweepErr
}

func (f *fakeDemo) Run(context.Context) {
	f.log.add("Run")
	close(f.ran)
}

func (f *fakeDemo) Shutdown(ctx context.Context) error {
	f.log.add("demo.Shutdown")
	f.mu.Lock()
	f.shutdownCtxErr, f.shutdownLeft = ctx.Err(), untilDeadline(ctx)
	f.mu.Unlock()
	time.Sleep(f.shutdownTook)
	f.mu.Lock()
	f.shutdownReturned = time.Now()
	f.mu.Unlock()
	f.log.add("demo.Shutdown returned")
	return f.shutdownErr
}

// fakeServer is a shutdowner that records its call and its deadline.
type fakeServer struct {
	name     string
	log      *callLog
	err      error
	deadline time.Time
}

func (s *fakeServer) Shutdown(ctx context.Context) error {
	s.log.add(s.name + ".Shutdown")
	s.deadline, _ = ctx.Deadline()
	return s.err
}

// testLogger logs JSON lines into buf.
func testLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, nil))
}

// fastRetries is defaultDemoTimings with a short pause between switch-off
// attempts; every deadline stays the default.
func fastRetries() demoTimings {
	t := defaultDemoTimings
	t.switchOffPause = time.Millisecond
	return t
}

// The numbers the start-up and shutdown orders run on.
func TestDefaultDemoTimings(t *testing.T) {
	d := defaultDemoTimings
	if d.switchOffTimeout != 10*time.Second || d.switchOffAttempts != 3 || d.sweepTimeout != 2*time.Minute ||
		d.shutdownTimeout != 15*time.Second {
		t.Errorf("defaultDemoTimings = %+v, want a switch-off of 3 attempts of 10 s each, a 2 min sweep and a 15 s shutdown", d)
	}
	if serverShutdownBudget != 10*time.Second {
		t.Errorf("serverShutdownBudget = %v, want 10s", serverShutdownBudget)
	}
}

// startDemoReturns runs startDemo and fails the test if it does not return
// within 5 s (the sweep must run in a goroutine of its own).
func startDemoReturns(t *testing.T, ctx context.Context, d demoLifecycle, tm demoTimings, listen func(), log *slog.Logger) {
	t.Helper()
	ret := make(chan struct{})
	go func() {
		defer close(ret)
		startDemo(ctx, d, tm, listen, log)
	}()
	select {
	case <-ret:
	case <-time.After(5 * time.Second):
		t.Fatal("startDemo did not return while the sweep runs: the sweep and the worker must run in a goroutine of their own")
	}
}

// waitRan waits for the fake's Run.
func waitRan(t *testing.T, f *fakeDemo) {
	t.Helper()
	select {
	case <-f.ran:
	case <-time.After(5 * time.Second):
		t.Fatalf("Run was never called; calls: %v", f.log.get())
	}
}

// Start-up: the switch-off comes before the listeners (the operator listener
// serves desired state the moment it is up), the sweep after them (its
// cancels wait on the fleet, which reaches manteion only through that
// listener) and before the worker, under a 2-minute deadline; startDemo
// returns with the sweep still running.
func TestStartDemo_Order(t *testing.T) {
	calls := &callLog{}
	f := newFakeDemo(calls)
	f.sweepGate = make(chan struct{})
	var buf bytes.Buffer
	startDemoReturns(t, context.Background(), f, fastRetries(), func() { calls.add("listen") }, testLogger(&buf))
	if got := calls.get(); len(got) < 2 || !slices.Equal(got[:2], []string{"SwitchOffAll", "listen"}) || slices.Contains(got, "Run") {
		t.Fatalf("calls when startDemo returned = %v, want SwitchOffAll, listen, and no Run yet", got)
	}
	close(f.sweepGate)
	waitRan(t, f)
	if got, want := calls.get(), []string{"SwitchOffAll", "listen", "Sweep", "Run"}; !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if left := f.switchOffLeft[0]; left <= 9*time.Second || left > 10*time.Second {
		t.Errorf("the switch-off's deadline was %v away, want about 10s", left)
	}
	if f.sweepLeft <= 119*time.Second || f.sweepLeft > 2*time.Minute {
		t.Errorf("the sweep's deadline was %v away, want about 2m", f.sweepLeft)
	}
}

// A failed switch-off is tried again, each attempt with a deadline of its own,
// before the listeners start.
func TestStartDemo_SwitchOffRetried(t *testing.T) {
	calls := &callLog{}
	f := newFakeDemo(calls)
	f.switchOffErrs = []error{errors.New("db down"), errors.New("db down")}
	var buf bytes.Buffer
	startDemoReturns(t, context.Background(), f, fastRetries(), func() { calls.add("listen") }, testLogger(&buf))
	waitRan(t, f)
	if got, want := calls.get(), []string{"SwitchOffAll", "SwitchOffAll", "SwitchOffAll", "listen", "Sweep", "Run"}; !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, left := range f.switchOffLeft {
		if left <= 9*time.Second || left > 10*time.Second {
			t.Errorf("attempt %d's deadline was %v away, want about 10s of its own", i+1, left)
		}
	}
}

// A switch-off that still fails after three attempts is logged at error
// level, and start-up goes on: the sweep repeats the step, and no run is
// accepted before the sweep has run.
func TestStartDemo_SwitchOffFailureDoesNotStopStartUp(t *testing.T) {
	calls := &callLog{}
	f := newFakeDemo(calls)
	f.switchOffErrs = []error{errors.New("db down 1"), errors.New("db down 2"), errors.New("db down 3"), errors.New("db down 4")}
	var buf bytes.Buffer
	startDemoReturns(t, context.Background(), f, fastRetries(), func() { calls.add("listen") }, testLogger(&buf))
	waitRan(t, f)
	if got, want := calls.get(), []string{"SwitchOffAll", "SwitchOffAll", "SwitchOffAll", "listen", "Sweep", "Run"}; !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	if out := buf.String(); !strings.Contains(out, `"level":"ERROR"`) || !strings.Contains(out, "db down 3") {
		t.Errorf("log = %s, want an ERROR line with the last attempt's error", out)
	}
}

// A signal during start-up stops the retries; the start-up goes on and main's
// shutdown follows.
func TestStartDemo_SignalStopsRetries(t *testing.T) {
	calls := &callLog{}
	f := newFakeDemo(calls)
	f.switchOffErrs = []error{context.Canceled, context.Canceled, context.Canceled}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	startDemoReturns(t, ctx, f, fastRetries(), func() { calls.add("listen") }, testLogger(&buf))
	waitRan(t, f)
	if got, want := calls.get(), []string{"SwitchOffAll", "listen", "Sweep", "Run"}; !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}

// A sweep that fails is logged and does not keep the worker from running.
func TestStartDemo_SweepErrorStillRuns(t *testing.T) {
	calls := &callLog{}
	f := newFakeDemo(calls)
	f.sweepErr = errors.New("sweep incomplete")
	var buf bytes.Buffer
	startDemoReturns(t, context.Background(), f, fastRetries(), func() { calls.add("listen") }, testLogger(&buf))
	waitRan(t, f)
	if got, want := calls.get(), []string{"SwitchOffAll", "listen", "Sweep", "Run"}; !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	if out := buf.String(); !strings.Contains(out, "sweep incomplete") {
		t.Errorf("log = %s, want the sweep's error", out)
	}
}

// Shutdown: the demo's Shutdown returns before either server's Shutdown is
// called (it switches the run's rule off, which the SDKs pick up only from
// the operator listener, and ends every event stream, which a server's
// Shutdown would otherwise wait out). Its context is its own, 15 s long, not
// the signal's; the servers' 10 s budget starts once it has returned, and the
// after step runs within what is left of it.
func TestShutdownInOrder_DemoFirst(t *testing.T) {
	calls := &callLog{}
	f := newFakeDemo(calls)
	f.shutdownTook = 20 * time.Millisecond
	demoSrv := &fakeServer{name: "demoServer", log: calls}
	srv := &fakeServer{name: "server", log: calls}
	var afterLeft time.Duration
	var buf bytes.Buffer
	err := shutdownInOrder(f, demoSrv, srv, defaultDemoTimings, func(ctx context.Context) {
		calls.add("after")
		afterLeft = untilDeadline(ctx)
	}, testLogger(&buf))
	if err != nil {
		t.Errorf("shutdownInOrder = %v, want nil", err)
	}
	want := []string{"demo.Shutdown", "demo.Shutdown returned", "demoServer.Shutdown", "server.Shutdown", "after"}
	if got := calls.get(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.shutdownCtxErr != nil {
		t.Errorf("the demo's Shutdown got a context already done (%v), want one of its own", f.shutdownCtxErr)
	}
	if f.shutdownLeft <= 14*time.Second || f.shutdownLeft > 15*time.Second {
		t.Errorf("the demo's Shutdown deadline was %v away, want about 15s", f.shutdownLeft)
	}
	if !demoSrv.deadline.Equal(srv.deadline) {
		t.Errorf("server deadlines %v and %v differ, want one budget for both", demoSrv.deadline, srv.deadline)
	}
	if earliest := f.shutdownReturned.Add(serverShutdownBudget); srv.deadline.Before(earliest) {
		t.Errorf("the servers' deadline %v is before the demo's return + %v (%v): the budget must start after the demo's shutdown",
			srv.deadline, serverShutdownBudget, earliest)
	}
	if afterLeft <= 9*time.Second || afterLeft > serverShutdownBudget {
		t.Errorf("after's deadline was %v away, want what is left of the 10 s budget", afterLeft)
	}
}

// The demo's Shutdown error is logged and changes nothing else: the servers
// still shut down, and the exit status does not change.
func TestShutdownInOrder_DemoErrorLogged(t *testing.T) {
	calls := &callLog{}
	f := newFakeDemo(calls)
	f.shutdownErr = errors.New("rule still on")
	demoSrv := &fakeServer{name: "demoServer", log: calls}
	srv := &fakeServer{name: "server", log: calls}
	var buf bytes.Buffer
	err := shutdownInOrder(f, demoSrv, srv, defaultDemoTimings, func(context.Context) { calls.add("after") }, testLogger(&buf))
	if err != nil {
		t.Errorf("shutdownInOrder = %v, want nil: the demo's error does not change the exit status", err)
	}
	want := []string{"demo.Shutdown", "demo.Shutdown returned", "demoServer.Shutdown", "server.Shutdown", "after"}
	if got := calls.get(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	if out := buf.String(); !strings.Contains(out, "rule still on") {
		t.Errorf("log = %s, want the demo's error", out)
	}
}

// A server that does not shut down cleanly is logged and returned (main
// exits 1), and the other server and the after step still run.
func TestShutdownInOrder_ServerErrors(t *testing.T) {
	calls := &callLog{}
	f := newFakeDemo(calls)
	demoSrv := &fakeServer{name: "demoServer", log: calls, err: errors.New("demo listener stuck")}
	srv := &fakeServer{name: "server", log: calls, err: errors.New("operator listener stuck")}
	var buf bytes.Buffer
	err := shutdownInOrder(f, demoSrv, srv, defaultDemoTimings, func(context.Context) { calls.add("after") }, testLogger(&buf))
	if err == nil || !strings.Contains(err.Error(), "demo listener stuck") || !strings.Contains(err.Error(), "operator listener stuck") {
		t.Errorf("shutdownInOrder = %v, want both servers' errors", err)
	}
	if got := calls.get(); !slices.Contains(got, "server.Shutdown") || !slices.Contains(got, "after") {
		t.Errorf("calls = %v, want the operator server shut down and the after step run", got)
	}
	if out := buf.String(); !strings.Contains(out, "demo listener stuck") || !strings.Contains(out, "operator listener stuck") {
		t.Errorf("log = %s, want both servers' errors", out)
	}
}

// Without demo mode the shutdown is today's: the operator server under the
// 10 s budget, then the after step; its error is returned.
func TestShutdownInOrder_DemoOff(t *testing.T) {
	calls := &callLog{}
	srv := &fakeServer{name: "server", log: calls, err: errors.New("stuck")}
	var buf bytes.Buffer
	err := shutdownInOrder(nil, nil, srv, defaultDemoTimings, func(context.Context) { calls.add("after") }, testLogger(&buf))
	if err == nil || !strings.Contains(err.Error(), "stuck") {
		t.Errorf("shutdownInOrder = %v, want the server's error", err)
	}
	if got, want := calls.get(), []string{"server.Shutdown", "after"}; !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
	if left := time.Until(srv.deadline); left <= 9*time.Second || left > serverShutdownBudget {
		t.Errorf("the server's deadline was %v away, want about 10s", left)
	}
}
