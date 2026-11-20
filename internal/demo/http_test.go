package demo

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// The contract's six paths (the events route is registered by the next task).
const (
	pathHealth = "/api/v1/demo/health"
	pathState  = "/api/v1/demo/state"
	pathRuns   = "/api/v1/demo/runs"
	pathRecent = "/api/v1/demo/runs/recent"
)

func pathRun(runID string) string { return pathRuns + "/" + runID }

// errLeakyAll carries everything a response must never show: a driver
// prefix, a SQL fragment, an IP address, a hostname, a run's internal name,
// and experiment, phase and rule ids.
var errLeakyAll = errors.New(`pq: SELECT id FROM experiments WHERE name = 'demo-run-x': ` +
	`dial tcp 10.0.0.250:5432: lookup pg-0.db.example.internal: exp-123 phase-9 rule-7`)

var leakMarkers = []string{"pq:", "SELECT", "10.0.0.250", "192.0.2.10", "example.internal",
	"dial tcp", "demo-run-", "exp-", "phase-", "rule-", "spec-", "panic", "goroutine"}

// requireNoLeak fails when the response's body or any header value carries
// anything internal.
func requireNoLeak(t *testing.T, what string, rec *httptest.ResponseRecorder) {
	t.Helper()
	parts := []string{rec.Body.String()}
	for k, vs := range rec.Header() {
		parts = append(parts, k+": "+strings.Join(vs, ","))
	}
	all := strings.Join(parts, "\n")
	for _, bad := range leakMarkers {
		if strings.Contains(all, bad) {
			t.Errorf("%s: the response carries %q:\n%s", what, bad, all)
		}
	}
}

// demoConfig is the test configuration: the brief's limits, a short status
// poll and query deadline, a run cap no test reaches.
func demoConfig() Config {
	cfg := DefaultConfig()
	cfg.EdgeSecret = NewSecret(goodSecret)
	cfg.StatusPoll = 5 * time.Millisecond
	cfg.QueryTimeout = 250 * time.Millisecond
	cfg.RunCap = 10 * time.Second
	return cfg
}

// demoHarness is one Demo over one fake lab, its log in a buffer.
type demoHarness struct {
	t    *testing.T
	f    *fakeLab
	d    *Demo
	h    http.Handler
	logs *syncBuffer
}

// newDemo is a demo that is ready (as after its start-up sweep), its worker
// not running.
func newDemo(t *testing.T, tweak func(*Config, *Deps)) *demoHarness {
	t.Helper()
	x := newUnsweptDemo(t, tweak)
	x.d.markReady()
	return x
}

// newUnsweptDemo is a demo whose start-up sweep has not run.
func newUnsweptDemo(t *testing.T, tweak func(*Config, *Deps)) *demoHarness {
	t.Helper()
	f := newFakeLab(t, testNow)
	cfg := demoConfig()
	deps := f.deps()
	logs := &syncBuffer{}
	deps.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	if tweak != nil {
		tweak(&cfg, &deps)
	}
	d := New(deps, cfg)
	d.w.offRetry = testOffRetry
	return &demoHarness{t: t, f: f, d: d, h: d.Handler(), logs: logs}
}

// start runs the demo's worker. The test's cleanup shuts the demo down and
// waits for the worker and its goroutines.
func (x *demoHarness) start() {
	ctx, cancel := context.WithCancel(context.Background())
	go x.d.Run(ctx)
	x.t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer scancel()
		_ = x.d.Shutdown(sctx)
		cancel()
		waitClosed(x.t, x.d.w.done, "the worker to stop")
		bg := make(chan struct{})
		go func() { x.d.w.bg.Wait(); close(bg) }()
		waitClosed(x.t, bg, "the worker's goroutines to end")
	})
}

// serve sends one request through the demo's handler.
func (x *demoHarness) serve(method, target string, hdr http.Header, body io.Reader) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, body)
	for k, vs := range hdr {
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	x.h.ServeHTTP(rec, r)
	return rec
}

func clientHeader(client string) http.Header {
	h := http.Header{}
	if client != "" {
		h.Set(HeaderClient, client)
	}
	return h
}

func (x *demoHarness) get(target, client string) *httptest.ResponseRecorder {
	return x.serve(http.MethodGet, target, clientHeader(client), nil)
}

// post queues a run for client, with the right edge secret.
func (x *demoHarness) post(client string) *httptest.ResponseRecorder {
	h := clientHeader(client)
	h.Set(HeaderEdgeSecret, goodSecret)
	return x.serve(http.MethodPost, pathRuns, h, nil)
}

// postOK queues a run for client and returns its id.
func (x *demoHarness) postOK(client string) string {
	x.t.Helper()
	rec := x.post(client)
	if rec.Code != http.StatusCreated {
		x.t.Fatalf("POST as %s: %d %s; want 201", client, rec.Code, rec.Body)
	}
	return decodeBody(x.t, rec)["run_id"].(string)
}

// health answers GET /health and its decoded checks.
func (x *demoHarness) health() (*httptest.ResponseRecorder, map[string]any) {
	x.t.Helper()
	rec := x.get(pathHealth, "health")
	return rec, decodeBody(x.t, rec)["checks"].(map[string]any)
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body %q is not a JSON object: %v", rec.Body, err)
	}
	return m
}

// requireError checks an error response: its status, its exact body, and a
// Retry-After on a 429 and nowhere else.
func requireError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Errorf("status %d; want %d (body %s)", rec.Code, status, rec.Body)
	}
	if want := `{"error":"` + code + `"}`; rec.Body.String() != want {
		t.Errorf("body %s; want %s", rec.Body, want)
	}
	if status == http.StatusTooManyRequests {
		retryAfter(t, rec)
	} else if ra := rec.Header().Get("Retry-After"); ra != "" {
		t.Errorf("Retry-After %q on a %d", ra, status)
	}
}

// retryAfter returns a 429's Retry-After, which must be a positive integer.
func retryAfter(t *testing.T, rec *httptest.ResponseRecorder) int {
	t.Helper()
	v := rec.Header().Get("Retry-After")
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || strconv.Itoa(n) != v {
		t.Errorf("Retry-After %q; want a positive integer number of seconds", v)
	}
	return n
}

// allChecks is the health checks with every one true but those named.
func allChecks(failing ...string) map[string]any {
	m := map[string]any{}
	for _, k := range []string{"database", "load_generator", "metrics", "target", "idle"} {
		m[k] = !slices.Contains(failing, k)
	}
	return m
}

func requireChecks(t *testing.T, got map[string]any, failing ...string) {
	t.Helper()
	want := allChecks(failing...)
	for k, v := range want {
		if got[k] != v {
			t.Errorf("checks.%s = %v; want %v (all: %v)", k, got[k], v, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("checks %v; want exactly %v", got, want)
	}
}

// ---- POST /runs ----

// TestPostRun_Secret: without the edge secret, or with any other value, POST
// is 403 forbidden, the same body whatever was wrong, and nothing is queued;
// with it, 201 with exactly run_id, status and position.
func TestPostRun_Secret(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	for _, tc := range []struct {
		name   string
		values []string
	}{
		{"none", nil},
		{"empty", []string{""}},
		{"wrong", []string{"not-the-secret"}},
		{"longer", []string{goodSecret + "x"}},
		{"shorter", []string{goodSecret[:len(goodSecret)-1]}},
		{"a guess before the right one", []string{"guess", goodSecret}},
		{"the right one twice", []string{goodSecret, goodSecret}},
	} {
		h := clientHeader("a")
		for _, v := range tc.values {
			h.Add(HeaderEdgeSecret, v)
		}
		rec := x.serve(http.MethodPost, pathRuns, h, nil)
		requireError(t, rec, http.StatusForbidden, "forbidden")
	}
	if n := len(x.d.q.Snapshot()); n != 0 {
		t.Fatalf("%d runs queued by refused requests", n)
	}
	rec := x.post("a")
	if rec.Code != http.StatusCreated {
		t.Fatalf("with the secret: %d %s; want 201", rec.Code, rec.Body)
	}
	m := decodeBody(t, rec)
	wantKeys(t, "201 body", m, "run_id", "status", "position")
	if m["status"] != "queued" || m["position"] != float64(0) || !validRunID(m["run_id"].(string)) {
		t.Errorf("201 body %s; want a run id, queued, position 0", rec.Body)
	}
}

// TestPostRun_FourthIsQueueFull: three runs fill the line (positions 0, 1, 2);
// a fourth client gets 429 queue_full, told to wait the three phases of the
// run in front.
func TestPostRun_FourthIsQueueFull(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	for i, c := range []string{"a", "b", "c"} {
		rec := x.post(c)
		if rec.Code != http.StatusCreated || decodeBody(t, rec)["position"] != float64(i) {
			t.Fatalf("POST %s: %d %s; want 201 at position %d", c, rec.Code, rec.Body, i)
		}
	}
	rec := x.post("d")
	requireError(t, rec, http.StatusTooManyRequests, "queue_full")
	if n := retryAfter(t, rec); n != 3*PhaseDurationS {
		t.Errorf("Retry-After %d; want %d (the running run's three phases)", n, 3*PhaseDurationS)
	}
	if n := len(x.d.q.Snapshot()); n != 3 {
		t.Errorf("%d runs in line; want 3", n)
	}
}

// TestPostRun_SameClient: a client with a run in line gets client_has_run; once
// that run has ended, it may not enqueue again until five minutes after its
// enqueue (rate_limited), and may at five minutes.
func TestPostRun_SameClient(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	runID := x.postOK("a")
	again := x.post("a")
	requireError(t, again, http.StatusTooManyRequests, "client_has_run")
	if n := retryAfter(t, again); n != 300 {
		t.Errorf("client_has_run Retry-After %d; want 300 (its enqueue limit outlasts its run's 120 s)", n)
	}

	// The run ends at once: preflight finds the load generator down.
	x.f.fail("zeus.Healthy", errLeakyAll)
	x.start()
	waitFor(t, "the run to end", func() bool {
		r, err := x.d.w.View(context.Background(), runID)
		return err == nil && r.Position == nil
	})

	x.f.clock.Advance(4 * time.Minute)
	limited := x.post("a")
	requireError(t, limited, http.StatusTooManyRequests, "rate_limited")
	if n := retryAfter(t, limited); n != 60 {
		t.Errorf("rate_limited Retry-After %d; want 60", n)
	}
	x.f.clock.Advance(time.Minute)
	if rec := x.post("a"); rec.Code != http.StatusCreated {
		t.Errorf("at +5 min: %d %s; want 201", rec.Code, rec.Body)
	}
}

// TestPostRun_RecordedOnlyAfter201: a refused POST (queue_full,
// client_has_run, forbidden, unavailable) does not count as the client's
// enqueue.
func TestPostRun_RecordedOnlyAfter201(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	first := x.postOK("a")
	x.postOK("b")
	x.postOK("c")
	requireError(t, x.post("x"), http.StatusTooManyRequests, "queue_full")
	requireError(t, x.post("x"), http.StatusTooManyRequests, "queue_full")
	requireError(t, x.serve(http.MethodPost, pathRuns, clientHeader("x"), nil), http.StatusForbidden, "forbidden")
	if !x.d.lim.AllowEnqueue("x") {
		t.Fatal("refused POSTs counted as an enqueue")
	}
	x.d.q.remove(first) // a slot frees
	if rec := x.post("x"); rec.Code != http.StatusCreated {
		t.Fatalf("after a slot freed: %d %s; want 201", rec.Code, rec.Body)
	}
	if x.d.lim.AllowEnqueue("x") {
		t.Error("a 201 was not counted")
	}
}

// TestPostRun_UnavailableUntilSwept: before the start-up sweep has run, POST
// is 503 unavailable and the lab reports not accepting; reads work. A sweep
// that reports incomplete still opens the lab, and is logged.
func TestPostRun_UnavailableUntilSwept(t *testing.T) {
	t.Parallel()
	x := newUnsweptDemo(t, nil)
	requireError(t, x.post("a"), http.StatusServiceUnavailable, "unavailable")
	if rec := x.get(pathState, "a"); rec.Code != http.StatusOK || decodeBody(t, rec)["accepting"] != false {
		t.Errorf("state before the sweep: %d %s; want 200, not accepting", rec.Code, rec.Body)
	}
	if rec, _ := x.health(); decodeBody(t, rec)["accepting"] != false {
		t.Errorf("health before the sweep: %s; want not accepting", rec.Body)
	}
	x.f.fail("rules.List", errLeakyAll)
	err := x.d.Sweep(context.Background())
	if !errors.Is(err, ErrSweepIncomplete) {
		t.Fatalf("Sweep: %v; want ErrSweepIncomplete", err)
	}
	if !logLineHas(x.logs.String(), `"level":"ERROR"`, "accepting runs anyway") {
		t.Errorf("the incomplete sweep was not logged:\n%s", x.logs)
	}
	if rec := x.post("a"); rec.Code != http.StatusCreated {
		t.Errorf("after the sweep: %d %s; want 201", rec.Code, rec.Body)
	}
	if rec := x.get(pathState, "a"); decodeBody(t, rec)["accepting"] != true {
		t.Errorf("state after the sweep: %s; want accepting", rec.Body)
	}
}

// TestPostRun_QueueClosed: once the worker was halted the queue is closed:
// 503 unavailable, and the lab reports not accepting.
func TestPostRun_QueueClosed(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	x.d.w.halt()
	requireError(t, x.post("a"), http.StatusServiceUnavailable, "unavailable")
	if rec := x.get(pathState, "a"); decodeBody(t, rec)["accepting"] != false {
		t.Errorf("state: %s; want not accepting", rec.Body)
	}
}

// TestPostRun_Body: POST takes no body; a small one is ignored, a larger one
// (declared or chunked) is 400 bad_request and closes the connection.
func TestPostRun_Body(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	h := clientHeader("a")
	h.Set(HeaderEdgeSecret, goodSecret)
	if rec := x.serve(http.MethodPost, pathRuns, h, strings.NewReader(`{"preset":"evil"}`)); rec.Code != http.StatusCreated {
		t.Fatalf("small body: %d %s; want 201", rec.Code, rec.Body)
	}
	big := strings.Repeat("x", 2048)
	h.Set(HeaderClient, "b")
	rec := x.serve(http.MethodPost, pathRuns, h, strings.NewReader(big))
	requireError(t, rec, http.StatusBadRequest, "bad_request")
	if rec.Header().Get("Connection") != "close" {
		t.Errorf("Connection %q; want close", rec.Header().Get("Connection"))
	}
	r := httptest.NewRequest(http.MethodPost, pathRuns, strings.NewReader(big))
	r.ContentLength = -1 // chunked: only reading finds out
	r.Header = h.Clone()
	chunked := httptest.NewRecorder()
	x.h.ServeHTTP(chunked, r)
	requireError(t, chunked, http.StatusBadRequest, "bad_request")
	requireError(t, x.serve(http.MethodGet, pathState, clientHeader("b"), strings.NewReader(big)), http.StatusBadRequest, "bad_request")
	if n := len(x.d.q.Snapshot()); n != 1 {
		t.Errorf("%d runs queued; want 1", n)
	}
}

// TestPostRun_Concurrent: racing POSTs fill the line exactly; one client
// racing itself gets one run.
func TestPostRun_Concurrent(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	codes := make([]int, 24)
	var wg sync.WaitGroup
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := fmt.Sprintf("c%d", i)
			if i >= 12 {
				client = "same"
			}
			codes[i] = x.post(client).Code
		}()
	}
	wg.Wait()
	created := 0
	for _, c := range codes {
		if c == http.StatusCreated {
			created++
		} else if c != http.StatusTooManyRequests {
			t.Errorf("status %d; want 201 or 429", c)
		}
	}
	if created != 3 || len(x.d.q.Snapshot()) != 3 {
		t.Errorf("%d created, %d in line; want 3 and 3", created, len(x.d.q.Snapshot()))
	}
	same := 0
	x.d.q.mu.Lock()
	for _, e := range x.d.q.line {
		if e.client == "same" {
			same++
		}
	}
	x.d.q.mu.Unlock()
	if same > 1 {
		t.Errorf("client 'same' holds %d runs", same)
	}
}

// ---- client key ----

// TestClientKey: no X-Demo-Client is the one shared client "local"; a key
// over 200 bytes, empty, sent twice, not UTF-8 or with a control character is
// 400 bad_request on every route, and nothing is queued or counted.
func TestClientKey(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	h := http.Header{}
	h.Set(HeaderEdgeSecret, goodSecret)
	if rec := x.serve(http.MethodPost, pathRuns, h, nil); rec.Code != http.StatusCreated {
		t.Fatalf("no client key: %d; want 201", rec.Code)
	}
	requireError(t, x.serve(http.MethodPost, pathRuns, h, nil), http.StatusTooManyRequests, "client_has_run")
	requireError(t, x.post(DefaultClient), http.StatusTooManyRequests, "client_has_run")
	if rec := x.post(strings.Repeat("k", MaxClientBytes)); rec.Code != http.StatusCreated {
		t.Errorf("a %d-byte key: %d; want 201", MaxClientBytes, rec.Code)
	}

	bad := map[string][]string{
		"too long":       {strings.Repeat("k", MaxClientBytes+1)},
		"empty":          {""},
		"twice":          {"a", "b"},
		"control char":   {"a\x01b"},
		"DEL":            {"a\x7fb"},
		"tab":            {"a\tb"},
		"C1 control":     {"a\u0085b"},
		"invalid UTF-8":  {"a\xffb"},
		"newline inside": {"a\nb"},
	}
	for name, values := range bad {
		hdr := http.Header{}
		hdr.Set(HeaderEdgeSecret, goodSecret)
		hdr[HeaderClient] = values
		for _, target := range []string{pathHealth, pathState, pathRecent, pathRun(id.New(RunIDPrefix))} {
			rec := x.serve(http.MethodGet, target, hdr, nil)
			if rec.Code != http.StatusBadRequest || rec.Body.String() != `{"error":"bad_request"}` {
				t.Errorf("%s, GET %s: %d %s; want 400 bad_request", name, target, rec.Code, rec.Body)
			}
		}
		rec := x.serve(http.MethodPost, pathRuns, hdr, nil)
		if rec.Code != http.StatusBadRequest || rec.Body.String() != `{"error":"bad_request"}` {
			t.Errorf("%s, POST: %d %s; want 400 bad_request", name, rec.Code, rec.Body)
		}
	}
	if n := len(x.d.q.Snapshot()); n != 2 {
		t.Errorf("%d runs queued; want 2", n)
	}
	if n := x.d.lim.clientCount(); n != 2 {
		t.Errorf("%d clients in the limiter; want 2 (local and the 200-byte key)", n)
	}
}

// ---- reads ----

// TestReads_RateLimitedPerClient: every GET route counts against one budget of
// 120 reads a minute per client; the 121st is 429 rate_limited for that
// client only; POST is not a read.
func TestReads_RateLimitedPerClient(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	for i := 0; i < 120; i++ {
		if rec := x.get(pathState, "a"); rec.Code != http.StatusOK {
			t.Fatalf("read %d: %d", i+1, rec.Code)
		}
	}
	rec := x.get(pathState, "a")
	requireError(t, rec, http.StatusTooManyRequests, "rate_limited")
	if n := retryAfter(t, rec); n != 60 {
		t.Errorf("Retry-After %d; want 60", n)
	}
	if rec := x.get(pathState, "b"); rec.Code != http.StatusOK {
		t.Errorf("client b: %d; want 200", rec.Code)
	}
	if rec := x.post("a"); rec.Code != http.StatusCreated {
		t.Errorf("POST from a rate-limited reader: %d; want 201", rec.Code)
	}

	unknown := pathRun(id.New(RunIDPrefix))
	targets := []string{pathHealth, pathState, pathRecent, unknown}
	for i := 0; i < 120; i++ {
		target := targets[i%len(targets)]
		if rec := x.get(target, "c"); rec.Code == http.StatusTooManyRequests {
			t.Fatalf("read %d (%s) refused", i+1, target)
		}
	}
	for _, target := range targets {
		requireError(t, x.get(target, "c"), http.StatusTooManyRequests, "rate_limited")
	}
	x.f.clock.Advance(time.Minute)
	if rec := x.get(pathState, "a"); rec.Code != http.StatusOK {
		t.Errorf("a minute later: %d; want 200", rec.Code)
	}
}

// ---- unknown routes ----

// unknownRoutes are requests that must all answer JSON 404 not_found: other
// paths, other methods on known paths, and paths that are not in their
// canonical form. The events path is not here: the next task registers it.
var unknownRoutes = []struct{ method, target string }{
	{"GET", "/api/v1/rules"},
	{"GET", "/api/v1/experiments"},
	{"GET", "/api/v1/sdk/desired-state"},
	{"GET", "/healthz"},
	{"GET", "/readyz"},
	{"GET", "/metrics"},
	{"GET", "/"},
	{"GET", "/api"},
	{"GET", "/api/v1/demo"},
	{"GET", "/api/v1/demo/"},
	{"GET", "/api/v1/demo/state/"},
	{"GET", "//api/v1/demo/state"},
	{"GET", "/api/v1/demo//state"},
	{"GET", "/api/v1/demo/./state"},
	{"GET", "/api/v1/demo/x/../state"},
	{"GET", "/api/v1/demo/%73tate"},
	{"GET", "/api/v1/demo/runs%2Frecent"},
	{"GET", "/api/v1/demo/runs/recent/"},
	{"GET", "/API/V1/DEMO/STATE"},
	{"GET", "/api/v1/demo/runs"},
	{"GET", "/api/v1/demo/runs/not-a-run"},
	{"GET", "/api/v1/demo/runs/exp-123"},
	{"GET", "/api/v1/demo/runs/DEMO-0F1E2D3C-4B5A-6978-8796-A5B4C3D2E1F0"},
	{"GET", "/api/v1/demo/runs/demo-0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0/other"},
	{"POST", "/api/v1/demo/state"},
	{"POST", "/api/v1/demo/health"},
	{"POST", "/api/v1/demo/runs/recent"},
	{"POST", "/api/v1/demo/runs/"},
	{"PUT", "/api/v1/demo/runs"},
	{"DELETE", "/api/v1/demo/runs"},
	{"PATCH", "/api/v1/demo/state"},
	{"PUT", "/api/v1/demo/state"},
	{"OPTIONS", "/api/v1/demo/runs"},
	{"OPTIONS", "/api/v1/demo/state"},
	{"HEAD", "/api/v1/demo/state"},
	{"HEAD", "/api/v1/demo/health"},
	{"TRACE", "/api/v1/demo/state"},
	{"GET", "/api/v1/rules/rule-7?host=10.0.0.250"},
}

func requireNotFound(t *testing.T, what string, status int, body string, hdr http.Header, method string) {
	t.Helper()
	if status != http.StatusNotFound {
		t.Errorf("%s: status %d; want 404", what, status)
	}
	if method != http.MethodHead && body != `{"error":"not_found"}` {
		t.Errorf("%s: body %q; want {\"error\":\"not_found\"}", what, body)
	}
	for _, h := range []string{"Location", "Allow", "Server", "Access-Control-Allow-Origin", "Retry-After"} {
		if v := hdr.Get(h); v != "" {
			t.Errorf("%s: %s: %q; want none", what, h, v)
		}
	}
	if ct := hdr.Get("Content-Type"); ct != "application/json" {
		t.Errorf("%s: Content-Type %q; want application/json", what, ct)
	}
}

// TestUnknownRoutes404: through the handler.
func TestUnknownRoutes404(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	for _, rt := range unknownRoutes {
		rec := x.serve(rt.method, rt.target, clientHeader("a"), nil)
		requireNotFound(t, rt.method+" "+rt.target, rec.Code, rec.Body.String(), rec.Header(), rt.method)
		requireNoLeak(t, rt.method+" "+rt.target, rec)
	}
	// A path outside the six is not a read: it never runs into the limit.
	for i := 0; i < 200; i++ {
		if rec := x.get("/api/v1/rules", "u"); rec.Code != http.StatusNotFound {
			t.Fatalf("request %d: %d; want 404", i+1, rec.Code)
		}
	}
}

// TestUnknownRoutes404_RealServer: the same requests through a real net/http
// server and client that does not follow redirects, so nothing net/http or
// ServeMux adds (a redirect for a cleaned path, a 405 with Allow, a Server
// header) can hide. Also OPTIONS *, with the server option the wiring sets.
func TestUnknownRoutes404_RealServer(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	srv := httptest.NewUnstartedServer(x.h)
	srv.Config.DisableGeneralOptionsHandler = true
	srv.Start()
	t.Cleanup(srv.Close)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, rt := range unknownRoutes {
		req, err := http.NewRequest(rt.method, srv.URL+rt.target, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", rt.method, rt.target, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		requireNotFound(t, rt.method+" "+rt.target, resp.StatusCode, string(body), resp.Header, rt.method)
	}

	// A known route answers through the same server, with no Server header.
	resp, err := client.Get(srv.URL + pathState)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Server") != "" {
		t.Errorf("GET state: %d, Server %q; want 200 and no Server", resp.StatusCode, resp.Header.Get("Server"))
	}

	// OPTIONS * reaches the handler only with DisableGeneralOptionsHandler.
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "OPTIONS * HTTP/1.1\r\nHost: demo\r\nConnection: close\r\n\r\n")
	raw, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(raw.Body)
	raw.Body.Close()
	requireNotFound(t, "OPTIONS *", raw.StatusCode, string(body), raw.Header, "OPTIONS")
}

// TestResponseHeaders: every response is JSON, never cached, never sniffed,
// and carries no CORS, Server or redirect header.
func TestResponseHeaders(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	runID := x.postOK("a")
	recs := map[string]*httptest.ResponseRecorder{
		"health":         x.get(pathHealth, "a"),
		"state":          x.get(pathState, "a"),
		"recent":         x.get(pathRecent, "a"),
		"run":            x.get(pathRun(runID), "a"),
		"run not found":  x.get(pathRun("nope"), "a"),
		"client_has_run": x.post("a"),
		"forbidden":      x.serve(http.MethodPost, pathRuns, clientHeader("b"), nil),
		"bad_request":    x.get(pathState, strings.Repeat("k", 300)),
		"unknown":        x.get("/api/v1/rules", "a"),
	}
	for name, rec := range recs {
		for h, want := range map[string]string{
			"Content-Type":           "application/json",
			"Cache-Control":          "no-store",
			"X-Content-Type-Options": "nosniff",
		} {
			if got := rec.Header().Get(h); got != want {
				t.Errorf("%s: %s %q; want %q", name, h, got, want)
			}
		}
		for h := range rec.Header() {
			if strings.HasPrefix(h, "Access-Control-") || h == "Server" || h == "Location" || h == "X-Powered-By" || h == "Allow" {
				t.Errorf("%s: header %s present", name, h)
			}
		}
	}
}

// ---- health ----

// TestHealth: 200 when every check passes; the answer is cached for HealthTTL
// (two calls within 5 s probe once); a failing Prometheus is 503 with
// checks.metrics false.
func TestHealth(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	rec := x.get(pathHealth, "a")
	want := `{"ok":true,"accepting":true,"checks":{"database":true,"load_generator":true,"metrics":true,"target":true,"idle":true}}`
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("health: %d %s; want 200 %s", rec.Code, rec.Body, want)
	}
	probes := func() []int {
		var n []int
		for _, op := range []string{"db.PingContext", "zeus.Healthy", "prom.Healthy", "instances.InstancesForService", "experiments.RunningExperimentIDs"} {
			n = append(n, len(x.f.callList(op)))
		}
		return n
	}
	if p := probes(); !slices.Equal(p, []int{1, 1, 1, 1, 1}) {
		t.Fatalf("probes %v; want one of each", p)
	}
	x.f.clock.Advance(5*time.Second - time.Millisecond)
	if rec := x.get(pathHealth, "a"); rec.Code != http.StatusOK {
		t.Errorf("cached: %d", rec.Code)
	}
	if p := probes(); !slices.Equal(p, []int{1, 1, 1, 1, 1}) {
		t.Errorf("probes %v within 5 s; want still one of each", p)
	}
	x.f.fail("prom.Healthy", errLeakyAll)
	x.f.clock.Advance(time.Millisecond)
	rec, checks := x.health()
	if rec.Code != http.StatusServiceUnavailable || decodeBody(t, rec)["ok"] != false {
		t.Errorf("Prometheus down: %d %s; want 503, ok false", rec.Code, rec.Body)
	}
	requireChecks(t, checks, "metrics")
	if p := probes(); !slices.Equal(p, []int{2, 2, 2, 2, 2}) {
		t.Errorf("probes %v after 5 s; want two of each", p)
	}
	requireNoLeak(t, "health", rec)
}

// TestHealth_BackToBackRefreshes: a refresh that follows the previous one at
// once probes every check again; none is reported failed because the last
// probe's goroutine had not finished yet.
func TestHealth_BackToBackRefreshes(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	const rounds = 300
	for i := 0; i < rounds; i++ {
		rec, checks := x.health()
		if rec.Code != http.StatusOK {
			t.Fatalf("round %d: %d %v", i, rec.Code, checks)
		}
		x.f.clock.Advance(5 * time.Second)
	}
	if n := len(x.f.callList("db.PingContext")); n != rounds {
		t.Errorf("%d pings for %d refreshes", n, rounds)
	}
}

// TestHealth_EachCheck: each dependency failing turns its own check false,
// and only that one.
func TestHealth_EachCheck(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		check string
		setup func(t *testing.T, f *fakeLab)
	}{
		{"database down", "database", func(_ *testing.T, f *fakeLab) { f.fail("db.PingContext", errLeakyAll) }},
		{"zeus down", "load_generator", func(_ *testing.T, f *fakeLab) { f.fail("zeus.Healthy", errLeakyAll) }},
		{"no dataset", "load_generator", func(_ *testing.T, f *fakeLab) { f.fail("zeus.ListDatasets", errLeakyAll) }},
		{"Prometheus down", "metrics", func(_ *testing.T, f *fakeLab) { f.fail("prom.Healthy", errLeakyAll) }},
		{"Prometheus probe panics", "metrics", func(_ *testing.T, f *fakeLab) {
			f.onCall("prom.Healthy", func() { panic(errLeakyAll.Error()) })
		}},
		{"no alive target", "target", func(_ *testing.T, f *fakeLab) {
			f.mu.Lock()
			f.instances = []*model.SDKInstance{{ID: "inst-1", Service: TargetService, Status: "stale"}}
			f.mu.Unlock()
		}},
		{"target unreadable", "target", func(_ *testing.T, f *fakeLab) { f.fail("instances.InstancesForService", errLeakyAll) }},
		{"another experiment running", "idle", func(t *testing.T, f *fakeLab) {
			addExperiment(t, f, "exp-op", "op-run", "lab-smoke", "running", testNow)
		}},
		{"running experiments unreadable", "idle", func(_ *testing.T, f *fakeLab) {
			f.fail("experiments.RunningExperimentIDs", errLeakyAll)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newDemo(t, nil)
			tc.setup(t, x.f)
			rec, checks := x.health()
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("status %d; want 503", rec.Code)
			}
			requireChecks(t, checks, tc.check)
			requireNoLeak(t, tc.name, rec)
			if decodeBody(t, rec)["accepting"] != true {
				t.Errorf("accepting %v; a failing check does not close the queue", decodeBody(t, rec)["accepting"])
			}
		})
	}
}

// stuckPinger is a database whose ping ignores its context and blocks until
// released.
type stuckPinger struct {
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (p *stuckPinger) PingContext(context.Context) error {
	p.calls.Add(1)
	<-p.release
	return nil
}

func (p *stuckPinger) free() { p.once.Do(func() { close(p.release) }) }

// healthWithin is GET /health, failing the test if it takes over a second
// (the query deadline in these tests is 50 ms).
func healthWithin(t *testing.T, x *demoHarness) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	got := make(chan *httptest.ResponseRecorder, 1)
	go func() { got <- x.get(pathHealth, "health") }()
	select {
	case rec := <-got:
		return rec, decodeBody(t, rec)["checks"].(map[string]any)
	case <-time.After(time.Second):
		t.Fatal("health did not answer within a second")
		return nil, nil
	}
}

// TestHealth_HungDependency: a dependency that hangs makes its check false
// within the query deadline, whether it honours its context or not; one that
// ignores its context is not probed again until it returns.
func TestHealth_HungDependency(t *testing.T) {
	t.Parallel()
	t.Run("honours its context", func(t *testing.T) {
		t.Parallel()
		x := newDemo(t, func(c *Config, _ *Deps) { c.QueryTimeout = 50 * time.Millisecond })
		x.f.hang("db.PingContext")
		rec, checks := healthWithin(t, x)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status %d; want 503", rec.Code)
		}
		requireChecks(t, checks, "database")
	})
	t.Run("ignores its context", func(t *testing.T) {
		t.Parallel()
		p := &stuckPinger{release: make(chan struct{})}
		x := newDemo(t, func(c *Config, d *Deps) { c.QueryTimeout = 50 * time.Millisecond; d.DB = p })
		t.Cleanup(p.free)
		_, checks := healthWithin(t, x)
		requireChecks(t, checks, "database")
		x.f.clock.Advance(5 * time.Second)
		_, checks = healthWithin(t, x)
		requireChecks(t, checks, "database")
		if n := p.calls.Load(); n != 1 {
			t.Errorf("%d pings; want 1 while the first is stuck", n)
		}
		p.free()
		waitFor(t, "the stuck probe to return", func() bool { return !x.d.health.busy[checkDatabase].Load() })
		x.f.clock.Advance(5 * time.Second)
		rec, checks := healthWithin(t, x)
		requireChecks(t, checks)
		if rec.Code != http.StatusOK || p.calls.Load() != 2 {
			t.Errorf("after release: %d, %d pings; want 200 and 2", rec.Code, p.calls.Load())
		}
	})
}

// TestHealth_NoStampede: one refresh at a time. With no answer yet, every
// caller waits for the one refresh; once there is an answer, the caller that
// starts a refresh waits for it and every other gets the previous answer at
// once.
func TestHealth_NoStampede(t *testing.T) {
	t.Parallel()
	x := newDemo(t, func(c *Config, _ *Deps) { c.QueryTimeout = 2 * time.Second })
	block := func() chan struct{} {
		ch := make(chan struct{})
		x.f.onCall("prom.Healthy", func() { <-ch })
		return ch
	}
	release := block()
	results := make(chan int, 20)
	for i := 0; i < 20; i++ {
		go func() { results <- x.get(pathHealth, fmt.Sprintf("c%d", i)).Code }()
	}
	waitFor(t, "the first probe", func() bool { return len(x.f.callList("prom.Healthy")) == 1 })
	select {
	case code := <-results:
		t.Fatalf("a caller got %d before the first answer existed", code)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	for i := 0; i < 20; i++ {
		if code := <-results; code != http.StatusOK {
			t.Errorf("status %d; want 200", code)
		}
	}
	if n := len(x.f.callList("prom.Healthy")); n != 1 {
		t.Fatalf("%d probes for 20 concurrent first calls; want 1", n)
	}

	x.f.clock.Advance(5 * time.Second)
	release = block()
	leader := make(chan int, 1)
	go func() { leader <- x.get(pathHealth, "leader").Code }()
	waitFor(t, "the second probe", func() bool { return len(x.f.callList("prom.Healthy")) == 2 })
	for i := 0; i < 19; i++ {
		go func() { results <- x.get(pathHealth, fmt.Sprintf("f%d", i)).Code }()
	}
	for i := 0; i < 19; i++ {
		select {
		case code := <-results:
			if code != http.StatusOK {
				t.Errorf("follower: %d; want the previous answer, 200", code)
			}
		case <-time.After(time.Second):
			t.Fatal("a follower waited for the refresh")
		}
	}
	select {
	case code := <-leader:
		t.Fatalf("the caller that started the refresh returned %d before it ended", code)
	default:
	}
	close(release)
	if code := <-leader; code != http.StatusOK {
		t.Errorf("leader: %d", code)
	}
	if n := len(x.f.callList("prom.Healthy")); n != 2 {
		t.Errorf("%d probes; want 2", n)
	}
}

// TestHealth_IdleAndAccepting: idle ignores the run in progress (by its
// experiment id), but not an ended run whose switch-off is pending: that
// run's experiment counts while it runs. accepting is false while the
// switch-off is pending, whatever idle says.
func TestHealth_IdleAndAccepting(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	own := "demo-0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"
	addExperiment(t, x.f, "exp-own", RunNamePrefix+own, CreatedBy, "running", testNow)
	cur := &active{runID: own, expID: "exp-own", sw: newRuleSwitch(fakeRules{x.f}, x.d.log)}
	x.d.w.mu.Lock()
	x.d.w.cur = cur
	x.d.w.mu.Unlock()
	rec, checks := x.health()
	requireChecks(t, checks)
	if rec.Code != http.StatusOK || decodeBody(t, rec)["accepting"] != true {
		t.Errorf("own run in progress: %d %s; want 200, accepting", rec.Code, rec.Body)
	}

	addExperiment(t, x.f, "exp-op", "op-run", "lab-smoke", "running", testNow)
	x.f.clock.Advance(5 * time.Second)
	_, checks = x.health()
	requireChecks(t, checks, "idle")
	x.f.setExperimentStatus("exp-op", "completed")

	// The run ended; its rule's switch-off failed and is being retried.
	x.d.w.mu.Lock()
	x.d.w.cur, x.d.w.pending = nil, cur
	x.d.w.mu.Unlock()
	x.f.clock.Advance(5 * time.Second)
	rec, checks = x.health()
	requireChecks(t, checks, "idle")
	if decodeBody(t, rec)["accepting"] != false {
		t.Errorf("switch-off pending: %s; want not accepting", rec.Body)
	}
	x.f.setExperimentStatus("exp-own", "completed")
	x.f.clock.Advance(5 * time.Second)
	rec, checks = x.health()
	requireChecks(t, checks)
	if rec.Code != http.StatusOK || decodeBody(t, rec)["accepting"] != false {
		t.Errorf("experiment ended, switch-off pending: %d %s; want 200, not accepting", rec.Code, rec.Body)
	}
	// POST still queues: the run waits in line until the switch-off lands.
	if rec := x.post("a"); rec.Code != http.StatusCreated {
		t.Errorf("POST while pending: %d; want 201", rec.Code)
	}
}

// ---- state ----

// TestState: the line in order with the running run first; running is the
// position-0 run's phase detail while it runs, null otherwise.
func TestState(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	if rec := x.get(pathState, "a"); rec.Body.String() != `{"capacity":3,"accepting":true,"queue":[],"running":null}` {
		t.Fatalf("empty: %s", rec.Body)
	}
	a, b := x.postOK("a"), x.postOK("b")
	want := fmt.Sprintf(`{"capacity":3,"accepting":true,"queue":[{"run_id":%q,"position":0,"status":"queued"},{"run_id":%q,"position":1,"status":"queued"}],"running":null}`, a, b)
	if rec := x.get(pathState, "a"); rec.Body.String() != want {
		t.Errorf("queued:\n got %s\nwant %s", rec.Body, want)
	}

	started := testNow.Add(-10 * time.Second)
	x.d.q.update(a, func(r *Run) {
		r.Status, r.StartedAt = RunRunning, &started
		r.Phases[0].Status, r.Phases[0].StartedAt = PhaseRunning, &started
	})
	running := func(detail string) string {
		return fmt.Sprintf(`{"capacity":3,"accepting":true,"queue":[{"run_id":%q,"position":0,"status":"running"},{"run_id":%q,"position":1,"status":"queued"}],"running":%s}`, a, b, detail)
	}
	if rec := x.get(pathState, "a"); rec.Body.String() != running(fmt.Sprintf(
		`{"run_id":%q,"phase":"plain","phase_index":0,"phase_started_at":"2026-11-20T11:59:50.000Z","phase_duration_s":40,"progress":0.25}`, a)) {
		t.Errorf("first phase running: %s", rec.Body)
	}

	ended := testNow.Add(-2 * time.Second)
	x.d.q.update(a, func(r *Run) { r.Phases[0].Status, r.Phases[0].EndedAt = PhaseCompleted, &ended })
	if rec := x.get(pathState, "a"); rec.Body.String() != running(fmt.Sprintf(
		`{"run_id":%q,"phase":"catalog-latency-50ms","phase_index":1,"phase_started_at":null,"phase_duration_s":40,"progress":0}`, a)) {
		t.Errorf("between phases: %s", rec.Body)
	}

	late := testNow.Add(-50 * time.Second)
	x.d.q.update(a, func(r *Run) { r.Phases[1].Status, r.Phases[1].StartedAt = PhaseRunning, &late })
	if rec := x.get(pathState, "a"); rec.Body.String() != running(fmt.Sprintf(
		`{"run_id":%q,"phase":"catalog-latency-50ms","phase_index":1,"phase_started_at":"2026-11-20T11:59:10.000Z","phase_duration_s":40,"progress":1}`, a)) {
		t.Errorf("overrunning phase: %s", rec.Body)
	}

	x.d.q.update(a, func(r *Run) {
		r.Phases[1].Status = PhaseCompleted
		r.Phases[2].Status, r.Phases[2].StartedAt = PhaseCompleted, &late
	})
	if rec := x.get(pathState, "a"); rec.Body.String() != running(fmt.Sprintf(
		`{"run_id":%q,"phase":"plain-end","phase_index":2,"phase_started_at":"2026-11-20T11:59:10.000Z","phase_duration_s":40,"progress":1}`, a)) {
		t.Errorf("all phases ended: %s", rec.Body)
	}
}

// TestState_SwitchOffPending: while an ended run's switch-off is pending, the
// next run waits at position 0, queued, with running null, and the lab is
// not accepting.
func TestState_SwitchOffPending(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	a := x.postOK("a")
	x.d.w.mu.Lock()
	x.d.w.pending = &active{runID: "demo-ended", sw: newRuleSwitch(fakeRules{x.f}, x.d.log)}
	x.d.w.mu.Unlock()
	want := fmt.Sprintf(`{"capacity":3,"accepting":false,"queue":[{"run_id":%q,"position":0,"status":"queued"}],"running":null}`, a)
	if rec := x.get(pathState, "a"); rec.Body.String() != want {
		t.Errorf("got  %s\nwant %s", rec.Body, want)
	}
}

// ---- runs ----

// TestRunJSONShape: GET /runs/{id} and /runs/recent carry exactly the
// contract's keys.
func TestRunJSONShape(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	runID := x.postOK("a")
	rec := x.get(pathRun(runID), "a")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET run: %d %s", rec.Code, rec.Body)
	}
	requireRunShape(t, decodeBody(t, rec))
	if m := decodeBody(t, rec); m["run_id"] != runID || m["status"] != "queued" || m["position"] != float64(0) || m["reason"] != nil {
		t.Errorf("run %s", rec.Body)
	}

	s := seed(t, x.f, "completed", false)
	rec = x.get(pathRecent, "a")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET recent: %d %s", rec.Code, rec.Body)
	}
	m := decodeBody(t, rec)
	wantKeys(t, "recent", m, "runs")
	runs := m["runs"].([]any)
	if len(runs) != 1 {
		t.Fatalf("recent %s; want one run", rec.Body)
	}
	r := runs[0].(map[string]any)
	requireRunShape(t, r)
	if r["run_id"] != s.runID || r["status"] != "completed" || r["position"] != nil {
		t.Errorf("recent run %v", r)
	}
	requireNoLeak(t, "recent", rec)
}

func requireRunShape(t *testing.T, m map[string]any) {
	t.Helper()
	wantKeys(t, "run", m, "run_id", "status", "reason", "position", "queued_at", "started_at", "ended_at", "phases", "testbed")
	phases, _ := m["phases"].([]any)
	if len(phases) != 3 {
		t.Fatalf("phases %v; want 3", m["phases"])
	}
	for _, p := range phases {
		wantKeys(t, "phase", p.(map[string]any), "name", "fault", "status", "started_at", "ended_at", "duration_s", "result")
	}
	wantKeys(t, "testbed", m["testbed"].(map[string]any), "machine", "cpu_limits_m", "phase_duration_s", "note")
}

// TestRun_Errors: a malformed id is 404 without being echoed or looked up; an
// unknown one is 404; a database that cannot be read is 503 for a run and a
// 200 with what is in memory for the recent list.
func TestRun_Errors(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	var viewed atomic.Int32
	view := x.d.view
	x.d.view = func(ctx context.Context, runID string) (Run, error) {
		viewed.Add(1)
		return view(ctx, runID)
	}
	for _, bad := range []string{"x", "demo-123", "exp-123", "demo-0F1E2D3C-4B5A-6978-8796-A5B4C3D2E1F0", strings.Repeat("a", 300), "%3Cscript%3E"} {
		rec := x.get(pathRun(bad), "a")
		requireError(t, rec, http.StatusNotFound, "not_found")
	}
	if n := viewed.Load(); n != 0 {
		t.Errorf("malformed ids reached the run store %d times", n)
	}
	if n := len(x.f.callList("experiments.List")); n != 0 {
		t.Errorf("malformed ids were looked up %d times", n)
	}
	unknown := id.New(RunIDPrefix)
	requireError(t, x.get(pathRun(unknown), "a"), http.StatusNotFound, "not_found")

	ended := newRun(id.New(RunIDPrefix), testNow)
	ended.Status, ended.Reason, ended.EndedAt = RunFailed, ReasonLabBusy, &testNow
	x.d.w.mu.Lock()
	x.d.w.finished = append(x.d.w.finished, ended)
	x.d.w.mu.Unlock()
	x.f.fail("experiments.List", errLeakyAll)
	rec := x.get(pathRun(unknown), "a")
	requireError(t, rec, http.StatusServiceUnavailable, "unavailable")
	requireNoLeak(t, "run, database down", rec)
	rec = x.get(pathRecent, "a")
	if rec.Code != http.StatusOK {
		t.Fatalf("recent, database down: %d %s; want 200", rec.Code, rec.Body)
	}
	runs := decodeBody(t, rec)["runs"].([]any)
	if len(runs) != 1 || runs[0].(map[string]any)["run_id"] != ended.RunID {
		t.Errorf("recent, database down: %s; want the run in memory", rec.Body)
	}
	requireNoLeak(t, "recent, database down", rec)
	if !logLineHas(x.logs.String(), "recent runs", `"level":"WARN"`) {
		t.Errorf("the incomplete recent list was not logged:\n%s", x.logs)
	}
}

// ---- nothing internal reaches a response ----

// TestNoInternalLeak: with every dependency failing with an error that
// carries an IP, a hostname, ids and SQL, with dependencies that panic, with
// unexpected errors from the run store and with a response that cannot be
// encoded, no response of any route or error path carries any of it; a 500's
// detail is in the log only.
func TestNoInternalLeak(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, what string, rec *httptest.ResponseRecorder) {
		t.Helper()
		requireNoLeak(t, what, rec)
		if !json.Valid(rec.Body.Bytes()) {
			t.Errorf("%s: body %q is not JSON", what, rec.Body)
		}
	}

	t.Run("dependencies fail", func(t *testing.T) {
		t.Parallel()
		x := newDemo(t, nil)
		x.postOK("a")
		x.postOK("b")
		x.postOK("c")
		for _, op := range []string{"experiments.List", "experiments.ListPhasesForExperiment", "experiments.RunningExperimentIDs",
			"zeus.Healthy", "zeus.ListDatasets", "instances.InstancesForService", "prom.Healthy", "db.PingContext"} {
			x.f.fail(op, errLeakyAll)
		}
		secret := clientHeader("z")
		secret.Set(HeaderEdgeSecret, goodSecret)
		long := clientHeader(strings.Repeat("k", 201))
		long.Set(HeaderEdgeSecret, goodSecret)
		for what, rec := range map[string]*httptest.ResponseRecorder{
			"health":              x.get(pathHealth, "a"),
			"state":               x.get(pathState, "a"),
			"recent":              x.get(pathRecent, "a"),
			"run unknown":         x.get(pathRun(id.New(RunIDPrefix)), "a"),
			"run malformed":       x.get(pathRun("exp-123"), "a"),
			"post forbidden":      x.serve(http.MethodPost, pathRuns, clientHeader("z"), nil),
			"post bad key":        x.serve(http.MethodPost, pathRuns, long, nil),
			"post client_has_run": x.post("a"),
			"post queue_full":     x.serve(http.MethodPost, pathRuns, secret, nil),
			"unknown":             x.get("/api/v1/rules/rule-7?host=10.0.0.250", "a"),
		} {
			check(t, what, rec)
		}
		for i := 0; i < 130; i++ {
			x.get(pathState, "r")
		}
		check(t, "rate_limited", x.get(pathState, "r"))
	})

	t.Run("unexpected errors and panics", func(t *testing.T) {
		t.Parallel()
		x := newDemo(t, nil)
		runID := x.postOK("a")
		x.d.view = func(context.Context, string) (Run, error) { return Run{}, errLeakyAll }
		x.d.enqueue = func(string, time.Time) (Run, error) { return Run{}, errLeakyAll }
		rec := x.get(pathRun(runID), "a")
		requireError(t, rec, http.StatusInternalServerError, "internal")
		check(t, "view error", rec)
		rec = x.post("b")
		requireError(t, rec, http.StatusInternalServerError, "internal")
		check(t, "enqueue error", rec)
		x.d.recent = func(context.Context, int) ([]Run, bool) { panic(errLeakyAll.Error()) }
		rec = x.get(pathRecent, "a")
		requireError(t, rec, http.StatusInternalServerError, "internal")
		check(t, "recent panics", rec)
		logs := x.logs.String()
		if strings.Count(logs, "10.0.0.250") < 3 {
			t.Errorf("the 500s' detail is not in the log:\n%s", logs)
		}
		if !logLineHas(logs, `"level":"ERROR"`, "panicked", "stack") {
			t.Errorf("the panic was not logged with its stack:\n%s", logs)
		}
		// The handler goes on serving.
		if rec := x.get(pathState, "a"); rec.Code != http.StatusOK {
			t.Errorf("after the panic: %d", rec.Code)
		}
	})

	t.Run("a store call panics", func(t *testing.T) {
		t.Parallel()
		x := newDemo(t, nil)
		x.f.onCall("experiments.List", func() { panic(errLeakyAll) })
		rec := x.get(pathRun(id.New(RunIDPrefix)), "a")
		requireError(t, rec, http.StatusInternalServerError, "internal")
		check(t, "run, List panics", rec)
		rec = x.get(pathRecent, "a")
		requireError(t, rec, http.StatusInternalServerError, "internal")
		check(t, "recent, List panics", rec)
	})

	t.Run("a response that does not encode", func(t *testing.T) {
		t.Parallel()
		x := newDemo(t, nil)
		runID := x.postOK("a")
		x.d.q.update(runID, func(r *Run) { r.Status = "paused" })
		for what, target := range map[string]string{"run": pathRun(runID), "state": pathState} {
			rec := x.get(target, "a")
			requireError(t, rec, http.StatusInternalServerError, "internal")
			check(t, what, rec)
		}
		if !logLineHas(x.logs.String(), `"level":"ERROR"`, "outside the public list") {
			t.Errorf("the encoding failure was not logged:\n%s", x.logs)
		}
	})
}

// ---- logging ----

// requestLines returns the request log's entries.
func requestLines(t *testing.T, logs string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "demo: request" {
			out = append(out, m)
		}
	}
	return out
}

// TestRequestLog: one line per request with its method, route pattern,
// status, error code and duration, and the client key only as a keyed hash;
// never the edge secret, never a header.
func TestRequestLog(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	ip := "203.0.113.7"
	x.post(ip)
	x.get(pathState, ip)
	x.get(pathState, "other")
	x.get("/api/v1/rules", ip)
	x.get(pathState, strings.Repeat("k", 300))
	x.serve(http.MethodPost, pathRuns, clientHeader(ip), nil)
	logs := x.logs.String()
	lines := requestLines(t, logs)
	if len(lines) != 6 {
		t.Fatalf("%d request lines; want 6:\n%s", len(lines), logs)
	}
	want := []struct {
		method, route string
		status        float64
		code          string
	}{
		{"POST", "POST /api/v1/demo/runs", 201, ""},
		{"GET", "GET /api/v1/demo/state", 200, ""},
		{"GET", "GET /api/v1/demo/state", 200, ""},
		{"GET", "unmatched", 404, "not_found"},
		{"GET", "GET /api/v1/demo/state", 400, "bad_request"},
		{"POST", "POST /api/v1/demo/runs", 403, "forbidden"},
	}
	for i, w := range want {
		l := lines[i]
		if l["method"] != w.method || l["route"] != w.route || l["status"] != w.status {
			t.Errorf("line %d: %v; want %s %s %v", i, l, w.method, w.route, w.status)
		}
		if code, _ := l["code"].(string); code != w.code {
			t.Errorf("line %d: code %q; want %q", i, code, w.code)
		}
		if _, ok := l["duration"]; !ok {
			t.Errorf("line %d has no duration", i)
		}
	}
	tag, _ := lines[0]["client"].(string)
	if len(tag) != 12 || strings.Trim(tag, "0123456789abcdef") != "" {
		t.Errorf("client %q; want 12 hex digits", tag)
	}
	if lines[1]["client"] != tag || lines[2]["client"] == tag {
		t.Errorf("client tags %v, %v, %v: one client must map to one tag", lines[0]["client"], lines[1]["client"], lines[2]["client"])
	}
	for _, bad := range []string{ip, goodSecret, strings.Repeat("k", 300), HeaderEdgeSecret, HeaderClient} {
		if strings.Contains(logs, bad) {
			t.Errorf("the log carries %q:\n%s", bad, logs)
		}
	}
}

// ---- the Demo ----

// TestDemo_OnRunEndedIsTheWorkersRetain: after a run ends, the worker's own
// retention runs; it alone cancels a stale planned leftover (the package's
// Retain never cancels anything).
func TestDemo_OnRunEndedIsTheWorkersRetain(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	stale := seed(t, x.f, "planned", false)
	x.f.clock.Advance(2*x.d.cfg.RunCap + time.Second)
	x.f.fail("zeus.Healthy", errLeakyAll) // the run ends at preflight
	x.postOK("a")
	x.start()
	waitFor(t, "the stale planned experiment to be cancelled", func() bool {
		return x.f.experimentStatus(stale.ExperimentID) == "cancelled"
	})
}

// TestDemo_SwitchOffAll: the start-up switch-off, through the Demo.
func TestDemo_SwitchOffAll(t *testing.T) {
	t.Parallel()
	x := newUnsweptDemo(t, nil)
	s := seed(t, x.f, "running", true)
	if err := x.d.SwitchOffAll(context.Background()); err != nil {
		t.Fatalf("SwitchOffAll: %v", err)
	}
	if r := x.f.rule(s.RuleID); r == nil || r.Enabled {
		t.Errorf("rule %+v; want off", r)
	}
	requireError(t, x.post("a"), http.StatusServiceUnavailable, "unavailable")
}

// TestDemo_Shutdown: POST is refused at once, the worker's shutdown error is
// logged and returned, every event stream's channel is closed, and Shutdown
// returns by its context's deadline although the switch-off is stuck.
func TestDemo_Shutdown(t *testing.T) {
	t.Parallel()
	x := newDemo(t, nil)
	x.start()
	runID := x.postOK("a")
	waitFor(t, "the run's rule to be on", func() bool {
		r := x.f.rule(x.f.ruleIDOfRun(runID))
		return r != nil && r.Enabled
	})
	events, unsub := x.d.hub.Subscribe(runID)
	defer unsub()
	release := make(chan struct{})
	var once sync.Once
	x.f.onCall("rules.SetEnabled", func() { <-release })
	t.Cleanup(func() { once.Do(func() { close(release) }) })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	began := time.Now()
	err := x.d.Shutdown(ctx)
	if d := time.Since(began); d > time.Second {
		t.Errorf("Shutdown took %v with a 100 ms deadline", d)
	}
	if err == nil {
		t.Error("Shutdown returned nil although the switch-off did not finish")
	}
	if !logLineHas(x.logs.String(), `"level":"ERROR"`, "demo: shutdown") {
		t.Errorf("the worker's shutdown error was not logged:\n%s", x.logs)
	}
	requireError(t, x.post("b"), http.StatusServiceUnavailable, "unavailable")
	if rec := x.get(pathState, "a"); decodeBody(t, rec)["accepting"] != false {
		t.Errorf("state after shutdown: %s; want not accepting", rec.Body)
	}
	closed := false
	for !closed {
		select {
		case _, ok := <-events:
			closed = !ok
		case <-time.After(time.Second):
			t.Fatal("the run's event channel is still open after Shutdown")
		}
	}
	once.Do(func() { close(release) })
}
