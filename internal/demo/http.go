package demo

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// The public routes, as ServeMux patterns. They are the only routes: every
// other request, an unsupported method on one of these paths included, is
// 404 not_found.
const (
	routeHealth = "GET /api/v1/demo/health"
	routeState  = "GET /api/v1/demo/state"
	routePost   = "POST /api/v1/demo/runs"
	routeRecent = "GET /api/v1/demo/runs/recent"
	routeRun    = "GET /api/v1/demo/runs/{id}"
	// The sixth, routeEvents, is in events.go.
)

// maxBodyBytes caps a request body. No route takes one: a body this small is
// read and ignored, a larger one is 400 bad_request.
const maxBodyBytes = 1 << 10

// errEnqueueLimited: the client's last enqueue is less than 5 minutes old (429
// rate_limited).
var errEnqueueLimited = errors.New("demo: the client enqueued less than 5 minutes ago")

// Handler returns the demo's router: a fresh http.ServeMux with the public
// routes and a catch-all that answers 404 not_found, behind a layer that
//
//   - answers 404 for any path not in canonical form (a double or trailing
//     slash, a . or .. segment, any percent-encoding the default escaping
//     would not produce), so ServeMux never redirects;
//   - sets Content-Type: application/json, Cache-Control: no-store and
//     X-Content-Type-Options: nosniff on every response (an event stream that
//     starts replaces the Content-Type), and no CORS header;
//   - turns a panic into 500 internal, with the detail in the log only;
//   - logs one line per request: method, route pattern, status, error code,
//     duration and a keyed hash of the client key (never the key, never a
//     header, never the edge secret).
//
// Each route answers only its own method (ServeMux would also route HEAD to a
// GET pattern), caps and drains the request body, and checks the client key
// (X-Demo-Client: absent is "local"; empty, repeated, over 200 bytes, not
// UTF-8 or holding a control character is 400 bad_request). A GET counts as
// one read of the client's 120 a minute. POST checks the edge secret first.
//
// Pass the wiring's http.Server DisableGeneralOptionsHandler: true, or
// net/http answers OPTIONS * itself, before this handler.
func (d *Demo) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(routeHealth, d.read(d.handleHealth))
	mux.Handle(routeState, d.read(d.handleState))
	mux.Handle(routePost, d.only(http.MethodPost, http.HandlerFunc(d.handlePostRun)))
	mux.Handle(routeRecent, d.read(d.handleRecent))
	mux.Handle(routeRun, d.read(d.handleRun))
	mux.Handle(routeEvents, d.read(d.handleEvents))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { d.writeError(w, CodeNotFound, 0) })
	return d.serve(mux)
}

// ---- the layers ----

// serve is the outer layer around the router (see Handler).
func (d *Demo) serve(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := d.d.Clock.Now()
		rec := &recorder{ResponseWriter: w}
		h := rec.Header()
		h.Set("Content-Type", "application/json")
		h.Set("Cache-Control", "no-store")
		h.Set("X-Content-Type-Options", "nosniff")
		defer d.finishRequest(rec, r, start)
		if !canonicalPath(r.URL) {
			d.writeError(rec, CodeNotFound, 0)
			return
		}
		mux.ServeHTTP(rec, r)
		if !rec.wrote {
			d.log.Error("demo: a route wrote no response", "route", r.Pattern)
			d.writeError(rec, CodeInternal, 0)
		}
	})
}

// finishRequest, deferred by serve, turns a panic into 500 internal (when
// nothing was written yet) and logs the request.
func (d *Demo) finishRequest(rec *recorder, r *http.Request, start time.Time) {
	p := recover()
	if p != nil && p != http.ErrAbortHandler {
		d.log.Error("demo: a request handler panicked", "route", r.Pattern, "panic", fmt.Sprint(p),
			"stack", string(debug.Stack()))
		if !rec.wrote {
			d.writeError(rec, CodeInternal, 0)
		}
	}
	d.logRequest(rec, r, start)
	if p == http.ErrAbortHandler {
		panic(p) // net/http aborts the response quietly
	}
}

// logRequest writes the request's one log line.
func (d *Demo) logRequest(rec *recorder, r *http.Request, start time.Time) {
	route := r.Pattern
	if route == "" || route == "/" {
		route = "unmatched"
	}
	method := r.Method
	if len(method) > 16 {
		method = method[:16]
	}
	attrs := []any{"method", method, "route", route, "status", rec.status, "duration", d.d.Clock.Now().Sub(start)}
	if rec.code != "" {
		attrs = append(attrs, "code", rec.code)
	}
	if rec.client != "" {
		attrs = append(attrs, "client", rec.client)
	}
	d.log.Info("demo: request", attrs...)
}

// only answers 404 not_found to any method but method, and 400 bad_request
// to a body over maxBodyBytes; it reads and drops a smaller one.
func (d *Demo) only(method string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			d.writeError(w, CodeNotFound, 0)
			return
		}
		if !d.dropBody(w, r) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

// dropBody reads and drops the request body, at most maxBodyBytes of it; a
// larger body is 400 bad_request and closes the connection.
func (d *Demo) dropBody(w http.ResponseWriter, r *http.Request) bool {
	if r.Body == nil {
		return true
	}
	tooLarge := r.ContentLength > maxBodyBytes
	if !tooLarge {
		_, err := io.Copy(io.Discard, http.MaxBytesReader(unwrap(w), r.Body, maxBodyBytes))
		tooLarge = err != nil
	}
	if tooLarge {
		w.Header().Set("Connection", "close")
		d.writeError(w, CodeBadRequest, 0)
		return false
	}
	return true
}

// read is a GET route that counts one read for its client.
func (d *Demo) read(h func(w http.ResponseWriter, r *http.Request, client string)) http.Handler {
	return d.only(http.MethodGet, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		client, ok := clientKey(r)
		if !ok {
			d.writeError(w, CodeBadRequest, 0)
			return
		}
		d.noteClient(w, client)
		if ok, wait := d.lim.takeRead(client); !ok {
			d.writeError(w, CodeRateLimited, wait)
			return
		}
		h(w, r, client)
	}))
}

// ---- the routes ----

// handleHealth: 200 when every check passes, else 503; checks are cached
// (healthCache), accepting is read fresh.
func (d *Demo) handleHealth(w http.ResponseWriter, r *http.Request, _ string) {
	c := d.healthChecks(r.Context())
	body := Health{Accepting: d.accepting(), Checks: HealthChecks{
		Database:      c[checkDatabase],
		LoadGenerator: c[checkLoadGenerator],
		Metrics:       c[checkMetrics],
		Target:        c[checkTarget],
		Idle:          c[checkIdle],
	}}
	body.OK = body.Checks.All()
	status := http.StatusOK
	if !body.OK {
		status = http.StatusServiceUnavailable
	}
	d.writeJSON(w, r, status, body)
}

// handleState: the line and the running run's phase.
func (d *Demo) handleState(w http.ResponseWriter, r *http.Request, _ string) {
	d.writeJSON(w, r, http.StatusOK, d.state())
}

// state is the State now: every run that has not ended, in line order, and,
// while the position-0 run is running, where it is. A position-0 run that is
// still queued (the worker has not taken it, or holds it while a switch-off
// is pending) has no detail: running is null.
func (d *Demo) state() State {
	line := d.q.Snapshot()
	st := State{Capacity: d.cfg.Capacity, Accepting: d.accepting(), Queue: make([]QueueEntry, 0, len(line))}
	for i, r := range line {
		pos := i
		if r.Position != nil {
			pos = *r.Position
		}
		st.Queue = append(st.Queue, QueueEntry{RunID: r.RunID, Position: pos, Status: r.Status})
	}
	if len(line) > 0 && line[0].Status == RunRunning {
		rp := runningPhase(line[0], d.d.Clock.Now())
		st.Running = &rp
	}
	return st
}

// runningPhase is where run r is at now: its running phase, else the next
// pending one, else the last. Progress is the phase's elapsed fraction while
// it runs, 0 before it starts, 1 once it has ended.
func runningPhase(r Run, now time.Time) RunningPhase {
	var statuses [len(PhaseNames)]PhaseStatus
	for i, p := range r.Phases {
		statuses[i] = p.Status
	}
	name, running := currentPhase(statuses)
	i := slices.Index(PhaseNames[:], name)
	p := r.Phases[i]
	rp := RunningPhase{RunID: r.RunID, Phase: name, PhaseIndex: i, PhaseStartedAt: clonePtr(p.StartedAt), PhaseDurationS: PhaseDurationS}
	switch {
	case running && p.StartedAt != nil:
		rp.Progress = now.Sub(*p.StartedAt).Seconds() / PhaseDurationS
	case running || p.Status == PhasePending:
		rp.Progress = 0
	default:
		rp.Progress = 1
	}
	return rp
}

// handlePostRun queues a run. In order: the edge secret (403 forbidden,
// whether missing or wrong), the client key (400), the lab open (503
// unavailable before the start-up sweep, during shutdown, or once the queue
// is closed), the client's run in line (429 client_has_run), the client's
// enqueue limit (429 rate_limited), a free slot (429 queue_full); then 201,
// and only then is the enqueue counted against the client.
func (d *Demo) handlePostRun(w http.ResponseWriter, r *http.Request) {
	if s := r.Header.Values(HeaderEdgeSecret); len(s) != 1 || !d.cfg.EdgeSecret.Matches(s[0]) {
		d.writeError(w, CodeForbidden, 0)
		return
	}
	client, ok := clientKey(r)
	if !ok {
		d.writeError(w, CodeBadRequest, 0)
		return
	}
	d.noteClient(w, client)
	if !d.ready.Load() || d.stopping.Load() {
		d.writeError(w, CodeUnavailable, 0)
		return
	}
	run, wait, err := d.admit(client)
	if err == nil && run.Position == nil {
		err = errors.New("demo: an enqueued run came back without a position")
	}
	if err != nil {
		d.fail(w, r, err, wait)
		return
	}
	d.writeJSON(w, r, http.StatusCreated, Enqueued{RunID: run.RunID, Status: run.Status, Position: *run.Position})
}

// admit enqueues a run for client, or says why not and how long to wait. The
// wait is an estimate where the queue decides: for queue_full, the phases the
// run at the head has not finished, 40 s each; for client_has_run, the
// phases left up to and including the client's own run, or the client's
// enqueue limit if that ends later (until both have passed, a retry is
// refused again).
func (d *Demo) admit(client string) (Run, time.Duration, error) {
	d.admitMu.Lock()
	defer d.admitMu.Unlock()
	if pos, err := d.q.check(client); err != nil {
		if errors.Is(err, ErrClientHasRun) {
			return Run{}, d.hasRunWait(client, pos), err
		}
		return Run{}, 0, err
	}
	if ok, wait := d.lim.enqueueWait(client); !ok {
		return Run{}, wait, errEnqueueLimited
	}
	run, err := d.enqueue(client, d.d.Clock.Now())
	switch {
	case errors.Is(err, ErrQueueFull):
		return Run{}, lineWait(d.q.Snapshot(), 0), err
	case errors.Is(err, ErrClientHasRun):
		return Run{}, d.hasRunWait(client, d.cfg.Capacity-1), err
	case err != nil:
		return Run{}, 0, err
	}
	d.lim.RecordEnqueue(client)
	return run, 0, nil
}

// hasRunWait is client_has_run's wait for a client whose run is at pos.
func (d *Demo) hasRunWait(client string, pos int) time.Duration {
	_, limit := d.lim.enqueueWait(client)
	return max(lineWait(d.q.Snapshot(), pos), limit)
}

// lineWait estimates how long until the runs at positions 0..upto have all
// ended: one phase length for each of their phases that has not ended, and
// at least one.
func lineWait(line []Run, upto int) time.Duration {
	phases := 0
	for i := 0; i <= upto && i < len(line); i++ {
		for _, p := range line[i].Phases {
			if p.Status == PhasePending || p.Status == PhaseRunning {
				phases++
			}
		}
	}
	return time.Duration(max(phases, 1)) * PhaseDurationS * time.Second
}

// handleRun: one run's view. An id not of the shape this package mints is 404
// before anything is looked up, and never echoed.
func (d *Demo) handleRun(w http.ResponseWriter, r *http.Request, _ string) {
	runID := r.PathValue("id")
	if !validRunID(runID) {
		d.writeError(w, CodeNotFound, 0)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), d.cfg.routeTimeout())
	defer cancel()
	run, err := d.view(ctx, runID)
	if err != nil {
		d.fail(w, r, err, 0)
		return
	}
	d.writeJSON(w, r, http.StatusOK, run)
}

// handleRecent: the last Config.Recent ended runs. When the database could
// not be read in full, it still answers 200 with what was found.
func (d *Demo) handleRecent(w http.ResponseWriter, r *http.Request, _ string) {
	ctx, cancel := context.WithTimeout(r.Context(), d.cfg.routeTimeout())
	defer cancel()
	runs, complete := d.recent(ctx, d.cfg.Recent)
	if !complete {
		d.log.Warn("demo: recent runs answered with what could be read", "runs", len(runs))
	}
	d.writeJSON(w, r, http.StatusOK, RecentRuns{Runs: runs})
}

// ---- responses ----

// fail maps an error of the worker, the queue or the limiter to its code.
// Anything else is 500 internal, logged with its detail.
func (d *Demo) fail(w http.ResponseWriter, r *http.Request, err error, wait time.Duration) {
	var code Code
	switch {
	case errors.Is(err, ErrRunNotFound):
		code = CodeNotFound
	case errors.Is(err, ErrRunUnavailable), errors.Is(err, ErrQueueClosed):
		code = CodeUnavailable
	case errors.Is(err, ErrQueueFull):
		code = CodeQueueFull
	case errors.Is(err, ErrClientHasRun):
		code = CodeClientHasRun
	case errors.Is(err, errEnqueueLimited):
		code = CodeRateLimited
	default:
		d.log.Error("demo: a route failed", "route", r.Pattern, "error", err)
		code = CodeInternal
	}
	d.writeError(w, code, wait)
}

// writeJSON writes v as the body with status. A value that does not encode
// (a status or reason outside the contract) is 500 internal, logged.
func (d *Demo) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		d.log.Error("demo: a response did not encode", "route", r.Pattern, "error", err)
		d.writeError(w, CodeInternal, 0)
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// writeError writes {"error": code} with the code's status. A 429 always
// carries Retry-After: wait in whole seconds, rounded up, at least 1.
func (d *Demo) writeError(w http.ResponseWriter, code Code, wait time.Duration) {
	body, err := json.Marshal(ErrorBody{Error: code})
	if err != nil {
		d.log.Error("demo: an error code outside the public list", "code", code.String())
		code, body = CodeInternal, []byte(`{"error":"internal"}`)
	}
	status := code.HTTPStatus()
	if status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", strconv.Itoa(retrySeconds(wait)))
	} else {
		w.Header().Del("Retry-After")
	}
	if rec, ok := w.(*recorder); ok {
		rec.code = code.String()
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// retrySeconds is a Retry-After value: wait in whole seconds, rounded up, at
// least 1.
func retrySeconds(wait time.Duration) int {
	return max(1, int((wait+time.Second-1)/time.Second))
}

// ---- requests ----

// clientKey is the request's client key: X-Demo-Client, or DefaultClient
// when it is absent. ok is false for an empty or repeated header, or a value
// over MaxClientBytes, not UTF-8, or holding a control character.
func clientKey(r *http.Request) (key string, ok bool) {
	vals := r.Header.Values(HeaderClient)
	switch len(vals) {
	case 0:
		return DefaultClient, true
	case 1:
	default:
		return "", false
	}
	key = vals[0]
	if key == "" || len(key) > MaxClientBytes || !utf8.ValidString(key) {
		return "", false
	}
	if strings.ContainsFunc(key, unicode.IsControl) {
		return "", false
	}
	return key, true
}

// noteClient records, for the request log, a keyed hash of the client key:
// stable within this process, so one client's requests can be followed, and
// not reversible, though a key may be an address.
func (d *Demo) noteClient(w http.ResponseWriter, client string) {
	rec, ok := w.(*recorder)
	if !ok {
		return
	}
	m := hmac.New(sha256.New, d.logKey)
	m.Write([]byte(client))
	rec.client = hex.EncodeToString(m.Sum(nil))[:12]
}

// canonicalPath reports whether u's path is one ServeMux would route without
// a redirect and without decoding: absolute, clean (no empty, . or ..
// segment, no trailing slash) and in the default escaping.
func canonicalPath(u *url.URL) bool {
	p := u.Path
	return u.Opaque == "" && u.RawPath == "" && strings.HasPrefix(p, "/") && path.Clean(p) == p
}

// ---- the response recorder ----

// recorder is the response writer every route writes through: it remembers
// the status, the error code and the client's log tag. It passes Flush on,
// and Unwrap lets http.ResponseController reach the connection (the event
// stream clears its write deadline through it).
type recorder struct {
	http.ResponseWriter
	status int
	wrote  bool
	code   string
	client string
}

func (r *recorder) WriteHeader(status int) {
	if r.wrote {
		return
	}
	r.status, r.wrote = status, true
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

func (r *recorder) Flush() {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// unwrap returns the writer under a recorder.
func unwrap(w http.ResponseWriter) http.ResponseWriter {
	if rec, ok := w.(*recorder); ok {
		return rec.ResponseWriter
	}
	return w
}
