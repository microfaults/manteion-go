package demo

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"manteion-go/internal/id"
)

// ---- harness ----

func pathEvents(runID string) string { return pathRun(runID) + "/events" }

// streamTiming is the brief's stream timing: a heartbeat every 10 ms, streams
// closed after 300 ms.
func streamTiming(c *Config) {
	c.Heartbeat = 10 * time.Millisecond
	c.StreamMaxAge = 300 * time.Millisecond
}

// streamOpts shapes a stream harness. tweak edits the configuration after
// streamTiming; wrap goes around the demo's handler; server and listener edit
// the test server before it starts.
type streamOpts struct {
	tweak    func(*Config, *Deps)
	wrap     func(http.Handler) http.Handler
	server   func(*http.Server)
	listener func(net.Listener) net.Listener
}

// streamHarness is a demo behind a real net/http server: an event stream
// needs a connection whose write deadline it can control.
type streamHarness struct {
	*demoHarness
	srv *httptest.Server
}

func newStreamDemo(t *testing.T, o streamOpts) *streamHarness {
	t.Helper()
	x := newDemo(t, func(c *Config, d *Deps) {
		streamTiming(c)
		if o.tweak != nil {
			o.tweak(c, d)
		}
	})
	var h http.Handler = x.h
	if o.wrap != nil {
		h = o.wrap(h)
	}
	srv := httptest.NewUnstartedServer(h)
	srv.Config.DisableGeneralOptionsHandler = true
	if o.server != nil {
		o.server(srv.Config)
	}
	if o.listener != nil {
		srv.Listener = o.listener(srv.Listener)
	}
	srv.Start()
	// Registered before any x.start, so it runs after the demo's own
	// shutdown: then every stream has ended, and a stream a broken test left
	// hanging is cut by closing its connection.
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
	})
	return &streamHarness{demoHarness: x, srv: srv}
}

// openStreamCount is the number of stream slots the limiter holds.
func openStreamCount(l *Limiter) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.streams)
}

// requireReleased waits until no stream holds a hub subscription or a slot.
func (x *streamHarness) requireReleased(t *testing.T, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		subs, slots := x.d.hub.Count(), openStreamCount(x.d.lim)
		if subs == 0 && slots == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %v: %d hub subscriptions and %d stream slots still held; want none", within, subs, slots)
		}
		time.Sleep(time.Millisecond)
	}
}

// sseFrame is one frame of a stream, as an EventSource reads it: a comment, or
// an event with its type and data.
type sseFrame struct {
	raw     string // the frame's bytes, its blank line included
	comment bool
	event   string
	data    string
	bad     string // why the frame breaks the contract's framing, if it does
}

func parseFrame(raw string) sseFrame {
	f := sseFrame{raw: raw}
	if strings.ContainsRune(raw, '\r') {
		f.bad = "carries a carriage return"
	}
	lines := strings.Split(strings.TrimSuffix(raw, "\n\n"), "\n")
	if strings.HasPrefix(lines[0], ":") {
		f.comment = true
		for _, l := range lines {
			if !strings.HasPrefix(l, ":") {
				f.bad = "a comment frame holds a field line"
			}
		}
		return f
	}
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "event: ") || !strings.HasPrefix(lines[1], "data: ") {
		f.bad = "not exactly one event line and one data line"
		return f
	}
	f.event = strings.TrimPrefix(lines[0], "event: ")
	f.data = strings.TrimPrefix(lines[1], "data: ")
	return f
}

// sseStream is the client side of one open stream: its frames, in order, as
// they arrive.
type sseStream struct {
	t      *testing.T
	resp   *http.Response
	cancel context.CancelFunc
	frames chan sseFrame
	opened time.Time

	mu  sync.Mutex
	err error // what ended the reading; nil for a clean end
	eof time.Time
}

// request opens GET /runs/{id}/events as client and returns the response,
// whatever its status.
func (x *streamHarness) request(t *testing.T, runID, client string) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, x.srv.URL+pathEvents(runID), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(HeaderEdgeSecret, goodSecret) // as the edge sends every request
	if client != "" {
		req.Header.Set(HeaderClient, client)
	}
	resp, err := x.srv.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatalf("open the stream of %s: %v", runID, err)
	}
	t.Cleanup(func() {
		cancel()
		resp.Body.Close()
	})
	return resp, cancel
}

// open opens a stream that must answer 200 and starts reading it.
func (x *streamHarness) open(t *testing.T, runID, client string) *sseStream {
	t.Helper()
	opened := time.Now()
	resp, cancel := x.request(t, runID, client)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("stream of %s for %s: %d %s; want 200", runID, client, resp.StatusCode, body)
	}
	st := &sseStream{t: t, resp: resp, cancel: cancel, frames: make(chan sseFrame, 1024), opened: opened}
	go st.read()
	return st
}

func (st *sseStream) read() {
	defer close(st.frames)
	br := bufio.NewReader(st.resp.Body)
	var raw strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			st.mu.Lock()
			st.eof = time.Now()
			switch {
			case line != "" || raw.Len() > 0:
				st.err = fmt.Errorf("the stream ended inside a frame: %q", raw.String()+line)
			case !errors.Is(err, io.EOF):
				st.err = err
			}
			st.mu.Unlock()
			return
		}
		raw.WriteString(line)
		if line == "\n" {
			st.frames <- parseFrame(raw.String())
			raw.Reset()
		}
	}
}

// ended reports what ended the reading and when.
func (st *sseStream) ended() (time.Time, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.eof, st.err
}

// next returns the next frame, or false once the stream has ended; it fails
// the test if neither comes within d, or if the frame breaks the framing.
func (st *sseStream) next(d time.Duration) (sseFrame, bool) {
	st.t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case f, ok := <-st.frames:
		if ok && f.bad != "" {
			st.t.Errorf("frame %q: %s", f.raw, f.bad)
		}
		return f, ok
	case <-timer.C:
		st.t.Fatalf("no frame and no end within %v", d)
		return sseFrame{}, false
	}
}

// nextEvent returns the next event frame, skipping comments.
func (st *sseStream) nextEvent() sseFrame {
	st.t.Helper()
	for {
		f, ok := st.next(3 * time.Second)
		if !ok {
			_, err := st.ended()
			st.t.Fatalf("the stream ended (%v); want another event", err)
		}
		if !f.comment {
			return f
		}
	}
}

// eventsUntil returns the event frames up to and including the first of type
// typ.
func (st *sseStream) eventsUntil(typ string) []sseFrame {
	st.t.Helper()
	var out []sseFrame
	for {
		f := st.nextEvent()
		out = append(out, f)
		if f.event == typ {
			return out
		}
	}
}

// requireEOF requires the stream to end cleanly within d. With comments
// false, nothing at all may come first (after an end, not even a heartbeat);
// otherwise only comments may.
func (st *sseStream) requireEOF(d time.Duration, comments bool) {
	st.t.Helper()
	deadline := time.Now().Add(d)
	for {
		f, ok := st.next(time.Until(deadline) + time.Millisecond)
		if !ok {
			break
		}
		if !comments || !f.comment {
			st.t.Errorf("a frame before the stream's end: %q", f.raw)
		}
	}
	if _, err := st.ended(); err != nil {
		st.t.Errorf("the stream did not end cleanly: %v", err)
	}
}

// decodeRun decodes an event's data as a Run.
func decodeRun(t *testing.T, f sseFrame) Run {
	t.Helper()
	var r Run
	if err := json.Unmarshal([]byte(f.data), &r); err != nil {
		t.Fatalf("%s data %q is not a run: %v", f.event, f.data, err)
	}
	return r
}

func eventNames(fs []sseFrame) []string {
	var out []string
	for _, f := range fs {
		out = append(out, f.event)
	}
	return out
}

// ---- the brief's tests ----

// TestEvents_StateFirstThenEnd: a stream opened on a queued run gets state
// first, then the run's events as it runs, then end, then EOF within 100 ms;
// end's data is the run as GET /runs/{id} shows it.
func TestEvents_StateFirstThenEnd(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
	x.f.autopilot = true
	runID := x.postOK("a")
	st := x.open(t, runID, "a")
	state := st.nextEvent()
	if state.event != EventState {
		t.Fatalf("first event %q; want state", state.event)
	}
	if r := decodeRun(t, state); r.RunID != runID || r.Status != RunQueued || r.Position == nil || *r.Position != 0 {
		t.Errorf("state %s; want the queued run at position 0", state.data)
	}
	x.start()
	evs := st.eventsUntil(EventEnd)
	endAt := time.Now()
	st.requireEOF(time.Second, false)
	if eof, _ := st.ended(); eof.Sub(endAt) > 100*time.Millisecond {
		t.Errorf("EOF %v after the end frame; want within 100 ms", eof.Sub(endAt))
	}
	end := decodeRun(t, evs[len(evs)-1])
	requireEnded(t, end, RunCompleted, Reason{})
	if rec := x.get(pathRun(runID), "a"); strings.TrimSpace(rec.Body.String()) != evs[len(evs)-1].data {
		t.Errorf("end data\n %s\nGET run\n %s", evs[len(evs)-1].data, rec.Body)
	}
	var trail []string
	for _, f := range evs[:len(evs)-1] {
		if f.event != EventPhase {
			t.Errorf("event %s %s between state and end; want only phase events", f.event, f.data)
			continue
		}
		var pe struct {
			RunID      string `json:"run_id"`
			PhaseIndex int    `json:"phase_index"`
			Status     string `json:"status"`
		}
		if err := json.Unmarshal([]byte(f.data), &pe); err != nil || pe.RunID != runID {
			t.Errorf("phase data %s", f.data)
		}
		trail = append(trail, fmt.Sprintf("%d:%s", pe.PhaseIndex, pe.Status))
	}
	if want := []string{"0:running", "0:completed", "1:running", "1:completed", "2:running", "2:completed"}; !slices.Equal(trail, want) {
		t.Errorf("phase events %v; want %v", trail, want)
	}
	x.requireReleased(t, time.Second)
}

// TestEvents_FinishedRun: a run that ended in this process: state, end (the
// same run), EOF, without waiting for anything.
func TestEvents_FinishedRun(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{})
	x.f.autopilot = true
	runID := x.postOK("a")
	x.start()
	waitFor(t, "the run to end", func() bool {
		r, err := x.d.view(context.Background(), runID)
		return err == nil && r.Status == RunCompleted
	})
	st := x.open(t, runID, "a")
	requireStateEndEOF(t, st, runID, RunCompleted, Reason{})
	x.requireReleased(t, time.Second)
}

// requireStateEndEOF: state, then end with the same run, then EOF within
// 100 ms of opening, so long before the maximum age.
func requireStateEndEOF(t *testing.T, st *sseStream, runID string, status RunStatus, reason Reason) {
	t.Helper()
	state, end := st.nextEvent(), st.nextEvent()
	if state.event != EventState || end.event != EventEnd {
		t.Fatalf("events %s, %s; want state, end", state.event, end.event)
	}
	if state.data != end.data {
		t.Errorf("state %s\nend   %s\nwant the same run", state.data, end.data)
	}
	r := decodeRun(t, end)
	if r.RunID != runID {
		t.Errorf("end of run %s; want %s", r.RunID, runID)
	}
	requireEnded(t, r, status, reason)
	st.requireEOF(time.Second, false)
	if eof, _ := st.ended(); eof.Sub(st.opened) > 100*time.Millisecond {
		t.Errorf("EOF %v after opening; want within 100 ms", eof.Sub(st.opened))
	}
}

// TestEvents_EndedBeyondTheHubsMemory: a run whose end the hub cannot replay,
// because manteion restarted (the run is read back from the database) or the
// run left the hub's memory of ended runs, still gets state, end, EOF at once:
// a reconnecting client never waits for the maximum age.
func TestEvents_EndedBeyondTheHubsMemory(t *testing.T) {
	t.Parallel()
	t.Run("after a restart", func(t *testing.T) {
		t.Parallel()
		x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
		for _, tc := range []struct {
			status string
			want   RunStatus
			reason Reason
		}{{"completed", RunCompleted, Reason{}}, {"failed", RunFailed, ReasonPhaseFailed}, {"cancelled", RunCancelled, ReasonInterrupted}} {
			s := seed(t, x.f, tc.status, false)
			st := x.open(t, s.runID, "a")
			requireStateEndEOF(t, st, s.runID, tc.want, tc.reason)
		}
		x.requireReleased(t, time.Second)
	})
	t.Run("forgotten by the hub", func(t *testing.T) {
		t.Parallel()
		x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
		x.f.autopilot = true
		runID := x.postOK("a")
		x.start()
		waitFor(t, "the run to end", func() bool {
			r, err := x.d.view(context.Background(), runID)
			return err == nil && r.Status == RunCompleted
		})
		// hubSealed more runs end after it: the hub no longer remembers its end.
		for i := 0; i < hubSealed; i++ {
			other := newRun(id.New(RunIDPrefix), DefaultScenario(), testNow)
			other.Status, other.Reason = RunCancelled, ReasonInterrupted
			x.d.hub.Publish(other.RunID, Event{Type: EventEnd, Data: other})
		}
		replay, unsub := x.d.hub.Subscribe(runID)
		unsub()
		if ev, ok := <-replay; ok {
			t.Fatalf("the hub still replays %s for the run", ev.Type)
		}
		st := x.open(t, runID, "a")
		requireStateEndEOF(t, st, runID, RunCompleted, Reason{})
		x.requireReleased(t, time.Second)
	})
}

// TestEvents_SubscribeBeforeView: what is published while the stream reads
// the run's view is not lost. A queue event published then follows the state;
// an end published then (and gone from the hub's memory at once, so no replay
// can stand in for it) ends the stream at once.
func TestEvents_SubscribeBeforeView(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		publish func(x *streamHarness, r Run)
		want    []string
	}{
		{"queue", func(x *streamHarness, r Run) {
			x.d.hub.Publish(r.RunID, Event{Type: EventQueue, Data: QueueEvent{RunID: r.RunID, Position: 0}})
		}, []string{EventState, EventQueue}},
		{"end", func(x *streamHarness, r Run) {
			ended := cloneRun(r)
			ended.Status, ended.Reason, ended.Position, ended.EndedAt = RunCancelled, ReasonInterrupted, nil, &testNow
			x.d.hub.Publish(r.RunID, Event{Type: EventEnd, Data: ended})
			for i := 0; i < hubSealed; i++ {
				other := newRun(id.New(RunIDPrefix), DefaultScenario(), testNow)
				x.d.hub.Publish(other.RunID, Event{Type: EventEnd, Data: other})
			}
		}, []string{EventState, EventEnd}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
			x.postOK("a")
			runID := x.postOK("b")
			view := x.d.view
			x.d.view = func(ctx context.Context, id string) (Run, error) {
				r, err := view(ctx, id)
				if err == nil {
					tc.publish(x, r)
				}
				return r, err
			}
			st := x.open(t, runID, "b")
			var got []string
			for range tc.want {
				got = append(got, st.nextEvent().event)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("events %v; want %v", got, tc.want)
			}
			if tc.want[len(tc.want)-1] == EventEnd {
				st.requireEOF(time.Second, false)
				if eof, _ := st.ended(); eof.Sub(st.opened) > 100*time.Millisecond {
					t.Errorf("EOF %v after opening; want at once", eof.Sub(st.opened))
				}
			}
		})
	}
}

// TestEvents_UnknownRun404: a malformed or unknown run id is 404 not_found and
// a database that cannot be read is 503 unavailable, in the package's JSON
// error shape, before any stream starts; nothing stays held.
func TestEvents_UnknownRun404(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{})
	var viewed atomic.Int32
	view := x.d.view
	x.d.view = func(ctx context.Context, runID string) (Run, error) {
		viewed.Add(1)
		return view(ctx, runID)
	}
	for _, bad := range []string{"nope", "demo-123", "exp-123", "demo-0F1E2D3C-4B5A-6978-8796-A5B4C3D2E1F0"} {
		requireError(t, x.get(pathEvents(bad), "a"), http.StatusNotFound, "not_found")
	}
	if n := viewed.Load(); n != 0 {
		t.Errorf("malformed ids reached the run store %d times", n)
	}
	rec := x.get(pathEvents(id.New(RunIDPrefix)), "a")
	requireError(t, rec, http.StatusNotFound, "not_found")
	requireJSONHeaders(t, "unknown run", rec.Header())

	resp, _ := x.request(t, id.New(RunIDPrefix), "a")
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNotFound || string(body) != `{"error":"not_found"}` {
		t.Errorf("unknown run through the server: %d %s", resp.StatusCode, body)
	}
	requireJSONHeaders(t, "unknown run through the server", resp.Header)

	x.f.fail("experiments.List", errLeakyAll)
	rec = x.get(pathEvents(id.New(RunIDPrefix)), "a")
	requireError(t, rec, http.StatusServiceUnavailable, "unavailable")
	requireNoLeak(t, "events, database down", rec)
	x.requireReleased(t, time.Second)
}

func requireJSONHeaders(t *testing.T, what string, h http.Header) {
	t.Helper()
	for k, want := range map[string]string{"Content-Type": "application/json", "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"} {
		if got := h.Get(k); got != want {
			t.Errorf("%s: %s %q; want %q", what, k, got, want)
		}
	}
	if v := h.Get("X-Accel-Buffering"); v != "" {
		t.Errorf("%s: X-Accel-Buffering %q on an error", what, v)
	}
}

// TestEvents_DisconnectReleasesSlot: a client that goes away after the first
// frame releases its hub subscription, its stream slot and its goroutines
// within 200 ms, and can open three streams again. Not parallel: it counts
// goroutines.
func TestEvents_DisconnectReleasesSlot(t *testing.T) {
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
	runID := x.postOK("a")
	before := runtime.NumGoroutine()
	var open []*sseStream
	for i := 0; i < 3; i++ {
		st := x.open(t, runID, "a")
		if f := st.nextEvent(); f.event != EventState {
			t.Fatalf("first event %s", f.event)
		}
		open = append(open, st)
	}
	if subs, slots := x.d.hub.Count(), openStreamCount(x.d.lim); subs != 3 || slots != 3 {
		t.Fatalf("%d subscriptions and %d slots with three streams open", subs, slots)
	}
	requireStatus(t, x, runID, "a", http.StatusTooManyRequests)

	// The client goes away: cancelling a request closes its connection.
	for _, st := range open {
		st.cancel()
	}
	x.requireReleased(t, 200*time.Millisecond)
	x.srv.Client().CloseIdleConnections()
	waitGoroutines(t, before, time.Second)

	for i := 0; i < 3; i++ {
		st := x.open(t, runID, "a")
		if f := st.nextEvent(); f.event != EventState {
			t.Fatalf("reopened stream %d: first event %s", i, f.event)
		}
	}
	if slots := openStreamCount(x.d.lim); slots != 3 {
		t.Errorf("%d slots after reopening three streams; want 3", slots)
	}
}

// requireStatus opens a stream and requires status, then closes it.
func requireStatus(t *testing.T, x *streamHarness, runID, client string, status int) *http.Response {
	t.Helper()
	resp, cancel := x.request(t, runID, client)
	defer cancel()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != status {
		t.Fatalf("stream of %s for %s: %d %s; want %d", runID, client, resp.StatusCode, body, status)
	}
	if status == http.StatusTooManyRequests {
		if string(body) != `{"error":"too_many_streams"}` {
			t.Errorf("429 body %s; want too_many_streams", body)
		}
		if n, err := strconv.Atoi(resp.Header.Get("Retry-After")); err != nil || n < 1 {
			t.Errorf("Retry-After %q; want a positive number of seconds", resp.Header.Get("Retry-After"))
		}
		requireJSONHeaders(t, "429", resp.Header)
	}
	return resp
}

// waitGoroutines waits until no more goroutines run than before.
func waitGoroutines(t *testing.T, before int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, true)
			t.Fatalf("%d goroutines after %v; want at most %d as before:\n%s", runtime.NumGoroutine(), within, before, buf[:n])
		}
		time.Sleep(time.Millisecond)
	}
}

// TestEvents_MaxAgeCloses: a stream whose run never ends is closed at its
// maximum age (300 ms): EOF by 400 ms, no end frame, nothing held. Not
// parallel: it counts goroutines.
func TestEvents_MaxAgeCloses(t *testing.T) {
	x := newStreamDemo(t, streamOpts{})
	runID := x.postOK("a")
	before := runtime.NumGoroutine()
	st := x.open(t, runID, "a")
	if f := st.nextEvent(); f.event != EventState {
		t.Fatalf("first event %s", f.event)
	}
	st.requireEOF(time.Second, true)
	eof, _ := st.ended()
	if age := eof.Sub(st.opened); age < 250*time.Millisecond || age > 400*time.Millisecond {
		t.Errorf("EOF %v after opening; want at the 300 ms maximum age, by 400 ms", age)
	}
	x.requireReleased(t, 100*time.Millisecond)
	x.srv.Client().CloseIdleConnections()
	waitGoroutines(t, before, time.Second)
}

// TestEvents_StreamCaps: a client's fourth stream and the 51st in all are 429
// too_many_streams with Retry-After, in the JSON error shape; a closed stream
// frees its slot.
func TestEvents_StreamCaps(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
	runID := x.postOK("a")
	var first *sseStream
	for i := 0; i < 3; i++ {
		st := x.open(t, runID, "a")
		if i == 0 {
			first = st
		}
	}
	requireStatus(t, x, runID, "a", http.StatusTooManyRequests)

	// 47 more from other clients: 50 in all.
	for i := 0; i < 47; i++ {
		x.open(t, runID, fmt.Sprintf("c%02d", i/3))
	}
	if slots := openStreamCount(x.d.lim); slots != 50 {
		t.Fatalf("%d slots; want 50", slots)
	}
	requireStatus(t, x, runID, "fresh", http.StatusTooManyRequests)

	first.cancel()
	waitFor(t, "the slot to free", func() bool { return openStreamCount(x.d.lim) == 49 })
	st := x.open(t, runID, "a")
	if f := st.nextEvent(); f.event != EventState {
		t.Errorf("stream after a slot freed: first event %s", f.event)
	}
}

// TestEvents_ShutdownCloses: Shutdown ends every open stream (an end frame,
// if any, last), releases everything, and a stream opened afterwards ends at
// once.
func TestEvents_ShutdownCloses(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
	a, b := x.postOK("a"), x.postOK("b")
	x.start()
	waitFor(t, "the first run's phase to run", func() bool { _, _, running := x.d.w.Current(); return running })
	streams := []*sseStream{x.open(t, a, "a"), x.open(t, a, "b"), x.open(t, b, "b")}
	for _, st := range streams {
		if f := st.nextEvent(); f.event != EventState {
			t.Fatalf("first event %s", f.event)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = x.d.Shutdown(ctx)
	for i, st := range streams {
		for {
			f, ok := st.next(time.Second)
			if !ok {
				break
			}
			if f.event == EventEnd {
				requireEnded(t, decodeRun(t, f), RunCancelled, ReasonInterrupted)
				st.requireEOF(time.Second, false)
				break
			}
			if !f.comment && f.event != EventQueue && f.event != EventPhase {
				t.Errorf("stream %d: %s during shutdown", i, f.event)
			}
		}
		if _, err := st.ended(); err != nil {
			t.Errorf("stream %d did not end cleanly: %v", i, err)
		}
	}
	x.requireReleased(t, time.Second)

	st := x.open(t, b, "c")
	state, end := st.nextEvent(), st.nextEvent()
	if state.event != EventState || end.event != EventEnd {
		t.Errorf("after shutdown: %s, %s; want state, end", state.event, end.event)
	}
	st.requireEOF(time.Second, false)
}

// TestEvents_HubClosedWithoutEnd: when the hub closes a stream's channel
// without an end (the demo shutting down), the response just ends: no end
// frame, nothing held.
func TestEvents_HubClosedWithoutEnd(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
	runID := x.postOK("a")
	st := x.open(t, runID, "a")
	if f := st.nextEvent(); f.event != EventState {
		t.Fatalf("first event %s", f.event)
	}
	x.d.hub.CloseAll()
	st.requireEOF(time.Second, true)
	x.requireReleased(t, time.Second)
}

// ---- beyond the brief ----

// TestEvents_Headers: a stream is text/event-stream, never cached, sniffed or
// buffered by a proxy, and carries no CORS header.
func TestEvents_Headers(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{})
	runID := x.postOK("a")
	st := x.open(t, runID, "a")
	for k, want := range map[string]string{
		"Content-Type":           "text/event-stream",
		"Cache-Control":          "no-store",
		"X-Accel-Buffering":      "no",
		"X-Content-Type-Options": "nosniff",
	} {
		if got := st.resp.Header.Get(k); got != want {
			t.Errorf("%s %q; want %q", k, got, want)
		}
	}
	for k := range st.resp.Header {
		if strings.HasPrefix(k, "Access-Control-") || k == "Server" || k == "Retry-After" {
			t.Errorf("header %s present", k)
		}
	}
}

// TestEvents_Heartbeats: every Config.Heartbeat a comment ": hb" frame comes;
// it is a comment, which an EventSource never turns into an event.
func TestEvents_Heartbeats(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 120 * time.Millisecond }})
	runID := x.postOK("a")
	st := x.open(t, runID, "a")
	var events, beats int
	for {
		f, ok := st.next(time.Second)
		if !ok {
			break
		}
		switch {
		case f.comment:
			beats++
			if f.raw != ": hb\n\n" {
				t.Errorf("heartbeat %q; want %q", f.raw, ": hb\n\n")
			}
		default:
			events++
			if f.event != EventState {
				t.Errorf("event %s; want only the state", f.event)
			}
		}
	}
	if events != 1 || beats < 5 {
		t.Errorf("%d events and %d heartbeats in 120 ms; want 1 and at least 5", events, beats)
	}
}

// TestEvents_TwoStreamsBothGetEnd: two streams of one run both receive its
// end, then EOF.
func TestEvents_TwoStreamsBothGetEnd(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
	x.f.autopilot = true
	runID := x.postOK("a")
	one, two := x.open(t, runID, "a"), x.open(t, runID, "b")
	x.start()
	var ends []string
	for _, st := range []*sseStream{one, two} {
		evs := st.eventsUntil(EventEnd)
		if evs[0].event != EventState {
			t.Errorf("first event %s", evs[0].event)
		}
		ends = append(ends, evs[len(evs)-1].data)
		st.requireEOF(time.Second, false)
	}
	if ends[0] != ends[1] {
		t.Errorf("the two ends differ:\n%s\n%s", ends[0], ends[1])
	}
	x.requireReleased(t, time.Second)
}

// TestEvents_QueuedRunSeesItsTurn: a stream of the third run in line gets its
// position as the runs ahead end, then its own phases once it starts, then
// its end.
func TestEvents_QueuedRunSeesItsTurn(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second }})
	x.f.autopilot = true
	x.postOK("a")
	x.postOK("b")
	runID := x.postOK("c")
	st := x.open(t, runID, "c")
	state := st.nextEvent()
	if r := decodeRun(t, state); state.event != EventState || r.Position == nil || *r.Position != 2 {
		t.Fatalf("state %s %s; want the run at position 2", state.event, state.data)
	}
	x.start()
	evs := st.eventsUntil(EventEnd)
	var positions []int
	firstPhase := -1
	for i, f := range evs {
		switch f.event {
		case EventQueue:
			if firstPhase >= 0 {
				t.Errorf("a queue event after the run's first phase event")
			}
			var q QueueEvent
			if err := json.Unmarshal([]byte(f.data), &q); err != nil || q.RunID != runID {
				t.Errorf("queue data %s", f.data)
			}
			positions = append(positions, q.Position)
		case EventPhase:
			if firstPhase < 0 {
				firstPhase = i
			}
		case EventEnd:
		default:
			t.Errorf("event %s", f.event)
		}
	}
	if !slices.Equal(positions, []int{1, 0}) {
		t.Errorf("positions %v; want [1 0]", positions)
	}
	if firstPhase < 0 {
		t.Errorf("no phase event before the end: %v", eventNames(evs))
	}
	requireEnded(t, decodeRun(t, evs[len(evs)-1]), RunCompleted, Reason{})
	st.requireEOF(time.Second, false)
}

// TestEvents_CountsOneRead: opening a stream is one read of the client's 120
// a minute.
func TestEvents_CountsOneRead(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{})
	runID := x.postOK("a")
	for i := 0; i < readLimit-1; i++ {
		if rec := x.get(pathState, "r"); rec.Code != http.StatusOK {
			t.Fatalf("read %d: %d", i+1, rec.Code)
		}
	}
	x.open(t, runID, "r")
	requireError(t, x.get(pathState, "r"), http.StatusTooManyRequests, "rate_limited")
	resp := requireStatusAny(t, x, runID, "r")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("a stream past the read limit: %d; want 429", resp.StatusCode)
	}
}

func requireStatusAny(t *testing.T, x *streamHarness, runID, client string) *http.Response {
	t.Helper()
	resp, cancel := x.request(t, runID, client)
	defer cancel()
	_, _ = io.ReadAll(resp.Body)
	return resp
}

// TestEvents_SurvivesServerWriteTimeout: the server's WriteTimeout does not cut
// a stream: frames keep coming long after it, until the maximum age.
func TestEvents_SurvivesServerWriteTimeout(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{server: func(s *http.Server) { s.WriteTimeout = 50 * time.Millisecond }})
	runID := x.postOK("a")
	st := x.open(t, runID, "a")
	var late int
	for {
		f, ok := st.next(time.Second)
		if !ok {
			break
		}
		if f.comment && time.Since(st.opened) > 150*time.Millisecond {
			late++
		}
	}
	if late < 5 {
		t.Errorf("%d heartbeats after 150 ms with a 50 ms server WriteTimeout; want the stream to live to its maximum age", late)
	}
	eof, err := st.ended()
	if err != nil {
		t.Errorf("the stream did not end cleanly: %v", err)
	}
	if age := eof.Sub(st.opened); age < 250*time.Millisecond {
		t.Errorf("the stream ended after %v; want at its 300 ms maximum age", age)
	}
}

// panicWriter panics on every write after the first after writes.
type panicWriter struct {
	http.ResponseWriter
	after  int32
	writes atomic.Int32
}

func (p *panicWriter) Write(b []byte) (int, error) {
	if p.writes.Add(1) > p.after {
		panic("demo test: the stream's writer broke")
	}
	return p.ResponseWriter.Write(b)
}

func (p *panicWriter) Flush() { p.ResponseWriter.(http.Flusher).Flush() }

func (p *panicWriter) Unwrap() http.ResponseWriter { return p.ResponseWriter }

// TestEvents_PanicReleasesSlot: a panic in the middle of a stream releases its
// slot and its subscription; the panic is logged, never sent.
func TestEvents_PanicReleasesSlot(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{
		tweak: func(c *Config, _ *Deps) { c.StreamMaxAge = 10 * time.Second },
		wrap: func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				h.ServeHTTP(&panicWriter{ResponseWriter: w, after: 1}, r)
			})
		},
	})
	runID := x.postOK("a")
	st := x.open(t, runID, "a")
	if f := st.nextEvent(); f.event != EventState {
		t.Fatalf("first event %s", f.event)
	}
	for {
		f, ok := st.next(time.Second)
		if !ok {
			break
		}
		if strings.Contains(f.raw, "panic") || strings.Contains(f.raw, "goroutine") {
			t.Errorf("the panic reached the client: %q", f.raw)
		}
	}
	x.requireReleased(t, time.Second)
	if !logLineHas(x.logs.String(), "panicked", `"level":"ERROR"`) {
		t.Errorf("the panic was not logged:\n%s", x.logs)
	}
}

// smallBuffers makes every accepted connection's send buffer small, so a
// client that stops reading blocks the server's writes quickly.
type smallBuffers struct{ net.Listener }

func (l smallBuffers) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetWriteBuffer(4096)
	}
	return c, err
}

// TestEvents_StalledClientIsCut: a client that keeps its connection open but
// stops reading is cut once a write has not completed within the per-write
// deadline (100 ms here; the maximum age is far off), and its slot and
// subscription are released.
func TestEvents_StalledClientIsCut(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{
		tweak: func(c *Config, _ *Deps) {
			c.StreamMaxAge = 30 * time.Second
			c.StreamWriteTimeout = 100 * time.Millisecond
		},
		listener: func(l net.Listener) net.Listener { return smallBuffers{l} },
	})
	runID := x.postOK("a")
	dialer := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		}); err != nil {
			return err
		}
		return serr
	}}
	conn, err := dialer.Dial("tcp", x.srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: demo\r\nX-Demo-Client: stalled\r\n\r\n", pathEvents(runID))
	waitFor(t, "the stream to subscribe", func() bool { return x.d.hub.Count() == 1 })

	// The client never reads. Publish until the server's writes block and
	// the stream is cut.
	ev := Event{Type: EventPhase, Data: PhaseEvent{RunID: runID, Phase: PhaseNames[0], Status: PhaseRunning, At: testNow}}
	deadline := time.Now().Add(10 * time.Second)
	for x.d.hub.Count() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the stalled client still holds its stream after 10 s")
		}
		for i := 0; i < hubBuffer; i++ {
			x.d.hub.Publish(runID, ev)
		}
		time.Sleep(100 * time.Microsecond)
	}
	x.requireReleased(t, time.Second)
}

// TestEncodeFrame: each contract event encodes as exactly "event: <type>\n
// data: <json>\n\n", whatever its strings hold; anything that is not a
// contract event with its own payload type does not encode, and an empty
// sample encodes to nothing.
func TestEncodeFrame(t *testing.T) {
	t.Parallel()
	nasty := "demo-a\nb\r\n\ndata: x\n\nevent: end"
	run := newRun(nasty, DefaultScenario(), testNow)
	v := 5.0
	good := []Event{
		{EventState, run},
		{EventEnd, run},
		{EventQueue, QueueEvent{RunID: nasty, Position: 1}},
		{EventPhase, PhaseEvent{RunID: nasty, Phase: PhaseNames[1], PhaseIndex: 1, Status: PhaseRunning, At: testNow}},
		{EventSample, Sample{RunID: nasty, Phase: nasty, At: testNow, RPS: &v}},
	}
	for _, ev := range good {
		frame, err := encodeFrame(ev)
		if err != nil {
			t.Errorf("%s: %v", ev.Type, err)
			continue
		}
		s := string(frame)
		data, _ := json.Marshal(ev.Data)
		if want := "event: " + ev.Type + "\ndata: " + string(data) + "\n\n"; s != want {
			t.Errorf("%s frame\n %q\nwant\n %q", ev.Type, s, want)
		}
		if strings.Count(s, "\n") != 3 || strings.ContainsRune(s, '\r') || strings.Index(s, "\n\n") != len(s)-2 {
			t.Errorf("%s frame %q: want exactly three line feeds, the last two ending it, and no carriage return", ev.Type, s)
		}
		if f := parseFrame(s); f.bad != "" || f.event != ev.Type || f.data != string(data) {
			t.Errorf("%s frame %q parses as %+v", ev.Type, s, f)
		}
	}

	nan := 0.0 / zero()
	for name, ev := range map[string]Event{
		"unknown type":            {"debug", run},
		"empty type":              {"", run},
		"state with a phase":      {EventState, PhaseEvent{}},
		"end as a pointer":        {EventEnd, &run},
		"phase with a run":        {EventPhase, run},
		"queue with a string":     {EventQueue, "position 1"},
		"sample with an error":    {EventSample, errors.New("pq: 192.0.2.10")},
		"state with an error":     {EventState, errors.New("pq: 192.0.2.10")},
		"run outside the list":    {EventState, Run{RunID: "demo-x", Status: "paused"}},
		"phase status not public": {EventPhase, PhaseEvent{Status: "draining"}},
		"nil data":                {EventQueue, nil},
	} {
		if frame, err := encodeFrame(ev); err == nil {
			t.Errorf("%s: encoded as %q; want an error", name, frame)
		}
	}
	for name, s := range map[string]Sample{
		"all nil": {RunID: "demo-x", Phase: PhaseNames[0], At: testNow},
		"all NaN": {RunID: "demo-x", Phase: PhaseNames[0], At: testNow, RPS: &nan, P95Ms: &nan, ErrorRate: &nan},
	} {
		if frame, err := encodeFrame(Event{EventSample, s}); err != nil || frame != nil {
			t.Errorf("empty sample (%s): %q, %v; want nothing and no error", name, frame, err)
		}
	}
}

func zero() float64 { return 0 }

// TestEvents_SamplesReachTheStream: while a phase runs, the demo's sampler
// publishes the frontend's three metrics and the phase's target's own CPU and
// memory, and the stream forwards them as sample frames with exactly the
// contract's keys.
func TestEvents_SamplesReachTheStream(t *testing.T) {
	t.Parallel()
	x := newStreamDemo(t, streamOpts{tweak: func(c *Config, _ *Deps) {
		c.StreamMaxAge = 10 * time.Second
		c.SampleEvery = 20 * time.Millisecond
		c.QueryTimeout = 500 * time.Millisecond // answers come at once; a round ends with its last
	}})
	target := DefaultScenario().Memory.Target // baseline's: the memory fault's target
	x.f.mu.Lock()
	x.f.promValues[QueryRPS], x.f.promValues[QueryP95Ms], x.f.promValues[QueryErrorRate] = 5, 250.5, 0.02
	x.f.promValues[liveTargetQuery(FaultCPU, target)] = 0.031
	x.f.promValues[liveTargetQuery(FaultMemory, target)] = 47_185_920
	x.f.mu.Unlock()
	runID := x.postOK("a")
	x.start()
	waitFor(t, "the first phase to run", func() bool { _, _, running := x.d.w.Current(); return running })
	st := x.open(t, runID, "a")
	evs := st.eventsUntil(EventSample)
	f := evs[len(evs)-1]
	want := fmt.Sprintf(`{"run_id":%q,"phase":"baseline","at":%q,"rps":5,"p95_ms":250.5,"error_rate":0.02,"target_cpu_cores":0.031,"target_memory_bytes":47185920}`, runID, stamp(testNow))
	if f.data != want {
		t.Errorf("sample\n %s\nwant\n %s", f.data, want)
	}
}

// TestDemo_RunStartsAndStopsTheSampler: Run samples while a phase runs; once
// Shutdown has stopped the worker, Run stops the sampler before it returns,
// and once the queries in flight then have ended (the worker's group), no
// query follows.
func TestDemo_RunStartsAndStopsTheSampler(t *testing.T) {
	t.Parallel()
	x := newDemo(t, func(c *Config, _ *Deps) {
		c.SampleEvery = 10 * time.Millisecond
		c.QueryTimeout = 5 * time.Millisecond
	})
	x.f.mu.Lock()
	x.f.promValues[QueryRPS] = 1
	x.f.mu.Unlock()
	x.postOK("a")
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		x.d.Run(context.Background())
	}()
	waitFor(t, "a query", func() bool { return len(x.f.callList("prom.QueryInstant ")) >= 3 })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = x.d.Shutdown(ctx)
	waitClosed(t, ran, "Run to return after Shutdown")
	// A query started before the stop may still be in the client; each is
	// counted in the worker's group, and one that had not reached the client
	// by then never does.
	waitClosed(t, groupDone(&x.d.w.bg), "the demo's goroutines to end")
	n := len(x.f.callList("prom.QueryInstant "))
	time.Sleep(50 * time.Millisecond)
	if m := len(x.f.callList("prom.QueryInstant ")); m != n {
		t.Errorf("%d queries after Run returned and its goroutines ended; want none", m-n)
	}
}
