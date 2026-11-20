package demo

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestLimiter() (*Limiter, *fakeClock) {
	c := &fakeClock{now: testNow}
	return NewLimiter(c), c
}

// Inspection helpers.

func (l *Limiter) clientCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.clients)
}

func (l *Limiter) has(client string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.clients[client]
	return ok
}

func (l *Limiter) openStreams() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.streams)
}

// sweepNow sweeps at the clock's time, as use would once a minute.
func (l *Limiter) sweepNow() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweep(l.clock.Now().UnixNano())
}

// TestLimiter_Reads: 120 reads in a rolling minute per client; the 121st is
// refused (and not counted), and allowed once the first read is a minute old.
func TestLimiter_Reads(t *testing.T) {
	t.Parallel()
	l, c := newTestLimiter()
	for i := 0; i < 120; i++ {
		if !l.AllowRead("a") {
			t.Fatalf("read %d refused", i+1)
		}
	}
	if l.AllowRead("a") {
		t.Fatal("the 121st read was allowed")
	}
	if ok, wait := l.takeRead("a"); ok || wait != time.Minute {
		t.Errorf("121st again: ok %v, wait %v; want refused, 1m", ok, wait)
	}
	if !l.AllowRead("b") {
		t.Error("another client's first read was refused")
	}
	c.Advance(59 * time.Second)
	if ok, wait := l.takeRead("a"); ok || wait != time.Second {
		t.Errorf("at +59 s: ok %v, wait %v; want refused, 1s", ok, wait)
	}
	c.Advance(time.Second)
	if !l.AllowRead("a") {
		t.Fatal("at +60 s the read was refused")
	}

	// Rolling, not fixed windows: 60 reads at t, 60 at t+30 s; at t+60 s only
	// the first 60 have left the window.
	l, c = newTestLimiter()
	for i := 0; i < 60; i++ {
		l.AllowRead("r")
	}
	c.Advance(30 * time.Second)
	for i := 0; i < 60; i++ {
		if !l.AllowRead("r") {
			t.Fatalf("read %d at +30 s refused", 61+i)
		}
	}
	if l.AllowRead("r") {
		t.Fatal("a 121st read within the minute was allowed")
	}
	c.Advance(30 * time.Second)
	for i := 0; i < 60; i++ {
		if !l.AllowRead("r") {
			t.Fatalf("read %d at +60 s refused", i+1)
		}
	}
	if ok, wait := l.takeRead("r"); ok || wait != 30*time.Second {
		t.Errorf("after refilling: ok %v, wait %v; want refused, 30s", ok, wait)
	}
}

// TestLimiter_Enqueue: one enqueue per 5 minutes per client, counted only by
// RecordEnqueue.
func TestLimiter_Enqueue(t *testing.T) {
	t.Parallel()
	l, c := newTestLimiter()
	if !l.AllowEnqueue("a") || !l.AllowEnqueue("a") {
		t.Fatal("AllowEnqueue refused before any enqueue was recorded (it must not count by itself)")
	}
	l.RecordEnqueue("a")
	if l.AllowEnqueue("a") {
		t.Fatal("allowed right after an enqueue")
	}
	if !l.AllowEnqueue("b") {
		t.Error("another client refused")
	}
	c.Advance(4*time.Minute + 59*time.Second)
	if ok, wait := l.enqueueWait("a"); ok || wait != time.Second {
		t.Errorf("at +4m59s: ok %v, wait %v; want refused, 1s", ok, wait)
	}
	c.Advance(time.Second)
	if !l.AllowEnqueue("a") {
		t.Error("at +5m: refused")
	}
}

// TestLimiter_Streams: 3 streams per client and 50 in all; a release frees
// its slot once, however often it is called.
func TestLimiter_Streams(t *testing.T) {
	t.Parallel()
	l, _ := newTestLimiter()
	var rel []func()
	for i := 0; i < 3; i++ {
		r, ok := l.AcquireStream("a")
		if !ok {
			t.Fatalf("stream %d refused", i+1)
		}
		rel = append(rel, r)
	}
	r4, ok := l.AcquireStream("a")
	if ok {
		t.Fatal("a 4th stream for one client was allowed")
	}
	r4() // a refusal hands back a release that does nothing
	if _, ok, wait := l.takeStream("a"); ok || wait < time.Second || wait > PhaseDurationS*time.Second {
		t.Errorf("4th: ok %v, wait %v; want refused, 1 s..%d s", ok, wait, PhaseDurationS)
	}
	rel[0]()
	rel[0]()
	if _, ok := l.AcquireStream("a"); !ok {
		t.Fatal("refused after a release")
	}
	if _, ok := l.AcquireStream("a"); ok {
		t.Fatal("a double release freed two slots")
	}

	l, _ = newTestLimiter()
	var all []func()
	for i := 0; i < 50; i++ {
		r, ok := l.AcquireStream(fmt.Sprintf("c%d", i/2))
		if !ok {
			t.Fatalf("stream %d refused", i+1)
		}
		all = append(all, r)
	}
	if _, ok := l.AcquireStream("fresh"); ok {
		t.Fatal("the 51st stream in all was allowed")
	}
	all[7]()
	if _, ok := l.AcquireStream("fresh"); !ok {
		t.Fatal("refused after one of the 50 was released")
	}
}

// TestLimiter_StreamRetryAfter: the wait for a stream is the time until the
// oldest blocking stream reaches its maximum age, at most one phase.
func TestLimiter_StreamRetryAfter(t *testing.T) {
	t.Parallel()
	l, c := newTestLimiter()
	l.streamMaxAge = time.Minute
	for i := 0; i < 3; i++ {
		l.AcquireStream("a")
		c.Advance(10 * time.Second)
	}
	// Opened at 0, 10 s, 20 s; now 30 s; the oldest closes at 60 s at the latest.
	if _, ok, wait := l.takeStream("a"); ok || wait != 30*time.Second {
		t.Errorf("wait %v (ok %v); want 30s", wait, ok)
	}
	c.Advance(29*time.Second + 500*time.Millisecond)
	if _, _, wait := l.takeStream("a"); wait != 500*time.Millisecond {
		t.Errorf("wait %v; want 500ms (the route rounds it up to 1 s)", wait)
	}
	l.streamMaxAge = time.Hour
	if _, _, wait := l.takeStream("a"); wait != PhaseDurationS*time.Second {
		t.Errorf("wait %v; want capped at one phase", wait)
	}
}

// TestLimiter_ClientCap: the table holds at most 10 000 clients. While it is
// full, a client it does not hold is not refused: every such client shares
// one overflow bucket with the per-client limits; clients it holds keep their
// own entries; DefaultClient has a bucket of its own outside the table. An
// entry with no live state expires a minute after its last use, which makes
// room again; an entry with a recent enqueue or an open stream does not.
func TestLimiter_ClientCap(t *testing.T) {
	t.Parallel()
	l, c := newTestLimiter()
	for i := 0; i < maxClients; i++ {
		if !l.AllowRead(fmt.Sprintf("c%05d", i)) {
			t.Fatalf("client %d refused before the table was full", i)
		}
	}
	if n := l.clientCount(); n != maxClients {
		t.Fatalf("%d clients; want %d", n, maxClients)
	}

	// New clients are served from one shared bucket: 120 reads between all.
	for i := 0; i < readLimit; i++ {
		if !l.AllowRead(fmt.Sprintf("new%03d", i)) {
			t.Fatalf("new client %d refused while the table is full", i)
		}
	}
	if ok, wait := l.takeRead("yet-another"); ok || wait != time.Minute {
		t.Errorf("the overflow bucket's 121st read: ok %v, wait %v; want refused, 1m", ok, wait)
	}
	if !l.AllowEnqueue("newE") {
		t.Fatal("a new client's enqueue refused while the table is full")
	}
	l.RecordEnqueue("newE")
	if l.AllowEnqueue("newF") {
		t.Error("the overflow bucket allowed a second enqueue within 5 minutes")
	}
	for i := 0; i < streamsPerClient; i++ {
		if _, ok := l.AcquireStream(fmt.Sprintf("newS%d", i)); !ok {
			t.Fatalf("overflow stream %d refused", i+1)
		}
	}
	if _, ok := l.AcquireStream("newS9"); ok {
		t.Error("the overflow bucket allowed a 4th stream")
	}
	if n := l.clientCount(); n != maxClients {
		t.Fatalf("%d clients after the overflow; want %d", n, maxClients)
	}
	for _, k := range []string{"new000", "newE", "newS0"} {
		if l.has(k) {
			t.Errorf("%s got a table entry while the table is full", k)
		}
	}
	// Known clients and DefaultClient are not affected.
	if !l.AllowRead("c00042") {
		t.Error("a known client was refused while the table is full")
	}
	for i := 0; i < readLimit; i++ {
		if !l.AllowRead(DefaultClient) {
			t.Fatalf("%s's read %d refused: it has a bucket of its own", DefaultClient, i+1)
		}
	}
	if l.has(DefaultClient) {
		t.Errorf("%s holds a table entry", DefaultClient)
	}

	// Three clients keep live state: an enqueue, an open stream, a read.
	l.RecordEnqueue("c00001")
	if _, ok := l.AcquireStream("c00002"); !ok {
		t.Fatal("stream refused for a known client")
	}
	c.Advance(30 * time.Second)
	l.AllowRead("c00003")

	c.Advance(31 * time.Second) // 61 s after the fill: the fill's reads are idle
	if !l.AllowRead("new") || !l.has("new") {
		t.Fatal("a new client has no entry of its own after the idle entries expired")
	}
	// Kept: c00001 (enqueue), c00002 (open stream), c00003 (read 31 s ago),
	// and the new client. Every other entry was last used 61 s ago.
	if n := l.clientCount(); n != 4 {
		t.Errorf("%d clients after expiry; want 4", n)
	}
	for _, k := range []string{"c00001", "c00002", "c00003", "new"} {
		if !l.has(k) {
			t.Errorf("%s expired; it holds live state", k)
		}
	}
}

// TestLimiter_Expiry: what keeps an entry and for how long.
func TestLimiter_Expiry(t *testing.T) {
	t.Parallel()
	l, c := newTestLimiter()
	l.AllowRead("reader")
	l.RecordEnqueue("enqueuer")
	rel, _ := l.AcquireStream("streamer")
	c.Advance(time.Minute)
	l.sweepNow()
	if l.has("reader") {
		t.Error("an entry whose only read is a minute old was kept")
	}
	if !l.has("enqueuer") || !l.has("streamer") {
		t.Error("an entry with live state expired")
	}
	c.Advance(4 * time.Minute)
	l.sweepNow()
	if l.has("enqueuer") {
		t.Error("an entry whose enqueue is 5 minutes old was kept")
	}
	c.Advance(time.Hour)
	l.sweepNow()
	if !l.has("streamer") {
		t.Error("an entry with an open stream expired")
	}
	rel()
	c.Advance(time.Minute)
	l.sweepNow()
	if l.has("streamer") || l.clientCount() != 0 {
		t.Errorf("%d entries left; want 0", l.clientCount())
	}

	// Sweeps happen in the course of use: a read a minute later sweeps.
	l.AllowRead("x")
	c.Advance(2 * time.Minute)
	l.AllowRead("y")
	if l.has("x") {
		t.Error("no sweep happened during use")
	}
}

// TestLimiter_ClockGoesBack: a wall clock stepped back never locks a client
// out for longer than the limits themselves.
func TestLimiter_ClockGoesBack(t *testing.T) {
	t.Parallel()
	l, c := newTestLimiter()
	for i := 0; i < 120; i++ {
		l.AllowRead("a")
	}
	l.RecordEnqueue("a")
	c.Advance(-time.Hour)
	if !l.AllowRead("a") {
		t.Error("reads stay refused after the clock went back")
	}
	if ok, wait := l.enqueueWait("a"); ok || wait > 5*time.Minute {
		t.Errorf("enqueue: ok %v, wait %v; want refused for at most 5m", ok, wait)
	}
}

// TestLimiter_Concurrent: concurrent use is safe and counts exactly.
func TestLimiter_Concurrent(t *testing.T) {
	t.Parallel()
	l, _ := newTestLimiter()
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if l.AllowRead("shared") {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
				if r, ok := l.AcquireStream("shared"); ok {
					r()
				}
			}
		}()
	}
	wg.Wait()
	if allowed != 120 {
		t.Errorf("%d reads allowed; want exactly 120", allowed)
	}
	if n := l.openStreams(); n != 0 {
		t.Errorf("%d streams still open", n)
	}
}
