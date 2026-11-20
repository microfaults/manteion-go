package demo

import (
	"sync"
	"time"
)

// The per-client limits of the public routes (the contract's numbers).
const (
	readLimit        = 120             // reads per client in any rolling readWindow
	readWindow       = time.Minute     //
	enqueueEvery     = 5 * time.Minute // one enqueue per client this often
	streamsPerClient = 3               // open event streams per client
	streamsTotal     = 50              // open event streams in all
	maxClients       = 10_000          // clients the limiter tracks at once

	// sweepEvery is how often, in the course of use, the client map is swept
	// of entries that hold nothing.
	sweepEvery = time.Minute
	// fullSweepEvery is how often a full map may be swept early, on behalf of
	// a new client: bounded, so a stream of new clients cannot turn every
	// request into a scan of the whole map.
	fullSweepEvery = time.Second
	// idleAfter is how long an entry that holds nothing is kept after its
	// last use.
	idleAfter = readWindow
	// streamWaitCap bounds the wait a refused stream is told: a stream
	// usually ends with its run, well before its maximum age.
	streamWaitCap = PhaseDurationS * time.Second
)

// Limiter enforces the per-client limits: 120 reads in any rolling minute,
// one enqueue every 5 minutes, 3 open event streams; and 50 open streams in
// all. A client is the key the routes resolve (clientOf): the edge's
// X-Demo-Client on a request with the edge secret, else DefaultClient.
//
// Buckets. DefaultClient ("local", every caller without the edge secret) has
// a bucket of its own that never expires and is not counted in the table.
// Every other client has an entry in a table of at most 10 000.
//
// Degraded mode, while the table is full: a client the table does not hold
// is not refused; it is accounted to one shared overflow bucket, and every
// such client shares that bucket's limits: 120 reads a minute between all of
// them, one enqueue every 5 minutes between all of them, and 3 open streams
// between all of them (the 50 in all still apply). Clients the table holds
// keep their own entries. The queue's one queued-or-running run per client is
// not the limiter's rule: the queue keys it by the client id itself, not by
// the bucket, so overflow clients are still told apart there. The overflow
// bucket is not reachable by any key: it is a field, not a table entry. The
// mode ends by itself as entries expire.
//
// An entry expires once it holds nothing (no read within the last minute, no
// enqueue within the last 5 minutes, no open stream) and was last used a
// minute ago or more. Expired entries are dropped by a sweep of the table,
// made at most once a minute in the course of use, and at most once a second
// when the table is full and a new client asks; so the table never grows past
// its cap, and a full table of idle clients empties within a minute.
//
// Time comes from the Clock. A wall clock stepped backwards never locks a
// client out for longer than the limits themselves: reads stamped in the
// future are forgotten, and an enqueue stamped in the future counts as made
// now.
//
// A refused request is not counted. The methods are safe for concurrent use.
type Limiter struct {
	clock        Clock
	streamMaxAge time.Duration // an event stream is closed after this long (Config.StreamMaxAge)

	mu        sync.Mutex
	clients   map[string]*clientState // every client but DefaultClient, at most maxClients
	local     *clientState            // DefaultClient's bucket
	overflow  *clientState            // the bucket of clients met while the table is full
	streams   map[*openStream]struct{}
	lastSweep int64 // UnixNano of the last sweep
}

// clientState is one client's entry. Times are UnixNano.
type clientState struct {
	reads []int64 // ring buffer of the reads in the window, oldest at head; grows to readLimit
	head  int
	n     int

	enqueued   bool
	enqueuedAt int64

	streams  int
	lastSeen int64
}

// openStream is one acquired event-stream slot.
type openStream struct {
	c      *clientState
	opened int64
}

// NewLimiter returns a limiter that reads the time from c.
func NewLimiter(c Clock) *Limiter {
	if c == nil {
		panic("demo: NewLimiter needs a Clock")
	}
	return &Limiter{
		clock:        c,
		streamMaxAge: DefaultConfig().StreamMaxAge,
		clients:      map[string]*clientState{},
		local:        &clientState{},
		overflow:     &clientState{},
		streams:      map[*openStream]struct{}{},
	}
}

// AllowRead counts one read for client and reports whether it is allowed: at
// most 120 in any rolling minute. A refused read is not counted.
func (l *Limiter) AllowRead(client string) bool {
	ok, _ := l.takeRead(client)
	return ok
}

// takeRead is AllowRead that also says, when it refuses, how long until a
// read would be allowed.
func (l *Limiter) takeRead(client string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	c := l.entry(client, now)
	c.pruneReads(now)
	if c.n >= readLimit {
		return false, time.Duration(c.reads[c.head] + int64(readWindow) - now)
	}
	c.pushRead(now)
	return true, 0
}

// AllowEnqueue reports whether client may enqueue now: its last recorded
// enqueue is at least 5 minutes old. It records nothing; RecordEnqueue does,
// once the enqueue has succeeded.
func (l *Limiter) AllowEnqueue(client string) bool {
	ok, _ := l.enqueueWait(client)
	return ok
}

// enqueueWait is AllowEnqueue that also says, when it refuses, how long until
// an enqueue would be allowed.
func (l *Limiter) enqueueWait(client string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	c := l.entry(client, now)
	if !c.enqueued {
		return true, 0
	}
	if c.enqueuedAt > now {
		c.enqueuedAt = now // the clock went back
	}
	if left := c.enqueuedAt + int64(enqueueEvery) - now; left > 0 {
		return false, time.Duration(left)
	}
	return true, 0
}

// RecordEnqueue records an enqueue by client now, in the bucket AllowEnqueue
// checked. Call it only once the run was accepted (the 201).
func (l *Limiter) RecordEnqueue(client string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	c := l.entry(client, now)
	c.enqueued, c.enqueuedAt = true, now
}

// AcquireStream takes an event-stream slot for client: at most 3 per client
// and 50 in all. When ok, release frees the slot; it may be called any number
// of times and frees it once. When refused, release does nothing.
func (l *Limiter) AcquireStream(client string) (release func(), ok bool) {
	release, ok, _ = l.takeStream(client)
	return release, ok
}

// takeStream is AcquireStream that also says, when it refuses, how long until
// a slot frees at the latest: the oldest blocking stream reaches its maximum
// age then (it usually ends sooner, with its run), at most streamWaitCap.
func (l *Limiter) takeStream(client string) (release func(), ok bool, wait time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	c := l.entry(client, now)
	if c.streams >= streamsPerClient {
		return func() {}, false, l.streamWait(now, c)
	}
	if len(l.streams) >= streamsTotal {
		return func() {}, false, l.streamWait(now, nil)
	}
	s := &openStream{c: c, opened: now}
	c.streams++
	l.streams[s] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			delete(l.streams, s)
			s.c.streams--
			s.c.lastSeen = max(s.c.lastSeen, l.now())
		})
	}, true, 0
}

// streamWait is how long until the oldest stream of c (of all, when c is
// nil) reaches its maximum age, at most streamWaitCap.
func (l *Limiter) streamWait(now int64, c *clientState) time.Duration {
	wait := streamWaitCap
	for s := range l.streams {
		if c != nil && s.c != c {
			continue
		}
		wait = min(wait, time.Duration(s.opened+int64(l.streamMaxAge)-now))
	}
	return wait
}

// now is the clock's time in UnixNano.
func (l *Limiter) now() int64 { return l.clock.Now().UnixNano() }

// entry returns the bucket client is accounted to, marking it used:
// DefaultClient's own; the client's entry, made if the table has room (after
// an early sweep if it is full); else the shared overflow bucket. Called with
// l.mu held.
func (l *Limiter) entry(client string, now int64) *clientState {
	if l.lastSweep > now {
		l.lastSweep = now // the clock went back
	}
	if now-l.lastSweep >= int64(sweepEvery) {
		l.sweep(now)
	}
	c := l.local
	if client != DefaultClient {
		c = l.tableEntry(client, now)
	}
	c.lastSeen = max(c.lastSeen, now)
	return c
}

// tableEntry is client's table entry, or the overflow bucket while the table
// is full. Called with l.mu held.
func (l *Limiter) tableEntry(client string, now int64) *clientState {
	if c, ok := l.clients[client]; ok {
		return c
	}
	if len(l.clients) >= maxClients && now-l.lastSweep >= int64(fullSweepEvery) {
		l.sweep(now)
	}
	if len(l.clients) >= maxClients {
		return l.overflow
	}
	c := &clientState{}
	l.clients[client] = c
	return c
}

// sweep drops every entry that holds nothing and was last used idleAfter ago
// or more. Called with l.mu held.
func (l *Limiter) sweep(now int64) {
	for k, c := range l.clients {
		if c.idle(now) {
			delete(l.clients, k)
		}
	}
	l.lastSweep = now
}

// idle reports whether the entry holds nothing at now: no open stream, no
// enqueue within enqueueEvery, no read within the window, and no use within
// idleAfter.
func (c *clientState) idle(now int64) bool {
	if c.streams > 0 || now-c.lastSeen < int64(idleAfter) {
		return false
	}
	if c.enqueued && now-c.enqueuedAt < int64(enqueueEvery) {
		return false
	}
	return c.n == 0 || c.newest() <= now-int64(readWindow)
}

// pruneReads forgets the reads that left the window, and every read stamped
// after now (the clock went back).
func (c *clientState) pruneReads(now int64) {
	if c.n > 0 && c.newest() > now {
		c.head, c.n = 0, 0
	}
	for c.n > 0 && c.reads[c.head] <= now-int64(readWindow) {
		c.head = (c.head + 1) % len(c.reads)
		c.n--
	}
}

// pushRead records a read at now; the caller checked c.n < readLimit.
func (c *clientState) pushRead(now int64) {
	if c.n == len(c.reads) {
		grown := make([]int64, min(max(2*len(c.reads), 8), readLimit))
		for i := 0; i < c.n; i++ {
			grown[i] = c.reads[(c.head+i)%len(c.reads)]
		}
		c.reads, c.head = grown, 0
	}
	c.reads[(c.head+c.n)%len(c.reads)] = now
	c.n++
}

func (c *clientState) newest() int64 { return c.reads[(c.head+c.n-1)%len(c.reads)] }
