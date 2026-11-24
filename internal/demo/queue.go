package demo

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"manteion-go/internal/id"
)

// Enqueue's refusals. They are for the routes to map to error codes
// (client_has_run, queue_full); their text never reaches a client.
var (
	// ErrQueueFull: capacity runs are already queued or running.
	ErrQueueFull = errors.New("demo: the queue is full")
	// ErrClientHasRun: the client already has a run queued or running.
	ErrClientHasRun = errors.New("demo: the client already has a run queued or running")
	// ErrQueueClosed: the worker has been halted or has stopped; no run will
	// be picked up any more (a route answers 503 unavailable).
	ErrQueueClosed = errors.New("demo: the queue is closed")
)

// Queue is the line of runs that have not ended, in memory: the run the
// worker is on first, then the waiting ones in the order they came. Its
// capacity counts the running run. A run's place in the line is its position
// (0 for the first, even before the worker has taken it).
//
// A run leaves the line only through remove, which the worker calls exactly
// once per run, when the run has ended: that is what frees its slot and its
// client. A second remove of the same run frees nothing.
//
// The queue also keeps each waiting or running run's live view (its Run, less
// the position, which is computed); the worker updates it as the run goes.
// Every Run it hands out is a deep copy. A run's ended view never lives here:
// the worker records it and takes the run out of the line in one step.
//
// The worker closes the queue when it is halted or stops; Enqueue then
// refuses. The worker reads the runs still waiting after closing it, so a run
// is either refused or ended, never left in line.
type Queue struct {
	mu       sync.Mutex
	capacity int
	line     []*queued
	closed   bool
	wake     chan struct{} // holds a token when a run was added since the worker last looked
}

// queued is one run in the line.
type queued struct {
	client string
	run    Run  // Position is not kept here: it is the entry's index in line
	taken  bool // the worker has picked the run up
}

// NewQueue returns an empty queue holding at most capacity runs (at least 1).
func NewQueue(capacity int) *Queue {
	if capacity < 1 {
		capacity = 1
	}
	return &Queue{capacity: capacity, wake: make(chan struct{}, 1)}
}

// Enqueue adds a run for client at the end of the line and returns its view:
// status queued, its position, queued at now. It refuses with ErrQueueClosed
// once the worker was halted or stopped, then with ErrClientHasRun when the
// client already has a run in the line, and with ErrQueueFull when the line is
// at capacity. The run's id is id.New(RunIDPrefix). Nothing is built here: the
// worker builds a run when it picks it up.
func (q *Queue) Enqueue(client string, now time.Time) (Run, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Run{}, ErrQueueClosed
	}
	for _, e := range q.line {
		if e.client == client {
			return Run{}, ErrClientHasRun
		}
	}
	if len(q.line) >= q.capacity {
		return Run{}, ErrQueueFull
	}
	e := &queued{client: client, run: newRun(id.New(RunIDPrefix), now)}
	q.line = append(q.line, e)
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return withPosition(e.run, len(q.line)-1), nil
}

// Snapshot returns every run in the line, in line order (position 0 first),
// each with its position.
func (q *Queue) Snapshot() []Run {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Run, len(q.line))
	for i, e := range q.line {
		out[i] = withPosition(e.run, i)
	}
	return out
}

// check answers as Enqueue would for client now, short of adding a run and of
// the capacity check: ErrQueueClosed, or ErrClientHasRun with the position of
// the client's run, or nil.
func (q *Queue) check(client string) (pos int, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, ErrQueueClosed
	}
	for i, e := range q.line {
		if e.client == client {
			return i, ErrClientHasRun
		}
	}
	return 0, nil
}

// isClosed reports whether the queue was closed (Enqueue refuses).
func (q *Queue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// closeLine closes the queue: Enqueue refuses from now on and next returns
// false. Idempotent.
func (q *Queue) closeLine() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// next waits until the first run in the line is one the worker has not taken
// yet, marks it taken and returns its id. It returns false once ctx is done or
// the queue is closed, without taking anything.
func (q *Queue) next(ctx context.Context) (string, bool) {
	for {
		if ctx.Err() != nil {
			return "", false
		}
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return "", false
		}
		if len(q.line) > 0 && !q.line[0].taken {
			q.line[0].taken = true
			runID := q.line[0].run.RunID
			q.mu.Unlock()
			return runID, true
		}
		q.mu.Unlock()
		select {
		case <-q.wake:
		case <-ctx.Done():
			return "", false
		}
	}
}

// waiting returns the ids of the runs in the line the worker has not taken.
func (q *Queue) waiting() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []string
	for _, e := range q.line {
		if !e.taken {
			out = append(out, e.run.RunID)
		}
	}
	return out
}

// get returns a copy of the run's view with its position.
func (q *Queue) get(runID string) (Run, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, e := range q.line {
		if e.run.RunID == runID {
			return withPosition(e.run, i), true
		}
	}
	return Run{}, false
}

// update applies fn to the run's live view. fn must replace pointer fields,
// never write through them: views handed out earlier share nothing with it,
// but fn's own writes must not reach into them either. It reports false when
// the run is not in the line.
func (q *Queue) update(runID string, fn func(*Run)) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, e := range q.line {
		if e.run.RunID == runID {
			fn(&e.run)
			return true
		}
	}
	return false
}

// remove takes the run out of the line, freeing its slot and its client, and
// returns the new positions of the runs that were behind it. ok is false when
// the run is not in the line: removing a run twice frees one slot, once.
func (q *Queue) remove(runID string) (moved []QueueEvent, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, e := range q.line {
		if e.run.RunID != runID {
			continue
		}
		q.line = append(q.line[:i:i], q.line[i+1:]...)
		for j := i; j < len(q.line); j++ {
			moved = append(moved, QueueEvent{RunID: q.line[j].run.RunID, Position: j})
		}
		return moved, true
	}
	return nil, false
}

// ---- run views ----

// newRun is a queued run's first view: the preset's three phases pending, the
// fixed testbed, no position (callers add it).
func newRun(runID string, queuedAt time.Time) Run {
	r := Run{RunID: runID, Status: RunQueued, QueuedAt: queuedAt, Testbed: FixedTestbed()}
	for i := range r.Phases {
		r.Phases[i] = PhaseView{Name: PhaseNames[i], Status: PhasePending, DurationS: PhaseDurationS}
		if i == FaultPhaseIndex {
			label := FaultLabel
			r.Phases[i].Fault = &label
		}
	}
	return r
}

// withPosition returns a deep copy of r at position pos.
func withPosition(r Run, pos int) Run {
	c := cloneRun(r)
	c.Position = &pos
	return c
}

// cloneRun returns a deep copy of r: no pointer in it is shared with r.
func cloneRun(r Run) Run {
	c := r
	c.Position = clonePtr(r.Position)
	c.StartedAt = clonePtr(r.StartedAt)
	c.EndedAt = clonePtr(r.EndedAt)
	for i := range c.Phases {
		p := &c.Phases[i]
		p.Fault = clonePtr(p.Fault)
		p.Target = clonePtr(p.Target)
		p.Intensity = clonePtr(p.Intensity)
		p.StartedAt = clonePtr(p.StartedAt)
		p.EndedAt = clonePtr(p.EndedAt)
		p.Result = clonePtr(p.Result)
		p.FaultObserved = clonePtr(p.FaultObserved)
		if p.Measure != nil {
			m := *p.Measure
			m.Value, m.Baseline = clonePtr(m.Value), clonePtr(m.Baseline)
			p.Measure = &m
		}
	}
	return c
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// validRunID reports whether s has the shape of a run id this package mints:
// RunIDPrefix, a dash, and a UUID in lower-case canonical form. Only such ids
// are ever looked up in the database.
func validRunID(s string) bool {
	rest, ok := strings.CutPrefix(s, RunIDPrefix+"-")
	if !ok || len(rest) != 36 {
		return false
	}
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
				return false
			}
		}
	}
	return true
}
