package demo

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Demo ties the package together: the queue, the event hub, the per-client
// limiter and the worker, behind the public routes (Handler).
//
// The wiring's order (see also sweep.go):
//
//	d := demo.New(deps, cfg)
//	d.SwitchOffAll(ctx)              // before the listener starts
//	start the listener with d.Handler()
//	d.Sweep(ctx with a 2-minute deadline) // then POST is accepted
//	go d.Run(ctx)
//	...
//	d.Shutdown(ctx with about 15 s)  // on SIGTERM, before the listener's own Shutdown
//
// Until Sweep has run, POST /runs answers 503 unavailable and the lab reports
// not accepting; the read routes work from the start.
type Demo struct {
	d   Deps
	cfg Config
	log *slog.Logger

	q   *Queue
	hub *Hub
	w   *Worker
	lim *Limiter

	ready    atomic.Bool // the start-up sweep has run
	stopping atomic.Bool // Shutdown has begun

	// admitMu serialises POST's admission (the queue's checks, the enqueue
	// limit, the enqueue and its record), so two racing POSTs of one client
	// cannot both pass the limit. Nothing in it waits on I/O.
	admitMu sync.Mutex

	health healthCache
	logKey []byte // keys the client tags of the request log

	// The run store as the routes reach it: the worker's views and the
	// queue. Tests replace them to reach error paths the real ones do not
	// take.
	view    func(ctx context.Context, runID string) (Run, error)
	recent  func(ctx context.Context, n int) ([]Run, bool)
	enqueue func(client string, now time.Time) (Run, error)
}

// New builds the demo over d: a queue of cfg.Capacity, a hub, a limiter and
// the worker, whose OnRunEnded runs the worker's own retention pass
// (Worker.Retain, never the package's Retain). d.Clock is required; a
// non-positive duration or count in cfg takes its DefaultConfig value. New
// starts nothing.
func New(d Deps, cfg Config) *Demo {
	if d.Clock == nil {
		panic("demo: New needs Deps.Clock")
	}
	cfg = cfg.withDefaults()
	q, hub := NewQueue(cfg.Capacity), NewHub()
	w := NewWorker(d, cfg, q, hub)
	lim := NewLimiter(d.Clock)
	lim.streamMaxAge = cfg.StreamMaxAge
	x := &Demo{d: d, cfg: cfg, log: d.logger(), q: q, hub: hub, w: w, lim: lim, logKey: make([]byte, 32)}
	_, _ = rand.Read(x.logKey)
	x.view, x.recent, x.enqueue = w.View, w.Recent, q.Enqueue
	w.OnRunEnded = func(ctx context.Context) {
		if err := w.Retain(ctx); err != nil {
			x.log.Warn("demo: retention after a run did not complete; the next pass tries again", "error", err)
		}
	}
	return x
}

// withDefaults replaces every non-positive duration and count with its
// DefaultConfig value.
func (c Config) withDefaults() Config {
	def := DefaultConfig()
	for _, f := range []struct{ v, d *time.Duration }{
		{&c.RunCap, &def.RunCap}, {&c.StatusPoll, &def.StatusPoll}, {&c.SampleEvery, &def.SampleEvery},
		{&c.QueryTimeout, &def.QueryTimeout}, {&c.Heartbeat, &def.Heartbeat}, {&c.StreamMaxAge, &def.StreamMaxAge},
		{&c.HealthTTL, &def.HealthTTL},
	} {
		if *f.v <= 0 {
			*f.v = *f.d
		}
	}
	for _, f := range []struct{ v, d *int }{{&c.Capacity, &def.Capacity}, {&c.Retain, &def.Retain}, {&c.Recent, &def.Recent}} {
		if *f.v <= 0 {
			*f.v = *f.d
		}
	}
	return c
}

// routeTimeout bounds a route's reads (a run's view, the recent list): a few
// store reads, each bounded by QueryTimeout.
func (c Config) routeTimeout() time.Duration { return 4 * c.QueryTimeout }

// Run works the queue, one run at a time (Worker.Run), and samples the live
// metrics while a phase runs (RunSampler), until ctx is done or Shutdown is
// called; the sampler has stopped when it returns. Call it once, after Sweep.
func (d *Demo) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, prom, hub, cfg := d.w, d.d.Prom, d.hub, d.cfg
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		RunSampler(ctx, w, prom, hub, cfg)
	}()
	w.Run(ctx)
	cancel()
	<-sampled
}

// SwitchOffAll is the start-up switch-off (the package's SwitchOffAll): every
// enabled demo rule off, database only. Call it before the listener starts.
// A failure is logged here and returned; Sweep tries again.
func (d *Demo) SwitchOffAll(ctx context.Context) error {
	err := SwitchOffAll(ctx, d.d)
	if err != nil {
		d.log.Error("demo: the start-up switch-off did not complete; the sweep tries again", "error", err)
	}
	return err
}

// Sweep runs the start-up sweep (the package's Sweep, keeping Config.Retain)
// and then opens the lab: POST is accepted from now on. It opens the lab even
// when the sweep reports incomplete, which is logged here and returned: the
// lab must not stay closed because one clean-up step failed, and the worker's
// guarantees (each run's rule off on every ending) do not depend on the
// sweep; the next start's sweep tries again. Give ctx a deadline (the wiring
// gives 2 minutes).
func (d *Demo) Sweep(ctx context.Context) error {
	err := Sweep(ctx, d.d, d.cfg.Retain)
	if err != nil {
		d.log.Error("demo: the start-up sweep did not complete; accepting runs anyway", "error", err)
	}
	d.markReady()
	return err
}

// markReady opens the lab to POST.
func (d *Demo) markReady() {
	if !d.ready.Swap(true) {
		d.log.Info("demo: accepting runs")
	}
}

// Shutdown stops the demo as manteion shuts down: POST answers 503
// unavailable from now on; the worker shuts down (Worker.Shutdown: the
// current rule switched off, every run in line ended interrupted), and the
// error it returns is logged and returned; then every event stream's channel
// is closed (Hub.CloseAll). It returns by ctx's deadline (the wiring gives it
// its own context of about 15 s). Safe to call more than once.
func (d *Demo) Shutdown(ctx context.Context) error {
	d.stopping.Store(true)
	err := d.w.Shutdown(ctx)
	if err != nil {
		d.log.Error("demo: shutdown: the worker did not stop cleanly; the next start's switch-off and sweep finish the job", "error", err)
	}
	d.hub.CloseAll()
	return err
}

// accepting reports whether a POST now would be picked up in its turn: the
// start-up sweep has run, no shutdown has begun, the queue is open, and no
// ended run's switch-off is pending (the worker holds the next run until it
// lands; POST still queues meanwhile).
func (d *Demo) accepting() bool {
	return d.ready.Load() && !d.stopping.Load() && !d.q.isClosed() && !d.w.SwitchOffPending()
}

// ---- health ----

// The health checks, in the order of HealthChecks.
const (
	checkDatabase = iota
	checkLoadGenerator
	checkMetrics
	checkTarget
	checkIdle
	numChecks
)

var checkNames = [numChecks]string{"database", "load_generator", "metrics", "target", "idle"}

// discardLog is the logger of a probe whose check already failed last time:
// a failing dependency is described once, when its check turns false.
var discardLog = slog.New(slog.DiscardHandler)

// healthCache holds the last health answer, at most Config.HealthTTL old, and
// runs at most one refresh at a time, in a goroutine of its own, so no lock is
// held while probing and a caller that goes away does not cut a refresh
// short. With no answer yet, every caller waits for the refresh. Once there is
// one, the caller that starts a refresh waits for it (bounded by
// Config.QueryTimeout) and every other caller meanwhile gets the previous
// answer.
type healthCache struct {
	mu       sync.Mutex
	have     bool
	checks   [numChecks]bool
	at       time.Time
	inflight chan struct{} // closed when the refresh in flight ends; nil when none runs

	// busy marks a check whose probe has not returned yet. A probe that
	// ignores its deadline stays busy; until it returns, its check is
	// reported false without probing again, so a hung dependency costs one
	// goroutine, not one per refresh.
	busy [numChecks]atomic.Bool
}

// healthChecks returns the current checks; see healthCache. A caller whose
// ctx ends while it waits gets every check false.
func (d *Demo) healthChecks(ctx context.Context) [numChecks]bool {
	hc := &d.health
	hc.mu.Lock()
	now := d.d.Clock.Now()
	if hc.have && !now.Before(hc.at) && now.Sub(hc.at) < d.cfg.HealthTTL {
		c := hc.checks
		hc.mu.Unlock()
		return c
	}
	if hc.inflight != nil && hc.have {
		c := hc.checks
		hc.mu.Unlock()
		return c
	}
	done := hc.inflight
	if done == nil {
		done = make(chan struct{})
		hc.inflight = done
		go d.refreshHealth(done, hc.checks, hc.have)
	}
	hc.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return [numChecks]bool{}
	}
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.checks
}

// refreshHealth probes every check and stores the answer. A panic stores
// every check false, stamped so that the next caller refreshes again.
func (d *Demo) refreshHealth(done chan struct{}, prev [numChecks]bool, had bool) {
	hc := &d.health
	var (
		checks [numChecks]bool
		at     time.Time
	)
	defer func() {
		if r := recover(); r != nil {
			d.log.Error("demo: a health refresh panicked", "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			checks, at = [numChecks]bool{}, time.Time{}
		}
		hc.mu.Lock()
		hc.checks, hc.have, hc.at, hc.inflight = checks, true, at, nil
		hc.mu.Unlock()
		close(done)
	}()
	checks = d.probeAll(prev, had)
	at = d.d.Clock.Now()
	if !had || checks != prev {
		var failing []string
		for i, ok := range checks {
			if !ok {
				failing = append(failing, checkNames[i])
			}
		}
		if len(failing) == 0 {
			d.log.Info("demo: health: every check passes")
		} else {
			d.log.Warn("demo: health: checks failing", "failing", failing)
		}
	}
}

// probeAll runs the five probes at once, each with its own QueryTimeout
// deadline:
//
//   - database: the database answers a ping;
//   - load_generator: zeus answers and holds a usable demo dataset for a run
//     of Config.RunCap (preflight's check);
//   - metrics: Prometheus answers;
//   - target: productcatalogservice has an alive instance (preflight's check);
//   - idle: no experiment is running except the run in progress's own (the
//     worker's current experiment). An ended run whose switch-off is pending
//     is not the run in progress: while its experiment still runs (its cancel
//     failed too), idle is false; once it has ended, idle is true again, and
//     accepting stays false until the switch-off lands.
//
// A probe's failure is logged only when its check was true before (or on the
// first refresh), with the cause, for manteion's own logs.
func (d *Demo) probeAll(prev [numChecks]bool, had bool) [numChecks]bool {
	own := d.w.currentExperimentID()
	probes := [numChecks]func(context.Context, *slog.Logger) bool{
		checkDatabase: func(ctx context.Context, log *slog.Logger) bool {
			if d.d.DB == nil {
				return false
			}
			if err := d.d.DB.PingContext(ctx); err != nil {
				log.Warn("demo: health: the database did not answer", "error", err)
				return false
			}
			return true
		},
		checkLoadGenerator: func(ctx context.Context, log *slog.Logger) bool {
			return loadGenReady(ctx, d.d, d.cfg.RunCap, log)
		},
		checkMetrics: func(ctx context.Context, log *slog.Logger) bool {
			if d.d.Prom == nil || !d.d.Prom.Healthy(ctx) {
				log.Warn("demo: health: Prometheus is not healthy")
				return false
			}
			return true
		},
		checkTarget: func(ctx context.Context, log *slog.Logger) bool {
			return targetReady(ctx, d.d, log)
		},
		checkIdle: func(ctx context.Context, log *slog.Logger) bool {
			idle, err := labIdle(ctx, d.d, own)
			if err != nil {
				log.Warn("demo: health could not list the running experiments", "error", err)
			}
			return err == nil && idle
		},
	}
	var (
		out [numChecks]bool
		wg  sync.WaitGroup
	)
	for i, p := range probes {
		log := discardLog
		if !had || prev[i] {
			log = d.log
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i] = d.probe(i, log, p)
		}()
	}
	wg.Wait()
	return out
}

// probe runs fn in a goroutine with a QueryTimeout deadline and returns its
// answer, or false at the deadline whether fn honours it or not; a panic in
// fn is false. While an earlier probe of the same check has not returned, it
// returns false without calling fn.
func (d *Demo) probe(i int, log *slog.Logger, fn func(context.Context, *slog.Logger) bool) bool {
	busy := &d.health.busy[i]
	if !busy.CompareAndSwap(false, true) {
		log.Warn("demo: health: the previous probe has not returned; the check fails", "check", checkNames[i])
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.cfg.QueryTimeout)
	result := make(chan bool, 1)
	go func() {
		ok := false
		// In this order: the mark clears before the answer is handed over
		// (a refresh that follows at once must find the check free), and the
		// context is cancelled only after (the waiting side must not take the
		// cancel for the deadline).
		defer func() {
			if r := recover(); r != nil {
				d.log.Error("demo: a health probe panicked", "check", checkNames[i], "panic", fmt.Sprint(r),
					"stack", string(debug.Stack()))
				ok = false
			}
			busy.Store(false)
			result <- ok
			cancel()
		}()
		ok = fn(ctx, log)
	}()
	select {
	case ok := <-result:
		return ok
	case <-ctx.Done():
		select {
		case ok := <-result:
			return ok
		default:
			log.Warn("demo: health: a probe did not answer in time", "check", checkNames[i], "deadline", d.cfg.QueryTimeout)
			return false
		}
	}
}
