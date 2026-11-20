package demo

import (
	"slices"
	"strings"
	"sync"

	"manteion-go/internal/model"
)

// Run lookups for the routes. A run that is not in memory (the line, or the
// worker's ended runs) is read from the database by Worker.View, for GET
// /runs/{id} and for an event stream. Three rules keep any request rate from
// tying up the database's small connection pool, which the SDKs' polls, the
// worker's rule switch and the orchestrator also need:
//
//   - The bound. At most maxDBLookups of View's database lookups run at once;
//     each holds one slot across its few sequential reads, so the routes
//     occupy at most that many database round-trips at once. A lookup that
//     finds every slot taken is ErrRunUnavailable (503) at once: it neither
//     queues nor waits on the pool. The worker's own reads and writes, the
//     sweep, retention and the single-flight refreshes of the recent list and
//     of health take no slot.
//   - The run index (runIndex). While the latest listing of the demo's
//     experiments completed, an id outside the index is ErrRunNotFound without
//     a query.
//   - Ended views. An ended run reads the same from the database for ever, so
//     its view is kept (at most maxCachedViews) and a visitor re-reading an
//     old run costs the store once.

// maxDBLookups is the number of View's database lookups that may run at once.
const maxDBLookups = 2

// maxCachedViews bounds the ended runs' views kept from the database.
const maxCachedViews = 32

// runIndex knows which demo runs may exist, and keeps the views of ended runs
// read from the database. Its zero value is ready: incomplete and empty.
//
// The index holds the run ids of the demo experiments the latest complete
// listing found (the start-up sweep and every retention pass list them all, by
// name demo-run-<id>), plus every run of this process that has ended since:
// until a run ends, the line holds it, and an ended run's experiment, if it has
// one, was created before it ended. An id leaves it when retention deletes its
// experiment, when a lookup finds no experiment for it, and when a complete
// listing that began after the id was recorded does not find it. So it holds at
// most the demo experiments that exist (retention keeps 20 finished ones)
// and the runs ended since the latest complete listing.
//
// While the latest listing completed, an id outside the index has no
// experiment. Before the first listing, and after one that failed, the index
// is incomplete and says nothing.
type runIndex struct {
	mu sync.Mutex
	// known maps each id to the listing epoch at which it was recorded.
	known    map[string]uint64
	complete bool
	// epoch counts the listings begun; only the latest one's result is
	// applied, so a slow listing cannot overwrite a newer one.
	epoch uint64
	// drops counts the ids dropped; a view read across a drop is not kept, as
	// its run may have just been deleted.
	drops uint64
	views map[string]Run // ended runs' views, read from the database
	order []string       // the keys of views, oldest kept first
}

// beginListing starts a listing of the demo's experiments; pass its epoch to
// listed.
func (x *runIndex) beginListing() uint64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.epoch++
	return x.epoch
}

// listed applies a listing that began at epoch, unless a later one has begun
// since. An incomplete listing makes the index incomplete. A complete one
// replaces the listed ids with the demo runs in exps, keeping only the ids
// recorded since it began (runs that ended while it read); the kept views of
// ids no longer known go too.
func (x *runIndex) listed(epoch uint64, exps []*model.Experiment, complete bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if epoch != x.epoch {
		return
	}
	if !complete {
		x.complete = false
		return
	}
	next := make(map[string]uint64, len(exps))
	for id, at := range x.known {
		if at >= epoch {
			next[id] = at
		}
	}
	for _, e := range exps {
		if runID, ok := runIDOf(e); ok {
			next[runID] = epoch
		}
	}
	x.known, x.complete = next, true
	for _, id := range x.order {
		if _, ok := x.known[id]; !ok {
			x.dropViewLocked(id)
		}
	}
}

// ended records a run of this process that has just ended.
func (x *runIndex) ended(runID string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.known == nil {
		x.known = map[string]uint64{}
	}
	x.known[runID] = x.epoch
}

// drop forgets runID and its kept view: retention deleted its experiment, or a
// lookup found none.
func (x *runIndex) drop(runID string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.known, runID)
	x.dropViewLocked(runID)
	x.drops++
}

// lookup answers what the index knows of runID: absent is true when the index
// is complete and does not hold it (the run does not exist); otherwise view
// and ok are its kept view, if any.
func (x *runIndex) lookup(runID string) (view Run, ok, absent bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if _, known := x.known[runID]; x.complete && !known {
		return Run{}, false, true
	}
	if r, ok := x.views[runID]; ok {
		return cloneRun(r), true, false
	}
	return Run{}, false, false
}

// mark is what keep needs to know that no id was dropped meanwhile; take it
// before the database is read.
func (x *runIndex) mark() uint64 {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.drops
}

// keep stores an ended run's view read from the database, unless an id was
// dropped since mark (the run may be gone) or the index knows the run does
// not exist; the oldest view goes when maxCachedViews are kept.
func (x *runIndex) keep(r Run, mark uint64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.drops != mark {
		return
	}
	if _, known := x.known[r.RunID]; x.complete && !known {
		return
	}
	if x.views == nil {
		x.views = map[string]Run{}
	}
	if _, ok := x.views[r.RunID]; !ok {
		if len(x.order) >= maxCachedViews {
			x.dropViewLocked(x.order[0])
		}
		x.order = append(x.order, r.RunID)
	}
	x.views[r.RunID] = cloneRun(r)
}

func (x *runIndex) dropViewLocked(runID string) {
	if _, ok := x.views[runID]; !ok {
		return
	}
	delete(x.views, runID)
	if i := slices.Index(x.order, runID); i >= 0 {
		x.order = append(x.order[:i:i], x.order[i+1:]...)
	}
}

// size is how many ids the index holds and how many views it keeps.
func (x *runIndex) size() (ids, views int) {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.known), len(x.views)
}

// runIDOf is the run id of one of the demo's experiments, demo-run-<run id>.
func runIDOf(e *model.Experiment) (string, bool) {
	if !ownExperiment(e) {
		return "", false
	}
	runID := strings.TrimPrefix(e.Name, RunNamePrefix)
	return runID, validRunID(runID)
}

// settled reports whether a view read from the database will not change: its
// experiment has ended and so has every phase (a cancel ends the experiment
// first, then its phases).
func settled(exp *model.Experiment, r Run) bool {
	if !terminalStatus(exp.Status) {
		return false
	}
	for _, p := range r.Phases {
		switch p.Status {
		case PhaseCompleted, PhaseFailed, PhaseSkipped:
		default:
			return false
		}
	}
	return true
}
