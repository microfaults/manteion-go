package orchestrator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"manteion-go/internal/model"
)

// TestFinishPhase_StopsLoadBeforeThawAndClear pins M1: on an operator StopPhase
// of a still-firing isolation phase (no drain barrier — persist_cache is false),
// the zeus attacks and runs MUST be stopped BEFORE the frozen service is thawed
// and its replay rule is cleared. In the buggy order the service was un-frozen
// and its rule cleared while tail traffic was still arriving, so that traffic hit
// the service un-frozen and was harvested as frozen data — silent, plausible
// numbers that invalidate the isolation delta.
func TestFinishPhase_StopsLoadBeforeThawAndClear(t *testing.T) {
	ctx := context.Background()

	var mu sync.Mutex
	var order []string
	rec := func(ev string) { mu.Lock(); order = append(order, ev); mu.Unlock() }

	// One recording server standing in for BOTH zeus (/api/v1/*) and the frozen
	// service's SDK (/admin/*, /cachebox/*) — the path namespaces are disjoint.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/rules", func(w http.ResponseWriter, _ *http.Request) {
		rec("clear_rules")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /admin/cachebox", func(w http.ResponseWriter, _ *http.Request) {
		rec("thaw")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /cachebox/fidelity", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	})
	mux.HandleFunc("DELETE /api/v1/attacks/{id}", func(w http.ResponseWriter, _ *http.Request) {
		rec("stop_attack")
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /api/v1/runs/{run_id}", func(w http.ResponseWriter, _ *http.Request) {
		rec("stop_run")
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	o := newOrch(t, srv.URL)

	exp := mkExp(t)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 0) // frozen, NOT persist_cache → no drain barrier
	registerSDK(t, "frontend", srv.URL)

	// A phase workflow carrying live attack + run handles so teardown has load to
	// stop; both DELETEs land on the recording server.
	_ = attachAttackWorkflow(t, iso.ID, model.PhaseWorkflow{
		VUs: 1, DurationSec: 1,
		ZeusAttackID: "atk-" + iso.ID, ZeusRunID: "run-" + iso.ID,
	})

	if !o.finishPhase(ctx, iso.ID, "completed", "running") {
		t.Fatal("finishPhase returned false, want true")
	}

	mu.Lock()
	defer mu.Unlock()
	idx := func(ev string) int {
		for i, e := range order {
			if e == ev {
				return i
			}
		}
		return -1
	}
	for _, ev := range []string{"stop_attack", "stop_run", "thaw", "clear_rules"} {
		if idx(ev) < 0 {
			t.Fatalf("teardown event %q never happened; order=%v", ev, order)
		}
	}
	// The whole point: load stops before the service is thawed and its rule cleared.
	for _, load := range []string{"stop_attack", "stop_run"} {
		if idx(load) > idx("thaw") {
			t.Errorf("%s (%d) happened AFTER thaw (%d); load must stop first. order=%v",
				load, idx(load), idx("thaw"), order)
		}
		if idx(load) > idx("clear_rules") {
			t.Errorf("%s (%d) happened AFTER clear_rules (%d); load must stop first. order=%v",
				load, idx(load), idx("clear_rules"), order)
		}
	}
}

// TestEnterPhase_BumpsVersionOnResume pins M5: the rule version must be bumped on
// EVERY enter of a cache-box phase, not just a fresh one. Version is the SDK poll
// gate — a resume (fresh=false) that skips the bump leaves an SDK stuck at its old
// version 304ing forever, so the frozen service never receives the synthesized
// replay rule and runs live (no synthetic delay, live downstream calls, zero
// counters) for the rest of the phase.
func TestEnterPhase_BumpsVersionOnResume(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")    // nil zeus → no load drivers
	o.autoComplete = false // don't spawn an async finishPhase that would also bump
	exp := mkExp(t)

	version := func() uint64 {
		v, err := o.rules.Version(ctx)
		if err != nil {
			t.Fatalf("read rule version: %v", err)
		}
		return v
	}

	// Fresh enter of a cache-box (persist_cache baseline) phase bumps.
	base := mkRunningBaseline(t, exp.ID, 0)
	v0 := version()
	if err := o.enterPhase(ctx, base, true); err != nil {
		t.Fatalf("enterPhase fresh baseline: %v", err)
	}
	if v1 := version(); v1 <= v0 {
		t.Fatalf("fresh enter of cache-box phase did not bump rule version: %d -> %d", v0, v1)
	}

	// Resume (fresh=false) of a frozen isolation phase MUST also bump. A live SDK
	// instance is needed so the freeze fan-out has a target (an empty fleet is a
	// freeze-gate error, not a no-op).
	sdk := newFakeSDK(t, true)
	registerSDK(t, "frontend", sdk.server.URL)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	v2 := version()
	if err := o.enterPhase(ctx, iso, false); err != nil {
		t.Fatalf("enterPhase resume isolation: %v", err)
	}
	if v3 := version(); v3 <= v2 {
		t.Fatalf("resume (fresh=false) of frozen phase did not bump rule version: %d -> %d "+
			"(M5: SDK 304s forever, frozen service goes live)", v2, v3)
	}
}
