package orchestrator

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/id"
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

// TestEnterPhase_BumpsGenerationOnEveryEnter pins M5 on desired-state v1: every
// enter of a phase (fresh AND resume) bumps the generation of each service the
// phase's desired state touches — the frozen services of the phase, the rules'
// target services, and for a baseline the services frozen anywhere in the
// experiment (their record rule is what the baseline synthesizes). A resume
// that skipped the bump would leave a long-polling SDK holding until its wait
// elapsed with no replay rule, running live.
func TestEnterPhase_BumpsGenerationOnEveryEnter(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")    // nil zeus → no load drivers
	o.autoComplete = false // don't spawn an async finishPhase that would also bump
	exp := mkExp(t)

	gen := func(svc string) uint64 {
		t.Helper()
		g, err := o.generations.Get(ctx, svc)
		if err != nil {
			t.Fatalf("read generation: %v", err)
		}
		return g
	}

	// A baseline records the services frozen later in the experiment: entering
	// it bumps "frontend" (frozen in the isolation phase) and nothing else.
	base := mkRunningBaseline(t, exp.ID, 0)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 1)
	f0, c0 := gen("frontend"), gen("checkoutservice")
	if err := o.enterPhase(ctx, base, true); err != nil {
		t.Fatalf("enterPhase fresh baseline: %v", err)
	}
	if gen("frontend") != f0+1 || gen("checkoutservice") != c0 {
		t.Fatalf("fresh baseline enter: frontend %d→%d, checkout %d→%d; want frontend +1 only", f0, gen("frontend"), c0, gen("checkoutservice"))
	}

	// Resume (fresh=false) of the frozen isolation phase bumps again. A resume
	// skips the preload gate but not the freeze gate, and an empty fleet is a
	// freeze-gate error, so a live SDK instance gives the freeze fan-out a target.
	sdk := newFakeSDK(t, true)
	registerSDK(t, "frontend", sdk.server.URL)
	f1 := gen("frontend")
	if err := o.enterPhase(ctx, iso, false); err != nil {
		t.Fatalf("enterPhase resume isolation: %v", err)
	}
	if gen("frontend") != f1+1 {
		t.Fatalf("resume of frozen phase did not bump frontend: %d → %d", f1, gen("frontend"))
	}
}

// generation reads svc's current generation through the orchestrator's store.
func generation(t *testing.T, o *Orchestrator, svc string) uint64 {
	t.Helper()
	g, err := o.generations.Get(context.Background(), svc)
	if err != nil {
		t.Fatalf("read generation of %s: %v", svc, err)
	}
	return g
}

// TestBumpDesired_OncePerServiceReturnsTargets pins the bump behind every
// transition: a baseline's desired services are its own (rule targets, frozen
// services) plus every service frozen anywhere in its experiment, a service
// that is both is bumped once, and the result maps each service to the
// generation it reached (the convergence targets plan 2 waits on).
func TestBumpDesired_OncePerServiceReturnsTargets(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	// Services no other test touches, so an exact count cannot see a stray bump.
	both, frozen := id.New("svc-both"), id.New("svc-frozen")

	base := mkRunningBaseline(t, exp.ID, 0)
	mkRunningIsolation(t, exp.ID, both, 1)
	mkRunningIsolation(t, exp.ID, frozen, 2)
	// A baseline phase rule on a service the experiment also freezes.
	rule := &model.Rule{
		ID: id.New("rule"), Name: "bump-dedup", Service: both, Enabled: true,
		Action:    model.RuleAction{Type: "cachebox", CacheBox: &model.CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: "exact"}},
		Mode:      "inline",
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := testRuleRepo.Create(ctx, rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rule.ID) })
	if err := testExpRepo.AttachPhaseRules(ctx, base.ID, []string{rule.ID}); err != nil {
		t.Fatalf("attach rule: %v", err)
	}

	b0, f0 := generation(t, o, both), generation(t, o, frozen)
	targets := o.bumpDesired(ctx, base)
	if want := map[string]uint64{both: b0 + 1, frozen: f0 + 1}; !maps.Equal(targets, want) {
		t.Fatalf("bumpDesired targets = %v, want %v", targets, want)
	}
	if b, f := generation(t, o, both), generation(t, o, frozen); b != b0+1 || f != f0+1 {
		t.Fatalf("generations %s %d→%d, %s %d→%d; want each +1 exactly", both, b0, b, frozen, f0, f)
	}
}

// TestPhaseExit_BumpsGenerationPerTransition pins the exit side: the drain
// barrier (running → draining) and the terminal transition each bump every
// desired service once. The drain bump is the one the SDK's drain tracker
// needs: the record rule left the document when the phase stopped running,
// and only a bump wakes a held long-poll to see it go. The terminal bump is
// what wakes an isolation phase's pollers to its replay rule's removal.
func TestPhaseExit_BumpsGenerationPerTransition(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.WithDrainTimeout(5 * time.Second) // the gate waits for the report below, never times out
	exp := mkExp(t)                     // planned: the re-advance after each finish starts nothing
	svc := id.New("svc-exit")
	base := mkRunningBaseline(t, exp.ID, 0)
	iso := mkRunningIsolation(t, exp.ID, svc, 1)

	// Recording phase: a pusher owes a drain report, so the phase holds at
	// 'draining' — one bump there, a second at draining → completed.
	ingestN(t, o, exp.ID, base.ID, svc, "i1", 1, 2)
	g0 := generation(t, o, svc)
	done := make(chan bool, 1)
	go func() { done <- o.finishPhase(context.Background(), base.ID, "completed", "running") }()
	waitFor(t, "the drain barrier's bump", 2*time.Second, func() bool { return generation(t, o, svc) == g0+1 })
	if s := getPhase(t, base.ID).Status; s != "draining" {
		t.Fatalf("phase %q at the first bump, want draining (the drain barrier must bump)", s)
	}
	report(o, exp.ID, base.ID, svc, "i1", 2)
	if !<-done {
		t.Fatal("finishPhase(baseline) = false, want true")
	}
	if g := generation(t, o, svc); g != g0+2 {
		t.Fatalf("baseline exit: %d → %d, want +2 (one at draining, one at completed)", g0, g)
	}

	// Isolation phase: no drain, one bump at the terminal transition.
	g1 := generation(t, o, svc)
	if !o.finishPhase(ctx, iso.ID, "completed", "running") {
		t.Fatal("finishPhase(isolation) = false, want true")
	}
	if g := generation(t, o, svc); g != g1+1 {
		t.Fatalf("isolation exit: %d → %d, want +1", g1, g)
	}
}

// TestPausePhase_BumpsEachServiceOnce pins the pause side: a paused phase is
// not running, so its rules and its cache-box role leave the desired state of
// every service it touches, and pause bumps each of those services exactly
// once. Without the bump a held long-poll keeps the paused phase's faults (and
// its frozen service replaying) until its wait runs out. A paused baseline
// takes the record rule away from the services frozen elsewhere in the
// experiment, so those are bumped too.
func TestPausePhase_BumpsEachServiceOnce(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	// Services no other test touches, so an exact count cannot see a stray bump.
	frozen, target, bystander := id.New("svc-pause-frozen"), id.New("svc-pause-target"), id.New("svc-pause-bystander")
	base := mkRunningBaseline(t, exp.ID, 0)
	iso := mkRunningIsolation(t, exp.ID, frozen, 1)
	// The isolation phase also attaches a rule targeting a second service.
	rule := &model.Rule{
		ID: id.New("rule"), Name: "pause-target", Service: target, Enabled: true,
		Action:    model.RuleAction{Type: "cachebox", CacheBox: &model.CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: "exact"}},
		Mode:      "inline",
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := testRuleRepo.Create(ctx, rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rule.ID) })
	if err := testExpRepo.AttachPhaseRules(ctx, iso.ID, []string{rule.ID}); err != nil {
		t.Fatalf("attach rule: %v", err)
	}
	gens := func() [3]uint64 {
		return [3]uint64{generation(t, o, frozen), generation(t, o, target), generation(t, o, bystander)}
	}

	g0 := gens()
	if err := o.PausePhase(ctx, iso.ID); err != nil {
		t.Fatalf("pause isolation: %v", err)
	}
	if s := getPhase(t, iso.ID).Status; s != "paused" {
		t.Fatalf("isolation status %q after pause, want paused", s)
	}
	if g, want := gens(), [3]uint64{g0[0] + 1, g0[1] + 1, g0[2]}; g != want {
		t.Fatalf("isolation pause: generations (frozen, target, bystander) %v → %v, want %v", g0, g, want)
	}

	g1 := gens()
	if err := o.PausePhase(ctx, base.ID); err != nil {
		t.Fatalf("pause baseline: %v", err)
	}
	if g, want := gens(), [3]uint64{g1[0] + 1, g1[1], g1[2]}; g != want {
		t.Fatalf("baseline pause: generations (frozen, target, bystander) %v → %v, want %v (the record rule leaves the frozen service)", g1, g, want)
	}

	// A pause that loses (the phase is no longer running) bumps nothing.
	g2 := gens()
	if err := o.PausePhase(ctx, iso.ID); err == nil {
		t.Fatal("second pause of the isolation phase succeeded, want an invalid-state error")
	}
	if g := gens(); g != g2 {
		t.Fatalf("refused pause bumped: %v → %v", g2, g)
	}
}

// TestRecover_DrainingPhaseCompletes pins M2(a): a phase left 'draining' by a
// crash mid-barrier must be recovered. Recover's switch had no 'draining' case,
// so the phase wedged forever and advanceExperiment stalled on it. Recovery
// re-enters the drain barrier (finishPhase's from-set includes 'draining', so the
// self-CAS re-drives it) and drives the phase to 'completed' with a drain row.
func TestRecover_DrainingPhaseCompletes(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.WithDrainTimeout(500 * time.Millisecond)
	o.WithDrainPollInterval(20 * time.Millisecond)

	exp := mkExp(t)
	base := mkRunningBaseline(t, exp.ID, 0) // persist_cache, running
	if err := testExpRepo.UpdatePhaseStatus(ctx, base.ID, "draining"); err != nil {
		t.Fatalf("wedge phase at draining: %v", err)
	}
	if err := testExpRepo.UpdateStatus(ctx, exp.ID, "running"); err != nil {
		t.Fatalf("set experiment running: %v", err)
	}

	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	waitFor(t, "draining phase recovered to completed", 5*time.Second, func() bool {
		return getPhase(t, base.ID).Status == "completed"
	})
	drain, err := testExpRepo.GetPhaseDrain(ctx, base.ID)
	if err != nil || drain == nil {
		t.Fatalf("expected a drain row after recovery: drain=%v err=%v", drain, err)
	}
}
