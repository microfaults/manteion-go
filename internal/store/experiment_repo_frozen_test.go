package store

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/desired"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/testutil"
)

// TestExperimentRepo_FrozenServiceForService pins the replay half of the
// cache-box role: the running phase that freezes a service, and that phase's
// config for it. Recording a service into a running baseline is not freezing
// it (ActiveCacheBoxPhaseForService answers a passthrough record context
// there, FrozenServiceForService nothing), a phase that is not running
// freezes nothing, and wherever both answer they name the same phase: the
// replay set and the freeze config are installed under the pair the replay
// rule's context names.
func TestExperimentRepo_FrozenServiceForService(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	repo := NewExperimentRepo(conn)

	// Per-run service names: earlier suites leave running phases behind (they
	// do not all truncate), and none of them may answer for these services.
	tag := id.New("fz")
	svc, other, unrelated := "frontend-"+tag, "cart-"+tag, "nobody-"+tag
	mu, sigma := 1.5, 0.4

	e := &model.Experiment{ID: id.New("exp"), Name: "frozen-" + tag, Status: "running", CreatedAt: time.Now()}
	if err := repo.Create(ctx, e); err != nil {
		t.Fatal(err)
	}
	base := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "baseline", Position: 0,
		Status: "pending", PersistCache: true}
	iso := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "isolation", Position: 1, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{
			{Service: other, Mode: "replay", MutationPolicy: "deny"},
			{Service: svc, Mode: "replay_with_delay", KeyStrategy: "exact", KeyHeaders: []string{"x-tenant"}, MutationPolicy: "deny",
				SyntheticDelay: &model.SyntheticDelayConfig{FitMu: &mu, FitSigma: &sigma}},
		}}
	for _, p := range []*model.ExperimentPhase{base, iso} {
		if err := repo.CreatePhase(ctx, p); err != nil {
			t.Fatalf("create %s: %v", p.Name, err)
		}
	}
	setStatus := func(p *model.ExperimentPhase, status string) {
		t.Helper()
		if err := repo.UpdatePhaseStatus(ctx, p.ID, status); err != nil {
			t.Fatalf("%s -> %s: %v", p.Name, status, err)
		}
	}
	frozen := func(service string) (*model.ExperimentPhase, *model.CacheBoxConfig) {
		t.Helper()
		p, cfg, err := repo.FrozenServiceForService(ctx, service)
		if err != nil {
			t.Fatalf("FrozenServiceForService(%s): %v", service, err)
		}
		if (p == nil) != (cfg == nil) {
			t.Fatalf("FrozenServiceForService(%s) = %+v, %+v: the phase and its config come together", service, p, cfg)
		}
		return p, cfg
	}
	wantNone := func(step string) {
		t.Helper()
		for _, service := range []string{svc, other, unrelated} {
			if p, cfg := frozen(service); p != nil {
				t.Errorf("%s: %s is frozen in %s (%+v), want not frozen", step, service, p.ID, cfg)
			}
		}
	}

	wantNone("nothing running")

	setStatus(base, "running")
	rc, err := repo.ActiveCacheBoxPhaseForService(ctx, svc)
	if err != nil || rc == nil || rc.PhaseID != base.ID || rc.Mode != "passthrough" {
		t.Fatalf("while recording, ActiveCacheBoxPhaseForService = %+v, %v; want the passthrough record context of the baseline", rc, err)
	}
	wantNone("baseline recording")

	setStatus(base, "completed")
	setStatus(iso, "running")
	p, cfg := frozen(svc)
	if p == nil {
		t.Fatalf("the isolation phase is running and freezes %s, got nothing", svc)
	}
	if p.ID != iso.ID || p.ExperimentID != e.ID || p.Status != "running" {
		t.Errorf("phase = %s in %s (%s), want %s in %s (running)", p.ID, p.ExperimentID, p.Status, iso.ID, e.ID)
	}
	if cfg.Service != svc || cfg.Mode != "replay_with_delay" || cfg.KeyStrategy != "exact" || !slices.Equal(cfg.KeyHeaders, []string{"x-tenant"}) ||
		cfg.SyntheticDelay == nil || cfg.SyntheticDelay.FitMu == nil || *cfg.SyntheticDelay.FitMu != mu {
		t.Errorf("config = %+v, want %s's own entry of the phase", cfg, svc)
	}
	if p2, cfg2 := frozen(other); p2 == nil || p2.ID != iso.ID || cfg2.Service != other || cfg2.Mode != "replay" {
		t.Errorf("second frozen service: %+v, %+v; want its own entry of the same phase", p2, cfg2)
	}
	if p3, _ := frozen(unrelated); p3 != nil {
		t.Errorf("a service no phase freezes is frozen in %s", p3.ID)
	}
	rc, err = repo.ActiveCacheBoxPhaseForService(ctx, svc)
	if err != nil || rc == nil {
		t.Fatalf("ActiveCacheBoxPhaseForService = %+v, %v", rc, err)
	}
	if rc.PhaseID != p.ID || rc.ExperimentID != p.ExperimentID || rc.Mode != cfg.Mode || rc.KeyStrategy != cfg.KeyStrategy {
		t.Errorf("replay rule context = %+v, frozen phase = %s in %s with %+v; want the same pair and config", rc, p.ID, p.ExperimentID, cfg)
	}

	setStatus(iso, "paused")
	wantNone("isolation paused")
	setStatus(iso, "running")
	if p, _ := frozen(svc); p == nil || p.ID != iso.ID {
		t.Errorf("resumed isolation phase: frozen in %+v, want %s", p, iso.ID)
	}
	setStatus(iso, "completed")
	wantNone("isolation completed")
}

// TestExperimentRepo_RunningPhaseOrderIsTotal pins the order in which running
// phases are considered: newest-started first, and phases started in the same
// instant by id. Two running phases can freeze the same service (admission
// control is a check at experiment start, StartPhase has no such guard), and
// the one that wins decides the replay rule's context, the freeze config and
// the replay set of the materialized document, whose ETag must be a function
// of the stored rows: a tie left to the scan follows physical position.
func TestExperimentRepo_RunningPhaseOrderIsTotal(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	repo := NewExperimentRepo(conn)

	tag := id.New("tie")
	svc := "frontend-" + tag
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)

	// One experiment per phase, all running and all freezing svc. Created in
	// reverse id order so the scan order is the reverse of the id order.
	phaseID := func(name string) string { return "phase-" + name + "-" + tag }
	for _, name := range []string{"z", "m", "a"} {
		e := &model.Experiment{ID: "exp-" + name + "-" + tag, Name: "tie-" + name + "-" + tag, Status: "running", CreatedAt: time.Now()}
		if err := repo.Create(ctx, e); err != nil {
			t.Fatal(err)
		}
		started := t0
		p := &model.ExperimentPhase{ID: phaseID(name), ExperimentID: e.ID, Name: "isolation", Position: 0, Status: "running",
			StartedAt: &started, FrozenServices: []model.CacheBoxConfig{{Service: svc, Mode: "replay", MutationPolicy: "deny"}}}
		if err := repo.CreatePhase(ctx, p); err != nil {
			t.Fatalf("create phase %s: %v", name, err)
		}
	}

	// ours is the order of this test's phases in ListRunningPhases (other
	// suites' running phases come and go between them).
	ours := func() []string {
		t.Helper()
		running, err := repo.ListRunningPhases(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, p := range running {
			if strings.HasSuffix(p.ID, tag) {
				got = append(got, p.ID)
			}
		}
		return got
	}
	check := func(step string, want ...string) {
		t.Helper()
		if got := ours(); !slices.Equal(got, want) {
			t.Errorf("%s: ListRunningPhases = %v, want %v", step, got, want)
		}
		winner := want[0]
		p, cfg, err := repo.FrozenServiceForService(ctx, svc)
		if err != nil || p == nil || cfg == nil || p.ID != winner {
			t.Errorf("%s: FrozenServiceForService = %+v, %+v, %v; want phase %s", step, p, cfg, err, winner)
		}
		rc, err := repo.ActiveCacheBoxPhaseForService(ctx, svc)
		if err != nil || rc == nil || rc.PhaseID != winner {
			t.Errorf("%s: ActiveCacheBoxPhaseForService = %+v, %v; want phase %s", step, rc, err, winner)
		}
	}

	check("started in the same instant", phaseID("a"), phaseID("m"), phaseID("z"))

	// Newest-started still comes first; the tie that remains breaks by id.
	if _, err := conn.ExecContext(ctx, `UPDATE experiment_phases SET started_at = $1 WHERE id = $2`,
		t0.Add(time.Minute), phaseID("z")); err != nil {
		t.Fatal(err)
	}
	check("z started a minute later", phaseID("z"), phaseID("a"), phaseID("m"))
}

// TestPhaseSources_OverTheRepos runs the document's frozen-phase inputs over
// the real repositories: the freeze config exists once the isolation phase
// runs, the replay set once an artifact is assigned to that phase and service
// (under the isolation phase's pair, whatever is assigned to other phases),
// and neither outlives the running phase.
func TestPhaseSources_OverTheRepos(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	experiments := NewExperimentRepo(conn)
	artifacts := NewReplayArtifactRepo(conn)
	sources := &desired.PhaseSources{Phases: experiments, Artifacts: artifacts}

	tag := id.New("ps")
	svc := "frontend-" + tag
	e := &model.Experiment{ID: id.New("exp"), Name: "sources-" + tag, Status: "running", CreatedAt: time.Now()}
	if err := experiments.Create(ctx, e); err != nil {
		t.Fatal(err)
	}
	base := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "baseline", Position: 0, Status: "completed", PersistCache: true}
	iso := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "isolation", Position: 1, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: svc, Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}}}
	for _, p := range []*model.ExperimentPhase{base, iso} {
		if err := experiments.CreatePhase(ctx, p); err != nil {
			t.Fatalf("create %s: %v", p.Name, err)
		}
	}

	check := func(step string, wantFreeze bool, wantSet *model.ReplaySetRef) {
		t.Helper()
		fc, err := sources.FreezeConfigForService(ctx, svc)
		if err != nil {
			t.Fatalf("%s: freeze config: %v", step, err)
		}
		if (fc != nil) != wantFreeze {
			t.Errorf("%s: freeze_cfg = %+v, want present = %v", step, fc, wantFreeze)
		}
		if fc != nil && (fc.Context == nil || fc.Context.ExperimentID != e.ID || fc.Context.PhaseID != iso.ID || fc.Context.KeyStrategy != "exact") {
			t.Errorf("%s: freeze_cfg context = %+v, want the isolation phase's pair (%s, %s) and its strategy", step, fc.Context, e.ID, iso.ID)
		}
		ref, err := sources.ReplaySetForService(ctx, svc)
		if err != nil {
			t.Fatalf("%s: replay set: %v", step, err)
		}
		if !reflect.DeepEqual(ref, wantSet) {
			t.Errorf("%s: replay_set = %+v, want %+v", step, ref, wantSet)
		}
	}
	setStatus := func(status string) {
		t.Helper()
		if err := experiments.UpdatePhaseStatus(ctx, iso.ID, status); err != nil {
			t.Fatalf("isolation -> %s: %v", status, err)
		}
	}

	check("isolation pending", false, nil)

	setStatus("running")
	check("isolation running, no artifact assigned", true, nil)

	// A set assigned to the recording baseline is not the frozen phase's.
	if err := artifacts.Upsert(ctx, &model.ReplayArtifact{ID: "sha256:" + id.New("h"), ExperimentID: e.ID, PhaseID: base.ID, Service: svc,
		EntryCount: 99, W5SHA256: "decoy", SizeBytes: 1, Path: "/tmp/decoy.ndjson"}); err != nil {
		t.Fatal(err)
	}
	check("artifact assigned to the baseline only", true, nil)

	a := &model.ReplayArtifact{ID: "sha256:" + id.New("h"), ExperimentID: e.ID, PhaseID: iso.ID, Service: svc,
		EntryCount: 12, W5SHA256: "w5", SizeBytes: 300, Path: "/tmp/x.ndjson"}
	if err := artifacts.Upsert(ctx, a); err != nil {
		t.Fatal(err)
	}
	want := &model.ReplaySetRef{ID: a.ID, ExperimentID: e.ID, PhaseID: iso.ID, KeyStrategy: "exact",
		Count: 12, SHA256: "w5", SizeBytes: 300, URL: desired.ArtifactURLPrefix + a.ID}
	check("artifact assigned to the isolation phase", true, want)

	setStatus("paused")
	check("isolation paused", false, nil)
	setStatus("running")
	check("isolation resumed", true, want)
	setStatus("completed")
	check("isolation completed", false, nil)
}
