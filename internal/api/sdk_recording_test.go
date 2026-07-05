package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/db"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// TestPoll_RulesCarryPhaseContext pins MANT-4/INV-5: recording (and replay)
// provenance rides on each service's synthesized cache-box rule's
// CacheBoxContext — never a global RecordingPhaseID. Two concurrent experiments
// recording different services each get their own (experiment_id, phase_id)
// pair and key strategy; the deleted RecordingPhaseID field is always empty.
func TestPoll_RulesCarryPhaseContext(t *testing.T) {
	if os.Getenv("MANTEION_TEST_DB") == "" {
		t.Skip("set MANTEION_TEST_DB=1 to run")
	}
	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	database, err := db.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	defer db.Close(database)

	rules := store.NewRuleRepo(database)
	exp := store.NewExperimentRepo(database)
	s := &Server{
		rules: rules, rulever: rules,
		faults:      store.NewFaultRepo(database),
		experiments: exp,
		logger:      discardLogger(),
	}

	poll := func(service string) atroposdk.RuleSync {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/rules?service="+service+"&version=0", nil)
		w := httptest.NewRecorder()
		s.handlePollRules(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("poll %s: status=%d body=%s", service, w.Code, w.Body.String())
		}
		var sync atroposdk.RuleSync
		if err := json.Unmarshal(w.Body.Bytes(), &sync); err != nil {
			t.Fatalf("decode RuleSync: %v", err)
		}
		return sync
	}

	// Build an experiment with a baseline that will record `svc` (frozen in the
	// experiment's isolation phase) using `strat`, then set the baseline running.
	buildRecording := func(name, svc, strat string) (expID, basePhaseID string) {
		t.Helper()
		e := &model.Experiment{ID: id.New("exp"), Name: name + id.New("n"), Status: "planned", CreatedAt: time.Now()}
		if err := exp.Create(ctx, e); err != nil {
			t.Fatalf("create exp: %v", err)
		}
		base := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "baseline", Position: 0, Status: "pending", PersistCache: true}
		iso := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "isolation", Position: 1,
			Status: "pending", FrozenServices: []model.CacheBoxConfig{{Service: svc, Mode: "replay", KeyStrategy: strat, MutationPolicy: "deny"}}}
		for _, p := range []*model.ExperimentPhase{base, iso} {
			if err := exp.CreatePhase(ctx, p); err != nil {
				t.Fatalf("create phase: %v", err)
			}
		}
		if err := exp.UpdatePhaseStatus(ctx, base.ID, "running"); err != nil {
			t.Fatalf("set baseline running: %v", err)
		}
		return e.ID, base.ID
	}

	// Per-run-unique service names so a prior run's leftover running phase (this
	// test transitions isoA → running below and does not truncate) can never
	// match this run's ListRunningPhases-driven synthesis.
	suffix := id.New("s")
	svcA, svcB, svcUnrelated := "svca-"+suffix, "svcb-"+suffix, "svcx-"+suffix
	expA, baseA := buildRecording("recA-", svcA, "exact")
	expB, baseB := buildRecording("recB-", svcB, "canonical_v2")

	cb := func(sync atroposdk.RuleSync) *atroposdk.CompiledCacheBox {
		t.Helper()
		var found *atroposdk.CompiledCacheBox
		for i := range sync.Rules {
			if sync.Rules[i].CacheBox != nil {
				if found != nil {
					t.Fatalf("expected exactly one cache-box rule, got >1")
				}
				found = sync.Rules[i].CacheBox
			}
		}
		if found == nil {
			t.Fatalf("no cache-box rule synthesized")
		}
		return found
	}

	// svc-a records into experiment A's baseline with the exact strategy.
	syncA := poll(svcA)
	if syncA.RecordingPhaseID != "" {
		t.Fatalf("RecordingPhaseID must be empty (deleted); got %q", syncA.RecordingPhaseID)
	}
	cbA := cb(syncA)
	if cbA.Mode != "passthrough" {
		t.Fatalf("svc-a mode=%q, want passthrough (record)", cbA.Mode)
	}
	if cbA.Context == nil || cbA.Context.ExperimentID != expA || cbA.Context.PhaseID != baseA {
		t.Fatalf("svc-a context = %+v, want (exp=%s, phase=%s)", cbA.Context, expA, baseA)
	}
	if cbA.Context.KeyStrategy != "exact" || cbA.Context.StrategyVersion != 1 {
		t.Fatalf("svc-a strategy = (%s, v%d), want (exact, v1)", cbA.Context.KeyStrategy, cbA.Context.StrategyVersion)
	}

	// svc-b records into experiment B's baseline — its own pair + strategy.
	cbB := cb(poll(svcB))
	if cbB.Context.ExperimentID != expB || cbB.Context.PhaseID != baseB {
		t.Fatalf("svc-b context = %+v, want (exp=%s, phase=%s)", cbB.Context, expB, baseB)
	}
	if cbB.Context.KeyStrategy != "canonical_v2" || cbB.Context.StrategyVersion != 2 {
		t.Fatalf("svc-b strategy = (%s, v%d), want (canonical_v2, v2)", cbB.Context.KeyStrategy, cbB.Context.StrategyVersion)
	}

	// A service in no experiment's frozen set gets no cache-box rule.
	if sync := poll(svcUnrelated); len(sync.Rules) != 0 {
		t.Fatalf("unrelated service got %d rules, want 0", len(sync.Rules))
	}

	// When A's baseline completes and its isolation phase runs, svc-a flips to
	// replay under the isolation phase's pair.
	phases, err := exp.ListPhasesForExperiment(ctx, expA)
	if err != nil {
		t.Fatalf("list phases: %v", err)
	}
	var isoA string
	for _, p := range phases {
		if p.Name == "isolation" {
			isoA = p.ID
		}
	}
	if err := exp.UpdatePhaseStatus(ctx, baseA, "completed"); err != nil {
		t.Fatalf("complete baseline A: %v", err)
	}
	if err := exp.UpdatePhaseStatus(ctx, isoA, "running"); err != nil {
		t.Fatalf("run isolation A: %v", err)
	}
	cbReplay := cb(poll(svcA))
	if cbReplay.Mode != "replay" {
		t.Fatalf("svc-a isolation mode=%q, want replay", cbReplay.Mode)
	}
	if cbReplay.Context.PhaseID != isoA {
		t.Fatalf("svc-a replay phase=%q, want isolation %q", cbReplay.Context.PhaseID, isoA)
	}
}
