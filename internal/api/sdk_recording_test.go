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

// TestPollRules_RecordingPhaseID verifies the SDK poll response carries
// recording_phase_id = the running persist_cache (baseline) phase, so SDKs
// know which phase to ingest recorded cache into — and empty when none runs.
func TestPollRules_RecordingPhaseID(t *testing.T) {
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

	poll := func() atroposdk.RuleSync {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/rules?service=frontend&version=0", nil)
		w := httptest.NewRecorder()
		s.handlePollRules(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("poll status=%d body=%s", w.Code, w.Body.String())
		}
		var sync atroposdk.RuleSync
		if err := json.Unmarshal(w.Body.Bytes(), &sync); err != nil {
			t.Fatalf("decode RuleSync: %v", err)
		}
		return sync
	}

	// No running persist_cache phase → empty recording_phase_id.
	if got := poll().RecordingPhaseID; got != "" {
		t.Errorf("recording_phase_id=%q with no baseline, want empty", got)
	}

	// A running persist_cache phase → its id is delivered.
	e := &model.Experiment{ID: id.New("exp"), Name: "rec-" + id.New("n"), Status: "planned", CreatedAt: time.Now()}
	if err := exp.Create(ctx, e); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	ph := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "baseline", Position: 0, Status: "pending", PersistCache: true}
	if err := exp.CreatePhase(ctx, ph); err != nil {
		t.Fatalf("create phase: %v", err)
	}
	if err := exp.UpdatePhaseStatus(ctx, ph.ID, "running"); err != nil {
		t.Fatalf("set running: %v", err)
	}
	if got := poll().RecordingPhaseID; got != ph.ID {
		t.Errorf("recording_phase_id=%q, want %q", got, ph.ID)
	}

	// Once completed, it clears.
	if err := exp.UpdatePhaseStatus(ctx, ph.ID, "completed"); err != nil {
		t.Fatalf("set completed: %v", err)
	}
	if got := poll().RecordingPhaseID; got != "" {
		t.Errorf("recording_phase_id=%q after completion, want empty", got)
	}
}
