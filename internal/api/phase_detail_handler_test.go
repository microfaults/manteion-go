package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"manteion-go/internal/db"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

func TestPhaseDetailHandlers(t *testing.T) {
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

	expRepo := store.NewExperimentRepo(database)
	feRepo := store.NewPhaseFaultEventRepo(database)
	s := &Server{experiments: expRepo, phaseFaultEvents: feRepo, logger: discardLogger()}

	// Seed an experiment + a frozen phase.
	exp := &model.Experiment{ID: id.New("exp"), Name: "api-test", Status: "planned", CreatedAt: time.Now()}
	if err := expRepo.Create(ctx, exp); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	ph := &model.ExperimentPhase{
		ID: id.New("phase"), ExperimentID: exp.ID, Name: "iso", Position: 0, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: "frontend", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
	}
	if err := expRepo.CreatePhase(ctx, ph); err != nil {
		t.Fatalf("create phase: %v", err)
	}

	// 1) List
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/phases?limit=50&offset=0", nil)
		w := httptest.NewRecorder()
		s.handleListPhases(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("list status=%d", w.Code)
		}
	}
	// 2) Detail (path value set manually)
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/phases/"+ph.ID, nil)
		req.SetPathValue("phaseId", ph.ID)
		w := httptest.NewRecorder()
		s.handleGetPhaseDetail(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("detail status=%d body=%s", w.Code, w.Body.String())
		}
		var got PhaseDetail
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode detail: %v", err)
		}
		if got.ExperimentName != "api-test" {
			t.Errorf("experiment_name=%q, want api-test", got.ExperimentName)
		}
	}
	// 3) Faults
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/phases/"+ph.ID+"/faults", nil)
		req.SetPathValue("phaseId", ph.ID)
		w := httptest.NewRecorder()
		s.handleGetPhaseFaults(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("faults status=%d", w.Code)
		}
		var got PhaseFaults
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode faults: %v", err)
		}
		if len(got.FrozenServices) != 1 || got.FrozenServices[0].Service != "frontend" {
			t.Errorf("frozen_services=%+v", got.FrozenServices)
		}
	}
	// 4) 404 on unknown
	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/phases/phase-nope", nil)
		req.SetPathValue("phaseId", "phase-nope")
		w := httptest.NewRecorder()
		s.handleGetPhaseDetail(w, req)
		if w.Code != http.StatusNotFound {
			t.Errorf("unknown-phase status=%d, want 404", w.Code)
		}
	}
}
