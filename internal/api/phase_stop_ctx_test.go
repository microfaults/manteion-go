package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/atropos"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/db"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
)

// TestHandleStopPhaseFlat_SurvivesClientDisconnect pins M2(b): the operator
// stop path must not abort teardown when the HTTP client disconnects. The
// handler passed r.Context() straight to StopPhase, so a disconnect mid-barrier
// cancelled the context: awaitDrain returned on ctx.Done and the completed-CAS
// then ran on the dead context and failed, wedging the phase at 'draining'.
// Wrapping the caller context in context.WithoutCancel detaches the barrier from
// the client's cancellation, so the phase still completes.
func TestHandleStopPhaseFlat_SurvivesClientDisconnect(t *testing.T) {
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

	logger := discardLogger()
	expRepo := store.NewExperimentRepo(database)
	controller := atrocontrol.New(
		atropos.NewClient(atropos.WithHTTPClient(&http.Client{Timeout: time.Second})),
		&atrocontrol.RepoResolver{Repo: store.NewSDKRepo(database)},
		atrocontrol.WithDefaultTimeout(time.Second), atrocontrol.WithLogger(logger),
	)
	orch := orchestrator.New(
		expRepo, store.NewRuleRepo(database), store.NewFaultRepo(database),
		store.NewWorkloadRepo(database), store.NewWorkflowRepo(database),
		controller, nil, nil, cachestore.New(t.TempDir()),
		store.NewPhaseFaultEventRepo(database), logger,
	)
	orch.WithDrainTimeout(2 * time.Second)
	orch.WithDrainPollInterval(20 * time.Millisecond)
	s := &Server{orch: orch, experiments: expRepo, logger: logger}

	// A running recording (persist_cache) phase — the completed-stop path runs
	// the drain barrier, which is where the cancellation bites.
	exp := &model.Experiment{ID: id.New("exp"), Name: "disc-" + id.New("n"), Status: "running", CreatedAt: time.Now()}
	if err := expRepo.Create(ctx, exp); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	t.Cleanup(func() { _, _ = database.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
	ph := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "baseline", Position: 0, Status: "pending", PersistCache: true}
	if err := expRepo.CreatePhase(ctx, ph); err != nil {
		t.Fatalf("create phase: %v", err)
	}
	if err := expRepo.UpdatePhaseStatus(ctx, ph.ID, "running"); err != nil {
		t.Fatalf("run phase: %v", err)
	}

	// Simulate a client disconnect: the request context is already cancelled.
	reqCtx, reqCancel := context.WithCancel(context.Background())
	reqCancel()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/phases/"+ph.ID+"/stop?status=completed", nil).WithContext(reqCtx)
	req.SetPathValue("phaseId", ph.ID)
	w := httptest.NewRecorder()
	s.handleStopPhaseFlat(w, req)

	// The stop must still drive the phase to terminal despite the dead caller ctx.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := expRepo.GetPhase(context.Background(), ph.ID)
		if err != nil {
			t.Fatalf("get phase: %v", err)
		}
		if got.Status == "completed" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("phase status = %q after client-disconnect stop, want completed "+
				"(client disconnect must not abort the drain barrier / completion)", got.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
