// Integration tests for the phase-aware Orchestrator FSM.
//
// These tests require a live PostgreSQL instance. They are opt-in: set
// MANTEION_TEST_DB=1 (or supply a DSN via MANTEION_DATABASE_URL) before
// running.
//
// Quick start:
//
//	cd manteion-go
//	docker-compose up -d          # starts Postgres on :5432
//	MANTEION_TEST_DB=1 go test ./internal/orchestrator/ -v -count=1
//
// Zeus is faked with an httptest server speaking the attacks/workflows
// surface; the atrocontrol controller runs against an empty SDK fleet
// (fanout to zero targets), exactly like the legacy FSM suite.
package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/atropos"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/db"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// ----------------------------------------------------------------------------
// Package-level test harness
// ----------------------------------------------------------------------------

var (
	testDB           *sql.DB
	testExpRepo      *store.ExperimentRepo
	testRuleRepo     *store.RuleRepo
	testFaultRepo    *store.FaultRepo
	testWorkloadRepo *store.WorkloadRepo
	testWorkflowRepo *store.WorkflowRepo
	testSDKRepo      *store.SDKRepo
)

func TestMain(m *testing.M) {
	if os.Getenv("MANTEION_TEST_DB") == "" {
		fmt.Fprintln(os.Stderr,
			"orchestrator integration tests skipped — set MANTEION_TEST_DB=1 to run")
		os.Exit(0)
	}

	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	testDB, err = db.Open(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator test: cannot connect to DB: %v\n", err)
		os.Exit(1)
	}

	testExpRepo = store.NewExperimentRepo(testDB)
	testRuleRepo = store.NewRuleRepo(testDB)
	testFaultRepo = store.NewFaultRepo(testDB)
	testWorkloadRepo = store.NewWorkloadRepo(testDB)
	testWorkflowRepo = store.NewWorkflowRepo(testDB)
	testSDKRepo = store.NewSDKRepo(testDB)

	code := m.Run()

	_ = db.Close(testDB)
	os.Exit(code)
}

// newOrch builds an orchestrator against the shared repos, an empty SDK
// fleet, and (optionally) a fake zeus. Poll cadence is tightened so the
// suite runs in seconds.
func newOrch(t *testing.T, zeusURL string) *Orchestrator {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	controller := atrocontrol.New(
		atropos.NewClient(atropos.WithHTTPClient(&http.Client{Timeout: time.Second})),
		&atrocontrol.RepoResolver{Repo: testSDKRepo},
		atrocontrol.WithDefaultTimeout(time.Second),
		atrocontrol.WithLogger(logger),
	)
	var zc *zeus.Client
	if zeusURL != "" {
		zc = zeus.NewClient(zeusURL)
	}
	o := New(testExpRepo, testRuleRepo, testFaultRepo, testWorkloadRepo, testWorkflowRepo,
		controller, nil, zc, cachestore.New(t.TempDir()), store.NewPhaseFaultEventRepo(testDB), logger)
	o.WithPollInterval(50 * time.Millisecond)
	o.WithMaxPollDuration(15 * time.Second)
	return o
}

// mkExperiment creates a planned experiment with n pending phases.
func mkExperiment(t *testing.T, n int) (*model.Experiment, []*model.ExperimentPhase) {
	t.Helper()
	ctx := context.Background()
	exp := &model.Experiment{
		ID:        id.New("exp"),
		Name:      "fsm-test-" + id.New("n"),
		Status:    "planned",
		CreatedAt: time.Now(),
	}
	if err := testExpRepo.Create(ctx, exp); err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	phases := make([]*model.ExperimentPhase, n)
	for i := 0; i < n; i++ {
		p := &model.ExperimentPhase{
			ID:           id.New("phase"),
			ExperimentID: exp.ID,
			Name:         fmt.Sprintf("phase-%d", i),
			Position:     i,
			Status:       "pending",
		}
		if err := testExpRepo.CreatePhase(ctx, p); err != nil {
			t.Fatalf("create phase %d: %v", i, err)
		}
		phases[i] = p
	}
	return exp, phases
}

// attachAttackWorkflow creates a workflow row and attaches it to the phase
// with the given attack config.
func attachAttackWorkflow(t *testing.T, phaseID string, pw model.PhaseWorkflow) string {
	t.Helper()
	ctx := context.Background()
	wf := &model.Workflow{
		ID:        id.New("wf"),
		Name:      "fsm-test-" + id.New("n"),
		DSL:       json.RawMessage(`{"type":"request","method":"GET","url":"http://target:8080/"}`),
		CreatedAt: time.Now(),
	}
	if err := testWorkflowRepo.Create(ctx, wf); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	pw.PhaseID = phaseID
	pw.WorkflowID = wf.ID
	if err := testExpRepo.AttachPhaseWorkflows(ctx, phaseID, []model.PhaseWorkflow{pw}); err != nil {
		t.Fatalf("attach phase workflow: %v", err)
	}
	return wf.ID
}

func getExp(t *testing.T, id string) *model.Experiment {
	t.Helper()
	exp, err := testExpRepo.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get experiment %s: %v", id, err)
	}
	return exp
}

func getPhase(t *testing.T, id string) *model.ExperimentPhase {
	t.Helper()
	p, err := testExpRepo.GetPhase(context.Background(), id)
	if err != nil {
		t.Fatalf("get phase %s: %v", id, err)
	}
	return p
}

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// ----------------------------------------------------------------------------
// Fake zeus
// ----------------------------------------------------------------------------

type fakeAttack struct {
	status string
	result *zeus.AttackResultInfo
}

type fakeZeus struct {
	mu      sync.Mutex
	attacks map[string]*fakeAttack
	srv     *httptest.Server
}

func newFakeZeus(t *testing.T) *fakeZeus {
	t.Helper()
	f := &fakeZeus{attacks: make(map[string]*fakeAttack)}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/attacks", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID string `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.ID == "" {
			req.ID = id.New("atk")
		}
		f.mu.Lock()
		f.attacks[req.ID] = &fakeAttack{status: "running"}
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"id": req.ID})
	})
	mux.HandleFunc("GET /api/v1/attacks/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		a, ok := f.attacks[r.PathValue("id")]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"id": r.PathValue("id"), "status": a.status,
		})
	})
	mux.HandleFunc("DELETE /api/v1/attacks/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		if a, ok := f.attacks[r.PathValue("id")]; ok {
			a.status = "stopped"
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /api/v1/attacks/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		a, ok := f.attacks[r.PathValue("id")]
		f.mu.Unlock()
		if !ok || a.result == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(a.result)
	})
	mux.HandleFunc("POST /api/v1/workflows", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("DELETE /api/v1/workflows/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// completeAll marks every known attack completed with (a copy of) the given
// result, so the poller observes natural completion and harvest finds data.
func (f *fakeZeus) completeAll(result zeus.AttackResultInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for atkID, a := range f.attacks {
		a.status = "completed"
		r := result
		r.AttackID = atkID
		a.result = &r
	}
}

func (f *fakeZeus) attackCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.attacks)
}

// ----------------------------------------------------------------------------
// Store CAS primitives
// ----------------------------------------------------------------------------

func TestTransitionCAS(t *testing.T) {
	ctx := context.Background()
	exp, phases := mkExperiment(t, 1)
	p := phases[0]

	// Wrong from-set loses.
	ok, err := testExpRepo.TransitionPhase(ctx, p.ID, "running", "paused")
	if err != nil || ok {
		t.Fatalf("transition from wrong state: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	// Right from-set wins exactly once.
	ok, err = testExpRepo.TransitionPhase(ctx, p.ID, "running", "pending")
	if err != nil || !ok {
		t.Fatalf("first transition: ok=%v err=%v, want ok=true", ok, err)
	}
	ok, err = testExpRepo.TransitionPhase(ctx, p.ID, "running", "pending")
	if err != nil || ok {
		t.Fatalf("second transition: ok=%v err=%v, want ok=false", ok, err)
	}
	if got := getPhase(t, p.ID); got.StartedAt == nil {
		t.Fatal("running transition did not stamp started_at")
	}

	ok, err = testExpRepo.TransitionExperiment(ctx, exp.ID, "running", "planned")
	if err != nil || !ok {
		t.Fatalf("experiment transition: ok=%v err=%v, want ok=true", ok, err)
	}
	ok, err = testExpRepo.TransitionExperiment(ctx, exp.ID, "cancelled", "planned")
	if err != nil || ok {
		t.Fatalf("experiment transition from stale state: ok=%v err=%v, want ok=false", ok, err)
	}
}

// ----------------------------------------------------------------------------
// FSM
// ----------------------------------------------------------------------------

// Driver-less phases auto-complete and the scheduler walks them in order to
// a completed experiment.
func TestExperimentLifecycle_DriverlessAutoComplete(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp, phases := mkExperiment(t, 2)

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	waitFor(t, "experiment completed", 5*time.Second, func() bool {
		return getExp(t, exp.ID).Status == "completed"
	})
	for i, p := range phases {
		got := getPhase(t, p.ID)
		if got.Status != "completed" {
			t.Errorf("phase %d: status %q, want completed", i, got.Status)
		}
		if got.StartedAt == nil || got.CompletedAt == nil {
			t.Errorf("phase %d: timestamps not stamped (%v, %v)", i, got.StartedAt, got.CompletedAt)
		}
	}

	// Restart is rejected: the experiment is no longer planned.
	if err := o.StartExperiment(ctx, exp.ID); err == nil {
		t.Fatal("second StartExperiment succeeded, want error")
	}
}

func TestPauseResumeCancelExperiment(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.autoComplete = false // keep driver-less phases running so we can pause them
	exp, phases := mkExperiment(t, 2)

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	if got := getPhase(t, phases[0].ID); got.Status != "running" {
		t.Fatalf("phase 0 status %q, want running", got.Status)
	}

	if err := o.PauseExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("pause experiment: %v", err)
	}
	if got := getPhase(t, phases[0].ID); got.Status != "paused" {
		t.Fatalf("phase 0 status %q after pause, want paused", got.Status)
	}
	// Experiment-level pause is derived: the row stays running.
	if got := getExp(t, exp.ID); got.Status != "running" {
		t.Fatalf("experiment status %q after pause, want running", got.Status)
	}
	// Nothing left to pause.
	if err := o.PauseExperiment(ctx, exp.ID); err == nil {
		t.Fatal("second pause succeeded, want error")
	}

	if err := o.ResumeExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("resume experiment: %v", err)
	}
	if got := getPhase(t, phases[0].ID); got.Status != "running" {
		t.Fatalf("phase 0 status %q after resume, want running", got.Status)
	}

	if err := o.CancelExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("cancel experiment: %v", err)
	}
	if got := getExp(t, exp.ID); got.Status != "cancelled" {
		t.Fatalf("experiment status %q after cancel, want cancelled", got.Status)
	}
	for i, p := range phases {
		if got := getPhase(t, p.ID); got.Status != "skipped" {
			t.Errorf("phase %d status %q after cancel, want skipped", i, got.Status)
		}
	}
	// Terminal experiments reject further lifecycle calls.
	if err := o.CancelExperiment(ctx, exp.ID); err == nil {
		t.Fatal("second cancel succeeded, want error")
	}
	if err := o.ResumeExperiment(ctx, exp.ID); err == nil {
		t.Fatal("resume after cancel succeeded, want error")
	}
}

// The pending→running claim is CAS-protected: a second start of the same
// phase loses.
func TestStartPhase_DoubleStartRejected(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.autoComplete = false
	exp, phases := mkExperiment(t, 1)

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	if err := o.StartPhase(ctx, phases[0].ID); err == nil {
		t.Fatal("double StartPhase succeeded, want error")
	}
}

// A failed phase skips everything still pending and fails the experiment —
// the sequential analogue of the legacy DAG failure cascade.
func TestPhaseFailureSkipsRemaining(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	o.autoComplete = false
	exp, phases := mkExperiment(t, 3)

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	if err := o.StopPhase(ctx, phases[0].ID, "failed"); err != nil {
		t.Fatalf("stop phase failed: %v", err)
	}

	waitFor(t, "experiment failed", 5*time.Second, func() bool {
		return getExp(t, exp.ID).Status == "failed"
	})
	for _, p := range phases[1:] {
		if got := getPhase(t, p.ID); got.Status != "skipped" {
			t.Errorf("phase %d status %q, want skipped", p.Position, got.Status)
		}
	}
}

// An attack-driven phase: the orchestrator persists the attack handle on the
// phase_workflows row, the poller observes completion, and harvest writes
// phase_workflow_results + phase_service_latency + the experiment rollup.
func TestAttackDrivenPhase_HarvestsResults(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	exp, phases := mkExperiment(t, 1)
	wfID := attachAttackWorkflow(t, phases[0].ID, model.PhaseWorkflow{
		VUs: 5, RateRPS: 10, DurationSec: 1,
		TargetURL: "http://frontend:8080/checkout", TargetMethod: "GET",
	})

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}

	// The attack handle lands on the row before/at start.
	var attackID string
	waitFor(t, "zeus_attack_id persisted", 5*time.Second, func() bool {
		pws, err := testExpRepo.ListPhaseWorkflows(ctx, phases[0].ID)
		if err != nil || len(pws) != 1 {
			return false
		}
		attackID = pws[0].ZeusAttackID
		return attackID != ""
	})
	if fz.attackCount() != 1 {
		t.Fatalf("fake zeus has %d attacks, want 1", fz.attackCount())
	}

	fz.completeAll(zeus.AttackResultInfo{
		Service:       "frontend",
		TotalRequests: 100,
		DurationMs:    1000,
		SuccessRate:   0.95,
		LatencyP50Us:  1000,
		LatencyP95Us:  2000,
		LatencyP99Us:  3000,
		ThroughputRPS: 100,
	})

	waitFor(t, "experiment completed", 10*time.Second, func() bool {
		return getExp(t, exp.ID).Status == "completed"
	})

	wfRes, err := testExpRepo.ListWorkflowResultsForPhase(ctx, phases[0].ID)
	if err != nil || len(wfRes) != 1 {
		t.Fatalf("workflow results: %d rows, err=%v; want 1", len(wfRes), err)
	}
	got := wfRes[0]
	if got.WorkflowID != wfID || got.RequestCount != 100 || got.ErrorCount != 5 {
		t.Errorf("workflow result row: %+v", got)
	}
	if got.LatencyP999Us != got.LatencyP99Us {
		t.Errorf("p999 %d != p99 %d (vegeta reports no p999)", got.LatencyP999Us, got.LatencyP99Us)
	}

	svcLat, err := testExpRepo.ListServiceLatencyForPhase(ctx, phases[0].ID)
	if err != nil || len(svcLat) != 1 {
		t.Fatalf("service latency: %d rows, err=%v; want 1", len(svcLat), err)
	}
	if svcLat[0].Service != "frontend" || svcLat[0].WorkflowID != wfID {
		t.Errorf("service latency row: %+v", svcLat[0])
	}

	rollup, err := testExpRepo.GetExperimentResults(ctx, exp.ID)
	if err != nil {
		t.Fatalf("get rollup: %v", err)
	}
	if rollup.PhaseCount != 1 || rollup.CompletedPhaseCount != 1 || rollup.TotalRequestCount != 100 {
		t.Errorf("rollup: %+v", rollup)
	}
	if rollup.WorstP99PhaseID != phases[0].ID {
		t.Errorf("rollup worst phase %q, want %q", rollup.WorstP99PhaseID, phases[0].ID)
	}
}

// Recover after a crash: a running phase whose zeus attack is gone is failed
// (cascading to the rest of the experiment); a paused phase is left alone.
func TestRecover(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t) // knows no attacks → every reconcile reports lost
	o := newOrch(t, fz.srv.URL)

	// Experiment A: phase 0 'running' with a stale attack handle, phase 1 pending.
	expA, phasesA := mkExperiment(t, 2)
	attachAttackWorkflow(t, phasesA[0].ID, model.PhaseWorkflow{
		VUs: 1, DurationSec: 1,
		TargetURL: "http://frontend:8080/", ZeusAttackID: "atk-lost-" + expA.ID,
	})
	if err := testExpRepo.UpdateStatus(ctx, expA.ID, "running"); err != nil {
		t.Fatal(err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, phasesA[0].ID, "running"); err != nil {
		t.Fatal(err)
	}

	// Experiment B: phase 0 paused.
	expB, phasesB := mkExperiment(t, 1)
	if err := testExpRepo.UpdateStatus(ctx, expB.ID, "running"); err != nil {
		t.Fatal(err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, phasesB[0].ID, "running"); err != nil {
		t.Fatal(err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, phasesB[0].ID, "paused"); err != nil {
		t.Fatal(err)
	}

	if err := o.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	waitFor(t, "experiment A failed", 10*time.Second, func() bool {
		return getExp(t, expA.ID).Status == "failed"
	})
	if got := getPhase(t, phasesA[0].ID); got.Status != "failed" {
		t.Errorf("A phase 0 status %q, want failed", got.Status)
	}
	if got := getPhase(t, phasesA[1].ID); got.Status != "skipped" {
		t.Errorf("A phase 1 status %q, want skipped", got.Status)
	}

	if got := getPhase(t, phasesB[0].ID); got.Status != "paused" {
		t.Errorf("B phase 0 status %q, want paused (recover must not touch it)", got.Status)
	}
	if got := getExp(t, expB.ID); got.Status != "running" {
		t.Errorf("B experiment status %q, want running", got.Status)
	}
}

func TestPhaseFaultEventsAuditTrail(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "") // no zeus → driver-less phases auto-complete
	feRepo := store.NewPhaseFaultEventRepo(testDB)
	exp, _ := mkExperiment(t, 1) // phase 0 is bare (no frozen services)

	// Add a second phase that freezes a service, so enterPhase records a
	// cachebox event that finishPhase must then close.
	fp := &model.ExperimentPhase{
		ID: id.New("phase"), ExperimentID: exp.ID, Name: "frozen", Position: 1, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: "frontend", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
	}
	if err := testExpRepo.CreatePhase(ctx, fp); err != nil {
		t.Fatal(err)
	}

	// Attach a cachebox-action rule to fp so recordRuleEvents fires.
	// A cachebox rule needs no FaultSpec, and ruleKind returns "rule" for it.
	rule := &model.Rule{
		ID:      id.New("rule"),
		Name:    "audit-rule",
		Service: "frontend",
		Enabled: true,
		Match:   model.MatchCriteria{},
		Action: model.RuleAction{
			Type:     "cachebox",
			CacheBox: &model.CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: "exact"},
		},
		Mode:      "inline",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := testRuleRepo.Create(ctx, rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() { _ = testRuleRepo.Delete(context.Background(), rule.ID) })
	if err := testExpRepo.AttachPhaseRules(ctx, fp.ID, []string{rule.ID}); err != nil {
		t.Fatalf("attach rule: %v", err)
	}

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, "experiment completed", 10*time.Second, func() bool {
		return getExp(t, exp.ID).Status == "completed"
	})

	events, err := feRepo.ListForPhase(ctx, fp.ID)
	if err != nil || len(events) == 0 {
		t.Fatalf("expected fault events for frozen phase, got %d err=%v", len(events), err)
	}

	var hasCachebox, hasRule bool
	for _, e := range events {
		if e.Service != "frontend" {
			t.Errorf("unexpected service on event: %+v", e)
		}
		if e.EndedAt == nil {
			t.Errorf("event %s (source=%s) not closed after phase completion", e.ID, e.Source)
		}
		switch e.Source {
		case "cachebox":
			hasCachebox = true
		case "rule":
			hasRule = true
		default:
			t.Errorf("unexpected event source %q: %+v", e.Source, e)
		}
	}
	if !hasCachebox {
		t.Error("no cachebox event recorded for frozen phase")
	}
	if !hasRule {
		t.Error("no rule event recorded for attached rule")
	}
}
