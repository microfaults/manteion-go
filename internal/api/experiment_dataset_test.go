package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
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
	"manteion-go/internal/zeus"
)

// fakeDatasetZeus serves GET /api/v1/datasets from a mutable list and counts
// the calls, so the tests can pin that a dataset-free plan never asks zeus.
type fakeDatasetZeus struct {
	mu       sync.Mutex
	datasets []map[string]any
	calls    int
	srv      *httptest.Server
}

func newFakeDatasetZeus(t *testing.T, datasets ...map[string]any) *fakeDatasetZeus {
	t.Helper()
	f := &fakeDatasetZeus{datasets: datasets}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/datasets", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"datasets": f.datasets})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeDatasetZeus) set(datasets ...map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.datasets = datasets
}

func (f *fakeDatasetZeus) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// zeusDataset builds one GET /datasets item in zeus's wire shape.
func zeusDataset(dsID string, ttlS int, createdAt time.Time) map[string]any {
	return map[string]any{
		"id": dsID, "name": dsID, "source": "upload", "size_bytes": 1,
		"ttl_s": ttlS, "created_at": createdAt,
	}
}

// phaseJSON is the phase shape the tests decode: the model fields that
// matter plus the derived dataset_ids.
type phaseJSON struct {
	ID         string                `json:"id"`
	Name       string                `json:"name"`
	Status     string                `json:"status"`
	Workflows  []model.PhaseWorkflow `json:"workflows"`
	DatasetIDs []string              `json:"dataset_ids"`
}

type experimentJSON struct {
	ID     string      `json:"id"`
	Status string      `json:"status"`
	Phases []phaseJSON `json:"phases"`
}

func (p phaseJSON) workflow(t *testing.T, workflowID string) model.PhaseWorkflow {
	t.Helper()
	for _, w := range p.Workflows {
		if w.WorkflowID == workflowID {
			return w
		}
	}
	t.Fatalf("phase %s has no workflow %s: %+v", p.ID, workflowID, p.Workflows)
	return model.PhaseWorkflow{}
}

func call(handler http.HandlerFunc, method, path, body string, pathValues map[string]string) *httptest.ResponseRecorder {
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, rdr)
	for k, v := range pathValues {
		req.SetPathValue(k, v)
	}
	w := httptest.NewRecorder()
	handler(w, req)
	return w
}

func decodeInto(t *testing.T, w *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(w.Body.Bytes(), v); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPhaseWorkflowDatasets pins the dataset-per-workflow wire contract
// (product decision 3): workflows[].dataset_id round-trips through create →
// store → every phase read-out, each phase read-out derives dataset_ids
// (set-union, first-seen order, [] never null), and both create handlers and
// both start handlers preflight the plan's datasets against zeus — 422
// dataset_missing / dataset_expiring with the offending ids, 502
// zeus_unreachable — before any row is written or any phase claimed.
func TestPhaseWorkflowDatasets(t *testing.T) {
	if os.Getenv("MANTEION_TEST_DB") == "" {
		t.Skip("set MANTEION_TEST_DB=1 to run")
	}
	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	database, err := db.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	defer db.Close(database)

	logger := discardLogger()
	expRepo := store.NewExperimentRepo(database)
	wfRepo := store.NewWorkflowRepo(database)
	controller := atrocontrol.New(
		atropos.NewClient(atropos.WithHTTPClient(&http.Client{Timeout: time.Second})),
		&atrocontrol.RepoResolver{Repo: store.NewSDKRepo(database)},
		atrocontrol.WithDefaultTimeout(time.Second), atrocontrol.WithLogger(logger),
	)
	// A real orchestrator with NO zeus client: a start that passes preflight
	// runs its phases driver-less (auto-complete), which is all the start
	// tests need — they pin that the preflight gates the orchestrator call.
	orch := orchestrator.New(
		expRepo, store.NewRuleRepo(database), store.NewFaultRepo(database),
		store.NewWorkloadRepo(database), wfRepo,
		controller, nil, nil, cachestore.New(t.TempDir()),
		store.NewPhaseFaultEventRepo(database), logger,
	)
	orch.WithPollInterval(20 * time.Millisecond)

	// Two workflows with deterministic ids so ListPhaseWorkflows' ORDER BY
	// workflow_id (and thus the first-seen union order) is fixed: wfA < wfB.
	suffix := id.New("n")
	wfA, wfB := "wf-a-"+suffix, "wf-b-"+suffix
	for _, wfID := range []string{wfA, wfB} {
		wf := &model.Workflow{
			ID: wfID, Name: "ds-test-" + wfID,
			DSL:       json.RawMessage(`{"type":"request","method":"GET","url":"http://target:8080/"}`),
			CreatedAt: time.Now(),
		}
		if err := wfRepo.Create(ctx, wf); err != nil {
			t.Fatalf("create workflow %s: %v", wfID, err)
		}
	}
	var expIDs []string
	t.Cleanup(func() {
		for _, e := range expIDs {
			_, _ = database.Exec("DELETE FROM experiments WHERE id = $1", e)
		}
		_, _ = database.Exec("DELETE FROM workflows WHERE id IN ($1, $2)", wfA, wfB)
	})
	newExpID := func() string {
		e := id.New("exp")
		expIDs = append(expIDs, e)
		return e
	}

	now := time.Now().UTC()
	fz := newFakeDatasetZeus(t,
		zeusDataset("ds-a", 86400, now),
		zeusDataset("ds-b", 0, now.Add(-48*time.Hour)), // ttl 0: never expires
		zeusDataset("ds-short", 60, now),               // gone in a minute
	)
	s := &Server{experiments: expRepo, zeus: zeus.NewClient(fz.srv.URL), orch: orch, logger: logger}
	deadZeus := &Server{experiments: expRepo, zeus: zeus.NewClient("http://127.0.0.1:1"), orch: orch, logger: logger}
	noZeus := &Server{experiments: expRepo, orch: orch, logger: logger}

	planBody := func(expID string) string {
		return fmt.Sprintf(`{"id": %q, "name": "ds-test-plan", "phases": [
			{"name": "p0", "position": 0, "workflows": [
				{"workflow_id": %q, "vus": 1, "duration_sec": 60, "dataset_id": "ds-a"},
				{"workflow_id": %q, "vus": 1, "duration_sec": 30, "dataset_id": "ds-b"}]},
			{"name": "p1", "position": 1, "workflows": [
				{"workflow_id": %q, "vus": 1, "duration_sec": 10, "dataset_id": "ds-a"}]},
			{"name": "p2", "position": 2, "workflows": [
				{"workflow_id": %q, "vus": 1, "duration_sec": 10}]}
		]}`, expID, wfA, wfB, wfA, wfA)
	}

	// ---- create: round trip + derived union -------------------------------
	planExp := newExpID()
	var created experimentJSON
	{
		w := call(s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", planBody(planExp), nil)
		if w.Code != http.StatusCreated {
			t.Fatalf("create: status=%d body=%s", w.Code, w.Body.String())
		}
		decodeInto(t, w, &created)
		if len(created.Phases) != 3 {
			t.Fatalf("created phases = %d, want 3", len(created.Phases))
		}
		p0, p1, p2 := created.Phases[0], created.Phases[1], created.Phases[2]
		if got := p0.workflow(t, wfA).DatasetID; got != "ds-a" {
			t.Errorf("p0/wfA dataset_id = %q, want ds-a", got)
		}
		if got := p0.workflow(t, wfB).DatasetID; got != "ds-b" {
			t.Errorf("p0/wfB dataset_id = %q, want ds-b", got)
		}
		if !equalIDs(p0.DatasetIDs, []string{"ds-a", "ds-b"}) {
			t.Errorf("p0 dataset_ids = %v, want [ds-a ds-b]", p0.DatasetIDs)
		}
		if !equalIDs(p1.DatasetIDs, []string{"ds-a"}) {
			t.Errorf("p1 dataset_ids = %v, want [ds-a]", p1.DatasetIDs)
		}
		if p2.DatasetIDs == nil || len(p2.DatasetIDs) != 0 {
			t.Errorf("p2 dataset_ids = %v, want []", p2.DatasetIDs)
		}
		if !strings.Contains(w.Body.String(), `"dataset_ids":[]`) {
			t.Errorf("a dataset-free phase must serialize dataset_ids as [] (never null); body=%s", w.Body.String())
		}
		if got := p2.workflow(t, wfA).DatasetID; got != "" {
			t.Errorf("p2/wfA dataset_id = %q, want empty", got)
		}
		if n := fz.callCount(); n != 1 {
			t.Errorf("zeus /datasets called %d times during create, want exactly 1", n)
		}
	}

	// ---- every phase read-out embeds the field + derived union -------------
	{
		w := call(s.handleGetExperiment, http.MethodGet, "/api/v1/experiments/"+planExp, "", map[string]string{"id": planExp})
		if w.Code != http.StatusOK {
			t.Fatalf("get experiment: status=%d body=%s", w.Code, w.Body.String())
		}
		var got experimentJSON
		decodeInto(t, w, &got)
		if !equalIDs(got.Phases[0].DatasetIDs, []string{"ds-a", "ds-b"}) || got.Phases[0].workflow(t, wfB).DatasetID != "ds-b" {
			t.Errorf("GET experiment p0 = %+v, want dataset_ids [ds-a ds-b] and wfB dataset_id ds-b", got.Phases[0])
		}

		p0, p2 := created.Phases[0].ID, created.Phases[2].ID
		w = call(s.handleGetPhase, http.MethodGet, "/api/v1/experiments/"+planExp+"/phases/"+p0, "", map[string]string{"id": planExp, "phaseId": p0})
		if w.Code != http.StatusOK {
			t.Fatalf("get phase: status=%d body=%s", w.Code, w.Body.String())
		}
		var ph phaseJSON
		decodeInto(t, w, &ph)
		if !equalIDs(ph.DatasetIDs, []string{"ds-a", "ds-b"}) {
			t.Errorf("GET experiments/{id}/phases/{phaseId} dataset_ids = %v, want [ds-a ds-b]", ph.DatasetIDs)
		}

		w = call(s.handleGetPhaseDetail, http.MethodGet, "/api/v1/phases/"+p0, "", map[string]string{"phaseId": p0})
		if w.Code != http.StatusOK {
			t.Fatalf("get phase detail: status=%d body=%s", w.Code, w.Body.String())
		}
		var det PhaseDetail
		decodeInto(t, w, &det)
		if !equalIDs(det.DatasetIDs, []string{"ds-a", "ds-b"}) {
			t.Errorf("GET phases/{phaseId} dataset_ids = %v, want [ds-a ds-b]", det.DatasetIDs)
		}
		if got := det.Workflows; len(got) != 2 || got[0].DatasetID != "ds-a" || got[1].DatasetID != "ds-b" {
			t.Errorf("GET phases/{phaseId} workflows = %+v, want dataset ids ds-a, ds-b", got)
		}
		w = call(s.handleGetPhaseDetail, http.MethodGet, "/api/v1/phases/"+p2, "", map[string]string{"phaseId": p2})
		if !strings.Contains(w.Body.String(), `"dataset_ids":[]`) {
			t.Errorf("GET phases/{phaseId} of a dataset-free phase must carry dataset_ids []; body=%s", w.Body.String())
		}
	}

	// ---- POST /experiments/{id}/phases round trip ---------------------------
	{
		body := fmt.Sprintf(`{"name": "p3", "position": 3, "workflows": [
			{"workflow_id": %q, "vus": 1, "duration_sec": 5, "dataset_id": "ds-b"}]}`, wfB)
		w := call(s.handleCreatePhase, http.MethodPost, "/api/v1/experiments/"+planExp+"/phases", body, map[string]string{"id": planExp})
		if w.Code != http.StatusCreated {
			t.Fatalf("create phase: status=%d body=%s", w.Code, w.Body.String())
		}
		var ph phaseJSON
		decodeInto(t, w, &ph)
		if ph.workflow(t, wfB).DatasetID != "ds-b" || !equalIDs(ph.DatasetIDs, []string{"ds-b"}) {
			t.Errorf("created phase = %+v, want wfB dataset_id ds-b and dataset_ids [ds-b]", ph)
		}
		rows, err := expRepo.ListPhaseWorkflows(ctx, ph.ID)
		if err != nil || len(rows) != 1 || rows[0].DatasetID != "ds-b" {
			t.Errorf("stored phase_workflows = %+v (err=%v), want one row with dataset_id ds-b", rows, err)
		}
	}

	// ---- a dataset-free plan never asks zeus -------------------------------
	{
		expID := newExpID()
		body := fmt.Sprintf(`{"id": %q, "name": "ds-test-nods", "phases": [
			{"name": "p0", "position": 0, "workflows": [{"workflow_id": %q, "vus": 1, "duration_sec": 5}]}]}`, expID, wfA)
		w := call(deadZeus.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", body, nil)
		if w.Code != http.StatusCreated {
			t.Errorf("dataset-free plan with unreachable zeus: status=%d, want 201 (zeus must not be consulted); body=%s", w.Code, w.Body.String())
		}
	}

	// ---- 422 dataset_missing at create (nothing written) -------------------
	{
		expID := newExpID()
		body := fmt.Sprintf(`{"id": %q, "name": "ds-test-missing", "phases": [
			{"name": "p0", "position": 0, "workflows": [{"workflow_id": %q, "vus": 1, "duration_sec": 60, "dataset_id": "ds-nope"}]}]}`, expID, wfA)
		w := call(s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", body, nil)
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("missing dataset: status=%d, want 422; body=%s", w.Code, w.Body.String())
		}
		want := `{"error":"dataset(s) not found in zeus: ds-nope","code":"dataset_missing","dataset_ids":["ds-nope"]}`
		if got := strings.TrimSpace(w.Body.String()); got != want {
			t.Errorf("dataset_missing body =\n%s\nwant\n%s", got, want)
		}
		if _, err := expRepo.Get(ctx, expID); err == nil {
			t.Errorf("experiment %s was written despite the 422", expID)
		}
	}

	// ---- 422 dataset_expiring at create ------------------------------------
	{
		expID := newExpID()
		body := fmt.Sprintf(`{"id": %q, "name": "ds-test-expiring", "phases": [
			{"name": "p0", "position": 0, "workflows": [{"workflow_id": %q, "vus": 1, "duration_sec": 60, "dataset_id": "ds-short"}]}]}`, expID, wfA)
		w := call(s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", body, nil)
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("expiring dataset: status=%d, want 422; body=%s", w.Code, w.Body.String())
		}
		want := `{"error":"dataset(s) expire before the plan would finish (plan length 6m0s, incl. 5m0s slack): ds-short","code":"dataset_expiring","dataset_ids":["ds-short"]}`
		if got := strings.TrimSpace(w.Body.String()); got != want {
			t.Errorf("dataset_expiring body =\n%s\nwant\n%s", got, want)
		}
		if _, err := expRepo.Get(ctx, expID); err == nil {
			t.Errorf("experiment %s was written despite the 422", expID)
		}
		// Same gate on POST /experiments/{id}/phases.
		phBody := fmt.Sprintf(`{"name": "p9", "position": 9, "workflows": [{"workflow_id": %q, "vus": 1, "duration_sec": 60, "dataset_id": "ds-short"}]}`, wfA)
		w = call(s.handleCreatePhase, http.MethodPost, "/api/v1/experiments/"+planExp+"/phases", phBody, map[string]string{"id": planExp})
		if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), `"code":"dataset_expiring"`) {
			t.Errorf("create phase with expiring dataset: status=%d body=%s, want 422 dataset_expiring", w.Code, w.Body.String())
		}
	}

	// ---- 502 zeus_unreachable -----------------------------------------------
	{
		body := fmt.Sprintf(`{"id": %q, "name": "ds-test-502", "phases": [
			{"name": "p0", "position": 0, "workflows": [{"workflow_id": %q, "vus": 1, "duration_sec": 60, "dataset_id": "ds-a"}]}]}`, newExpID(), wfA)
		for name, srv := range map[string]*Server{"dead zeus": deadZeus, "no zeus client": noZeus} {
			w := call(srv.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", body, nil)
			if w.Code != http.StatusBadGateway {
				t.Fatalf("%s: status=%d, want 502; body=%s", name, w.Code, w.Body.String())
			}
			var got map[string]any
			decodeInto(t, w, &got)
			if got["code"] != "zeus_unreachable" || !strings.HasPrefix(got["error"].(string), "zeus unreachable: ") {
				t.Errorf("%s: body=%s, want code zeus_unreachable and error prefixed 'zeus unreachable: '", name, w.Body.String())
			}
			if _, ok := got["dataset_ids"]; ok {
				t.Errorf("%s: 502 envelope must not carry dataset_ids; body=%s", name, w.Body.String())
			}
		}
	}

	// ---- start experiment: preflight gates the orchestrator ----------------
	{
		startPath := "/api/v1/experiments/" + planExp + "/start"
		pv := map[string]string{"id": planExp}

		fz.set(zeusDataset("ds-a", 86400, now)) // ds-b forgotten (zeus restarted)
		w := call(s.handleStartExperiment, http.MethodPost, startPath, "", pv)
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("start with missing dataset: status=%d, want 422; body=%s", w.Code, w.Body.String())
		}
		want := `{"error":"dataset(s) not found in zeus: ds-b","code":"dataset_missing","dataset_ids":["ds-b"]}`
		if got := strings.TrimSpace(w.Body.String()); got != want {
			t.Errorf("start dataset_missing body =\n%s\nwant\n%s", got, want)
		}
		if exp, _ := expRepo.Get(ctx, planExp); exp == nil || exp.Status != "planned" {
			t.Fatalf("experiment after refused start = %+v, want still planned", exp)
		}

		// Plan = p0 max(60,30) + p1 10 + p2 10 + p3 5 = 85s, + 5m slack = 6m25s;
		// ds-b now has 30s left.
		fz.set(zeusDataset("ds-a", 86400, now), zeusDataset("ds-b", 30, time.Now().UTC()))
		w = call(s.handleStartExperiment, http.MethodPost, startPath, "", pv)
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("start with expiring dataset: status=%d, want 422; body=%s", w.Code, w.Body.String())
		}
		want = `{"error":"dataset(s) expire before the plan would finish (plan length 6m25s, incl. 5m0s slack): ds-b","code":"dataset_expiring","dataset_ids":["ds-b"]}`
		if got := strings.TrimSpace(w.Body.String()); got != want {
			t.Errorf("start dataset_expiring body =\n%s\nwant\n%s", got, want)
		}

		w = call(deadZeus.handleStartExperiment, http.MethodPost, startPath, "", pv)
		if w.Code != http.StatusBadGateway || !strings.Contains(w.Body.String(), `"code":"zeus_unreachable"`) {
			t.Errorf("start with unreachable zeus: status=%d body=%s, want 502 zeus_unreachable", w.Code, w.Body.String())
		}
		if exp, _ := expRepo.Get(ctx, planExp); exp == nil || exp.Status != "planned" {
			t.Fatalf("experiment after refused starts = %+v, want still planned", exp)
		}

		// Healthy datasets: the start goes through to the orchestrator.
		fz.set(zeusDataset("ds-a", 86400, now), zeusDataset("ds-b", 0, now))
		w = call(s.handleStartExperiment, http.MethodPost, startPath, "", pv)
		if w.Code != http.StatusAccepted {
			t.Fatalf("start with healthy datasets: status=%d, want 202; body=%s", w.Code, w.Body.String())
		}
		waitForStatus(t, "experiment completes (driver-less phases auto-complete)", 20*time.Second, func() string {
			exp, _ := expRepo.Get(context.Background(), planExp)
			if exp == nil {
				return ""
			}
			return exp.Status
		}, "completed")
	}

	// ---- start phase: same gate --------------------------------------------
	{
		expID := newExpID()
		body := fmt.Sprintf(`{"id": %q, "name": "ds-test-phase-start", "phases": [
			{"name": "p0", "position": 0, "workflows": [{"workflow_id": %q, "vus": 1, "duration_sec": 60, "dataset_id": "ds-a"}]}]}`, expID, wfA)
		w := call(s.handleCreateExperiment, http.MethodPost, "/api/v1/experiments", body, nil)
		if w.Code != http.StatusCreated {
			t.Fatalf("create: status=%d body=%s", w.Code, w.Body.String())
		}
		var exp experimentJSON
		decodeInto(t, w, &exp)
		phID := exp.Phases[0].ID
		path := "/api/v1/experiments/" + expID + "/phases/" + phID + "/start"
		pv := map[string]string{"id": expID, "phaseId": phID}

		fz.set(zeusDataset("ds-b", 0, now)) // ds-a gone
		w = call(s.handleStartPhase, http.MethodPost, path, "", pv)
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("start phase with missing dataset: status=%d, want 422; body=%s", w.Code, w.Body.String())
		}
		want := `{"error":"dataset(s) not found in zeus: ds-a","code":"dataset_missing","dataset_ids":["ds-a"]}`
		if got := strings.TrimSpace(w.Body.String()); got != want {
			t.Errorf("start phase dataset_missing body =\n%s\nwant\n%s", got, want)
		}
		if ph, _ := expRepo.GetPhase(ctx, phID); ph == nil || ph.Status != "pending" {
			t.Fatalf("phase after refused start = %+v, want still pending", ph)
		}

		fz.set(zeusDataset("ds-a", 60, time.Now().UTC())) // 60s left, phase needs 60s + 5m
		w = call(s.handleStartPhase, http.MethodPost, path, "", pv)
		if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), `"code":"dataset_expiring"`) {
			t.Errorf("start phase with expiring dataset: status=%d body=%s, want 422 dataset_expiring", w.Code, w.Body.String())
		}

		fz.set(zeusDataset("ds-a", 86400, now))
		w = call(s.handleStartPhase, http.MethodPost, path, "", pv)
		if w.Code != http.StatusAccepted {
			t.Fatalf("start phase with healthy dataset: status=%d, want 202; body=%s", w.Code, w.Body.String())
		}
		waitForStatus(t, "phase completes (driver-less auto-complete)", 20*time.Second, func() string {
			ph, _ := expRepo.GetPhase(context.Background(), phID)
			if ph == nil {
				return ""
			}
			return ph.Status
		}, "completed")
	}
}

func waitForStatus(t *testing.T, what string, timeout time.Duration, status func() string, want string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if status() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s: status=%q, want %q", what, status(), want)
}
