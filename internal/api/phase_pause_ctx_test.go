package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// TestPauseHandlers_SurviveClientDisconnect is the pause twin of
// TestHandleStopPhaseFlat_SurvivesClientDisconnect. Once PausePhase has marked
// the phase paused, its rules and cache-box role are already out of the
// materialized documents; what is left is stopping the load (zeus attacks and
// k6 runs) and the generation bump that wakes the held long-polls. Run on the
// request's context, a client hanging up after the CAS skipped both: load kept
// running and faults kept injecting until the held polls timed out. Both pause
// handlers detach with context.WithoutCancel, as the stop handlers do.
//
// The fake zeus hangs the client up while it stops the attack, after the
// phase was marked paused; the run stop and the bump come after that.
func TestPauseHandlers_SurviveClientDisconnect(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause func(s *Server, ph *model.ExperimentPhase) (http.HandlerFunc, string, map[string]string)
	}{
		{"flat phase pause", func(s *Server, ph *model.ExperimentPhase) (http.HandlerFunc, string, map[string]string) {
			url, pv := flatPhaseAction(ph.ID, "pause")
			return s.handlePausePhaseFlat, url, pv
		}},
		{"experiment pause", func(s *Server, ph *model.ExperimentPhase) (http.HandlerFunc, string, map[string]string) {
			url, pv := expAction(ph.ExperimentID, "pause")
			return s.handlePauseExperiment, url, pv
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEditTestEnv(t)
			reqCtx, hangUp := context.WithCancel(context.Background())
			defer hangUp()

			var mu sync.Mutex
			var calls []string
			record := func(call string) { mu.Lock(); calls = append(calls, call); mu.Unlock() }
			zeusMux := http.NewServeMux()
			zeusMux.HandleFunc("DELETE /api/v1/attacks/{id}", func(w http.ResponseWriter, r *http.Request) {
				record("stop attack " + r.PathValue("id"))
				hangUp() // the operator's client goes away mid-pause
				w.WriteHeader(http.StatusNoContent)
			})
			zeusMux.HandleFunc("DELETE /api/v1/runs/{run_id}", func(w http.ResponseWriter, r *http.Request) {
				record("stop run " + r.PathValue("run_id"))
				w.WriteHeader(http.StatusNoContent)
			})
			zeusSrv := httptest.NewServer(zeusMux)
			t.Cleanup(zeusSrv.Close)
			e.s.orch = newTestOrch(t, e.db, zeusSrv.URL)

			// A running isolation phase with live load: one attack and one run.
			svc := "pause-" + id.New("svc")
			t.Cleanup(func() { _, _ = e.db.Exec(`DELETE FROM service_generations WHERE service = $1`, svc) })
			exp := e.experiment("running")
			ph := e.frozenPhase(exp, svc, 0)
			if err := e.exp.AttachPhaseWorkflows(e.ctx, ph.ID, []model.PhaseWorkflow{{WorkflowID: e.workflow(),
				VUs: 1, DurationSec: 1, ZeusAttackID: "atk-" + ph.ID, ZeusRunID: "run-" + ph.ID}}); err != nil {
				t.Fatalf("attach phase workflow: %v", err)
			}
			if err := e.exp.UpdatePhaseStatus(e.ctx, ph.ID, "running"); err != nil {
				t.Fatalf("run phase: %v", err)
			}
			gens := store.NewGenerationRepo(e.db)
			g0, err := gens.Get(e.ctx, svc)
			if err != nil {
				t.Fatalf("read generation: %v", err)
			}

			handler, url, pv := tc.pause(e.s, ph)
			req := httptest.NewRequest(http.MethodPost, url, nil).WithContext(reqCtx)
			for k, v := range pv {
				req.SetPathValue(k, v)
			}
			w := httptest.NewRecorder()
			handler(w, req)

			if reqCtx.Err() == nil {
				t.Fatal("the client never hung up: the attack stop was not reached, so this proves nothing")
			}
			if w.Code != http.StatusOK {
				t.Errorf("status = %d body = %s, want 200", w.Code, w.Body.String())
			}
			if got, err := e.exp.GetPhase(context.Background(), ph.ID); err != nil || got.Status != "paused" {
				t.Errorf("phase = %+v err = %v, want paused", got, err)
			}
			mu.Lock()
			got := slices.Clone(calls)
			mu.Unlock()
			if want := []string{"stop attack atk-" + ph.ID, "stop run run-" + ph.ID}; !slices.Equal(got, want) {
				t.Errorf("zeus calls = %q, want %q: the load must stop although the client left", got, want)
			}
			if g, err := gens.Get(context.Background(), svc); err != nil || g != g0+1 {
				t.Errorf("generation(%s) = %d err = %v, want %d: the pause's bump must run although the client left", svc, g, err, g0+1)
			}
		})
	}
}
