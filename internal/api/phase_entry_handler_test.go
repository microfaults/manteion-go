package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// entryFixture is a phase entry against one service with one alive SDK
// instance that observes nothing on its own: an entry's convergence wait
// lasts until the test makes the instance observe the bump (ack).
type entryFixture struct {
	e    *editTestEnv
	svc  string
	inst string
	sdk  *store.SDKRepo
	gens *store.GenerationRepo
}

func newEntryFixture(t *testing.T, convergeTimeout time.Duration) *entryFixture {
	t.Helper()
	e := newEditTestEnv(t)
	orch := newTestOrch(t, e.db, "")
	orch.WithConvergeTimeout(convergeTimeout)
	orch.WithConvergePollInterval(20 * time.Millisecond)
	e.s.orch = orch
	f := &entryFixture{e: e, svc: "entry-" + id.New("svc"), sdk: store.NewSDKRepo(e.db), gens: store.NewGenerationRepo(e.db)}
	t.Cleanup(func() { _, _ = e.db.Exec(`DELETE FROM service_generations WHERE service = $1`, f.svc) })
	inst := &model.SDKInstance{ID: id.New("sdk"), Service: f.svc, Version: "test", Address: "http://never-acks.invalid"}
	if err := f.sdk.Register(e.ctx, inst); err != nil {
		t.Fatalf("register sdk: %v", err)
	}
	t.Cleanup(func() { _ = f.sdk.Deregister(context.Background(), inst.ID) })
	f.inst = inst.ID
	return f
}

// rulePhase seeds a phase of exp, in status, whose one rule targets the
// fixture's service: entering it bumps that service and waits for its
// instance.
func (f *entryFixture) rulePhase(t *testing.T, exp *model.Experiment, status string) *model.ExperimentPhase {
	t.Helper()
	ph := f.e.phase(exp, "entry-"+id.New("p"), 0)
	rule := &model.Rule{
		ID: id.New("rule"), Name: "entry-rule", Service: f.svc, Mode: "inline", Enabled: true,
		Action:    model.RuleAction{Type: "cachebox", CacheBox: &model.CacheBoxRuleConfig{Mode: "passthrough", KeyStrategy: "exact"}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.NewRuleRepo(f.e.db, nil).Create(f.e.ctx, rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	t.Cleanup(func() {
		_, _ = f.e.db.Exec("DELETE FROM phase_rules WHERE rule_id = $1", rule.ID)
		_, _ = f.e.db.Exec("DELETE FROM rules WHERE id = $1", rule.ID)
	})
	if err := f.e.exp.AttachPhaseRules(f.e.ctx, ph.ID, []string{rule.ID}); err != nil {
		t.Fatalf("attach rule: %v", err)
	}
	for _, s := range map[string][]string{"pending": nil, "running": {"running"}, "paused": {"running", "paused"}}[status] {
		if err := f.e.exp.UpdatePhaseStatus(f.e.ctx, ph.ID, s); err != nil {
			t.Fatalf("phase → %s: %v", s, err)
		}
	}
	return ph
}

func (f *entryFixture) generation(t *testing.T) uint64 {
	t.Helper()
	g, err := f.gens.Get(context.Background(), f.svc)
	if err != nil {
		t.Fatalf("read generation: %v", err)
	}
	return g
}

// awaitBump blocks until the service's generation passes g0: the entry has
// bumped and is waiting for the fleet.
func (f *entryFixture) awaitBump(t *testing.T, g0 uint64, answered <-chan *httptest.ResponseRecorder) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); f.generation(t) <= g0; time.Sleep(5 * time.Millisecond) {
		select {
		case w := <-answered:
			t.Fatalf("answered %d (%s) before the entry bumped", w.Code, w.Body.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the entry never bumped")
		}
	}
}

// ack makes the instance observe the service's current generation.
func (f *entryFixture) ack(t *testing.T) {
	t.Helper()
	if err := f.sdk.RecordPoll(context.Background(), f.inst, f.generation(t)); err != nil {
		t.Fatalf("record poll: %v", err)
	}
}

type entryCall struct {
	name                   string
	expStatus, phaseStatus string
	route                  func(s *Server, exp *model.Experiment, ph *model.ExperimentPhase) (http.HandlerFunc, string, map[string]string)
	wantCode               int // on success
}

var entryCalls = []entryCall{
	{"start experiment", "planned", "pending", func(s *Server, exp *model.Experiment, _ *model.ExperimentPhase) (http.HandlerFunc, string, map[string]string) {
		url, pv := expAction(exp.ID, "start")
		return s.handleStartExperiment, url, pv
	}, http.StatusAccepted},
	{"start phase", "running", "pending", func(s *Server, exp *model.Experiment, ph *model.ExperimentPhase) (http.HandlerFunc, string, map[string]string) {
		url, pv := phaseAction(exp.ID, ph.ID, "start")
		return s.handleStartPhase, url, pv
	}, http.StatusAccepted},
	{"resume phase", "running", "paused", func(s *Server, _ *model.Experiment, ph *model.ExperimentPhase) (http.HandlerFunc, string, map[string]string) {
		url, pv := flatPhaseAction(ph.ID, "resume")
		return s.handleResumePhaseFlat, url, pv
	}, http.StatusOK},
	{"resume experiment", "running", "paused", func(s *Server, exp *model.Experiment, _ *model.ExperimentPhase) (http.HandlerFunc, string, map[string]string) {
		url, pv := expAction(exp.ID, "resume")
		return s.handleResumeExperiment, url, pv
	}, http.StatusOK},
}

// serveAsync runs handler for a POST to url on its own goroutine, with ctx as
// the request's context, and delivers the recorder.
func serveAsync(ctx context.Context, handler http.HandlerFunc, url string, pv map[string]string) <-chan *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, url, nil).WithContext(ctx)
	for k, v := range pv {
		req.SetPathValue(k, v)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		handler(w, req)
		done <- w
	}()
	return done
}

func await(t *testing.T, done <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case w := <-done:
		return w
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never answered")
		return nil
	}
}

// The start and resume handlers run the orchestrator detached from the
// request, as pause and stop do: the entry can wait MANTEION_CONVERGE_TIMEOUT
// for its fleet, and a client hanging up meanwhile must not abort it (which
// failed a fresh phase, or put a resumed one back to paused). The fleet
// converges after the client left, and the entry goes through.
func TestEntryHandlers_SurviveClientDisconnect(t *testing.T) {
	for _, tc := range entryCalls {
		t.Run(tc.name, func(t *testing.T) {
			f := newEntryFixture(t, 5*time.Second)
			exp := f.e.experiment(tc.expStatus)
			ph := f.rulePhase(t, exp, tc.phaseStatus)
			g0 := f.generation(t)
			reqCtx, hangUp := context.WithCancel(context.Background())
			defer hangUp()

			handler, url, pv := tc.route(f.e.s, exp, ph)
			done := serveAsync(reqCtx, handler, url, pv)
			f.awaitBump(t, g0, done)
			hangUp() // the operator's client goes away while the entry waits
			f.ack(t) // ...and the fleet converges after it left
			w := await(t, done)

			if w.Code != tc.wantCode {
				t.Fatalf("status = %d body = %s, want %d: the entry must not abort with its client", w.Code, w.Body.String(), tc.wantCode)
			}
			// Driver-less, the phase auto-completes once entered.
			got, err := f.e.exp.GetPhase(context.Background(), ph.ID)
			if err != nil || (got.Status != "running" && got.Status != "completed") || got.FailureReason != "" {
				t.Fatalf("phase = %+v, %v; want entered (running, or completed by its auto-complete) with no failure", got, err)
			}
		})
	}
}

// A phase start or resume whose fleet does not converge within the timeout is
// 504 not_converged, with the lagging instances (and the suspect ones the
// barrier did not wait on) in the body. A fresh phase is failed with the
// reason; a resumed one stays paused. The experiment start is not among
// them: it reports its first phase's entry on the phase, as it does every
// other entry failure.
func TestEntryHandlers_NotConvergedIs504(t *testing.T) {
	for _, tc := range entryCalls[1:] {
		t.Run(tc.name, func(t *testing.T) {
			f := newEntryFixture(t, 300*time.Millisecond)
			exp := f.e.experiment(tc.expStatus)
			ph := f.rulePhase(t, exp, tc.phaseStatus)

			handler, url, pv := tc.route(f.e.s, exp, ph)
			w := await(t, serveAsync(context.Background(), handler, url, pv))

			body := decodeErrBody(t, w, http.StatusGatewayTimeout, "not_converged")
			lagging, _ := body["lagging"].([]any)
			if len(lagging) != 1 || !strings.HasPrefix(lagging[0].(string), f.inst+"("+f.svc+"): observed=") {
				t.Errorf("lagging = %v, want the instance %s with its observed generation", body["lagging"], f.inst)
			}
			if suspect, ok := body["suspect"].([]any); !ok || len(suspect) != 0 {
				t.Errorf("suspect = %v, want an empty list", body["suspect"])
			}
			if msg, _ := body["error"].(string); !strings.Contains(msg, "converge gate") || !strings.Contains(msg, f.inst) {
				t.Errorf("error = %q, want the converge-gate message naming %s", msg, f.inst)
			}
			want := "paused"
			if tc.phaseStatus == "pending" {
				want = "failed"
			}
			if got, err := f.e.exp.GetPhase(context.Background(), ph.ID); err != nil || got.Status != want {
				t.Fatalf("phase = %+v, %v; want %s", got, err, want)
			}
		})
	}
}
