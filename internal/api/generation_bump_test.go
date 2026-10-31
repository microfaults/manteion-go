package api

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/db"
	"manteion-go/internal/desired"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// failingGenerationStore is a desired.Store whose every call fails.
type failingGenerationStore struct{}

func (failingGenerationStore) Get(context.Context, string) (uint64, error) {
	return 0, errors.New("store down")
}

func (failingGenerationStore) Bump(context.Context, *sql.Tx, string) (uint64, error) {
	return 0, errors.New("store down")
}

// TestBumpGeneration pins the out-of-band bump the fault-config handlers and
// the reaper use: it bumps the service, announces the new generation, and is
// best-effort — a store failure is logged, never propagated or announced.
func TestBumpGeneration(t *testing.T) {
	ctx := context.Background()
	broker := desired.NewBroker()
	ch := broker.Subscribe("cart")
	defer broker.Unsubscribe("cart", ch)

	s := &Server{logger: discardLogger(), generations: desired.NewGenerations(desired.NewMemoryStore(), broker)}
	s.bumpGeneration(ctx, "cart")
	if gen, _ := s.generations.Get(ctx, "cart"); gen != 2 {
		t.Fatalf("generation = %d, want 2", gen)
	}
	select {
	case ev := <-ch:
		if ev.Type != "rules_changed" || ev.Data != `{"generation":2}` {
			t.Fatalf("event = %+v, want rules_changed {\"generation\":2}", ev)
		}
	default:
		t.Fatal("bump was not announced")
	}

	s = &Server{logger: discardLogger(), generations: desired.NewGenerations(failingGenerationStore{}, broker)}
	s.bumpGeneration(ctx, "cart") // must not panic or announce
	select {
	case ev := <-ch:
		t.Fatalf("failed bump was announced: %+v", ev)
	default:
	}
}

// TestNewServer_SSEStreamsGenerationBumps pins that the SSE endpoint
// subscribes to the broker the Generations bump on: a bump made through the
// object handed to NewServer reaches a connected subscriber.
func TestNewServer_SSEStreamsGenerationBumps(t *testing.T) {
	gens := desired.NewGenerations(desired.NewMemoryStore(), desired.NewBroker())
	s := NewServer(discardLogger(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, gens, nil, nil, nil)
	srv := httptest.NewServer(http.HandlerFunc(s.handleSSEEvents))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?service=checkout")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	for deadline := time.Now().Add(2 * time.Second); gens.Broker().ClientCount() == 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("SSE handler never subscribed to the Generations broker")
		}
	}

	if _, err := gens.Bump(context.Background(), "checkout"); err != nil {
		t.Fatalf("bump: %v", err)
	}
	lines := make(chan string, 8)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	var got []string
	for len(got) < 2 {
		select {
		case line := <-lines:
			got = append(got, line)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for the SSE event, got %q", got)
		}
	}
	if got[0] != "event: rules_changed" || got[1] != `data: {"generation":2}` {
		t.Errorf("SSE lines = %q, want the rules_changed event for generation 2", got)
	}
}

// genEnv is the DB-backed fixture for the desired-state generation wiring
// (MANTEION_TEST_DB=1): the Server is built by NewServer over the real repos
// and a real Generations and driven through the route table. Service names and
// ids are minted per test and every row a test creates is deleted at cleanup.
type genEnv struct {
	t    *testing.T
	ctx  context.Context
	db   *sql.DB
	gens *desired.Generations
	s    *Server
	mux  *http.ServeMux
	tag  string
}

func newGenEnv(t *testing.T) *genEnv {
	t.Helper()
	if os.Getenv("MANTEION_TEST_DB") == "" {
		t.Skip("set MANTEION_TEST_DB=1 to run")
	}
	dsn := os.Getenv("MANTEION_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	database, err := db.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(database) })

	gens := desired.NewGenerations(store.NewGenerationRepo(database), desired.NewBroker())
	faults := store.NewFaultRepo(database)
	s := NewServer(discardLogger(), database, store.NewRuleRepo(database, gens), faults, faults,
		store.NewFaultConfigRepo(database), store.NewSDKRepo(database), store.NewExperimentRepo(database),
		nil, nil, nil, nil, nil, nil, gens, nil, nil, nil)
	mux := http.NewServeMux()
	s.routes(mux)
	return &genEnv{t: t, ctx: ctx, db: database, gens: gens, s: s, mux: mux, tag: id.New("g")}
}

func (e *genEnv) do(method, path string, body any) *httptest.ResponseRecorder {
	e.t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			e.t.Fatalf("marshal %s %s: %v", method, path, err)
		}
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	return w
}

func (e *genEnv) gen(service string) uint64 {
	e.t.Helper()
	g, err := e.gens.Get(e.ctx, service)
	if err != nil {
		e.t.Fatalf("generation(%s): %v", service, err)
	}
	return g
}

// watch subscribes to the service's rules_changed events, the way an SSE client does.
func (e *genEnv) watch(service string) chan desired.Event {
	ch := e.gens.Broker().Subscribe(service)
	e.t.Cleanup(func() { e.gens.Broker().Unsubscribe(service, ch) })
	return ch
}

// drainEvents returns, and consumes, the events announced on ch so far as
// "type data" strings. Every announcement is made on the writer's goroutine
// before its call returns, so once a handler has answered they are all already
// queued.
func drainEvents(ch chan desired.Event) []string {
	var got []string
	for {
		select {
		case ev := <-ch:
			got = append(got, ev.Type+" "+ev.Data)
		default:
			return got
		}
	}
}

// wantEvents asserts, and drains, the events announced on ch so far.
func (e *genEnv) wantEvents(ch chan desired.Event, generations ...uint64) {
	e.t.Helper()
	got := drainEvents(ch)
	var want []string
	for _, g := range generations {
		want = append(want, fmt.Sprintf(`rules_changed {"generation":%d}`, g))
	}
	if !slices.Equal(got, want) {
		e.t.Fatalf("events = %q, want %q", got, want)
	}
}

func (e *genEnv) wantGeneration(service string, want uint64) {
	e.t.Helper()
	if got := e.gen(service); got != want {
		e.t.Fatalf("generation(%s) = %d, want %d", service, got, want)
	}
}

// faultSpec seeds the fault spec a rule needs.
func (e *genEnv) faultSpec() string {
	e.t.Helper()
	spec := &model.FaultSpec{ID: "spec-" + e.tag, Name: "gen-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now()}
	if err := e.s.faults.CreateSpec(e.ctx, spec); err != nil {
		e.t.Fatalf("seed fault spec: %v", err)
	}
	e.t.Cleanup(func() { _ = e.s.faults.DeleteSpec(context.Background(), spec.ID) }) // after the rules (LIFO)
	return spec.ID
}

// rule persists an enabled rule through the repo.
func (e *genEnv) rule(name, service, specID string) {
	e.t.Helper()
	r := &model.Rule{ID: "rule-" + name + "-" + e.tag, Name: name, Service: service, Enabled: true, Mode: "background",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: specID}, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := e.s.rules.Create(e.ctx, r); err != nil {
		e.t.Fatalf("create rule %s: %v", name, err)
	}
	e.t.Cleanup(func() { _, _ = e.db.Exec(`DELETE FROM rules WHERE id = $1`, r.ID) })
}

// faultConfig persists a ready latency fault config on service.
func (e *genEnv) faultConfig(service string, durationMs int64) string {
	e.t.Helper()
	cfg := &model.FaultConfig{ID: id.New("fc"), Name: "gen-fc", Service: service, Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{"delay":"100ms"}`), DurationMs: durationMs, Status: model.FaultConfigReady}
	if err := e.s.faultConfigs.Create(e.ctx, cfg); err != nil {
		e.t.Fatalf("create fault config: %v", err)
	}
	e.t.Cleanup(func() { _, _ = e.db.Exec(`DELETE FROM fault_configs WHERE id = $1`, cfg.ID) })
	return cfg.ID
}

// TestRuleHandlers_BumpAndAnnounceTheServiceGeneration_DB pins that creating,
// moving and deleting a rule over HTTP bumps the generation of every service
// the write touches and announces each bump on SSE exactly once, now that the
// handlers no longer broadcast themselves: the repo notifies after the commit.
func TestRuleHandlers_BumpAndAnnounceTheServiceGeneration_DB(t *testing.T) {
	e := newGenEnv(t)
	svcA, svcB := "svca-"+e.tag, "svcb-"+e.tag
	specID := e.faultSpec()
	evA, evB := e.watch(svcA), e.watch(svcB)
	a0, b0 := e.gen(svcA), e.gen(svcB)

	ruleID := "rule-" + e.tag
	t.Cleanup(func() { _, _ = e.db.Exec(`DELETE FROM rules WHERE id = $1`, ruleID) })
	rule := func(service string) map[string]any {
		return map[string]any{"id": ruleID, "name": "gen-rule", "service": service, "enabled": true, "mode": "background",
			"action": map[string]any{"type": "fault_spec", "fault_spec_id": specID}}
	}

	if w := e.do(http.MethodPost, "/api/v1/rules", rule(svcA)); w.Code != http.StatusCreated {
		t.Fatalf("create: status = %d, body = %s", w.Code, w.Body.String())
	}
	e.wantGeneration(svcA, a0+1)
	e.wantEvents(evA, a0+1)

	if w := e.do(http.MethodPut, "/api/v1/rules/"+ruleID, rule(svcB)); w.Code != http.StatusOK {
		t.Fatalf("move: status = %d, body = %s", w.Code, w.Body.String())
	}
	e.wantGeneration(svcA, a0+2)
	e.wantGeneration(svcB, b0+1)
	e.wantEvents(evA, a0+2)
	e.wantEvents(evB, b0+1)

	if w := e.do(http.MethodDelete, "/api/v1/rules/"+ruleID, nil); w.Code != http.StatusNoContent {
		t.Fatalf("delete: status = %d, body = %s", w.Code, w.Body.String())
	}
	e.wantGeneration(svcB, b0+2)
	e.wantEvents(evB, b0+2)

	// Failed writes change nothing and announce nothing.
	if w := e.do(http.MethodDelete, "/api/v1/rules/"+ruleID, nil); w.Code != http.StatusNotFound {
		t.Fatalf("delete of a missing rule: status = %d, want 404", w.Code)
	}
	if w := e.do(http.MethodPut, "/api/v1/rules/"+ruleID, rule(svcA)); w.Code != http.StatusNotFound {
		t.Fatalf("update of a missing rule: status = %d, want 404", w.Code)
	}
	e.wantGeneration(svcA, a0+2)
	e.wantGeneration(svcB, b0+2)
	e.wantEvents(evA)
	e.wantEvents(evB)
}

// TestFaultConfigHandlers_BumpAndAnnounceTheServiceGeneration_DB pins that
// firing, cancelling and deleting a fault config each bump the config's service
// and announce it, and that a refused fire/cancel or a delete of nothing does
// neither.
func TestFaultConfigHandlers_BumpAndAnnounceTheServiceGeneration_DB(t *testing.T) {
	e := newGenEnv(t)
	svc, other := "svc-"+e.tag, "other-"+e.tag
	ev, evOther := e.watch(svc), e.watch(other)
	g0, o0 := e.gen(svc), e.gen(other)
	cfgID := e.faultConfig(svc, 0)

	e.wantGeneration(svc, g0) // a ready config is not applied to anything yet
	e.wantEvents(ev)

	if w := e.do(http.MethodPost, "/api/v1/faults/configs/"+cfgID+"/fire", nil); w.Code != http.StatusOK {
		t.Fatalf("fire: status = %d, body = %s", w.Code, w.Body.String())
	}
	e.wantGeneration(svc, g0+1)
	e.wantEvents(ev, g0+1)

	if w := e.do(http.MethodPost, "/api/v1/faults/configs/"+cfgID+"/fire", nil); w.Code != http.StatusConflict {
		t.Fatalf("second fire: status = %d, want 409", w.Code)
	}
	e.wantGeneration(svc, g0+1)
	e.wantEvents(ev)

	if w := e.do(http.MethodPost, "/api/v1/faults/configs/"+cfgID+"/cancel", nil); w.Code != http.StatusOK {
		t.Fatalf("cancel: status = %d, body = %s", w.Code, w.Body.String())
	}
	e.wantGeneration(svc, g0+2)
	e.wantEvents(ev, g0+2)

	if w := e.do(http.MethodPost, "/api/v1/faults/configs/"+cfgID+"/cancel", nil); w.Code != http.StatusConflict {
		t.Fatalf("second cancel: status = %d, want 409", w.Code)
	}
	e.wantGeneration(svc, g0+2)
	e.wantEvents(ev)

	// Deleting an active config stops it, so the delete bumps too; deleting what
	// is already gone bumps nothing.
	activeID := e.faultConfig(svc, 0)
	if w := e.do(http.MethodPost, "/api/v1/faults/configs/"+activeID+"/fire", nil); w.Code != http.StatusOK {
		t.Fatalf("fire second config: status = %d, body = %s", w.Code, w.Body.String())
	}
	e.wantGeneration(svc, g0+3)
	e.wantEvents(ev, g0+3)

	if w := e.do(http.MethodDelete, "/api/v1/faults/configs/"+activeID, nil); w.Code != http.StatusNoContent {
		t.Fatalf("delete: status = %d, body = %s", w.Code, w.Body.String())
	}
	e.wantGeneration(svc, g0+4)
	e.wantEvents(ev, g0+4)

	if w := e.do(http.MethodDelete, "/api/v1/faults/configs/"+activeID, nil); w.Code != http.StatusNotFound {
		t.Fatalf("second delete: status = %d, want 404", w.Code)
	}
	e.wantGeneration(svc, g0+4)
	e.wantEvents(ev)

	e.wantGeneration(other, o0) // another service is never bumped
	e.wantEvents(evOther)
}

// TestFaultConfigReaper_BumpsEachAffectedServiceOnce_DB pins that a sweep which
// completes several expired configs bumps each affected service once (not once
// per config), leaves other services alone, and that an empty sweep bumps nothing.
func TestFaultConfigReaper_BumpsEachAffectedServiceOnce_DB(t *testing.T) {
	e := newGenEnv(t)
	svc, other, idle := "svc-"+e.tag, "other-"+e.tag, "idle-"+e.tag
	ev, evOther, evIdle := e.watch(svc), e.watch(other), e.watch(idle)
	g0, o0, i0 := e.gen(svc), e.gen(other), e.gen(idle)

	expiring := []string{e.faultConfig(svc, 1), e.faultConfig(svc, 1), e.faultConfig(other, 1)}
	for _, cfgID := range expiring {
		if err := e.s.faultConfigs.MarkFired(e.ctx, cfgID); err != nil {
			t.Fatalf("fire %s: %v", cfgID, err)
		}
	}
	running := e.faultConfig(idle, 0) // no duration: never expires
	if err := e.s.faultConfigs.MarkFired(e.ctx, running); err != nil {
		t.Fatalf("fire %s: %v", running, err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		expired, err := e.s.faultConfigs.ListExpired(e.ctx)
		if err != nil {
			t.Fatalf("list expired: %v", err)
		}
		n := 0
		for _, f := range expired {
			if slices.Contains(expiring, f.ID) {
				n++
			}
		}
		if n == len(expiring) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d configs expired within 5s", n, len(expiring))
		}
	}

	e.s.reapExpiredFaultConfigs(e.ctx)
	e.wantGeneration(svc, g0+1)
	e.wantGeneration(other, o0+1)
	e.wantGeneration(idle, i0)
	e.wantEvents(ev, g0+1)
	e.wantEvents(evOther, o0+1)
	e.wantEvents(evIdle)
	for _, cfgID := range expiring {
		if cfg, err := e.s.faultConfigs.Get(e.ctx, cfgID); err != nil || cfg.Status != model.FaultConfigCompleted {
			t.Errorf("config %s after sweep = %+v, %v; want completed", cfgID, cfg, err)
		}
	}

	e.s.reapExpiredFaultConfigs(e.ctx) // nothing left to complete
	e.wantGeneration(svc, g0+1)
	e.wantEvents(ev)
}

// TestKillSwitch_BumpsEachDisabledRulesServiceGeneration_DB pins that the
// kill-switch's per-rule updates go through the rule repo's bump-and-notify:
// one generation per disabled rule, each announced, and none on a repeat.
func TestKillSwitch_BumpsEachDisabledRulesServiceGeneration_DB(t *testing.T) {
	e := newGenEnv(t)
	svc := "svc-" + e.tag
	specID := e.faultSpec()
	e.rule("hi", svc, specID)
	e.rule("lo", svc, specID)
	inst := &model.SDKInstance{ID: "inst-" + e.tag, Service: svc, Version: "1.0.0", Address: "10.0.0.1:9090", PollIntervalMs: 10000}
	if err := e.s.sdk.Register(e.ctx, inst); err != nil {
		t.Fatalf("register instance: %v", err)
	}
	t.Cleanup(func() { _ = e.s.sdk.Deregister(context.Background(), inst.ID) })
	ev := e.watch(svc)
	g0 := e.gen(svc)

	path := "/api/v1/sdk/instances/" + inst.ID + "/kill-switch"
	w := e.do(http.MethodPost, path, nil)
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), `"disabled_rule_ids":[]`) {
		t.Fatalf("kill-switch: status = %d, body = %s; want 200 with the disabled rules", w.Code, w.Body.String())
	}
	e.wantGeneration(svc, g0+2)
	e.wantEvents(ev, g0+1, g0+2)

	if w := e.do(http.MethodPost, path, nil); w.Code != http.StatusOK {
		t.Fatalf("second kill-switch: status = %d, body = %s", w.Code, w.Body.String())
	}
	e.wantGeneration(svc, g0+2)
	e.wantEvents(ev)
}
