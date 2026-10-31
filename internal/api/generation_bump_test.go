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

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

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

// bumpLog records, in call order, the bumps made through the spy below.
type bumpLog struct{ calls []string }

// spyGenerationStore wraps a desired.Store, recording Bump and refusing to run
// on a cancelled context, as a database driver would.
type spyGenerationStore struct {
	desired.Store
	log *bumpLog
}

func (s spyGenerationStore) Bump(ctx context.Context, tx *sql.Tx, service string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.log.calls = append(s.log.calls, "generation "+service)
	return s.Store.Bump(ctx, tx, service)
}

// TestBumpGeneration pins the out-of-band bump the fault-config handlers and the
// reaper use: it bumps the service's generation once and announces it, is
// best-effort (a failure is logged, never propagated or announced), and runs on
// a context a client hang-up cannot cancel, since the write that made it
// necessary has already committed.
func TestBumpGeneration(t *testing.T) {
	ctx := context.Background()
	setup := func(genStore desired.Store) (*Server, chan desired.Event) {
		broker := desired.NewBroker()
		ch := broker.Subscribe("cart")
		t.Cleanup(func() { broker.Unsubscribe("cart", ch) })
		return &Server{logger: discardLogger(), generations: desired.NewGenerations(genStore, broker)}, ch
	}
	wantAnnounced := func(t *testing.T, ch chan desired.Event) {
		t.Helper()
		if got, want := drainEvents(ch), []string{`rules_changed {"generation":2}`}; !slices.Equal(got, want) {
			t.Errorf("events = %q, want %q", got, want)
		}
	}

	t.Run("the generation is bumped once and announced", func(t *testing.T) {
		var log bumpLog
		s, ch := setup(spyGenerationStore{Store: desired.NewMemoryStore(), log: &log})
		s.bumpGeneration(ctx, "cart")
		if want := []string{"generation cart"}; !slices.Equal(log.calls, want) {
			t.Errorf("bumps = %q, want %q", log.calls, want)
		}
		if gen, _ := s.generations.Get(ctx, "cart"); gen != 2 {
			t.Errorf("generation = %d, want 2", gen)
		}
		wantAnnounced(t, ch)
	})

	t.Run("a failed bump is not propagated or announced", func(t *testing.T) {
		s, ch := setup(failingGenerationStore{})
		s.bumpGeneration(ctx, "cart") // must not panic
		if got := drainEvents(ch); len(got) != 0 {
			t.Errorf("failed bump was announced: %q", got)
		}
	})

	t.Run("a cancelled request does not skip the bump", func(t *testing.T) {
		var log bumpLog
		s, ch := setup(spyGenerationStore{Store: desired.NewMemoryStore(), log: &log})
		gone, cancel := context.WithCancel(ctx)
		cancel() // the client hung up after the write committed
		s.bumpGeneration(gone, "cart")
		if want := []string{"generation cart"}; !slices.Equal(log.calls, want) {
			t.Errorf("bumps = %q, want %q", log.calls, want)
		}
		wantAnnounced(t, ch)
	})
}

// TestNewServer_SSEStreamsGenerationBumps pins that the SSE endpoint
// subscribes to the broker the Generations bump on: a bump made through the
// object handed to NewServer reaches a connected subscriber.
func TestNewServer_SSEStreamsGenerationBumps(t *testing.T) {
	gens := desired.NewGenerations(desired.NewMemoryStore(), desired.NewBroker())
	s := NewServer(discardLogger(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, gens, nil, nil, nil, nil, nil)
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
// (MANTEION_TEST_DB=1): the Server is built by NewServer over the real repos,
// a real Generations and a real Materializer, and driven through the route table. Service names and
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
	rules, faults := store.NewRuleRepo(database, gens), store.NewFaultRepo(database)
	faultConfigs, experiments := store.NewFaultConfigRepo(database), store.NewExperimentRepo(database)
	mat := &desired.Materializer{Generations: gens, Rules: rules, FaultConfigs: faultConfigs, CacheBox: experiments, Specs: faults}
	s := NewServer(discardLogger(), database, rules, faults, faults, faultConfigs, store.NewSDKRepo(database), experiments,
		nil, nil, nil, nil, nil, gens, mat, nil, nil, nil, nil)
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

// awaitExpired blocks until every config in ids is past its duration, i.e. the
// reaper would complete it.
func (e *genEnv) awaitExpired(ids []string) {
	e.t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		expired, err := e.s.faultConfigs.ListExpired(e.ctx)
		if err != nil {
			e.t.Fatalf("list expired: %v", err)
		}
		n := 0
		for _, f := range expired {
			if slices.Contains(ids, f.ID) {
				n++
			}
		}
		if n == len(ids) {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("only %d of %d configs expired within 5s", n, len(ids))
		}
	}
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
	e.awaitExpired(expiring)

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

// TestFaultConfigChanges_ReachAnUpToDatePoll_DB pins that whatever changes a
// service's fault configs (fire, cancel, delete, expiry) reaches an SDK that is
// up to date: a poll naming the ETag it last applied gets a 200 at the bumped
// generation carrying the new active faults, and a poll naming the new ETag
// gets a 304. A bump that leaves the document unchanged (the sweep of a config
// that expired unseen) is a 304 at the new generation. Every config polled
// here is open-ended or already expired, so no document changes with the clock
// between two polls.
func TestFaultConfigChanges_ReachAnUpToDatePoll_DB(t *testing.T) {
	e := newGenEnv(t)
	svc := "svc-" + e.tag
	t.Cleanup(func() { _, _ = e.db.Exec(`DELETE FROM service_generations WHERE service = $1`, svc) })

	// poll polls as an SDK that last applied etag (none when empty) and expects
	// the status, the service's current generation and, on a 200, exactly
	// these active fault ids. Every change below bumps — the bump is what wakes
	// a held poll — so a 200 to an SDK holding a document must come at a
	// generation past the previous poll's. It returns the ETag of the answer.
	var lastGen uint64
	poll := func(step, etag string, wantCode int, wantFaults ...string) string {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/rules?service="+svc, nil)
		if etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
		w := httptest.NewRecorder()
		e.mux.ServeHTTP(w, req)
		gen := e.gen(svc)
		if got := w.Header().Get("X-Atropos-Generation"); w.Code != wantCode || got != fmt.Sprint(gen) {
			t.Fatalf("%s: poll = %d at generation %s, want %d at %d (body %s)", step, w.Code, got, wantCode, gen, w.Body.String())
		}
		if wantCode == http.StatusOK && etag != "" && gen <= lastGen {
			t.Fatalf("%s: 200 at generation %d, want a bump past %d", step, gen, lastGen)
		}
		lastGen = gen
		if wantCode == http.StatusOK {
			var sync atroposdk.RuleSync
			if err := json.Unmarshal(w.Body.Bytes(), &sync); err != nil {
				t.Fatalf("%s: decode RuleSync: %v", step, err)
			}
			faults := []string{}
			for _, f := range sync.ActiveFaults {
				faults = append(faults, f.ID)
			}
			if sync.Version != gen || !slices.Equal(faults, wantFaults) {
				t.Fatalf("%s: 200 at version %d with active faults %q, want %d with %q", step, sync.Version, faults, gen, wantFaults)
			}
		}
		return w.Header().Get("ETag")
	}
	act := func(method, path string, wantStatus int) {
		t.Helper()
		if w := e.do(method, path, nil); w.Code != wantStatus {
			t.Fatalf("%s %s: status = %d, want %d, body = %s", method, path, w.Code, wantStatus, w.Body.String())
		}
	}

	etag := poll("first poll", "", http.StatusOK)
	poll("up to date", etag, http.StatusNotModified)

	first := e.faultConfig(svc, 0)
	act(http.MethodPost, "/api/v1/faults/configs/"+first+"/fire", http.StatusOK)
	etag = poll("after fire", etag, http.StatusOK, first)
	poll("after fire, up to date", etag, http.StatusNotModified)

	act(http.MethodPost, "/api/v1/faults/configs/"+first+"/cancel", http.StatusOK)
	etag = poll("after cancel", etag, http.StatusOK)
	poll("after cancel, up to date", etag, http.StatusNotModified)

	second := e.faultConfig(svc, 0)
	act(http.MethodPost, "/api/v1/faults/configs/"+second+"/fire", http.StatusOK)
	etag = poll("after second fire", etag, http.StatusOK, second)
	act(http.MethodDelete, "/api/v1/faults/configs/"+second, http.StatusNoContent)
	etag = poll("after deleting the active config", etag, http.StatusOK)
	poll("after delete, up to date", etag, http.StatusNotModified)

	// Expiry: a config leaves the document by the clock, before the reaper
	// completes it. This one is fired without a bump and polled only once
	// expired, so it never shows: the document, and with it the ETag (the
	// content hash alone), stays the same through the sweep, whose bump moves
	// only the generation. The poll after the sweep is a 304 at the new
	// generation, which the SDK adopts without re-applying.
	expiring := e.faultConfig(svc, 1)
	if err := e.s.faultConfigs.MarkFired(e.ctx, expiring); err != nil {
		t.Fatalf("fire %s: %v", expiring, err)
	}
	e.awaitExpired([]string{expiring})
	poll("expired, before the sweep", etag, http.StatusNotModified)
	beforeSweep := e.gen(svc)
	e.s.reapExpiredFaultConfigs(e.ctx)
	e.wantGeneration(svc, beforeSweep+1)
	poll("after the sweep", etag, http.StatusNotModified) // the helper checks the 304 carries the new generation
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
