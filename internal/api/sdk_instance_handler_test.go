package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/db"
	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// ---------------------------------------------------------------------------
// Fakes — narrow, DB-free views of the three repos the instance handlers use.
// ---------------------------------------------------------------------------

type fakeSDKInstances struct {
	byID map[string]*model.SDKInstance
	err  error
}

func (f *fakeSDKInstances) Get(_ context.Context, id string) (*model.SDKInstance, error) {
	if f.err != nil {
		return nil, f.err
	}
	if inst, ok := f.byID[id]; ok {
		cp := *inst
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

// fakeServiceRules is an in-memory rules table. EnabledForService hands out
// copies so a handler-side mutation only lands through Update, as with the
// real repo.
type fakeServiceRules struct {
	rules     []*model.Rule
	listErr   error
	updateErr map[string]error // per-rule Update failure injection
	updates   []model.Rule     // every successful Update, in call order
}

func (f *fakeServiceRules) EnabledForService(_ context.Context, service string) ([]*model.Rule, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := []*model.Rule{}
	for _, r := range f.rules {
		if r.Service == service && r.Enabled {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (f *fakeServiceRules) Update(_ context.Context, rule *model.Rule) error {
	if err := f.updateErr[rule.ID]; err != nil {
		return err
	}
	for i, r := range f.rules {
		if r.ID == rule.ID {
			cp := *rule
			f.rules[i] = &cp
			f.updates = append(f.updates, cp)
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeServiceRules) get(id string) *model.Rule {
	for _, r := range f.rules {
		if r.ID == id {
			return r
		}
	}
	return nil
}

type fakePhaseHistory struct {
	byService map[string][]string
	err       error
	gotLimit  int
}

func (f *fakePhaseHistory) RecentPhaseIDsForService(_ context.Context, service string, limit int) ([]string, error) {
	f.gotLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	return f.byService[service], nil
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

var sdkTestTime = time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)

func sdkTestInstance() *model.SDKInstance {
	return &model.SDKInstance{
		ID: "cart-7f3a", Service: "cartservice", Version: "0.9.1", Address: "10.0.0.12:9090",
		PollIntervalMs: 10000, RegisteredAt: sdkTestTime, LastPollAt: sdkTestTime, Status: "alive",
	}
}

func sdkTestInstances() *fakeSDKInstances {
	inst := sdkTestInstance()
	return &fakeSDKInstances{byID: map[string]*model.SDKInstance{inst.ID: inst}}
}

func sdkTestRule(ruleID, service string, priority int, enabled bool) *model.Rule {
	return &model.Rule{
		ID: ruleID, Name: "rule " + ruleID, Service: service, Enabled: enabled, Priority: priority,
		Mode: "background", StartPolicy: "always_start",
		Action:    model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-latency"},
		Match:     model.MatchCriteria{InjectionPoint: "ingress", Labels: map[string]string{"atropos.workflow": "browse"}},
		CreatedAt: sdkTestTime, UpdatedAt: sdkTestTime,
	}
}

// cartservice: two enabled rules + one disabled; checkoutservice: one enabled.
func sdkTestRules() []*model.Rule {
	return []*model.Rule{
		sdkTestRule("rule-cart-hi", "cartservice", 20, true),
		sdkTestRule("rule-cart-lo", "cartservice", 10, true),
		sdkTestRule("rule-cart-off", "cartservice", 30, false),
		sdkTestRule("rule-checkout", "checkoutservice", 50, true),
	}
}

func newSDKInstanceServer(insts *fakeSDKInstances, rules *fakeServiceRules, phases *fakePhaseHistory) *Server {
	return &Server{sdkInstances: insts, serviceRules: rules, phaseHistory: phases, logger: discardLogger()}
}

// serveSDKInstance drives a request through the real route table so the
// method-path patterns and {id} extraction are exercised, not just the func.
func serveSDKInstance(t *testing.T, s *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	s.routes(mux)
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// killSwitchWire mirrors the kill-switch envelope with `at` kept raw so the
// test can pin its wire format.
type killSwitchWire struct {
	DisabledRuleIDs []string `json:"disabled_rule_ids"`
	At              string   `json:"at"`
}

// ---------------------------------------------------------------------------
// GET /api/v1/sdk/instances/{id}
// ---------------------------------------------------------------------------

func TestHandleGetInstance(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		insts      *fakeSDKInstances
		rules      *fakeServiceRules
		phases     *fakePhaseHistory
		wantStatus int
		wantError  string
		wantActive []string
		wantRecent []string
	}{
		{
			name:       "unknown instance → 404",
			id:         "nope",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{rules: sdkTestRules()},
			phases:     &fakePhaseHistory{},
			wantStatus: http.StatusNotFound,
			wantError:  "instance not found",
		},
		{
			name:       "list row + enabled rules for the service + recent phases",
			id:         "cart-7f3a",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{rules: sdkTestRules()},
			phases:     &fakePhaseHistory{byService: map[string][]string{"cartservice": {"phase-3", "phase-2", "phase-1"}}},
			wantStatus: http.StatusOK,
			wantActive: []string{"rule-cart-hi", "rule-cart-lo"},
			wantRecent: []string{"phase-3", "phase-2", "phase-1"},
		},
		{
			name:       "no rules, no phases → empty arrays, never null",
			id:         "cart-7f3a",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{},
			phases:     &fakePhaseHistory{},
			wantStatus: http.StatusOK,
			wantActive: []string{},
			wantRecent: []string{},
		},
		{
			name:       "instance lookup failure → 500",
			id:         "cart-7f3a",
			insts:      &fakeSDKInstances{err: errors.New("db down")},
			rules:      &fakeServiceRules{},
			phases:     &fakePhaseHistory{},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "rule lookup failure → 500",
			id:         "cart-7f3a",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{listErr: errors.New("db down")},
			phases:     &fakePhaseHistory{},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "phase lookup failure → 500",
			id:         "cart-7f3a",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{},
			phases:     &fakePhaseHistory{err: errors.New("db down")},
			wantStatus: http.StatusInternalServerError,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newSDKInstanceServer(tc.insts, tc.rules, tc.phases)
			w := serveSDKInstance(t, s, http.MethodGet, "/api/v1/sdk/instances/"+tc.id)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantStatus != http.StatusOK {
				var er ErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &er); err != nil {
					t.Fatalf("decode error envelope: %v (body %s)", err, w.Body.String())
				}
				if tc.wantError != "" && er.Error != tc.wantError {
					t.Errorf("error = %q, want %q", er.Error, tc.wantError)
				}
				return
			}

			// The list row rides along unchanged (same shape as GET /sdk/instances items).
			var inst model.SDKInstance
			if err := json.Unmarshal(w.Body.Bytes(), &inst); err != nil {
				t.Fatalf("decode instance: %v", err)
			}
			want := sdkTestInstance()
			if inst.ID != want.ID || inst.Service != want.Service || inst.Version != want.Version ||
				inst.Address != want.Address || inst.Status != want.Status || !inst.RegisteredAt.Equal(want.RegisteredAt) {
				t.Errorf("instance row = %+v, want %+v", inst, *want)
			}

			var raw map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
				t.Fatalf("decode raw: %v", err)
			}
			for key, want := range map[string][]string{"active_rule_ids": tc.wantActive, "recent_run_ids": tc.wantRecent} {
				msg, ok := raw[key]
				if !ok {
					t.Fatalf("%s missing from body %s", key, w.Body.String())
				}
				if string(msg) == "null" {
					t.Errorf("%s = null, want a JSON array", key)
				}
				var got []string
				if err := json.Unmarshal(msg, &got); err != nil {
					t.Fatalf("decode %s: %v", key, err)
				}
				if !slices.Equal(got, want) {
					t.Errorf("%s = %v, want %v", key, got, want)
				}
			}
			// Nothing tracks these yet (no sdk_instances column; the poll's
			// version param is not persisted) — they must stay absent, not
			// zero-valued, so the UI's optional fields don't render garbage.
			for _, key := range []string{"last_error", "last_rule_version_acked"} {
				if _, ok := raw[key]; ok {
					t.Errorf("%s present in body, want absent: %s", key, w.Body.String())
				}
			}
			if tc.phases.gotLimit != 5 {
				t.Errorf("recent phase limit = %d, want 5", tc.phases.gotLimit)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// POST /api/v1/sdk/instances/{id}/kill-switch
// ---------------------------------------------------------------------------

func TestHandleInstanceKillSwitch(t *testing.T) {
	tests := []struct {
		name         string
		id           string
		insts        *fakeSDKInstances
		rules        *fakeServiceRules
		wantStatus   int
		wantError    string   // substring of the error envelope
		wantDisabled []string // disabled_rule_ids in the response
		wantOff      []string // rules that must be persisted disabled afterwards
		wantOn       []string // rules that must still be enabled afterwards
	}{
		{
			name:       "unknown instance → 404, nothing touched",
			id:         "nope",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{rules: sdkTestRules()},
			wantStatus: http.StatusNotFound,
			wantError:  "instance not found",
			wantOn:     []string{"rule-cart-hi", "rule-cart-lo", "rule-checkout"},
		},
		{
			name:         "disables every enabled rule for the service, other services untouched",
			id:           "cart-7f3a",
			insts:        sdkTestInstances(),
			rules:        &fakeServiceRules{rules: sdkTestRules()},
			wantStatus:   http.StatusOK,
			wantDisabled: []string{"rule-cart-hi", "rule-cart-lo"},
			wantOff:      []string{"rule-cart-hi", "rule-cart-lo", "rule-cart-off"},
			wantOn:       []string{"rule-checkout"},
		},
		{
			name:         "no enabled rules → 200 with an empty list",
			id:           "cart-7f3a",
			insts:        sdkTestInstances(),
			rules:        &fakeServiceRules{rules: []*model.Rule{sdkTestRule("rule-cart-off", "cartservice", 30, false)}},
			wantStatus:   http.StatusOK,
			wantDisabled: []string{},
			wantOff:      []string{"rule-cart-off"},
		},
		{
			name:         "rule deleted between list and update is skipped",
			id:           "cart-7f3a",
			insts:        sdkTestInstances(),
			rules:        &fakeServiceRules{rules: sdkTestRules(), updateErr: map[string]error{"rule-cart-lo": store.ErrNotFound}},
			wantStatus:   http.StatusOK,
			wantDisabled: []string{"rule-cart-hi"},
			wantOff:      []string{"rule-cart-hi"},
		},
		{
			name:       "update failure → 500, the remaining rules are still disabled",
			id:         "cart-7f3a",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{rules: sdkTestRules(), updateErr: map[string]error{"rule-cart-hi": errors.New("boom")}},
			wantStatus: http.StatusInternalServerError,
			wantError:  "1 of 2",
			wantOff:    []string{"rule-cart-lo"},
			wantOn:     []string{"rule-cart-hi", "rule-checkout"},
		},
		{
			name:       "rule lookup failure → 500",
			id:         "cart-7f3a",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{listErr: errors.New("db down")},
			wantStatus: http.StatusInternalServerError,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newSDKInstanceServer(tc.insts, tc.rules, &fakePhaseHistory{})
			before := time.Now().UTC()
			w := serveSDKInstance(t, s, http.MethodPost, "/api/v1/sdk/instances/"+tc.id+"/kill-switch")
			after := time.Now().UTC()
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}

			for _, rid := range tc.wantOff {
				if r := tc.rules.get(rid); r == nil || r.Enabled {
					t.Errorf("rule %s enabled afterwards, want disabled", rid)
				}
			}
			for _, rid := range tc.wantOn {
				if r := tc.rules.get(rid); r == nil || !r.Enabled {
					t.Errorf("rule %s disabled afterwards, want enabled", rid)
				}
			}
			// Every write is a FULL update — the same shape PUT /rules/{id}
			// persists — with only the enabled flag flipped.
			for _, upd := range tc.rules.updates {
				orig := sdkTestRule(upd.ID, upd.Service, upd.Priority, true)
				if upd.Enabled {
					t.Errorf("update for %s left enabled=true", upd.ID)
				}
				if upd.Name != orig.Name || upd.Mode != orig.Mode || upd.StartPolicy != orig.StartPolicy ||
					upd.Action != orig.Action || upd.Match.InjectionPoint != orig.Match.InjectionPoint ||
					upd.Match.Labels["atropos.workflow"] != "browse" || !upd.CreatedAt.Equal(orig.CreatedAt) {
					t.Errorf("update for %s is not a full rule: %+v", upd.ID, upd)
				}
			}

			if tc.wantStatus != http.StatusOK {
				var er ErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &er); err != nil {
					t.Fatalf("decode error envelope: %v (body %s)", err, w.Body.String())
				}
				if tc.wantError != "" && !strings.Contains(er.Error, tc.wantError) {
					t.Errorf("error = %q, want it to contain %q", er.Error, tc.wantError)
				}
				return
			}

			var raw map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
				t.Fatalf("decode raw: %v", err)
			}
			if string(raw["disabled_rule_ids"]) == "null" {
				t.Errorf("disabled_rule_ids = null, want a JSON array")
			}
			var got killSwitchWire
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !slices.Equal(got.DisabledRuleIDs, tc.wantDisabled) {
				t.Errorf("disabled_rule_ids = %v, want %v", got.DisabledRuleIDs, tc.wantDisabled)
			}
			at, err := time.Parse(time.RFC3339Nano, got.At)
			if err != nil {
				t.Fatalf("at = %q is not RFC 3339: %v", got.At, err)
			}
			if !strings.HasSuffix(got.At, "Z") {
				t.Errorf("at = %q, want UTC (Z suffix)", got.At)
			}
			if at.Before(before) || at.After(after) {
				t.Errorf("at = %s outside [%s, %s]", at, before, after)
			}
		})
	}
}

// TestHandleInstanceKillSwitch_Idempotent pins the second-call contract: once
// the service's rules are disabled a repeat returns an empty list, persists
// nothing, and the detail endpoint reports no active rules.
func TestHandleInstanceKillSwitch_Idempotent(t *testing.T) {
	rules := &fakeServiceRules{rules: sdkTestRules()}
	s := newSDKInstanceServer(sdkTestInstances(), rules, &fakePhaseHistory{})
	path := "/api/v1/sdk/instances/cart-7f3a/kill-switch"

	first := serveSDKInstance(t, s, http.MethodPost, path)
	if first.Code != http.StatusOK {
		t.Fatalf("first: status = %d, body = %s", first.Code, first.Body.String())
	}
	var got killSwitchWire
	if err := json.Unmarshal(first.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if want := []string{"rule-cart-hi", "rule-cart-lo"}; !slices.Equal(got.DisabledRuleIDs, want) {
		t.Fatalf("first disabled_rule_ids = %v, want %v", got.DisabledRuleIDs, want)
	}

	second := serveSDKInstance(t, s, http.MethodPost, path)
	if second.Code != http.StatusOK {
		t.Fatalf("second: status = %d, body = %s", second.Code, second.Body.String())
	}
	if !strings.Contains(second.Body.String(), `"disabled_rule_ids":[]`) {
		t.Errorf("second body = %s, want an empty disabled_rule_ids array", second.Body.String())
	}
	if len(rules.updates) != 2 {
		t.Errorf("updates after second call = %d, want 2 (no re-writes)", len(rules.updates))
	}

	detail := serveSDKInstance(t, s, http.MethodGet, "/api/v1/sdk/instances/cart-7f3a")
	if detail.Code != http.StatusOK {
		t.Fatalf("detail: status = %d, body = %s", detail.Code, detail.Body.String())
	}
	if !strings.Contains(detail.Body.String(), `"active_rule_ids":[]`) {
		t.Errorf("detail body = %s, want no active rules", detail.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Against Postgres (MANTEION_TEST_DB=1): the real predicates + version bump
// + poll pickup, end to end.
// ---------------------------------------------------------------------------

func TestSDKInstanceHandlers_DB(t *testing.T) {
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
	faults := store.NewFaultRepo(database)
	sdk := store.NewSDKRepo(database)
	exp := store.NewExperimentRepo(database)
	s := &Server{
		rules: rules, rulever: rules, faults: faults, sdk: sdk, experiments: exp,
		sdkInstances: sdk, serviceRules: rules, phaseHistory: exp,
		logger: discardLogger(),
	}

	// Per-run-unique names so reruns and leftovers from sibling tests can't collide.
	tag := id.New("t")
	svc, other := "cart-"+tag, "checkout-"+tag

	spec := &model.FaultSpec{
		ID: "spec-" + tag, Name: "ks-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now(),
	}
	if err := faults.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed fault spec: %v", err)
	}
	t.Cleanup(func() { _ = faults.DeleteSpec(context.Background(), spec.ID) }) // runs last (LIFO)

	mkRule := func(ruleID, service string, priority int, enabled bool) string {
		t.Helper()
		r := &model.Rule{
			ID: ruleID, Name: ruleID, Service: service, Enabled: enabled, Priority: priority,
			Mode: "background", StartPolicy: "always_start",
			Action:    model.RuleAction{Type: "fault_spec", FaultSpecID: spec.ID},
			Match:     model.MatchCriteria{InjectionPoint: "ingress"},
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
		if err := rules.Create(ctx, r); err != nil {
			t.Fatalf("create rule %s: %v", ruleID, err)
		}
		t.Cleanup(func() { _ = rules.Delete(context.Background(), ruleID) })
		return ruleID
	}
	hi := mkRule("rule-hi-"+tag, svc, 20, true)
	lo := mkRule("rule-lo-"+tag, svc, 10, true)
	off := mkRule("rule-off-"+tag, svc, 30, false)
	oth := mkRule("rule-oth-"+tag, other, 50, true)

	// Experiment: phase A froze svc and already ran; phase B attaches `lo`
	// (pending); phase C froze only the other service.
	e := &model.Experiment{ID: id.New("exp"), Name: "ks-" + tag, Status: "planned", CreatedAt: time.Now()}
	if err := exp.Create(ctx, e); err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	// Registered after the rules → runs before them, releasing phase_rules' RESTRICT.
	t.Cleanup(func() { _, _ = database.Exec("DELETE FROM experiments WHERE id = $1", e.ID) })
	freeze := func(service string) []model.CacheBoxConfig {
		return []model.CacheBoxConfig{{Service: service, Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}}
	}
	phaseA := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "freeze-cart", Position: 0, Status: "pending", FrozenServices: freeze(svc)}
	phaseB := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "rule-cart", Position: 1, Status: "pending"}
	phaseC := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "freeze-checkout", Position: 2, Status: "pending", FrozenServices: freeze(other)}
	for _, p := range []*model.ExperimentPhase{phaseA, phaseB, phaseC} {
		if err := exp.CreatePhase(ctx, p); err != nil {
			t.Fatalf("create phase %s: %v", p.Name, err)
		}
	}
	if err := exp.AttachPhaseRules(ctx, phaseB.ID, []string{lo}); err != nil {
		t.Fatalf("attach phase rules: %v", err)
	}
	for _, status := range []string{"running", "completed"} {
		if err := exp.UpdatePhaseStatus(ctx, phaseA.ID, status); err != nil {
			t.Fatalf("phase A → %s: %v", status, err)
		}
	}

	inst := &model.SDKInstance{ID: "inst-" + tag, Service: svc, Version: "1.0.0", Address: "10.0.0.1:9090", PollIntervalMs: 10000}
	if err := sdk.Register(ctx, inst); err != nil {
		t.Fatalf("register instance: %v", err)
	}
	t.Cleanup(func() { _ = sdk.Deregister(context.Background(), inst.ID) })

	mux := http.NewServeMux()
	s.routes(mux)
	do := func(method, path string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w
	}
	detail := func() SDKInstanceDetail {
		t.Helper()
		w := do(http.MethodGet, "/api/v1/sdk/instances/"+inst.ID)
		if w.Code != http.StatusOK {
			t.Fatalf("detail: status = %d, body = %s", w.Code, w.Body.String())
		}
		var d SDKInstanceDetail
		if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
			t.Fatalf("decode detail: %v", err)
		}
		return d
	}
	poll := func(version uint64) (int, atroposdk.RuleSync) {
		t.Helper()
		w := do(http.MethodGet, "/api/v1/sdk/rules?service="+svc+"&version="+jsonNumber(version))
		var sync atroposdk.RuleSync
		if w.Code == http.StatusOK {
			if err := json.Unmarshal(w.Body.Bytes(), &sync); err != nil {
				t.Fatalf("decode RuleSync: %v", err)
			}
		}
		return w.Code, sync
	}

	// Detail: every enabled rule for the service (priority order), regardless
	// of phase attachment; phases newest-started first, never-started after.
	d := detail()
	if d.Service != svc || d.Status != "alive" {
		t.Errorf("detail row = %+v, want service %s alive", d.SDKInstance, svc)
	}
	if want := []string{hi, lo}; !slices.Equal(d.ActiveRuleIDs, want) {
		t.Errorf("active_rule_ids = %v, want %v", d.ActiveRuleIDs, want)
	}
	if want := []string{phaseA.ID, phaseB.ID}; !slices.Equal(d.RecentRunIDs, want) {
		t.Errorf("recent_run_ids = %v, want %v (phase C froze another service)", d.RecentRunIDs, want)
	}

	// Before: the poll serves only the unattached rule (lo is gated behind its
	// pending phase) — "active" is broader than "served", by design.
	before, err := rules.Version(ctx)
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if code, sync := poll(0); code != http.StatusOK || len(sync.Rules) != 1 || sync.Rules[0].Name != hi {
		t.Fatalf("poll before kill-switch: code=%d rules=%+v, want 200 with [%s]", code, sync.Rules, hi)
	}

	// Kill-switch: one version bump per rule, exactly as PUT /rules/{id}.
	w := do(http.MethodPost, "/api/v1/sdk/instances/"+inst.ID+"/kill-switch")
	if w.Code != http.StatusOK {
		t.Fatalf("kill-switch: status = %d, body = %s", w.Code, w.Body.String())
	}
	var ks killSwitchWire
	if err := json.Unmarshal(w.Body.Bytes(), &ks); err != nil {
		t.Fatalf("decode kill-switch: %v", err)
	}
	if want := []string{hi, lo}; !slices.Equal(ks.DisabledRuleIDs, want) {
		t.Errorf("disabled_rule_ids = %v, want %v", ks.DisabledRuleIDs, want)
	}
	after, err := rules.Version(ctx)
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if after != before+2 {
		t.Errorf("rule_version %d → %d, want +2 (one bump per disabled rule)", before, after)
	}
	for _, rid := range []string{hi, lo, off} {
		if r, err := rules.Get(ctx, rid); err != nil || r.Enabled {
			t.Errorf("rule %s: enabled=%v err=%v, want disabled", rid, r != nil && r.Enabled, err)
		}
	}
	if r, err := rules.Get(ctx, oth); err != nil || !r.Enabled {
		t.Errorf("other service's rule touched: enabled=%v err=%v", r != nil && r.Enabled, err)
	}

	// Poll pickup: an SDK holding the old version gets a 200 with an empty
	// set (the reconciler clears on []); one at the new version gets 304.
	if code, sync := poll(before); code != http.StatusOK || len(sync.Rules) != 0 || sync.Version != after {
		t.Errorf("poll(old) = %d version=%d rules=%d, want 200 version=%d rules=0", code, sync.Version, len(sync.Rules), after)
	}
	if code, _ := poll(after); code != http.StatusNotModified {
		t.Errorf("poll(new) = %d, want 304", code)
	}

	// Idempotent: nothing left to disable, no version churn.
	w = do(http.MethodPost, "/api/v1/sdk/instances/"+inst.ID+"/kill-switch")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"disabled_rule_ids":[]`) {
		t.Errorf("second kill-switch: status = %d, body = %s", w.Code, w.Body.String())
	}
	if v, _ := rules.Version(ctx); v != after {
		t.Errorf("second kill-switch bumped rule_version %d → %d", after, v)
	}
	if d := detail(); len(d.ActiveRuleIDs) != 0 {
		t.Errorf("active_rule_ids after kill-switch = %v, want none", d.ActiveRuleIDs)
	}

	for _, path := range []string{"/api/v1/sdk/instances/inst-nope", "/api/v1/sdk/instances/inst-nope/kill-switch"} {
		method := http.MethodGet
		if strings.HasSuffix(path, "/kill-switch") {
			method = http.MethodPost
		}
		if w := do(method, path); w.Code != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404", method, path, w.Code)
		}
	}
}

func jsonNumber(v uint64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
