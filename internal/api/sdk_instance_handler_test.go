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
	"manteion-go/internal/desired"
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

// fakeServiceFaultConfigs is an in-memory fault_configs table as the kill
// switch sees it: the active configs, of which ListActiveForService serves a
// service's that are not cancelled yet, oldest first, as the poll does.
type fakeServiceFaultConfigs struct {
	active    []*model.FaultConfig
	cancelled []string         // every successful MarkCancelled, in call order
	cancelErr map[string]error // per-config MarkCancelled failure injection
	listErr   error

	gens        *desired.Generations // the server's (newSDKInstanceServer), read at each MarkCancelled
	genAtCancel []uint64             // the config's service generation at each MarkCancelled call
}

func (f *fakeServiceFaultConfigs) ListActiveForService(_ context.Context, service string) ([]*model.FaultConfig, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := []*model.FaultConfig{}
	for _, c := range f.active {
		if c.Service == service && !slices.Contains(f.cancelled, c.ID) {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (f *fakeServiceFaultConfigs) MarkCancelled(ctx context.Context, id string) error {
	for _, c := range f.active {
		if c.ID == id && f.gens != nil {
			if g, err := f.gens.Get(ctx, c.Service); err == nil {
				f.genAtCancel = append(f.genAtCancel, g)
			}
		}
	}
	if err := f.cancelErr[id]; err != nil {
		return err
	}
	for _, c := range f.active {
		if c.ID == id {
			f.cancelled = append(f.cancelled, id)
			return nil
		}
	}
	return store.ErrNotFound
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

func sdkTestFaultConfig(configID, service string) *model.FaultConfig {
	return &model.FaultConfig{
		ID: configID, Name: "fault " + configID, Service: service, Category: "inline", FaultType: "latency",
		Params: json.RawMessage(`{"delay":"100ms"}`), Status: model.FaultConfigActive, CreatedAt: sdkTestTime,
	}
}

// cartservice: two active fault configs; checkoutservice: one.
func sdkTestFaultConfigs() *fakeServiceFaultConfigs {
	return &fakeServiceFaultConfigs{active: []*model.FaultConfig{
		sdkTestFaultConfig("fc-cart-1", "cartservice"),
		sdkTestFaultConfig("fc-cart-2", "cartservice"),
		sdkTestFaultConfig("fc-checkout", "checkoutservice"),
	}}
}

func newSDKInstanceServer(insts *fakeSDKInstances, rules *fakeServiceRules, phases *fakePhaseHistory, configs *fakeServiceFaultConfigs) *Server {
	gens := desired.NewGenerations(desired.NewMemoryStore(), desired.NewBroker())
	configs.gens = gens
	return &Server{
		sdkInstances: insts, serviceRules: rules, phaseHistory: phases, serviceFaultConfigs: configs,
		generations: gens,
		logger:      discardLogger(),
	}
}

// generationOf reads a service's generation from the server's store.
func generationOf(t *testing.T, s *Server, service string) uint64 {
	t.Helper()
	g, err := s.generations.Get(context.Background(), service)
	if err != nil {
		t.Fatalf("read the generation of %s: %v", service, err)
	}
	return g
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
	DisabledRuleIDs         []string `json:"disabled_rule_ids"`
	CancelledFaultConfigIDs []string `json:"cancelled_fault_config_ids"`
	At                      string   `json:"at"`
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
		wantRaw    map[string]string // body key → its exact JSON value
		wantAbsent []string          // body keys that must not appear (beyond the always-absent two)
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
			name: "host, capabilities and observed state ride along; zero observed_etag/observed_at stay absent",
			id:   "cart-7f3a",
			insts: func() *fakeSDKInstances {
				f := sdkTestInstances()
				inst := f.byID["cart-7f3a"]
				inst.Host, inst.Capabilities = "sidecar", []string{"inline"}
				inst.ObservedGeneration, inst.ReplaySetState = 7, "committed"
				return f
			}(),
			rules:      &fakeServiceRules{},
			phases:     &fakePhaseHistory{},
			wantStatus: http.StatusOK,
			wantActive: []string{},
			wantRecent: []string{},
			wantRaw: map[string]string{
				"host": `"sidecar"`, "capabilities": `["inline"]`,
				"observed_generation": `7`, "replay_set_state": `"committed"`,
			},
			wantAbsent: []string{"observed_etag", "observed_at"},
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
			s := newSDKInstanceServer(tc.insts, tc.rules, tc.phases, &fakeServiceFaultConfigs{})
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
			for key, want := range tc.wantRaw {
				if got := string(raw[key]); got != want {
					t.Errorf("%s = %s, want %s (body %s)", key, got, want, w.Body.String())
				}
			}
			// Nothing tracks last_error or last_rule_version_acked under those
			// names (observed_errors and observed_generation carry the observed
			// state) — they must stay absent, not zero-valued, so the UI's
			// optional fields don't render garbage.
			for _, key := range append([]string{"last_error", "last_rule_version_acked"}, tc.wantAbsent...) {
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
		name          string
		id            string
		insts         *fakeSDKInstances
		rules         *fakeServiceRules
		configs       *fakeServiceFaultConfigs // nil: none active
		wantStatus    int
		wantError     string            // substring of the error envelope
		wantDisabled  []string          // disabled_rule_ids in the response
		wantOff       []string          // rules that must be persisted disabled afterwards
		wantOn        []string          // rules that must still be enabled afterwards
		wantCancelled []string          // fault configs persisted cancelled, in order; also cancelled_fault_config_ids on a 200
		wantGens      map[string]uint64 // service generations afterwards (1: never bumped)
	}{
		{
			name:       "unknown instance → 404, nothing touched",
			id:         "nope",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{rules: sdkTestRules()},
			configs:    sdkTestFaultConfigs(),
			wantStatus: http.StatusNotFound,
			wantError:  "instance not found",
			wantOn:     []string{"rule-cart-hi", "rule-cart-lo", "rule-checkout"},
			wantGens:   map[string]uint64{"cartservice": 1, "checkoutservice": 1},
		},
		{
			name:          "disables every enabled rule for the service, other services untouched",
			id:            "cart-7f3a",
			insts:         sdkTestInstances(),
			rules:         &fakeServiceRules{rules: sdkTestRules()},
			wantStatus:    http.StatusOK,
			wantDisabled:  []string{"rule-cart-hi", "rule-cart-lo"},
			wantOff:       []string{"rule-cart-hi", "rule-cart-lo", "rule-cart-off"},
			wantOn:        []string{"rule-checkout"},
			wantCancelled: []string{},
			wantGens:      map[string]uint64{"cartservice": 2, "checkoutservice": 1},
		},
		{
			name:          "cancels active fault configs for the service and bumps once",
			id:            "cart-7f3a",
			insts:         sdkTestInstances(),
			rules:         &fakeServiceRules{rules: sdkTestRules()},
			configs:       sdkTestFaultConfigs(),
			wantStatus:    http.StatusOK,
			wantDisabled:  []string{"rule-cart-hi", "rule-cart-lo"},
			wantOff:       []string{"rule-cart-hi", "rule-cart-lo"},
			wantOn:        []string{"rule-checkout"},
			wantCancelled: []string{"fc-cart-1", "fc-cart-2"},
			wantGens:      map[string]uint64{"cartservice": 2, "checkoutservice": 1},
		},
		{
			name:          "fault configs alone: cancelled, bumped once",
			id:            "cart-7f3a",
			insts:         sdkTestInstances(),
			rules:         &fakeServiceRules{rules: []*model.Rule{sdkTestRule("rule-cart-off", "cartservice", 30, false)}},
			configs:       sdkTestFaultConfigs(),
			wantStatus:    http.StatusOK,
			wantDisabled:  []string{},
			wantCancelled: []string{"fc-cart-1", "fc-cart-2"},
			wantGens:      map[string]uint64{"cartservice": 2, "checkoutservice": 1},
		},
		{
			name:          "no enabled rules, no active fault configs → 200 with empty lists, no bump",
			id:            "cart-7f3a",
			insts:         sdkTestInstances(),
			rules:         &fakeServiceRules{rules: []*model.Rule{sdkTestRule("rule-cart-off", "cartservice", 30, false)}},
			wantStatus:    http.StatusOK,
			wantDisabled:  []string{},
			wantOff:       []string{"rule-cart-off"},
			wantCancelled: []string{},
			wantGens:      map[string]uint64{"cartservice": 1},
		},
		{
			name:          "rule deleted between list and update is skipped",
			id:            "cart-7f3a",
			insts:         sdkTestInstances(),
			rules:         &fakeServiceRules{rules: sdkTestRules(), updateErr: map[string]error{"rule-cart-lo": store.ErrNotFound}},
			wantStatus:    http.StatusOK,
			wantDisabled:  []string{"rule-cart-hi"},
			wantOff:       []string{"rule-cart-hi"},
			wantCancelled: []string{},
			wantGens:      map[string]uint64{"cartservice": 2},
		},
		{
			name:  "fault config gone between list and cancel is skipped",
			id:    "cart-7f3a",
			insts: sdkTestInstances(),
			rules: &fakeServiceRules{},
			configs: func() *fakeServiceFaultConfigs {
				f := sdkTestFaultConfigs()
				f.cancelErr = map[string]error{"fc-cart-1": store.ErrNotFound}
				return f
			}(),
			wantStatus:    http.StatusOK,
			wantDisabled:  []string{},
			wantCancelled: []string{"fc-cart-2"},
			wantGens:      map[string]uint64{"cartservice": 2},
		},
		{
			name:          "update failure → 500, the remaining rules are still disabled and the fault configs cancelled",
			id:            "cart-7f3a",
			insts:         sdkTestInstances(),
			rules:         &fakeServiceRules{rules: sdkTestRules(), updateErr: map[string]error{"rule-cart-hi": errors.New("boom")}},
			configs:       sdkTestFaultConfigs(),
			wantStatus:    http.StatusInternalServerError,
			wantError:     "1 of 2 rules could not be disabled",
			wantOff:       []string{"rule-cart-lo"},
			wantOn:        []string{"rule-cart-hi", "rule-checkout"},
			wantCancelled: []string{"fc-cart-1", "fc-cart-2"},
			wantGens:      map[string]uint64{"cartservice": 2},
		},
		{
			name:  "cancel failure → 500, rules still disabled",
			id:    "cart-7f3a",
			insts: sdkTestInstances(),
			rules: &fakeServiceRules{rules: sdkTestRules()},
			configs: func() *fakeServiceFaultConfigs {
				f := sdkTestFaultConfigs()
				f.cancelErr = map[string]error{"fc-cart-1": errors.New("boom")}
				return f
			}(),
			wantStatus:    http.StatusInternalServerError,
			wantError:     "1 of 2 fault configs could not be cancelled",
			wantOff:       []string{"rule-cart-hi", "rule-cart-lo"},
			wantOn:        []string{"rule-checkout"},
			wantCancelled: []string{"fc-cart-2"},
			wantGens:      map[string]uint64{"cartservice": 2, "checkoutservice": 1},
		},
		{
			name:  "a rule update and a config cancel both fail → 500 naming both, the rest done and bumped once",
			id:    "cart-7f3a",
			insts: sdkTestInstances(),
			rules: &fakeServiceRules{rules: sdkTestRules(), updateErr: map[string]error{"rule-cart-hi": errors.New("boom")}},
			configs: func() *fakeServiceFaultConfigs {
				f := sdkTestFaultConfigs()
				f.cancelErr = map[string]error{"fc-cart-1": errors.New("boom")}
				return f
			}(),
			wantStatus:    http.StatusInternalServerError,
			wantError:     "kill-switch: 1 of 2 rules could not be disabled, 1 of 2 fault configs could not be cancelled; retry",
			wantOff:       []string{"rule-cart-lo"},
			wantOn:        []string{"rule-cart-hi", "rule-checkout"},
			wantCancelled: []string{"fc-cart-2"},
			wantGens:      map[string]uint64{"cartservice": 2},
		},
		{
			name:       "rule lookup failure → 500",
			id:         "cart-7f3a",
			insts:      sdkTestInstances(),
			rules:      &fakeServiceRules{listErr: errors.New("db down")},
			configs:    sdkTestFaultConfigs(),
			wantStatus: http.StatusInternalServerError,
			wantGens:   map[string]uint64{"cartservice": 1},
		},
		{
			name:  "fault config lookup failure → 500, rules still disabled",
			id:    "cart-7f3a",
			insts: sdkTestInstances(),
			rules: &fakeServiceRules{rules: sdkTestRules()},
			configs: func() *fakeServiceFaultConfigs {
				f := sdkTestFaultConfigs()
				f.listErr = errors.New("db down")
				return f
			}(),
			wantStatus: http.StatusInternalServerError,
			wantError:  "failed to list fault configs",
			wantOff:    []string{"rule-cart-hi", "rule-cart-lo"},
			wantGens:   map[string]uint64{"cartservice": 1}, // 500 before the final bump; the real repo's rule updates bump on their own
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configs := tc.configs
			if configs == nil {
				configs = &fakeServiceFaultConfigs{}
			}
			s := newSDKInstanceServer(tc.insts, tc.rules, &fakePhaseHistory{}, configs)
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
			if !slices.Equal(configs.cancelled, tc.wantCancelled) {
				t.Errorf("fault configs cancelled = %v, want %v", configs.cancelled, tc.wantCancelled)
			}
			// One bump per kill switch that changed anything, after its last
			// write: each rule update bumps on its own in the real repo, never in
			// the fake, so the fake's count is the handler's alone.
			for svc, want := range tc.wantGens {
				if got := generationOf(t, s, svc); got != want {
					t.Errorf("generation of %s = %d, want %d", svc, got, want)
				}
			}
			// The bump comes after the last cancel: every cancel ran at the
			// generation from before it.
			if final := generationOf(t, s, "cartservice"); final > 1 {
				for _, g := range configs.genAtCancel {
					if g >= final {
						t.Errorf("a cancel ran at generation %d, after the kill switch's bump to %d", g, final)
					}
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
			for _, key := range []string{"disabled_rule_ids", "cancelled_fault_config_ids"} {
				if msg, ok := raw[key]; !ok || string(msg) == "null" {
					t.Errorf("%s = %s, want a JSON array (body %s)", key, msg, w.Body.String())
				}
			}
			var got killSwitchWire
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !slices.Equal(got.DisabledRuleIDs, tc.wantDisabled) {
				t.Errorf("disabled_rule_ids = %v, want %v", got.DisabledRuleIDs, tc.wantDisabled)
			}
			if !slices.Equal(got.CancelledFaultConfigIDs, tc.wantCancelled) {
				t.Errorf("cancelled_fault_config_ids = %v, want %v", got.CancelledFaultConfigIDs, tc.wantCancelled)
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
// the service's rules are disabled and its fault configs cancelled, a repeat
// returns empty lists, persists nothing, does not bump the generation again,
// and the detail endpoint reports no active rules.
func TestHandleInstanceKillSwitch_Idempotent(t *testing.T) {
	rules := &fakeServiceRules{rules: sdkTestRules()}
	configs := sdkTestFaultConfigs()
	s := newSDKInstanceServer(sdkTestInstances(), rules, &fakePhaseHistory{}, configs)
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
	if want := []string{"fc-cart-1", "fc-cart-2"}; !slices.Equal(got.CancelledFaultConfigIDs, want) {
		t.Fatalf("first cancelled_fault_config_ids = %v, want %v", got.CancelledFaultConfigIDs, want)
	}
	if g := generationOf(t, s, "cartservice"); g != 2 {
		t.Fatalf("generation after the first call = %d, want 2 (one bump)", g)
	}

	second := serveSDKInstance(t, s, http.MethodPost, path)
	if second.Code != http.StatusOK {
		t.Fatalf("second: status = %d, body = %s", second.Code, second.Body.String())
	}
	for _, want := range []string{`"disabled_rule_ids":[]`, `"cancelled_fault_config_ids":[]`} {
		if !strings.Contains(second.Body.String(), want) {
			t.Errorf("second body = %s, want %s", second.Body.String(), want)
		}
	}
	if len(rules.updates) != 2 {
		t.Errorf("updates after second call = %d, want 2 (no re-writes)", len(rules.updates))
	}
	if len(configs.cancelled) != 2 {
		t.Errorf("cancels after second call = %v, want the first call's two (no re-writes)", configs.cancelled)
	}
	if g := generationOf(t, s, "cartservice"); g != 2 {
		t.Errorf("generation after the second call = %d, want 2: a kill switch that changed nothing must not bump", g)
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
// Against Postgres (MANTEION_TEST_DB=1): the real predicates + version and
// generation bumps + poll pickup, end to end.
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

	gens := desired.NewGenerations(store.NewGenerationRepo(database), desired.NewBroker())
	rules := store.NewRuleRepo(database, gens)
	faults := store.NewFaultRepo(database)
	faultConfigs := store.NewFaultConfigRepo(database)
	sdk := store.NewSDKRepo(database)
	exp := store.NewExperimentRepo(database)
	s := &Server{
		rules: rules, rulever: rules, faults: faults, sdk: sdk, experiments: exp,
		sdkInstances: sdk, registry: sdk, serviceRules: rules, phaseHistory: exp,
		serviceFaultConfigs: faultConfigs, generations: gens, broker: gens.Broker(),
		materializer: &desired.Materializer{Generations: gens, Rules: rules, FaultConfigs: faultConfigs, CacheBox: exp, Specs: faults},
		logger:       discardLogger(),
	}

	// Per-run-unique names so reruns and leftovers from sibling tests can't collide.
	tag := id.New("t")
	svc, other := "cart-"+tag, "checkout-"+tag
	t.Cleanup(func() { // runs last (LIFO): the rule deletes below bump these rows
		_, _ = database.Exec(`DELETE FROM service_generations WHERE service IN ($1, $2)`, svc, other)
	})

	spec := &model.FaultSpec{
		ID: "spec-" + tag, Name: "ks-spec", Category: "inline", FaultType: "latency",
		Host: "process", Params: json.RawMessage(`{"delay":"100ms"}`), CreatedAt: time.Now(),
	}
	if err := faults.CreateSpec(ctx, spec); err != nil {
		t.Fatalf("seed fault spec: %v", err)
	}
	t.Cleanup(func() { _ = faults.DeleteSpec(context.Background(), spec.ID) }) // runs after the rule deletes (LIFO)

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

	// A long-running fault fired on each service: the kill switch cancels svc's.
	fireFault := func(service string) string {
		t.Helper()
		fc := &model.FaultConfig{
			ID: id.New("fc"), Name: "ks-fault", Service: service, Category: "inline", FaultType: "latency",
			Params: json.RawMessage(`{"delay":"100ms"}`), Status: model.FaultConfigReady,
		}
		if err := faultConfigs.Create(ctx, fc); err != nil {
			t.Fatalf("create fault config: %v", err)
		}
		t.Cleanup(func() { _ = faultConfigs.Delete(context.Background(), fc.ID) })
		if err := faultConfigs.MarkFired(ctx, fc.ID); err != nil {
			t.Fatalf("fire fault config: %v", err)
		}
		return fc.ID
	}
	fault, otherFault := fireFault(svc), fireFault(other)
	faultStatus := func(configID string) model.FaultConfigStatus {
		t.Helper()
		fc, err := faultConfigs.Get(ctx, configID)
		if err != nil {
			t.Fatalf("get fault config %s: %v", configID, err)
		}
		return fc.Status
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
	// poll polls as the instance holding generation version and, when not
	// empty, the document ifNoneMatch names.
	poll := func(version uint64, ifNoneMatch string) (*httptest.ResponseRecorder, atroposdk.RuleSync) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/rules?service="+svc+"&instance_id="+inst.ID+"&version="+jsonNumber(version), nil)
		if ifNoneMatch != "" {
			req.Header.Set("If-None-Match", ifNoneMatch)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		var sync atroposdk.RuleSync
		if w.Code == http.StatusOK {
			if err := json.Unmarshal(w.Body.Bytes(), &sync); err != nil {
				t.Fatalf("decode RuleSync: %v", err)
			}
		}
		return w, sync
	}
	gen := func() uint64 {
		t.Helper()
		g, err := gens.Get(ctx, svc)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	ruleVersion := func() uint64 {
		t.Helper()
		v, err := rules.Version(ctx)
		if err != nil {
			t.Fatalf("version: %v", err)
		}
		return v
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
	// pending phase) — "active" is broader than "served", by design — and the
	// fired fault. A poll naming the document it was served gets a 304.
	var pollsFrom time.Time // last_poll_at is written with the database's clock
	if err := database.QueryRowContext(ctx, `SELECT now()`).Scan(&pollsFrom); err != nil {
		t.Fatalf("read the database clock: %v", err)
	}
	g0, v0 := gen(), ruleVersion()
	w0, sync0 := poll(0, "")
	if w0.Code != http.StatusOK || len(sync0.Rules) != 1 || sync0.Rules[0].Name != hi || sync0.Version != g0 {
		t.Fatalf("poll before kill-switch: code=%d rules=%+v version=%d, want 200 [%s] generation %d", w0.Code, sync0.Rules, sync0.Version, hi, g0)
	}
	if len(sync0.ActiveFaults) != 1 {
		t.Fatalf("poll before kill-switch: active_faults=%+v, want the fault fired on %s", sync0.ActiveFaults, svc)
	}
	etag0 := w0.Header().Get("ETag")
	if w, _ := poll(g0, etag0); w.Code != http.StatusNotModified {
		t.Fatalf("poll(same etag) = %d, want 304", w.Code)
	}

	// Kill-switch: one rule_version bump and one generation bump per rule,
	// exactly as PUT /rules/{id}, then the service's active fault configs
	// cancelled and one more generation bump after the last write.
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
	if want := []string{fault}; !slices.Equal(ks.CancelledFaultConfigIDs, want) {
		t.Errorf("cancelled_fault_config_ids = %v, want %v", ks.CancelledFaultConfigIDs, want)
	}
	if v := ruleVersion(); v != v0+2 {
		t.Errorf("rule_version %d → %d, want +2 (one bump per disabled rule)", v0, v)
	}
	if g := gen(); g != g0+3 {
		t.Errorf("generation %d → %d, want +3 (one bump per disabled rule, then the kill switch's own)", g0, g)
	}
	if st := faultStatus(fault); st != model.FaultConfigCancelled {
		t.Errorf("fault config %s is %s after the kill switch, want cancelled", fault, st)
	}
	if st := faultStatus(otherFault); st != model.FaultConfigActive {
		t.Errorf("other service's fault config %s is %s, want still active", otherFault, st)
	}
	for _, rid := range []string{hi, lo, off} {
		if r, err := rules.Get(ctx, rid); err != nil || r.Enabled {
			t.Errorf("rule %s: enabled=%v err=%v, want disabled", rid, r != nil && r.Enabled, err)
		}
	}
	if r, err := rules.Get(ctx, oth); err != nil || !r.Enabled {
		t.Errorf("other service's rule touched: enabled=%v err=%v", r != nil && r.Enabled, err)
	}

	// Poll pickup: the ETag the instance holds no longer matches, so it gets a
	// 200 with no rules and no faults (the reconciler clears on []) at the new
	// generation; the new ETag gets a 304. Each poll records the generation
	// the instance holds, and its time.
	w1, sync1 := poll(g0, etag0)
	if w1.Code != http.StatusOK || len(sync1.Rules) != 0 || len(sync1.ActiveFaults) != 0 || sync1.Version != g0+3 {
		t.Errorf("poll(old etag) = %d version=%d rules=%d active_faults=%d, want 200 at generation %d with no rules and no faults",
			w1.Code, sync1.Version, len(sync1.Rules), len(sync1.ActiveFaults), g0+3)
	}
	if w, _ := poll(g0+3, w1.Header().Get("ETag")); w.Code != http.StatusNotModified {
		t.Errorf("poll(new etag) = %d, want 304", w.Code)
	}
	if d := detail(); d.ObservedGeneration != g0+3 || d.LastPollAt.Before(pollsFrom) {
		t.Errorf("detail after polls: observed_generation=%d last_poll_at=%v, want %d and a poll time from %v on", d.ObservedGeneration, d.LastPollAt, g0+3, pollsFrom)
	}

	// Idempotent: nothing left to disable or cancel, no version or generation churn.
	w = do(http.MethodPost, "/api/v1/sdk/instances/"+inst.ID+"/kill-switch")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"disabled_rule_ids":[]`) ||
		!strings.Contains(w.Body.String(), `"cancelled_fault_config_ids":[]`) {
		t.Errorf("second kill-switch: status = %d, body = %s", w.Code, w.Body.String())
	}
	if v := ruleVersion(); v != v0+2 {
		t.Errorf("second kill-switch bumped rule_version %d → %d", v0+2, v)
	}
	if g := gen(); g != g0+3 {
		t.Errorf("second kill-switch bumped the generation %d → %d", g0+3, g)
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
