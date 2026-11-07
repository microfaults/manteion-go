package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/promql"
	"manteion-go/internal/store"
)

// fakePromServer returns a Prometheus-shaped instant-query response with a fixed value.
func fakePromServer(t *testing.T, value float64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{
						"metric": map[string]any{},
						"value":  []any{1234567890.0, fmt.Sprintf("%g", value)},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fakePromServerUnavailable returns a server that always errors.
func fakePromServerUnavailable(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fakeRuleStore is an in-memory ruleStore: rules by id, and every Update it
// was asked to make (the rule CRUD path that bumps the service's generation
// in the same transaction).
type fakeRuleStore struct {
	mu      sync.Mutex
	rules   map[string]*model.Rule
	updates []model.Rule
}

func newFakeRuleStore(rules ...*model.Rule) *fakeRuleStore {
	f := &fakeRuleStore{rules: make(map[string]*model.Rule, len(rules))}
	for _, r := range rules {
		f.rules[r.ID] = r
	}
	return f
}

func (f *fakeRuleStore) Get(_ context.Context, id string) (*model.Rule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rules[id]
	if !ok {
		return nil, fmt.Errorf("rule %q: %w", id, store.ErrNotFound)
	}
	cp := *r
	return &cp, nil
}

func (f *fakeRuleStore) Update(_ context.Context, rule *model.Rule) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rules[rule.ID]; !ok {
		return store.ErrNotFound
	}
	cp := *rule
	f.rules[rule.ID] = &cp
	f.updates = append(f.updates, cp)
	return nil
}

func (f *fakeRuleStore) EnabledForService(_ context.Context, service string) ([]*model.Rule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.Rule
	for _, r := range f.rules {
		if r.Service == service && r.Enabled {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (f *fakeRuleStore) updateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.updates)
}

func (f *fakeRuleStore) enabled(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rules[id].Enabled
}

// mkRule is a disabled-or-enabled rule of service.
func mkRule(id, service string, enabled bool) *model.Rule {
	return &model.Rule{ID: id, Name: id, Service: service, Enabled: enabled,
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-1"}}
}

// fakePolicyRepo is a minimal in-memory PolicyRepo substitute.
type fakePolicyRepo struct {
	mu    sync.Mutex
	rules []*model.PolicyRule
}

func (r *fakePolicyRepo) ListEnabled(_ context.Context) ([]*model.PolicyRule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*model.PolicyRule
	for _, rule := range r.rules {
		if rule.Enabled {
			out = append(out, rule)
		}
	}
	return out, nil
}

// makeEngine assembles an Engine over the given policies, Prometheus server
// and rule store.
func makeEngine(t *testing.T, prom *promql.Client, rules ruleStore, policies []*model.PolicyRule) *Engine {
	t.Helper()
	return New(&fakePolicyRepo{rules: policies}, rules, prom, noopLogger())
}

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// --- Tests ---

func clearRulesPolicy(id, service string, op string, threshold float64, cooldown time.Duration) *model.PolicyRule {
	return &model.PolicyRule{
		ID: id, Enabled: true,
		Condition: model.PolicyCondition{Metric: "up", Operator: op, Threshold: threshold},
		Action:    model.PolicyAction{ActionType: "clear_rules", PushRules: &model.PushRulesAction{Service: service}},
		Cooldown:  cooldown,
	}
}

// A push_rules policy whose condition fires enables each listed rule through
// rule CRUD (Update, which bumps the service's generation in-tx): one update,
// the rule now enabled.
func TestConditionFires_AboveThreshold(t *testing.T) {
	prom := promql.NewClient(fakePromServer(t, 600.0).URL) // metric = 600, threshold = 500 → gt fires
	rules := newFakeRuleStore(mkRule("r-1", "checkout", false))
	policy := &model.PolicyRule{
		ID: "p-1", Enabled: true,
		Condition: model.PolicyCondition{Metric: "up", Operator: "gt", Threshold: 500},
		Action: model.PolicyAction{ActionType: "push_rules",
			PushRules: &model.PushRulesAction{Service: "checkout", RuleIDs: []string{"r-1"}}},
	}

	makeEngine(t, prom, rules, []*model.PolicyRule{policy}).tick(context.Background())

	if n := rules.updateCount(); n != 1 {
		t.Fatalf("expected 1 rule update, got %d", n)
	}
	if !rules.enabled("r-1") {
		t.Fatal("push_rules left r-1 disabled")
	}
}

func TestConditionDoesNotFire_BelowThreshold(t *testing.T) {
	prom := promql.NewClient(fakePromServer(t, 100.0).URL) // metric = 100, threshold = 500 → gt does not fire
	rules := newFakeRuleStore(mkRule("r-1", "checkout", true))

	makeEngine(t, prom, rules, []*model.PolicyRule{clearRulesPolicy("p-2", "checkout", "gt", 500, 0)}).tick(context.Background())

	if n := rules.updateCount(); n != 0 {
		t.Fatalf("expected 0 rule updates, got %d", n)
	}
}

func TestCooldownPreventDoubleFire(t *testing.T) {
	prom := promql.NewClient(fakePromServer(t, 1.0).URL) // always fires (eq 1)
	rules := newFakeRuleStore(mkRule("r-1", "checkout", false))
	policy := &model.PolicyRule{
		ID: "p-3", Enabled: true,
		Condition: model.PolicyCondition{Metric: "up", Operator: "eq", Threshold: 1},
		Action: model.PolicyAction{ActionType: "push_rules",
			PushRules: &model.PushRulesAction{Service: "checkout", RuleIDs: []string{"r-1"}}},
		Cooldown: 10 * time.Minute, // long cooldown
	}

	eng := makeEngine(t, prom, rules, []*model.PolicyRule{policy})
	eng.tick(context.Background()) // fires
	eng.tick(context.Background()) // blocked by cooldown
	eng.tick(context.Background()) // blocked by cooldown

	if n := rules.updateCount(); n != 1 {
		t.Fatalf("expected exactly 1 rule update due to cooldown, got %d", n)
	}
}

// clear_rules disables every enabled rule of the service through rule CRUD;
// another service's rules and the service's disabled ones are not touched.
func TestClearRulesAction_DisablesEveryEnabledRuleOfTheService(t *testing.T) {
	prom := promql.NewClient(fakePromServer(t, 1.0).URL)
	rules := newFakeRuleStore(
		mkRule("a", "svc", true), mkRule("b", "svc", true), mkRule("off", "svc", false),
		mkRule("other", "other-svc", true),
	)
	policy := clearRulesPolicy("p-4", "svc", "eq", 1, 0)

	if err := makeEngine(t, prom, rules, []*model.PolicyRule{policy}).evaluate(context.Background(), policy); err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if n := rules.updateCount(); n != 2 {
		t.Fatalf("expected 2 rule updates (the service's enabled rules), got %d", n)
	}
	for _, u := range rules.updates {
		if u.Service != "svc" || u.Enabled {
			t.Errorf("update %+v, want a rule of svc disabled", u)
		}
	}
	for id, want := range map[string]bool{"a": false, "b": false, "off": false, "other": true} {
		if got := rules.enabled(id); got != want {
			t.Errorf("rule %s enabled = %v, want %v", id, got, want)
		}
	}
}

func TestPrometheusUnavailable_DoesNotPanic(t *testing.T) {
	prom := promql.NewClient(fakePromServerUnavailable(t).URL)
	rules := newFakeRuleStore(mkRule("r-1", "checkout", true))

	// Must not panic; the tick should log a warning and skip.
	makeEngine(t, prom, rules, []*model.PolicyRule{clearRulesPolicy("p-5", "checkout", "gt", 0, 0)}).tick(context.Background())

	if n := rules.updateCount(); n != 0 {
		t.Fatalf("expected 0 rule updates when Prometheus is unavailable, got %d", n)
	}
}
