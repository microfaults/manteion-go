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
	"sync/atomic"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/atropos"
	"manteion-go/internal/atropostest"
	"manteion-go/internal/model"
	"manteion-go/internal/promql"
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

// fakeSDKServer starts an atropos admin server (real SDK control surface via
// atropos.Serve, offline) and returns its URL + a func that returns how many
// times PostRules has been called.
func fakeSDKServer(t *testing.T) (url string, pushCount func() int) {
	t.Helper()
	h := atropostest.NewHandler(t)
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/admin/rules" {
			count.Add(1)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() int { return int(count.Load()) }
}

// buildController builds an atrocontrol.Controller targeting a single fake SDK server.
func buildController(t *testing.T, sdkURL string, service string) *atrocontrol.Controller {
	t.Helper()
	resolver := &fakeResolver{
		services: map[string][]*model.SDKInstance{
			service: {{ID: "inst-0", Service: service, Address: sdkURL}},
		},
	}
	tx := atropos.NewClient()
	return atrocontrol.New(tx, resolver,
		atrocontrol.WithDefaultTimeout(3*time.Second),
		atrocontrol.WithDefaultConcurrency(4),
	)
}

// fakeResolver satisfies atrocontrol.InstanceResolver with an in-memory map.
type fakeResolver struct {
	services map[string][]*model.SDKInstance
}

func (f *fakeResolver) ForService(_ context.Context, service string) ([]*model.SDKInstance, error) {
	return f.services[service], nil
}

func (f *fakeResolver) ForInstance(_ context.Context, id string) (*model.SDKInstance, error) {
	for _, insts := range f.services {
		for _, inst := range insts {
			if inst.ID == id {
				return inst, nil
			}
		}
	}
	return nil, fmt.Errorf("instance %q not found", id)
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

// makeEngine assembles an Engine with a single policy rule, pointing at the given Prometheus server.
func makeEngine(
	t *testing.T,
	prom *promql.Client,
	ctrl *atrocontrol.Controller,
	policies []*model.PolicyRule,
) *Engine {
	t.Helper()
	var repo policyLister = &fakePolicyRepo{rules: policies}
	return &Engine{
		policies:    repo,
		rules:       nil, // not needed when action is clear_rules or rules slice is empty
		faults:      nil,
		controller:  ctrl,
		prom:        prom,
		logger:      noopLogger(),
		interval:    defaultTickInterval,
		lastFiredAt: make(map[string]time.Time),
	}
}

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// --- Tests ---

func TestConditionFires_AboveThreshold(t *testing.T) {
	promSrv := fakePromServer(t, 600.0) // metric = 600, threshold = 500 → gt fires
	sdkURL, pushCount := fakeSDKServer(t)
	ctrl := buildController(t, sdkURL, "checkout")
	prom := promql.NewClient(promSrv.URL)

	rule := &model.PolicyRule{
		ID:      "p-1",
		Enabled: true,
		Condition: model.PolicyCondition{
			Metric: "up", Operator: "gt", Threshold: 500,
		},
		Action: model.PolicyAction{
			ActionType: "clear_rules",
			PushRules:  &model.PushRulesAction{Service: "checkout"},
		},
		Cooldown: 0,
	}

	eng := makeEngine(t, prom, ctrl, []*model.PolicyRule{rule})
	eng.tick(context.Background())

	if pushCount() != 1 {
		t.Fatalf("expected 1 PushRules call, got %d", pushCount())
	}
}

func TestConditionDoesNotFire_BelowThreshold(t *testing.T) {
	promSrv := fakePromServer(t, 100.0) // metric = 100, threshold = 500 → gt does not fire
	sdkURL, pushCount := fakeSDKServer(t)
	ctrl := buildController(t, sdkURL, "checkout")
	prom := promql.NewClient(promSrv.URL)

	rule := &model.PolicyRule{
		ID:      "p-2",
		Enabled: true,
		Condition: model.PolicyCondition{
			Metric: "up", Operator: "gt", Threshold: 500,
		},
		Action: model.PolicyAction{
			ActionType: "clear_rules",
			PushRules:  &model.PushRulesAction{Service: "checkout"},
		},
		Cooldown: 0,
	}

	eng := makeEngine(t, prom, ctrl, []*model.PolicyRule{rule})
	eng.tick(context.Background())

	if pushCount() != 0 {
		t.Fatalf("expected 0 PushRules calls, got %d", pushCount())
	}
}

func TestCooldownPreventDoubleFire(t *testing.T) {
	promSrv := fakePromServer(t, 1.0) // always fires (eq 1)
	sdkURL, pushCount := fakeSDKServer(t)
	ctrl := buildController(t, sdkURL, "checkout")
	prom := promql.NewClient(promSrv.URL)

	rule := &model.PolicyRule{
		ID:      "p-3",
		Enabled: true,
		Condition: model.PolicyCondition{
			Metric: "up", Operator: "eq", Threshold: 1,
		},
		Action: model.PolicyAction{
			ActionType: "clear_rules",
			PushRules:  &model.PushRulesAction{Service: "checkout"},
		},
		Cooldown: 10 * time.Minute, // long cooldown
	}

	eng := makeEngine(t, prom, ctrl, []*model.PolicyRule{rule})
	eng.tick(context.Background()) // fires
	eng.tick(context.Background()) // blocked by cooldown
	eng.tick(context.Background()) // blocked by cooldown

	if pushCount() != 1 {
		t.Fatalf("expected exactly 1 PushRules call due to cooldown, got %d", pushCount())
	}
}

func TestClearRulesAction_CallsPushRulesWithNil(t *testing.T) {
	promSrv := fakePromServer(t, 1.0)
	sdkURL, _ := fakeSDKServer(t)
	ctrl := buildController(t, sdkURL, "svc")
	prom := promql.NewClient(promSrv.URL)

	rule := &model.PolicyRule{
		ID:      "p-4",
		Enabled: true,
		Condition: model.PolicyCondition{
			Metric: "up", Operator: "eq", Threshold: 1,
		},
		Action: model.PolicyAction{
			ActionType: "clear_rules",
			PushRules:  &model.PushRulesAction{Service: "svc"},
		},
		Cooldown: 0,
	}

	eng := makeEngine(t, prom, ctrl, []*model.PolicyRule{rule})
	if err := eng.evaluate(context.Background(), rule); err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	// Verify the SDK received an empty rule set (nil → []).
	getReq, _ := http.NewRequest(http.MethodGet, sdkURL+"/admin/rules", nil)
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET /admin/rules: %v", err)
	}
	defer resp.Body.Close()
	var rules []atroposdk.StaticRule
	json.NewDecoder(resp.Body).Decode(&rules)
	if len(rules) != 0 {
		t.Fatalf("expected empty rule set after clear_rules, got %d rules", len(rules))
	}
}

func TestPrometheusUnavailable_DoesNotPanic(t *testing.T) {
	promSrv := fakePromServerUnavailable(t)
	sdkURL, pushCount := fakeSDKServer(t)
	ctrl := buildController(t, sdkURL, "checkout")
	prom := promql.NewClient(promSrv.URL)

	rule := &model.PolicyRule{
		ID:      "p-5",
		Enabled: true,
		Condition: model.PolicyCondition{
			Metric: "up", Operator: "gt", Threshold: 0,
		},
		Action: model.PolicyAction{
			ActionType: "clear_rules",
			PushRules:  &model.PushRulesAction{Service: "checkout"},
		},
		Cooldown: 0,
	}

	eng := makeEngine(t, prom, ctrl, []*model.PolicyRule{rule})

	// Must not panic; the tick should log a warning and skip.
	eng.tick(context.Background())

	if pushCount() != 0 {
		t.Fatalf("expected 0 PushRules calls when Prometheus is unavailable, got %d", pushCount())
	}
}
