package atrocontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/atropos"
	"manteion-go/internal/atropostest"
	"manteion-go/internal/model"
	"manteion-go/internal/ruleconv"
)

// fakeResolver returns canned instance lists keyed by service name.
type fakeResolver struct {
	services map[string][]*model.SDKInstance
}

func (f *fakeResolver) ForService(_ context.Context, service string) ([]*model.SDKInstance, error) {
	insts, ok := f.services[service]
	if !ok {
		return nil, nil
	}
	return insts, nil
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

// setupAtroposServers starts N independent atropos admin servers (real SDK
// control surface via atropos.Serve, offline), returning their URLs.
// Cleanup is registered on t.
func setupAtroposServers(t *testing.T, n int) []string {
	t.Helper()
	var urls []string
	for range n {
		urls = append(urls, atropostest.NewServer(t).URL)
	}
	return urls
}

func newTestController(t *testing.T, urls []string) *Controller {
	t.Helper()
	instances := make([]*model.SDKInstance, len(urls))
	for i, u := range urls {
		instances[i] = &model.SDKInstance{
			ID:      fmt.Sprintf("pod-%d", i),
			Service: "productcatalog",
			Address: u,
		}
	}

	resolver := &fakeResolver{
		services: map[string][]*model.SDKInstance{
			"productcatalog": instances,
		},
	}

	tx := atropos.NewClient()
	return New(tx, resolver,
		WithDefaultTimeout(5*time.Second),
		WithDefaultConcurrency(4),
	)
}

func TestFreezeServiceFanout(t *testing.T) {
	urls := setupAtroposServers(t, 3)
	ctrl := newTestController(t, urls)
	ctx := context.Background()

	result, err := ctrl.FreezeService(ctx, "productcatalog", atroposdk.DelayRequest{
		Mu: 1.0, Sigma: 0.5, Seed: 42,
	}, WithRunID("run-1"))
	if err != nil {
		t.Fatalf("FreezeService: %v", err)
	}
	if !result.AllSucceeded() {
		t.Fatalf("expected all succeeded, got %d failures: %v", len(result.Failed), result.Failed)
	}
	if len(result.OK) != 3 {
		t.Fatalf("expected 3 OK, got %d", len(result.OK))
	}

	// Intent should be set.
	intent, ok := ctrl.IntentReader().Get("productcatalog")
	if !ok {
		t.Fatal("expected intent to be set")
	}
	if intent.FreezeCfg == nil {
		t.Fatal("expected FreezeCfg in intent")
	}
	if intent.RunID != "run-1" {
		t.Fatalf("expected run_id=run-1, got %s", intent.RunID)
	}
}

func TestClearServiceFanout(t *testing.T) {
	urls := setupAtroposServers(t, 2)
	ctrl := newTestController(t, urls)
	ctx := context.Background()

	// Freeze then clear.
	_, err := ctrl.FreezeService(ctx, "productcatalog", atroposdk.DelayRequest{Mu: 1.0, Sigma: 0.5})
	if err != nil {
		t.Fatalf("FreezeService: %v", err)
	}

	result, err := ctrl.ClearService(ctx, "productcatalog")
	if err != nil {
		t.Fatalf("ClearService: %v", err)
	}
	if !result.AllSucceeded() {
		t.Fatalf("expected all succeeded: %v", result.Failed)
	}

	// Intent should be cleared.
	if _, ok := ctrl.IntentReader().Get("productcatalog"); ok {
		t.Fatal("expected intent cleared after ClearService")
	}
}

func TestInjectFaultFanout(t *testing.T) {
	urls := setupAtroposServers(t, 2)
	ctrl := newTestController(t, urls)
	ctx := context.Background()

	result, err := ctrl.InjectFault(ctx, "productcatalog", atroposdk.FaultRequest{
		FaultType: "latency",
		Params:    json.RawMessage(`{"delay":"200ms"}`),
	})
	if err != nil {
		t.Fatalf("InjectFault: %v", err)
	}
	if !result.AllSucceeded() {
		t.Fatalf("expected all succeeded: %v", result.Failed)
	}

	// Verify intent.
	intent, ok := ctrl.IntentReader().Get("productcatalog")
	if !ok || intent.ActiveFault == nil {
		t.Fatal("expected fault intent to be set")
	}
}

func TestPartialFailure(t *testing.T) {
	urls := setupAtroposServers(t, 2)
	ctrl := newTestController(t, urls)
	ctx := context.Background()

	// Shut down one server to simulate partial failure.
	// The first URL's server was already created, so find it and close it.
	// We'll create a controller with one valid and one bogus address.
	instances := []*model.SDKInstance{
		{ID: "pod-good", Service: "frontend", Address: urls[0]},
		{ID: "pod-bad", Service: "frontend", Address: "http://127.0.0.1:1"},
	}
	resolver := &fakeResolver{
		services: map[string][]*model.SDKInstance{"frontend": instances},
	}
	tx := atropos.NewClient()
	ctrl = New(tx, resolver, WithDefaultTimeout(1*time.Second))

	result, err := ctrl.InjectFault(ctx, "frontend", atroposdk.FaultRequest{
		FaultType: "error",
		Params:    json.RawMessage(`{"status_code":503,"message":"down"}`),
	})
	if err != nil {
		t.Fatalf("InjectFault: %v", err)
	}
	if result.AllSucceeded() {
		t.Fatal("expected partial failure")
	}
	if !result.AnySucceeded() {
		t.Fatal("expected at least one success")
	}
	if len(result.Failed) != 1 {
		t.Fatalf("expected 1 failure, got %d", len(result.Failed))
	}
	if result.Failed[0].InstanceID != "pod-bad" {
		t.Fatalf("expected pod-bad to fail, got %s", result.Failed[0].InstanceID)
	}
}

func TestNoInstancesError(t *testing.T) {
	resolver := &fakeResolver{services: map[string][]*model.SDKInstance{}}
	tx := atropos.NewClient()
	ctrl := New(tx, resolver)
	ctx := context.Background()

	_, err := ctrl.FreezeService(ctx, "nonexistent", atroposdk.DelayRequest{Mu: 1.0})
	if err == nil {
		t.Fatal("expected error for unknown service")
	}
}

func TestStatusByService(t *testing.T) {
	urls := setupAtroposServers(t, 2)
	ctrl := newTestController(t, urls)
	ctx := context.Background()

	status, err := ctrl.StatusByService(ctx, "productcatalog")
	if err != nil {
		t.Fatalf("StatusByService: %v", err)
	}
	if len(status.Instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(status.Instances))
	}
	for _, inst := range status.Instances {
		if inst.Err != nil {
			t.Fatalf("instance %s had error: %v", inst.InstanceID, inst.Err)
		}
		if inst.Fault == nil {
			t.Fatalf("instance %s: expected fault status", inst.InstanceID)
		}
	}
}

func TestPushRules(t *testing.T) {
	urls := setupAtroposServers(t, 2)
	ctrl := newTestController(t, urls)
	ctx := context.Background()

	rules := []ruleconv.CompiledRule{
		{Name: "freeze-svc", InjectionPoint: "egress", Mode: "inline",
			CacheBox: &ruleconv.CompiledCacheBox{Mode: "replay", KeyStrategy: "exact"}},
	}
	result, err := ctrl.PushRules(ctx, "productcatalog", rules)
	if err != nil {
		t.Fatalf("PushRules: %v", err)
	}
	if !result.AllSucceeded() {
		t.Fatalf("expected all succeeded: %v", result.Failed)
	}

	intent, ok := ctrl.IntentReader().Get("productcatalog")
	if !ok || len(intent.Rules) != 1 {
		t.Fatal("expected rules intent to be set")
	}
}

// A fanout failure must name the instance, address, and error — counts alone
// made the schemeless-address bug undiagnosable from logs.
func TestLogFanoutIncludesFailureDetail(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	resolver := &fakeResolver{services: map[string][]*model.SDKInstance{
		"productcatalog": {{ID: "pod-dead", Service: "productcatalog", Address: "127.0.0.1:1"}},
	}}
	c := New(atropos.NewClient(), resolver,
		WithDefaultTimeout(2*time.Second),
		WithDefaultConcurrency(2),
		WithLogger(logger),
	)

	result, err := c.PushRules(context.Background(), "productcatalog", nil)
	if err != nil {
		t.Fatalf("PushRules: %v", err)
	}
	if len(result.Failed) != 1 {
		t.Fatalf("expected 1 failed target, got %+v", result)
	}
	out := buf.String()
	for _, want := range []string{"pod-dead", "127.0.0.1:1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("fanout log missing %q; log:\n%s", want, out)
		}
	}
}
