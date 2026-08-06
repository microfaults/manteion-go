package atropos

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
	"manteion-go/internal/atropostest"
	"manteion-go/internal/ruleconv"
)

// fakeAtroposAdmin builds an httptest server over the real SDK control
// surface (atropos.Serve, offline) — the handler a deployed SDK mounts.
func fakeAtroposAdmin(t *testing.T) *httptest.Server {
	t.Helper()
	return atropostest.NewServer(t)
}

func TestFaultRoundtrip(t *testing.T) {
	srv := fakeAtroposAdmin(t)
	c := NewClient(WithHTTPClient(srv.Client()))
	ctx := context.Background()

	// GET when no fault active
	status, err := c.GetFault(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetFault: %v", err)
	}
	if status.Active {
		t.Fatal("expected inactive initially")
	}

	// POST a latency fault using the unified wire shape: first-class
	// category/fault_type plus a typed params blob (faultparams schema).
	postStatus, err := c.PostFault(ctx, srv.URL, atroposdk.FaultRequest{
		FaultType: "latency",
		Params:    json.RawMessage(`{"delay":"100ms"}`),
	})
	if err != nil {
		t.Fatalf("PostFault: %v", err)
	}
	if !postStatus.Active {
		t.Fatal("expected active after POST")
	}
	if len(postStatus.Faults) == 0 || postStatus.Faults[0].FaultType != "latency" {
		t.Fatalf("expected latency fault in response, got %+v", postStatus.Faults)
	}

	// GET should show active
	status, err = c.GetFault(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetFault after POST: %v", err)
	}
	if !status.Active {
		t.Fatal("expected active after POST")
	}

	// DELETE
	if err := c.DeleteFault(ctx, srv.URL); err != nil {
		t.Fatalf("DeleteFault: %v", err)
	}

	// GET should show inactive
	status, err = c.GetFault(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetFault after DELETE: %v", err)
	}
	if status.Active {
		t.Fatal("expected inactive after DELETE")
	}
}

func TestRulesRoundtrip(t *testing.T) {
	srv := fakeAtroposAdmin(t)
	c := NewClient(WithHTTPClient(srv.Client()))
	ctx := context.Background()

	// GET empty
	rules, err := c.GetRules(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetRules: %v", err)
	}
	if len(rules) != 0 {
		t.Fatalf("expected 0 rules, got %d", len(rules))
	}

	// POST rules (now accepts CompiledRule wire format)
	compiled := []ruleconv.CompiledRule{
		{Name: "freeze-productcatalog", InjectionPoint: "egress", Mode: "inline",
			CacheBox: &ruleconv.CompiledCacheBox{Mode: "replay", KeyStrategy: "exact"}},
	}
	if err := c.PostRules(ctx, srv.URL, compiled); err != nil {
		t.Fatalf("PostRules: %v", err)
	}

	// GET should return them
	rules, err = c.GetRules(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetRules after POST: %v", err)
	}
	if len(rules) != 1 || rules[0].Name != "freeze-productcatalog" {
		t.Fatalf("unexpected rules: %+v", rules)
	}
}

func TestCacheBoxRoundtrip(t *testing.T) {
	srv := fakeAtroposAdmin(t)
	c := NewClient(WithHTTPClient(srv.Client()))
	ctx := context.Background()

	// GET stats
	stats, err := c.GetCacheBoxStats(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetCacheBoxStats: %v", err)
	}
	if stats.Store.Entries != 0 {
		t.Fatalf("expected 0 entries, got %d", stats.Store.Entries)
	}

	// POST delay
	if err := c.PostCacheBoxDelay(ctx, srv.URL, atroposdk.DelayRequest{
		Mu: 1.0, Sigma: 0.5, Seed: 42,
	}); err != nil {
		t.Fatalf("PostCacheBoxDelay: %v", err)
	}

	// DELETE (clear)
	if err := c.ClearCacheBox(ctx, srv.URL); err != nil {
		t.Fatalf("ClearCacheBox: %v", err)
	}
}

func TestHTTPError(t *testing.T) {
	srv := fakeAtroposAdmin(t)
	c := NewClient(WithHTTPClient(srv.Client()))
	ctx := context.Background()

	// POST invalid fault type -> 400
	_, err := c.PostFault(ctx, srv.URL, atroposdk.FaultRequest{FaultType: "explode"})
	if err == nil {
		t.Fatal("expected error for invalid fault type")
	}

	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected HTTPError, got %T: %v", err, err)
	}
	if httpErr.Status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", httpErr.Status)
	}
}

func TestTransportError(t *testing.T) {
	c := NewClient()
	ctx := context.Background()

	// Connect to a bogus address
	_, err := c.GetFault(ctx, "http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}

	var txErr *TransportError
	if !errors.As(err, &txErr) {
		t.Fatalf("expected TransportError, got %T: %v", err, err)
	}
}

// Registered SDK addresses are host:port with no scheme (the SDK advertises
// localIPv4()[:port]); the client must default them to http:// rather than
// failing URL parse/dial before any packet is sent.
func TestPostRulesSchemelessAddress(t *testing.T) {
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/rules", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "http://") // "127.0.0.1:PORT", as registered
	if err := NewClient().PostRules(context.Background(), addr, nil); err != nil {
		t.Fatalf("PostRules with schemeless addr: %v", err)
	}
	if gotPath != "/admin/rules" {
		t.Fatalf("request path = %q, want /admin/rules", gotPath)
	}
}

func TestNormalizeURLPassthrough(t *testing.T) {
	for _, u := range []string{"http://10.0.0.1:8080", "https://svc.local"} {
		if got := normalizeURL(u + "/x"); got != u+"/x" {
			t.Fatalf("normalizeURL(%q) = %q, want unchanged", u+"/x", got)
		}
	}
	if got := normalizeURL("10.0.0.1:8080/x"); got != "http://10.0.0.1:8080/x" {
		t.Fatalf("normalizeURL schemeless = %q, want http:// prefix", got)
	}
}
