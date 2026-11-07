package atropos

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"manteion-go/internal/atropostest"
)

// fakeAtroposAdmin builds an httptest server over the real SDK control
// surface (atropos.Serve, offline) — the handler a deployed SDK mounts.
func fakeAtroposAdmin(t *testing.T) *httptest.Server {
	t.Helper()
	return atropostest.NewServer(t)
}

// adminPost arms the SDK's control surface directly: the client itself only
// reads it (manteion pushes nothing; instances apply the documents they poll).
func adminPost(t *testing.T, srv *httptest.Server, path, body string, want int) {
	t.Helper()
	resp, err := srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("POST %s: status %d, want %d", path, resp.StatusCode, want)
	}
}

func TestGetFault(t *testing.T) {
	srv := fakeAtroposAdmin(t)
	c := NewClient(WithHTTPClient(srv.Client()))
	ctx := context.Background()

	status, err := c.GetFault(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetFault: %v", err)
	}
	if status.Active {
		t.Fatal("expected inactive initially")
	}

	adminPost(t, srv, "/admin/fault", `{"fault_type":"latency","params":{"delay":"100ms"}}`, http.StatusCreated)
	status, err = c.GetFault(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetFault after arming: %v", err)
	}
	if !status.Active || len(status.Faults) == 0 || status.Faults[0].FaultType != "latency" {
		t.Fatalf("expected the latency fault active, got %+v", status)
	}
}

func TestGetRules(t *testing.T) {
	srv := fakeAtroposAdmin(t)
	c := NewClient(WithHTTPClient(srv.Client()))
	ctx := context.Background()

	rules, err := c.GetRules(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetRules: %v", err)
	}
	if len(rules) != 0 {
		t.Fatalf("expected 0 rules, got %d", len(rules))
	}

	adminPost(t, srv, "/admin/rules", `[{"name":"freeze-productcatalog","injection_point":"egress","mode":"inline",`+
		`"cachebox":{"mode":"replay","key_strategy":"exact"}}]`, http.StatusNoContent)
	rules, err = c.GetRules(ctx, srv.URL)
	if err != nil {
		t.Fatalf("GetRules after install: %v", err)
	}
	if len(rules) != 1 || rules[0].Name != "freeze-productcatalog" {
		t.Fatalf("unexpected rules: %+v", rules)
	}
}

func TestGetCacheBoxStats(t *testing.T) {
	srv := fakeAtroposAdmin(t)
	c := NewClient(WithHTTPClient(srv.Client()))

	stats, err := c.GetCacheBoxStats(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("GetCacheBoxStats: %v", err)
	}
	if stats.Store.Entries != 0 {
		t.Fatalf("expected 0 entries, got %d", stats.Store.Entries)
	}
}

func TestHTTPError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/fault", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad", http.StatusBadRequest)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	_, err := NewClient(WithHTTPClient(srv.Client())).GetFault(context.Background(), srv.URL)
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
func TestSchemelessAddress(t *testing.T) {
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/rules", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte("[]"))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "http://") // "127.0.0.1:PORT", as registered
	if _, err := NewClient().GetRules(context.Background(), addr); err != nil {
		t.Fatalf("GetRules with schemeless addr: %v", err)
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
