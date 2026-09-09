package zeus

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Pins the GET /api/v1/datasets wire contract the dataset preflight reads:
// an envelope {"datasets": [{id, name, source, size_bytes, ttl_s,
// created_at}]} where zeus deletes a dataset once created_at + ttl_s has
// passed and ttl_s == 0 means it never expires.
func TestListDatasets(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"datasets":[
			{"id":"ds-1","name":"checkout-ids","source":"upload","size_bytes":1234,"ttl_s":86400,"created_at":"2026-09-08T10:00:00Z"},
			{"id":"ds-2","name":"forever","source":"inline","size_bytes":0,"ttl_s":0,"created_at":"2026-09-08T10:00:00Z","extra":"ignored"}
		]}`)
	}))
	defer srv.Close()

	got, err := NewClient(srv.URL).ListDatasets(context.Background())
	if err != nil {
		t.Fatalf("ListDatasets: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/api/v1/datasets" {
		t.Fatalf("request = %s %s, want GET /api/v1/datasets", gotMethod, gotPath)
	}
	if len(got) != 2 {
		t.Fatalf("got %d datasets, want 2: %+v", len(got), got)
	}
	created := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	want0 := Dataset{ID: "ds-1", Name: "checkout-ids", Source: "upload", SizeBytes: 1234, TTLS: 86400, CreatedAt: created}
	if got[0] != want0 {
		t.Errorf("datasets[0] = %+v, want %+v", got[0], want0)
	}
	if exp := got[0].ExpiresAt(); !exp.Equal(created.Add(24 * time.Hour)) {
		t.Errorf("ds-1 ExpiresAt = %v, want created_at + 24h", exp)
	}
	if got[1].ID != "ds-2" || got[1].TTLS != 0 {
		t.Errorf("datasets[1] = %+v, want ds-2 with ttl_s 0", got[1])
	}
	if exp := got[1].ExpiresAt(); !exp.IsZero() {
		t.Errorf("ttl_s 0 must never expire; ExpiresAt = %v, want zero time", exp)
	}
}

func TestListDatasets_EmptyAndErrors(t *testing.T) {
	t.Run("empty envelope", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"datasets":[]}`)
		}))
		defer srv.Close()
		got, err := NewClient(srv.URL).ListDatasets(context.Background())
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v; want empty, nil", got, err)
		}
	})
	t.Run("non-200 is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"error":"boom"}`)
		}))
		defer srv.Close()
		if _, err := NewClient(srv.URL).ListDatasets(context.Background()); err == nil {
			t.Fatal("want error on 500, got nil")
		}
	})
	t.Run("unreachable is an error", func(t *testing.T) {
		if _, err := NewClient("http://127.0.0.1:1").ListDatasets(context.Background()); err == nil {
			t.Fatal("want transport error, got nil")
		}
	})
}
