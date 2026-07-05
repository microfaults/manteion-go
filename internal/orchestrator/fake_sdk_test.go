package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// fakeSDK is an httptest SDK control server for orchestrator preload/freeze gate
// tests. It implements the staged preload endpoints (§W4) and the freeze/clear
// admin routes. When commitOK is true it echoes the requested count+checksum
// (a clean verify-commit); when false it returns a 409 checksum mismatch. It
// records whether freeze was ever asserted, so a test can prove the preload gate
// aborts BEFORE freeze.
type fakeSDK struct {
	server    *httptest.Server
	commitOK  bool
	freezeHit atomic.Bool
	// fidelity, when set, is served at GET /cachebox/fidelity; nil ⇒ 503 (used
	// to simulate a missing-telemetry instance).
	fidelity *atroposdk.FidelitySnapshot
}

func newFakeSDK(t *testing.T, commitOK bool) *fakeSDK {
	t.Helper()
	f := &fakeSDK{commitOK: commitOK}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /cachebox/preload/begin", func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(w, http.StatusOK, atroposdk.PreloadBeginResponse{OK: true})
	})
	mux.HandleFunc("POST /cachebox/preload/chunk", func(w http.ResponseWriter, r *http.Request) {
		var req atroposdk.PreloadChunkRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeTestJSON(w, http.StatusOK, atroposdk.PreloadChunkResponse{StagedTotal: len(req.Entries)})
	})
	mux.HandleFunc("POST /cachebox/preload/commit", func(w http.ResponseWriter, r *http.Request) {
		var req atroposdk.PreloadCommitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if f.commitOK {
			writeTestJSON(w, http.StatusOK, atroposdk.PreloadCommitResponse{OK: true, Loaded: req.TotalEntries, Checksum: req.Checksum})
			return
		}
		writeTestJSON(w, http.StatusConflict, atroposdk.PreloadCommitResponse{OK: false, Loaded: req.TotalEntries, Checksum: "deadbeef-mismatch"})
	})
	mux.HandleFunc("POST /cachebox/preload/abort", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /admin/cachebox/delay", func(w http.ResponseWriter, _ *http.Request) {
		f.freezeHit.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /admin/cachebox", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /cachebox/fidelity", func(w http.ResponseWriter, _ *http.Request) {
		if f.fidelity == nil {
			http.Error(w, "no snapshot", http.StatusServiceUnavailable)
			return
		}
		writeTestJSON(w, http.StatusOK, *f.fidelity)
	})

	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func writeTestJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// registerSDK registers one live instance of service at addr and deregisters it
// on cleanup. Register stamps last_poll_at=now(), so the instance is "alive".
func registerSDK(t *testing.T, service, addr string) {
	t.Helper()
	inst := &model.SDKInstance{ID: id.New("sdk"), Service: service, Version: "test", Address: addr}
	if err := testSDKRepo.Register(context.Background(), inst); err != nil {
		t.Fatalf("register sdk: %v", err)
	}
	t.Cleanup(func() { _ = testSDKRepo.Deregister(context.Background(), inst.ID) })
}
