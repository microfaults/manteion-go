package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/ruleconv"
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
	// onFidelity, when set, runs in the fidelity handler before it answers;
	// genAtPull is where a test's onFidelity records the generation it saw.
	onFidelity func()
	genAtPull  atomic.Uint64

	// mu guards pushed, the compiled rule names POSTed to /admin/rules — lets
	// tests assert the push channel is a strict projection of the poll
	// predicate (enabled rules only).
	mu     sync.Mutex
	pushed []string
}

// rulesPushed returns the names of every compiled rule the fake received on
// POST /admin/rules, in arrival order.
func (f *fakeSDK) rulesPushed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.pushed...)
}

func newFakeSDK(t *testing.T) *fakeSDK {
	t.Helper()
	f := &fakeSDK{commitOK: true}
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
	mux.HandleFunc("POST /admin/rules", func(w http.ResponseWriter, r *http.Request) {
		var rules []ruleconv.CompiledRule
		_ = json.NewDecoder(r.Body).Decode(&rules)
		f.mu.Lock()
		for _, cr := range rules {
			f.pushed = append(f.pushed, cr.Name)
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /admin/cachebox/delay", func(w http.ResponseWriter, _ *http.Request) {
		f.freezeHit.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /admin/cachebox", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /cachebox/fidelity", func(w http.ResponseWriter, _ *http.Request) {
		if f.onFidelity != nil {
			f.onFidelity()
		}
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

// registerSDK registers one live instance of service at addr, deregisters it
// on cleanup, and returns its id. Register stamps last_poll_at=now(), so the
// instance is "alive".
func registerSDK(t *testing.T, service, addr string) string {
	t.Helper()
	inst := &model.SDKInstance{ID: id.New("sdk"), Service: service, Version: "test", Address: addr}
	if err := testSDKRepo.Register(context.Background(), inst); err != nil {
		t.Fatalf("register sdk: %v", err)
	}
	t.Cleanup(func() { _ = testSDKRepo.Deregister(context.Background(), inst.ID) })
	return inst.ID
}

// ackGenerations simulates a converging instance: every 20ms it records the
// service's current generation as observed and, when artifact != "", reports
// the replay set committed under it. Stops at test cleanup.
func ackGenerations(t *testing.T, o *Orchestrator, instanceID, service, artifact string) {
	t.Helper()
	simulateInstance(t, o, instanceID, service, func(context.Context) *model.ObservedStatus {
		if artifact == "" {
			return nil
		}
		return &model.ObservedStatus{ReplaySet: &model.ReplaySetStatus{ID: artifact, State: "committed"}}
	})
}

// ackAssigned is ackGenerations for a replay set that is not built yet: every
// 20ms it records the service's current generation as observed and, once a
// set is assigned to (phaseID, service), reports that set in state, with errs
// as the instance's errors. Stops at test cleanup.
func ackAssigned(t *testing.T, o *Orchestrator, instanceID, phaseID, service, state string, errs ...string) {
	t.Helper()
	simulateInstance(t, o, instanceID, service, func(ctx context.Context) *model.ObservedStatus {
		a, err := o.artifacts.ForPhaseService(ctx, phaseID, service)
		if err != nil || a == nil {
			return nil
		}
		return &model.ObservedStatus{Errors: errs, ReplaySet: &model.ReplaySetStatus{ID: a.ID, State: state}}
	})
}

// simulateInstance runs the loop behind ackGenerations and ackAssigned on a
// goroutine that makes no testing.T calls: every 20ms it records the
// service's current generation as the instance's observed one (a poll) and,
// when report returns a status, stores it applied at that generation (a status
// report). It stops when the test is cleaned up.
func simulateInstance(t *testing.T, o *Orchestrator, instanceID, service string, report func(context.Context) *model.ObservedStatus) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				gen, err := o.generations.Get(ctx, service)
				if err != nil {
					continue
				}
				_ = testSDKRepo.RecordPoll(ctx, instanceID, gen)
				if st := report(ctx); st != nil {
					st.AppliedGeneration = gen
					_ = testSDKRepo.RecordStatus(ctx, instanceID, *st)
				}
			}
		}
	}()
}
