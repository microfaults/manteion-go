package testutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// MockZeus is a canned Archer API server for integration tests.
// It tracks started attacks so tests can assert against them.
type MockZeus struct {
	*httptest.Server

	mu      sync.Mutex
	attacks map[string]string // id → status
}

// NewMockZeus returns a running httptest.Server that implements the Zeus/Archer
// endpoints used by the orchestrator: health, start/get/stop attack, get result.
// Attacks honor client-supplied IDs and default to "completed" on GET.
func NewMockZeus(t *testing.T) *MockZeus {
	t.Helper()
	mz := &MockZeus{attacks: make(map[string]string)}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /api/v1/attacks", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		id, _ := req["id"].(string)
		if id == "" {
			id = "mock-attack"
		}
		mz.mu.Lock()
		mz.attacks[id] = "running"
		mz.mu.Unlock()
		writeJSON(w, 201, map[string]string{"id": id})
	})

	mux.HandleFunc("GET /api/v1/attacks/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		mz.mu.Lock()
		status, ok := mz.attacks[id]
		mz.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, 200, map[string]any{
			"id": id, "status": status,
			"workload_id": "wl-test", "service": "test-svc",
		})
	})

	mux.HandleFunc("DELETE /api/v1/attacks/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		mz.mu.Lock()
		mz.attacks[id] = "stopped"
		mz.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /api/v1/attacks/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		mz.mu.Lock()
		status := mz.attacks[id]
		mz.mu.Unlock()
		if status != "completed" && status != "stopped" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, 200, map[string]any{
			"attack_id": id, "service": "test-svc",
			"total_requests": 1000, "duration_ms": 30000,
			"rate_actual": 33.3, "success_rate": 0.99,
			"latency_p50_us": 5000, "latency_p90_us": 12000,
			"latency_p95_us": 18000, "latency_p99_us": 35000,
			"throughput_rps": 33.3,
		})
	})

	mz.Server = httptest.NewServer(mux)
	t.Cleanup(mz.Server.Close)
	return mz
}

// CompleteAttack marks an attack as "completed" so subsequent GET returns done.
func (mz *MockZeus) CompleteAttack(id string) {
	mz.mu.Lock()
	mz.attacks[id] = "completed"
	mz.mu.Unlock()
}

// AttackIDs returns all tracked attack IDs.
func (mz *MockZeus) AttackIDs() []string {
	mz.mu.Lock()
	defer mz.mu.Unlock()
	ids := make([]string, 0, len(mz.attacks))
	for id := range mz.attacks {
		ids = append(ids, id)
	}
	return ids
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
