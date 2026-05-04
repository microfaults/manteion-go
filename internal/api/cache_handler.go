package api

import (
	"errors"
	"net/http"

	atroposdk "atropos-go"

	"manteion-go/internal/store"
)

// ingestEnvelope is the request body for POST /api/v1/cache/ingest.
//
// RunID is caller-supplied: SDKs target a specific run rather than relying on
// implicit "current baseline" state. The run must have persist_cache=true and
// be in status=running for the ingest to be accepted. This generalizes ingest
// to non-baseline runs (e.g., a chaos run that wants to capture cache state)
// without coupling new use cases to baseline-specific server logic.
type ingestEnvelope struct {
	Service  string                     `json:"service"`
	Instance string                     `json:"instance"`
	RunID    string                     `json:"run_id"`
	Entries  []*atroposdk.CacheBoxEntry `json:"entries"`
}

// handleCacheIngest receives cache-box entries from SDK instances and persists
// them under the run identified by RunID. Rejects runs that have not opted
// into cache persistence (run.PersistCache=false) or that are not currently
// running.
func (s *Server) handleCacheIngest(w http.ResponseWriter, r *http.Request) {
	var env ingestEnvelope
	if err := readJSON(r, &env); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if env.Service == "" {
		writeError(w, http.StatusBadRequest, "service required")
		return
	}
	if env.RunID == "" {
		writeError(w, http.StatusBadRequest, "run_id required")
		return
	}
	if len(env.Entries) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	run, err := s.experiments.GetRun(r.Context(), env.RunID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		s.logger.Error("cache ingest: get run failed", "run_id", env.RunID, "error", err)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	if !run.PersistCache {
		writeError(w, http.StatusConflict, "run does not accept cache ingestion (persist_cache=false)")
		return
	}
	if run.Status != "running" {
		writeError(w, http.StatusConflict, "run is not running (status="+run.Status+")")
		return
	}

	if err := s.cacheStore.Write(env.RunID, env.Service, env.Entries); err != nil {
		s.logger.Error("cache ingest: write failed", "run_id", env.RunID, "error", err)
		writeError(w, http.StatusInternalServerError, "write failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleCacheEntries serves stored cache entries for a service, used by SDK
// instances to seed their cache on startup.
//
// Query param: service (required)
func (s *Server) handleCacheEntries(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, "service query param required")
		return
	}
	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		writeError(w, http.StatusBadRequest, "run_id query param required")
		return
	}

	entries, err := s.cacheStore.Read(runID, service)
	if err != nil {
		s.logger.Error("cache entries: read failed", "error", err)
		writeError(w, http.StatusInternalServerError, "read failed")
		return
	}
	if entries == nil {
		entries = []*atroposdk.CacheBoxEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}
