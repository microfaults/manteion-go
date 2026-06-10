package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/store"
)

const maxIngestBody = 64 << 20 // 64 MiB — cache batches can be large

// ingestEnvelope is the request body for POST /api/v1/cache/ingest.
//
// PhaseID identifies the experiment phase the SDK is contributing cache
// entries to. The phase must have persist_cache=true and be in
// status=running for the ingest to be accepted. Per migration #19 this
// field replaces the legacy run_id; the cachestore key is opaque so the
// underlying storage layout is unchanged.
type ingestEnvelope struct {
	Service  string                        `json:"service"`
	Instance string                        `json:"instance"`
	PhaseID  string                        `json:"phase_id"`
	Entries  []atroposdk.CacheBoxWireEntry `json:"entries"`
}

// handleCacheIngest receives cache-box entries from SDK instances and persists
// them under the phase identified by PhaseID. Rejects phases that have not
// opted into cache persistence (PersistCache=false) or that are not currently
// running.
func (s *Server) handleCacheIngest(w http.ResponseWriter, r *http.Request) {
	var env ingestEnvelope
	defer r.Body.Close()
	if err := json.NewDecoder(io.LimitReader(r.Body, maxIngestBody)).Decode(&env); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if env.Service == "" {
		writeError(w, http.StatusBadRequest, "service required")
		return
	}
	if env.PhaseID == "" {
		writeError(w, http.StatusBadRequest, "phase_id required")
		return
	}
	if len(env.Entries) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	phase, err := s.experiments.GetPhase(r.Context(), env.PhaseID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "phase not found")
		return
	}
	if err != nil {
		s.logger.Error("cache ingest: get phase failed", "phase_id", env.PhaseID, "error", err)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	if !phase.PersistCache {
		writeError(w, http.StatusConflict, "phase does not accept cache ingestion (persist_cache=false)")
		return
	}
	if phase.Status != "running" {
		writeError(w, http.StatusConflict, "phase is not running (status="+phase.Status+")")
		return
	}

	if err := s.cacheStore.Write(env.PhaseID, env.Service, env.Entries); err != nil {
		s.logger.Error("cache ingest: write failed", "phase_id", env.PhaseID, "error", err)
		writeError(w, http.StatusInternalServerError, "write failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleCacheEntries serves stored cache entries for a service, used by SDK
// instances to seed their cache on startup.
//
// Query params: service (required), phase_id (required)
func (s *Server) handleCacheEntries(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, "service query param required")
		return
	}
	phaseID := r.URL.Query().Get("phase_id")
	if phaseID == "" {
		writeError(w, http.StatusBadRequest, "phase_id query param required")
		return
	}

	entries, err := s.cacheStore.Read(phaseID, service)
	if err != nil {
		s.logger.Error("cache entries: read failed", "error", err)
		writeError(w, http.StatusInternalServerError, "read failed")
		return
	}
	if entries == nil {
		entries = []atroposdk.CacheBoxWireEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}
