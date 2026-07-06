package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

const maxIngestBody = 64 << 20 // 64 MiB — cache batches can be large

// phaseReader is the ingest handlers' narrow view of the experiment store: it
// resolves a phase by id so a push can be validated against its owning
// experiment and status. Injected as an interface so handler tests can supply
// active/inactive pairs without a database.
type phaseReader interface {
	GetPhase(ctx context.Context, id string) (*model.ExperimentPhase, error)
}

// ingestEnvelope is the request body for POST /api/v1/cache/ingest (wire spec §W2).
//
// The (ExperimentID, PhaseID) pair is authoritative: the push is accepted only
// when it names the exact experiment phase that is actively recording (INV-5),
// never inferred from PhaseID alone. BatchSeq is 1-based and monotonic per
// (experiment_id, phase_id, instance) so retried batches dedupe.
type ingestEnvelope struct {
	Service      string                        `json:"service"`
	Instance     string                        `json:"instance"`
	ExperimentID string                        `json:"experiment_id"`
	PhaseID      string                        `json:"phase_id"`
	BatchSeq     int                           `json:"batch_seq"`
	Entries      []atroposdk.CacheBoxWireEntry `json:"entries"`
}

// ingestResult is the ingest response body (wire spec §W2): the number of
// entries newly accepted and whether the batch was a duplicate.
type ingestResult struct {
	Accepted  int  `json:"accepted"`
	Duplicate bool `json:"duplicate"`
}

// isActiveRecordingPair reports whether the phase still accepts recorded pushes:
// it persists cache and is either actively recording (running) or draining. The
// drain state keeps the push endpoint open while SDKs flush their remaining
// batches (W2), which is what removes the mark-completed-then-409 loss window.
func isActiveRecordingPair(p *model.ExperimentPhase) bool {
	return p.PersistCache && (p.Status == "running" || p.Status == "draining")
}

// handleCacheIngest receives cache-box entries from SDK instances and persists
// them under the (experiment_id, phase_id) pair. The push is accepted only when
// that pair names an actively recording phase (INV-5); retried batches (same
// batch_seq) are deduped so received counts stay exact (INV-7).
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
	if env.ExperimentID == "" {
		writeError(w, http.StatusBadRequest, "experiment_id required")
		return
	}
	if env.PhaseID == "" {
		writeError(w, http.StatusBadRequest, "phase_id required")
		return
	}

	phase, err := s.phaseReader.GetPhase(r.Context(), env.PhaseID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "phase not found")
		return
	}
	if err != nil {
		s.logger.Error("cache ingest: get phase failed", "phase_id", env.PhaseID, "error", err)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}

	// INV-5: the push must name the exact (experiment_id, phase_id) pair that
	// is actively recording. There is no inference from phase_id alone — a
	// mismatched experiment or a closed phase is refused with 409
	// phase_not_recording so the SDK stops retrying into a dead pair.
	if phase.ExperimentID != env.ExperimentID || !isActiveRecordingPair(phase) {
		writeError(w, http.StatusConflict, "phase_not_recording")
		return
	}

	if len(env.Entries) == 0 {
		writeJSON(w, http.StatusOK, ingestResult{Accepted: 0, Duplicate: false})
		return
	}

	res, err := s.cacheStore.Ingest(env.ExperimentID, env.PhaseID, env.Service, env.Instance, env.BatchSeq, env.Entries)
	if err != nil {
		s.logger.Error("cache ingest: write failed",
			"experiment_id", env.ExperimentID, "phase_id", env.PhaseID, "error", err)
		writeError(w, http.StatusInternalServerError, "write failed")
		return
	}
	writeJSON(w, http.StatusOK, ingestResult{Accepted: res.Accepted, Duplicate: res.Duplicate})
}

// handleCacheDrain receives an SDK's W3 drain report at recording-phase end,
// accounting for every buffered record it flushed (or dropped). Idempotent on
// (experiment_id, phase_id, instance_id) — a retried report overwrites. The
// orchestrator's drain gate (MANT-2) reads these to decide clean vs degraded.
func (s *Server) handleCacheDrain(w http.ResponseWriter, r *http.Request) {
	var rep atroposdk.DrainReport
	defer r.Body.Close()
	if err := json.NewDecoder(io.LimitReader(r.Body, maxIngestBody)).Decode(&rep); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if rep.ExperimentID == "" || rep.PhaseID == "" || rep.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "experiment_id, phase_id, instance_id required")
		return
	}
	s.cacheStore.RecordDrainReport(rep)
	writeJSON(w, http.StatusOK, atroposdk.DrainReportResponse{Accepted: true})
}

// handleCacheEntries serves stored cache entries for a service, used by SDK
// instances to seed their cache on startup.
//
// Query params: service (required), phase_id (required). The experiment is
// resolved from the phase — the (exp, phase) store layout needs both ids while
// the seed endpoint's contract carries only phase_id.
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

	phase, err := s.phaseReader.GetPhase(r.Context(), phaseID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "phase not found")
		return
	}
	if err != nil {
		s.logger.Error("cache entries: get phase failed", "phase_id", phaseID, "error", err)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}

	entries, err := s.cacheStore.Read(phase.ExperimentID, phaseID, service)
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
