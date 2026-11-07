package api

import (
	"errors"
	"math"
	"net/http"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleSDKStatus stores an instance's applied-state report.
//
// @Summary      Report applied desired state
// @Description  Sent by an SDK instance after every apply and whenever its replay-set
// @Description  state changes (fetching|committed|failed). The report is a full snapshot
// @Description  of what the instance holds and replaces the previous one: it stores
// @Description  etag, unsupported rules and errors (empty lists clear) and the replay-set
// @Description  id and state (an absent replay_set means the instance holds none and
// @Description  clears them) on the instance row, together with applied_generation, the
// @Description  generation the report was applied at, so the row says which generation its
// @Description  replay-set state belongs to. It raises observed_generation to
// @Description  applied_generation, never lowering it (the poll records the version the
// @Description  instance holds and is its authoritative writer). A report applied below
// @Description  the stored applied_generation is a stale snapshot (a delayed older report):
// @Description  it is dropped with 200 and nothing is written. GET /sdk/instances exposes
// @Description  the columns and the orchestrator's convergence barrier reads them.
// @Tags         sdk
// @Accept       json
// @Produce      json
// @Param        report  body  atroposdk.StatusReport  true  "applied-state report"
// @Success      200  "stale report dropped: applied_generation is below the stored one; nothing written"
// @Success      204  "stored"
// @Failure      400  {object}  api.ErrorResponse  "invalid JSON, missing instance_id or service, applied_generation above 9223372036854775807, or unknown replay_set.state"
// @Failure      404  {object}  api.ErrorResponse  "unknown instance"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/status [post]
func (s *Server) handleSDKStatus(w http.ResponseWriter, r *http.Request) {
	var rep atroposdk.StatusReport
	if err := readJSON(r, &rep); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if rep.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "instance_id required")
		return
	}
	if rep.Service == "" {
		writeError(w, http.StatusBadRequest, "service required")
		return
	}
	// observed_generation is a BIGINT, so a larger value would fail the write
	// in pgx (a 500): refuse it before touching the store.
	if rep.AppliedGeneration > math.MaxInt64 {
		writeError(w, http.StatusBadRequest, "applied_generation out of range")
		return
	}
	if rep.ReplaySet != nil && !model.ValidReplaySetState(rep.ReplaySet.State) {
		writeError(w, http.StatusBadRequest, "replay_set.state must be fetching, committed or failed")
		return
	}
	err := s.registry.RecordStatus(r.Context(), rep.InstanceID, observedStatusFrom(rep))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "instance not found")
	case errors.Is(err, store.ErrStaleStatus):
		// A delayed older snapshot: the stored one is newer. Answer 2xx so the
		// SDK does not re-send it.
		s.logger.DebugContext(r.Context(), "sdk status: stale report dropped", "instance_id", rep.InstanceID, "error", err)
		w.WriteHeader(http.StatusOK)
	case err != nil:
		s.logger.Error("sdk status: record failed", "instance_id", rep.InstanceID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to record status")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
