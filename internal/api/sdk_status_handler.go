package api

import (
	"errors"
	"math"
	"net/http"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// StatusReport is the POST /api/v1/sdk/status body (desired-state protocol v1
// §2): the instance's full snapshot of what it holds, sent after every apply
// and whenever its replay-set state changes. Field names and tags follow the
// contract's StatusReport; Unsupported and Errors are never omitted, since an
// empty list is how a report clears the previous one.
//
// TODO(desired-state): swap for atroposdk.StatusReport, with model.UnsupportedRule
// and model.ReplaySetStatus, when the status shapes move to the atropos-go wire
// aliases (see model/desired.go).
type StatusReport struct {
	InstanceID        string                  `json:"instance_id"`
	Service           string                  `json:"service"`
	AppliedGeneration uint64                  `json:"applied_generation"`
	Etag              string                  `json:"etag"`
	Unsupported       []model.UnsupportedRule `json:"unsupported"`
	Errors            []string                `json:"errors"`
	ReplaySet         *model.ReplaySetStatus  `json:"replay_set,omitempty"`
}

// handleSDKStatus stores an instance's applied-state report.
//
// @Summary      Report applied desired state
// @Description  Sent by an SDK instance after every apply and whenever its replay-set
// @Description  state changes (fetching|committed|failed). The report is a full snapshot
// @Description  of what the instance holds and replaces the previous one: it stores
// @Description  etag, unsupported rules and errors (empty lists clear) and the replay-set
// @Description  id and state (an absent replay_set means the instance holds none and
// @Description  clears them) on the instance row, and raises observed_generation to
// @Description  applied_generation, never lowering it (the poll records the version the
// @Description  instance holds and is its authoritative writer). GET /sdk/instances
// @Description  exposes them and the orchestrator's convergence barrier reads them.
// @Tags         sdk
// @Accept       json
// @Produce      json
// @Param        report  body  api.StatusReport  true  "applied-state report"
// @Success      204  "stored"
// @Failure      400  {object}  api.ErrorResponse  "invalid JSON, missing instance_id or service, applied_generation above 9223372036854775807, or unknown replay_set.state"
// @Failure      404  {object}  api.ErrorResponse  "unknown instance"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/status [post]
func (s *Server) handleSDKStatus(w http.ResponseWriter, r *http.Request) {
	var rep StatusReport
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
	err := s.registry.RecordStatus(r.Context(), rep.InstanceID, model.ObservedStatus{
		AppliedGeneration: rep.AppliedGeneration, Etag: rep.Etag,
		Unsupported: rep.Unsupported, Errors: rep.Errors, ReplaySet: rep.ReplaySet,
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "instance not found")
	case err != nil:
		s.logger.Error("sdk status: record failed", "instance_id", rep.InstanceID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to record status")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
