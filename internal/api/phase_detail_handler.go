package api

import (
	"context"
	"errors"
	"net/http"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// PhaseDetail is GET /api/v1/phases/{phaseId}. Per-workflow results + summable
// totals only — no averaged percentiles (percentile-aggregation caveat).
type PhaseDetail struct {
	*model.ExperimentPhase
	ExperimentName    string                       `json:"experiment_name"`
	Workflows         []model.PhaseWorkflow        `json:"workflows"`
	DatasetIDs        []string                     `json:"dataset_ids"` // derived, read-only: union of workflows' dataset_id, first-seen order, [] when none
	WorkflowResults   []*model.PhaseWorkflowResult `json:"workflow_results"`
	TotalRequestCount int64                        `json:"total_request_count"`
	TotalErrorCount   int64                        `json:"total_error_count"`
	OverallErrorRate  float64                      `json:"overall_error_rate"`
}

// PhaseFaults is GET /api/v1/phases/{phaseId}/faults.
type PhaseFaults struct {
	FrozenServices []model.CacheBoxConfig   `json:"frozen_services"`
	PhaseRules     []model.PhaseRule        `json:"phase_rules"`
	FaultEvents    []*model.PhaseFaultEvent `json:"fault_events"`
}

func (s *Server) handleListPhases(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, total, err := s.experiments.ListPhasesPaged(r.Context(), store.Page{Limit: limit, Offset: offset})
	if err != nil {
		s.logger.Error("list phases failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list phases")
		return
	}
	writePage(w, http.StatusOK, items, total, limit, offset)
}

func (s *Server) handleGetPhaseDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("phaseId")
	ctx := r.Context()
	phase, err := s.experiments.GetPhase(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "phase not found")
		return
	}
	if err != nil {
		s.logger.Error("get phase detail failed", "phase_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get phase")
		return
	}
	exp, _ := s.experiments.Get(ctx, phase.ExperimentID)
	pws, _ := s.experiments.ListPhaseWorkflows(ctx, id)
	results, err := s.experiments.ListWorkflowResultsForPhase(ctx, id)
	if err != nil {
		s.logger.Warn("phase detail: list workflow results failed", "phase_id", id, "error", err)
	}

	var totalReq, totalErr int64
	for _, res := range results {
		totalReq += res.RequestCount
		totalErr += res.ErrorCount
	}
	rate := 0.0
	if totalReq > 0 {
		rate = float64(totalErr) / float64(totalReq)
	}
	detail := PhaseDetail{
		ExperimentPhase:   phase,
		Workflows:         nilToEmpty(pws),
		DatasetIDs:        datasetUnion(pws),
		WorkflowResults:   nilToEmpty(results),
		TotalRequestCount: totalReq,
		TotalErrorCount:   totalErr,
		OverallErrorRate:  rate,
	}
	if exp != nil {
		detail.ExperimentName = exp.Name
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleGetPhaseFaults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("phaseId")
	ctx := r.Context()
	phase, err := s.experiments.GetPhase(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "phase not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get phase")
		return
	}
	rules, _ := s.experiments.ListPhaseRules(ctx, id)
	var events []*model.PhaseFaultEvent
	if s.phaseFaultEvents != nil {
		events, _ = s.phaseFaultEvents.ListForPhase(ctx, id)
	}
	writeJSON(w, http.StatusOK, PhaseFaults{
		FrozenServices: phase.FrozenServices,
		PhaseRules:     nilToEmpty(rules),
		FaultEvents:    nilToEmpty(events),
	})
}

func (s *Server) handlePausePhaseFlat(w http.ResponseWriter, r *http.Request) {
	if err := s.orch.PausePhase(r.Context(), r.PathValue("phaseId")); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "phase not found")
			return
		}
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *Server) handleResumePhaseFlat(w http.ResponseWriter, r *http.Request) {
	// StartPhase resumes a paused phase (and would start a pending one).
	if err := s.orch.StartPhase(r.Context(), r.PathValue("phaseId")); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "phase not found")
			return
		}
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (s *Server) handleStopPhaseFlat(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status") // "", completed, failed, skipped
	// Detach from the request context (M2): a client disconnect mid-drain-barrier
	// must not cancel teardown and wedge the phase at 'draining'.
	if err := s.orch.StopPhase(context.WithoutCancel(r.Context()), r.PathValue("phaseId"), status); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "phase not found")
			return
		}
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
