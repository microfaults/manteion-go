package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// =========================================================================
// Experiment CRUD + lifecycle
//
// Wire shape (POST /api/v1/experiments) — full plan in one call:
//
//   {
//     "name": "checkout latency attribution",
//     "hypothesis": "productcatalog contributes 30% of checkout tail",
//     "workflows": ["checkout", "browse"],
//     "phases": [
//       {
//         "name": "baseline",
//         "position": 0,
//         "frozen_services": [],
//         "persist_cache": true,
//         "workflows": [
//           {"workflow_id": "checkout", "vus": 50, "duration_sec": 300}
//         ],
//         "rules": []
//       },
//       ...
//     ]
//   }
// =========================================================================

// Wire-shape naming convention:
//   - `_ids` suffix when the field is a list of bare references that the
//     server resolves to join-table rows (e.g. workflow_ids, rule_ids).
//   - plural-object name when each item carries data of its own
//     (e.g. phase-level `workflows` is `[]PhaseWorkflow` with vus, duration,
//     target_url, ... — not just an id list).
//
// This keeps the JSON shape semantically aligned with the Go field types:
// a reader sees `workflow_ids: ["a"]` (refs) vs `workflows: [{...}]`
// (configs) and immediately knows whether to expect an id string or a
// structured object.

// createPhaseRequest is the per-phase shape inside POST /experiments and
// POST /experiments/{id}/phases.
type createPhaseRequest struct {
	Name           string                 `json:"name"`
	Position       int                    `json:"position"`
	FrozenServices []model.CacheBoxConfig `json:"frozen_services"`
	PersistCache   bool                   `json:"persist_cache"`
	// Workflows carries full PhaseWorkflow attack config per (phase, workflow).
	Workflows []model.PhaseWorkflow `json:"workflows"`
	// RuleIDs is a bare-reference list resolved to phase_rules rows.
	RuleIDs []string `json:"rule_ids"`
}

type createExperimentRequest struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Hypothesis  string `json:"hypothesis,omitempty"`
	CreatedBy   string `json:"created_by,omitempty"`
	// WorkflowIDs is a bare-reference list resolved to experiment_workflows rows.
	WorkflowIDs []string             `json:"workflow_ids"`
	Phases      []createPhaseRequest `json:"phases"`
}

// experimentDetailResponse is the GET /experiments/{id} payload — the full
// plan: experiment row + associated workflow ids + phase rows (each with
// their per-(phase, workflow) configs and rule ids).
type experimentDetailResponse struct {
	*model.Experiment
	WorkflowIDs []string      `json:"workflow_ids"`
	Phases      []phaseDetail `json:"phases"`
}

type phaseDetail struct {
	*model.ExperimentPhase
	Workflows []model.PhaseWorkflow `json:"workflows"`
	RuleIDs   []string              `json:"rule_ids"`
}

func (s *Server) handleCreateExperiment(w http.ResponseWriter, r *http.Request) {
	var req createExperimentRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name required")
		return
	}

	exp := &model.Experiment{
		ID:          req.ID,
		Name:        req.Name,
		Description: req.Description,
		Hypothesis:  req.Hypothesis,
		Status:      "planned",
		CreatedBy:   req.CreatedBy,
		CreatedAt:   time.Now(),
	}
	if exp.ID == "" {
		exp.ID = generateID("exp")
	}

	ctx := r.Context()
	if err := s.experiments.Create(ctx, exp); err != nil {
		s.logger.Error("create experiment failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.WorkflowIDs) > 0 {
		if err := s.experiments.AttachWorkflows(ctx, exp.ID, req.WorkflowIDs); err != nil {
			s.logger.Error("attach workflows failed", "error", err, "experiment_id", exp.ID)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	for i, ph := range req.Phases {
		phase := &model.ExperimentPhase{
			ID:             generateID("phase"),
			ExperimentID:   exp.ID,
			Name:           ph.Name,
			Position:       ph.Position,
			Status:         "pending",
			FrozenServices: ph.FrozenServices,
			PersistCache:   ph.PersistCache,
		}
		if phase.Name == "" {
			writeError(w, http.StatusBadRequest, "phase name required")
			return
		}
		if err := s.experiments.CreatePhase(ctx, phase); err != nil {
			s.logger.Error("create phase failed", "error", err, "phase_index", i)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if len(ph.Workflows) > 0 {
			if err := s.experiments.AttachPhaseWorkflows(ctx, phase.ID, ph.Workflows); err != nil {
				s.logger.Error("attach phase workflows failed", "error", err, "phase_id", phase.ID)
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		if len(ph.RuleIDs) > 0 {
			if err := s.experiments.AttachPhaseRules(ctx, phase.ID, ph.RuleIDs); err != nil {
				s.logger.Error("attach phase rules failed", "error", err, "phase_id", phase.ID)
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
	}

	resp, err := s.composeExperimentDetail(ctx, exp.ID)
	if err != nil {
		s.logger.Error("compose detail failed", "error", err)
		writeError(w, http.StatusInternalServerError, "experiment created but detail unavailable")
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleListExperiments(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	filter := store.ExperimentFilter{
		Status: r.URL.Query().Get("status"),
	}
	exps, total, err := s.experiments.List(r.Context(),
		filter, store.Page{Limit: limit, Offset: offset})
	if err != nil {
		s.logger.Error("list experiments failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list experiments")
		return
	}
	writePage(w, http.StatusOK, exps, total, limit, offset)
}

func (s *Server) handleGetExperiment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	resp, err := s.composeExperimentDetail(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "experiment not found")
		return
	}
	if err != nil {
		s.logger.Error("get experiment failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get experiment")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleDeleteExperiment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.experiments.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "experiment not found")
			return
		}
		s.logger.Error("delete experiment failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete experiment")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStartExperiment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.orch.StartExperiment(r.Context(), id); err != nil {
		s.logger.Error("start experiment failed", "error", err, "experiment_id", id)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.composeExperimentDetail(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "experiment started but detail unavailable")
		return
	}
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) handleStopExperiment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	finalStatus := r.URL.Query().Get("status")
	if err := s.orch.StopExperiment(r.Context(), id, finalStatus); err != nil {
		s.logger.Error("stop experiment failed", "error", err, "experiment_id", id)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// =========================================================================
// Phase CRUD + lifecycle
// =========================================================================

func (s *Server) handleCreatePhase(w http.ResponseWriter, r *http.Request) {
	expID := r.PathValue("id")
	var req createPhaseRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name required")
		return
	}

	ctx := r.Context()
	// Allow caller to omit position; we'll append.
	pos := req.Position
	if pos == 0 {
		next, err := s.experiments.NextPhasePosition(ctx, expID)
		if err == nil {
			pos = next
		}
	}

	phase := &model.ExperimentPhase{
		ID:             generateID("phase"),
		ExperimentID:   expID,
		Name:           req.Name,
		Position:       pos,
		Status:         "pending",
		FrozenServices: req.FrozenServices,
		PersistCache:   req.PersistCache,
	}
	if err := s.experiments.CreatePhase(ctx, phase); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Workflows) > 0 {
		if err := s.experiments.AttachPhaseWorkflows(ctx, phase.ID, req.Workflows); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if len(req.RuleIDs) > 0 {
		if err := s.experiments.AttachPhaseRules(ctx, phase.ID, req.RuleIDs); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusCreated, s.composePhaseDetail(ctx, phase))
}

func (s *Server) handleGetPhase(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("phaseId")
	phase, err := s.experiments.GetPhase(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "phase not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get phase")
		return
	}
	writeJSON(w, http.StatusOK, s.composePhaseDetail(r.Context(), phase))
}

func (s *Server) handleDeletePhase(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("phaseId")
	if err := s.experiments.DeletePhase(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "phase not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete phase")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStartPhase(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("phaseId")
	if err := s.orch.StartPhase(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleStopPhase(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("phaseId")
	finalStatus := r.URL.Query().Get("status")
	if err := s.orch.StopPhase(r.Context(), id, finalStatus); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// =========================================================================
// Results
// =========================================================================

type phaseResultsResponse struct {
	WorkflowResults  []*model.PhaseWorkflowResult   `json:"workflow_results"`
	ServiceLatency   []*model.PhaseServiceLatency   `json:"service_latency"`
	ServiceResources []*model.PhaseServiceResources `json:"service_resources"`
	ServiceCache     []*model.PhaseServiceCache     `json:"service_cache"`
}

func (s *Server) handlePhaseResults(w http.ResponseWriter, r *http.Request) {
	phaseID := r.PathValue("phaseId")
	ctx := r.Context()

	wfRes, err := s.experiments.ListWorkflowResultsForPhase(ctx, phaseID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load workflow results")
		return
	}
	svcLat, err := s.experiments.ListServiceLatencyForPhase(ctx, phaseID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load service latency")
		return
	}
	svcRes, err := s.experiments.ListServiceResourcesForPhase(ctx, phaseID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load service resources")
		return
	}
	svcCache, err := s.experiments.ListServiceCacheForPhase(ctx, phaseID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load service cache")
		return
	}

	writeJSON(w, http.StatusOK, phaseResultsResponse{
		WorkflowResults:  nilToEmpty(wfRes),
		ServiceLatency:   nilToEmpty(svcLat),
		ServiceResources: nilToEmpty(svcRes),
		ServiceCache:     nilToEmpty(svcCache),
	})
}

func (s *Server) handleExperimentResults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	res, err := s.experiments.RecomputeExperimentResults(ctx, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to compute results")
		return
	}
	if res == nil {
		// No measurements yet — return empty but valid envelope.
		writeJSON(w, http.StatusOK, map[string]any{
			"experiment_id": id,
			"available":     false,
		})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// =========================================================================
// helpers
// =========================================================================

// composeExperimentDetail loads the full plan view (experiment + workflows
// + phases each with their workflows + rules). Returns ErrNotFound on miss.
func (s *Server) composeExperimentDetail(ctx context.Context, id string) (*experimentDetailResponse, error) {
	exp, err := s.experiments.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	wfs, err := s.experiments.ListWorkflowsForExperiment(ctx, id)
	if err != nil {
		return nil, err
	}
	phases, err := s.experiments.ListPhasesForExperiment(ctx, id)
	if err != nil {
		return nil, err
	}
	out := &experimentDetailResponse{Experiment: exp, WorkflowIDs: workflowIDsOf(wfs)}
	out.Phases = make([]phaseDetail, 0, len(phases))
	for _, p := range phases {
		out.Phases = append(out.Phases, *s.composePhaseDetail(ctx, p))
	}
	return out, nil
}

func (s *Server) composePhaseDetail(ctx context.Context, p *model.ExperimentPhase) *phaseDetail {
	pws, _ := s.experiments.ListPhaseWorkflows(ctx, p.ID)
	prs, _ := s.experiments.ListPhaseRules(ctx, p.ID)
	ruleIDs := make([]string, len(prs))
	for i, pr := range prs {
		ruleIDs[i] = pr.RuleID
	}
	return &phaseDetail{ExperimentPhase: p, Workflows: pws, RuleIDs: ruleIDs}
}

func workflowIDsOf(rows []model.ExperimentWorkflow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.WorkflowID
	}
	return out
}

// nilToEmpty replaces a nil slice with an empty slice so the JSON
// encoder emits `[]` rather than `null`.
func nilToEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
