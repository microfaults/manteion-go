package api

import (
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// --- Experiment CRUD ---

func (s *Server) handleCreateExperiment(w http.ResponseWriter, r *http.Request) {
	var exp model.Experiment
	if err := readJSON(r, &exp); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if exp.ID == "" {
		exp.ID = generateID("exp")
	}
	exp.CreatedAt = time.Now()

	if err := s.experiments.Create(r.Context(), &exp); err != nil {
		s.logger.Error("create experiment failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, exp)
}

func (s *Server) handleListExperiments(w http.ResponseWriter, r *http.Request) {
	exps, err := s.experiments.List(r.Context())
	if err != nil {
		s.logger.Error("list experiments failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list experiments")
		return
	}
	if exps == nil {
		exps = []*model.Experiment{}
	}
	writeJSON(w, http.StatusOK, exps)
}

func (s *Server) handleGetExperiment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	exp, err := s.experiments.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "experiment not found")
		return
	}
	if err != nil {
		s.logger.Error("get experiment failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get experiment")
		return
	}
	writeJSON(w, http.StatusOK, exp)
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

// --- ExperimentRun CRUD + lifecycle ---

func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	expID := r.PathValue("id")
	var run model.ExperimentRun
	if err := readJSON(r, &run); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if run.ID == "" {
		run.ID = generateID("run")
	}
	run.ExperimentID = expID
	run.CreatedAt = time.Now()

	// Verify all referenced rule IDs exist.
	ctx := r.Context()
	for _, ps := range run.PhaseRules {
		for _, ruleID := range ps.RuleIDs {
			if _, err := s.rules.Get(ctx, ruleID); err != nil {
				if errors.Is(err, store.ErrNotFound) {
					writeError(w, http.StatusUnprocessableEntity, "rule not found: "+ruleID)
					return
				}
				s.logger.Error("check rule existence failed", "rule_id", ruleID, "error", err)
				writeError(w, http.StatusInternalServerError, "failed to verify rule")
				return
			}
		}
	}

	if err := s.experiments.CreateRun(ctx, &run); err != nil {
		s.logger.Error("create run failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	expID := r.PathValue("id")
	runs, err := s.experiments.ListRunsByExperiment(r.Context(), expID)
	if err != nil {
		s.logger.Error("list runs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list runs")
		return
	}
	if runs == nil {
		runs = []*model.ExperimentRun{}
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runId")
	run, err := s.experiments.GetRun(r.Context(), runID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		s.logger.Error("get run failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get run")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleStartRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runId")
	if err := s.orch.StartRun(r.Context(), runID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		s.logger.Error("start run failed", "run_id", runID, "error", err)
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (s *Server) handleStopRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runId")
	if err := s.orch.StopRun(r.Context(), runID, "completed"); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		s.logger.Error("stop run failed", "run_id", runID, "error", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (s *Server) handlePauseRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runId")
	if err := s.orch.PauseRun(r.Context(), runID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		s.logger.Error("pause run failed", "run_id", runID, "error", err)
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *Server) handleResumeRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runId")
	if err := s.orch.ResumeRun(r.Context(), runID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		s.logger.Error("resume run failed", "run_id", runID, "error", err)
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (s *Server) handleStartExperiment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.orch.StartExperiment(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "experiment not found")
			return
		}
		s.logger.Error("start experiment failed", "experiment_id", id, "error", err)
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (s *Server) handleListRunResults(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runId")
	results, err := s.experiments.ListWorkflowResults(r.Context(), runID)
	if err != nil {
		s.logger.Error("list run results failed", "run_id", runID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list results")
		return
	}
	if results == nil {
		results = []*model.WorkflowRunResult{}
	}
	writeJSON(w, http.StatusOK, results)
}

func (s *Server) handleListContributions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	contributions, err := s.experiments.ListContributions(r.Context(), id)
	if err != nil {
		s.logger.Error("list contributions failed", "experiment_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list contributions")
		return
	}
	if contributions == nil {
		contributions = []*model.ContributionResult{}
	}
	writeJSON(w, http.StatusOK, contributions)
}
