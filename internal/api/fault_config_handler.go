package api

import (
	"errors"
	"net/http"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreateFaultConfig registers a long-running fault in the "ready" state.
// It is not applied to any instance until fired.
func (s *Server) handleCreateFaultConfig(w http.ResponseWriter, r *http.Request) {
	var cfg model.FaultConfig
	if err := readJSON(r, &cfg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if cfg.ID == "" {
		cfg.ID = generateID("fc")
	}
	cfg.Status = model.FaultConfigReady

	if err := cfg.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.faultConfigs.Create(r.Context(), &cfg); err != nil {
		s.logger.Error("create fault config failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create fault config")
		return
	}
	s.logger.Info("fault config created", "id", cfg.ID, "service", cfg.Service)
	writeJSON(w, http.StatusCreated, cfg)
}

// handleListFaultConfigs lists fault configs, optionally filtered by ?service
// and ?status.
func (s *Server) handleListFaultConfigs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	configs, err := s.faultConfigs.List(r.Context(), q.Get("service"), model.FaultConfigStatus(q.Get("status")))
	if err != nil {
		s.logger.Error("list fault configs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list fault configs")
		return
	}
	if configs == nil {
		configs = []*model.FaultConfig{}
	}
	writeJSON(w, http.StatusOK, configs)
}

// handleDeleteFaultConfig removes a fault config. Deleting an active config
// stops it: the next poll omits it and the SDK reconciles it away.
func (s *Server) handleDeleteFaultConfig(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.faultConfigs.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "fault config not found")
			return
		}
		s.logger.Error("delete fault config failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete fault config")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleFireFaultConfig activates a fault config. It only flips DB state;
// instances pick the fault up on their next poll (the SDK reconciles the
// poll's active_faults set), so there is no push fanout to fail or roll back.
func (s *Server) handleFireFaultConfig(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()

	cfg, err := s.faultConfigs.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "fault config not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to read fault config")
		return
	}
	if !cfg.CanFire() {
		writeError(w, http.StatusConflict, "fault config is already active")
		return
	}
	if cfg.FaultCompositionID != nil {
		writeError(w, http.StatusBadRequest, "long-running composition faults are not supported yet; use a direct fault_request")
		return
	}

	if err := s.faultConfigs.MarkFired(ctx, id); err != nil {
		s.logger.Error("fire fault config failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to fire fault config")
		return
	}
	// Bump the desired-state version so the next poll returns 200 and the SDK
	// reconciles the new fault into its active set.
	if err := s.rules.BumpVersion(ctx); err != nil {
		s.logger.Warn("fire fault config: bump version failed", "id", id, "error", err)
	}
	s.logger.Info("fault config fired", "id", id, "service", cfg.Service)

	fired, err := s.faultConfigs.Get(ctx, id)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": model.FaultConfigActive})
		return
	}
	writeJSON(w, http.StatusOK, fired)
}

// handleCancelFaultConfig deactivates an active fault config. As with delete,
// the SDK reconciles the fault away on its next poll.
func (s *Server) handleCancelFaultConfig(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()

	cfg, err := s.faultConfigs.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "fault config not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to read fault config")
		return
	}
	if cfg.Status != model.FaultConfigActive {
		writeError(w, http.StatusConflict, "fault config is not active")
		return
	}
	if err := s.faultConfigs.MarkCancelled(ctx, id); err != nil {
		s.logger.Error("cancel fault config failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel fault config")
		return
	}
	if err := s.rules.BumpVersion(ctx); err != nil {
		s.logger.Warn("cancel fault config: bump version failed", "id", id, "error", err)
	}
	s.logger.Info("fault config cancelled", "id", id, "service", cfg.Service)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": model.FaultConfigCancelled})
}
