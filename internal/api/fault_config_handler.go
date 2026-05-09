package api

import (
	"errors"
	"net/http"

	"manteion-go/internal/model"
	"manteion-go/internal/store"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
	"github.com/google/uuid"
)

// handleCreateFaultConfig creates a new fault config.
func (s *Server) handleCreateFaultConfig(w http.ResponseWriter, r *http.Request) {
	var cfg model.FaultConfig
	if err := readJSON(r, &cfg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if cfg.ID == "" {
		cfg.ID = "fc-" + uuid.NewString()[:8]
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

// handleListFaultConfigs lists all fault configs.
func (s *Server) handleListFaultConfigs(w http.ResponseWriter, r *http.Request) {
	filters := store.ListFaultConfigFilters{
		Service: r.URL.Query().Get("service"),
		Status:  model.FaultConfigStatus(r.URL.Query().Get("status")),
	}

	configs, err := s.faultConfigs.List(r.Context(), filters)
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

// handleDeleteFaultConfig deletes a fault config.
func (s *Server) handleDeleteFaultConfig(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.faultConfigs.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "fault config not found")
			return
		}
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "cannot delete active fault config")
			return
		}
		s.logger.Error("delete fault config failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete fault config")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleFireFaultConfig triggers an inactive fault config.
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

	// Check conflict
	hasConflict, err := s.faultConfigs.HasActiveConflict(ctx, cfg.Service, cfg.Category, cfg.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to check conflict")
		return
	}
	if hasConflict {
		writeError(w, http.StatusConflict, "an active fault already exists for this service and category")
		return
	}

	// Prepare the SDK payload
	req := atroposdk.FaultRequest{
		Category:   cfg.Category,
		Type:       cfg.FaultType,
		DurationMs: cfg.DurationMs,
	}

	if cfg.FaultCompositionID != nil {
		// Needs to resolve the composition and compile it. For now, DemoEvaluator doesn't evaluate compositions.
		writeError(w, http.StatusBadRequest, "compositions are not supported for manual persistent faults yet")
		return
	} else if len(cfg.FaultReq) > 0 {
		// Need to unmarshal inner Config so we can set it
		// The model says `cfg.FaultReq` is a json.RawMessage representing the inner `Config`
		// Wait, the client expects the config shape
		req.Config = cfg.FaultReq
	}

	// Actually mark fired in DB
	if err := s.faultConfigs.MarkFired(ctx, id); err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "fault config became active concurrently")
			return
		}
		s.logger.Error("mark fault fired failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to mark fault fired")
		return
	}

	// Inject to instances via atrocontrol
	res, err := s.controller.InjectFault(ctx, cfg.Service, req)
	if err != nil {
		s.logger.Error("inject fault fanout failed", "id", id, "error", err)
		// We could rollback the db status here, or let the reaper handle it as a failure.
	}

	s.logger.Info("fault config fired", "id", id, "service", cfg.Service, "successes", len(res.OK))
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "active",
		"successes": len(res.OK),
		"failures":  len(res.Failed),
	})
}

// handleCancelFaultConfig cancels an active fault config.
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

	// Mark canceled in DB
	if err := s.faultConfigs.MarkManuallyCancelled(ctx, id); err != nil {
		s.logger.Error("cancel fault failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel fault")
		return
	}

	// Clear from instances via atrocontrol
	res, err := s.controller.ClearFault(ctx, cfg.Service, cfg.Category)
	if err != nil {
		s.logger.Error("clear fault fanout failed", "id", id, "error", err)
	}

	s.logger.Info("fault config cancelled", "id", id, "service", cfg.Service, "successes", len(res.OK))
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "cancelled",
		"successes": len(res.OK),
		"failures":  len(res.Failed),
	})
}
