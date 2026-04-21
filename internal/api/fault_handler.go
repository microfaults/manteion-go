package api

import (
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreateFaultSpec creates a new fault spec.
func (s *Server) handleCreateFaultSpec(w http.ResponseWriter, r *http.Request) {
	var spec model.FaultSpec
	if err := readJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if spec.ID == "" {
		spec.ID = generateID("spec")
	}
	if spec.CreatedAt.IsZero() {
		spec.CreatedAt = time.Now()
	}

	if err := s.faultStore.CreateSpec(r.Context(), &spec); err != nil {
		s.logger.Error("create fault spec failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.logger.Info("fault spec created", "id", spec.ID, "category", spec.Category, "type", spec.FaultType)
	writeJSON(w, http.StatusCreated, spec)
}

// handleListFaultSpecs returns all fault specs.
func (s *Server) handleListFaultSpecs(w http.ResponseWriter, r *http.Request) {
	specs, err := s.faultStore.ListSpecs(r.Context())
	if err != nil {
		s.logger.Error("list fault specs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list specs")
		return
	}
	if specs == nil {
		specs = []*model.FaultSpec{}
	}
	writeJSON(w, http.StatusOK, specs)
}

// handleGetFaultSpec returns a single spec by id.
func (s *Server) handleGetFaultSpec(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	spec, err := s.faultStore.GetSpec(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "spec not found")
			return
		}
		s.logger.Error("get fault spec failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get spec")
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

// handleDeleteFaultSpec removes a fault spec.
func (s *Server) handleDeleteFaultSpec(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.faultStore.DeleteSpec(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "spec not found")
			return
		}
		s.logger.Error("delete fault spec failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete spec")
		return
	}
	s.logger.Info("fault spec deleted", "id", id)
	w.WriteHeader(http.StatusNoContent)
}

// handleCreateFaultComposition creates a composition after running the full
// model.ValidateComposition (depth, direction, incompatibilities) against
// current repo contents. This closes A13 — the repo's basic Validate is NOT
// enough, it doesn't catch depth/incompat violations because those need
// resolvers.
func (s *Server) handleCreateFaultComposition(w http.ResponseWriter, r *http.Request) {
	var comp model.FaultComposition
	if err := readJSON(r, &comp); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if comp.ID == "" {
		comp.ID = generateID("comp")
	}
	if comp.CreatedAt.IsZero() {
		comp.CreatedAt = time.Now()
	}

	ctx := r.Context()
	specResolver := s.faultStore.SpecResolver(ctx)
	compResolver := s.faultStore.CompositionResolver(ctx)

	if err := model.ValidateComposition(&comp, specResolver, compResolver); err != nil {
		s.logger.Warn("composition validation failed", "id", comp.ID, "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.faultStore.CreateComposition(ctx, &comp); err != nil {
		s.logger.Error("create composition failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.logger.Info("composition created", "id", comp.ID, "mode", comp.ExecutionMode, "members", len(comp.Members))
	writeJSON(w, http.StatusCreated, comp)
}

func (s *Server) handleListFaultCompositions(w http.ResponseWriter, r *http.Request) {
	comps, err := s.faultStore.ListCompositions(r.Context())
	if err != nil {
		s.logger.Error("list compositions failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list compositions")
		return
	}
	if comps == nil {
		comps = []*model.FaultComposition{}
	}
	writeJSON(w, http.StatusOK, comps)
}

func (s *Server) handleGetFaultComposition(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	comp, err := s.faultStore.GetComposition(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "composition not found")
			return
		}
		s.logger.Error("get composition failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get composition")
		return
	}
	writeJSON(w, http.StatusOK, comp)
}

func (s *Server) handleDeleteFaultComposition(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.faultStore.DeleteComposition(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "composition not found")
			return
		}
		s.logger.Error("delete composition failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete composition")
		return
	}
	s.logger.Info("composition deleted", "id", id)
	w.WriteHeader(http.StatusNoContent)
}
