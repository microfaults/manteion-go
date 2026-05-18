package api

import (
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreateFaultSpec creates a new fault spec.
//
// @Summary      Create fault spec
// @Description  Persist an atomic fault definition. The body is validated per
// @Description  model.FaultSpec.Validate() — category in {inline,network,resource},
// @Description  fault_type matches the category, and config is non-empty.
// @Tags         faults
// @Accept       json
// @Produce      json
// @Param        spec  body      model.FaultSpec  true  "Fault spec definition"
// @Success      201   {object}  model.FaultSpec
// @Failure      400   {object}  api.ErrorResponse  "validation error"
// @Failure      500   {object}  api.ErrorResponse
// @Router       /faults/specs [post]
func (s *Server) handleCreateFaultSpec(w http.ResponseWriter, r *http.Request) {
	var spec model.FaultSpec
	if err := readJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if spec.ID == "" {
		spec.ID = generateID("spec")
	}
	spec.CreatedAt = time.Now()

	if err := spec.Validate(); err != nil {
		s.logger.Warn("fault spec validation failed", "id", spec.ID, "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.faultStore.CreateSpec(r.Context(), &spec); err != nil {
		s.logger.Error("create fault spec failed", "id", spec.ID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create spec")
		return
	}
	s.logger.Info("fault spec created", "id", spec.ID, "category", spec.Category, "type", spec.FaultType)
	writeJSON(w, http.StatusCreated, spec)
}

// handleListFaultSpecs returns all fault specs.
//
// @Summary      List fault specs
// @Description  Returns all fault specs in creation order.
// @Tags         faults
// @Produce      json
// @Success      200  {array}   model.FaultSpec
// @Failure      500  {object}  api.ErrorResponse
// @Router       /faults/specs [get]
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
//
// @Summary      Get fault spec
// @Tags         faults
// @Produce      json
// @Param        id   path      string  true  "Fault spec ID"
// @Success      200  {object}  model.FaultSpec
// @Failure      404  {object}  api.ErrorResponse  "spec not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /faults/specs/{id} [get]
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

// handleUpdateFaultSpec replaces all mutable fields of an existing fault spec.
// The id in the path overrides any id in the body; created_at is preserved
// from the stored record (not updated). Mirrors handleCreateFaultSpec's
// validation pattern.
//
// @Summary      Update fault spec
// @Description  Replace all mutable fields of an existing fault spec. The body
// @Description  is validated identically to creation; id is taken from the URL
// @Description  and created_at is preserved from the stored record.
// @Tags         faults
// @Accept       json
// @Produce      json
// @Param        id    path      string           true  "Fault spec ID"
// @Param        spec  body      model.FaultSpec  true  "Updated fault spec"
// @Success      200   {object}  model.FaultSpec
// @Failure      400   {object}  api.ErrorResponse  "invalid JSON or validation error"
// @Failure      404   {object}  api.ErrorResponse  "spec not found"
// @Failure      500   {object}  api.ErrorResponse
// @Router       /faults/specs/{id} [put]
func (s *Server) handleUpdateFaultSpec(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var spec model.FaultSpec
	if err := readJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	spec.ID = id

	if err := spec.Validate(); err != nil {
		s.logger.Warn("fault spec validation failed", "id", spec.ID, "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.faultStore.UpdateSpec(r.Context(), &spec); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "spec not found")
			return
		}
		s.logger.Error("update fault spec failed", "id", spec.ID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to update spec")
		return
	}
	s.logger.Info("fault spec updated", "id", spec.ID, "category", spec.Category, "type", spec.FaultType)
	writeJSON(w, http.StatusOK, spec)
}

// handleDeleteFaultSpec removes a fault spec.
//
// @Summary      Delete fault spec
// @Tags         faults
// @Param        id   path  string  true  "Fault spec ID"
// @Success      204  "spec deleted"
// @Failure      404  {object}  api.ErrorResponse  "spec not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /faults/specs/{id} [delete]
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
//
// @Summary      Create fault composition
// @Description  Persist a composition (parallel or sequential group of fault members).
// @Description  Validates depth (max 3), per-member directions, and pairwise fault
// @Description  incompatibilities against the current spec/composition repos.
// @Tags         faults
// @Accept       json
// @Produce      json
// @Param        composition  body      model.FaultComposition  true  "Composition definition"
// @Success      201          {object}  model.FaultComposition
// @Failure      400          {object}  api.ErrorResponse  "validation error (incl. depth or incompatibility)"
// @Failure      500          {object}  api.ErrorResponse
// @Router       /faults/compositions [post]
func (s *Server) handleCreateFaultComposition(w http.ResponseWriter, r *http.Request) {
	var comp model.FaultComposition
	if err := readJSON(r, &comp); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if comp.ID == "" {
		comp.ID = generateID("comp")
	}
	comp.CreatedAt = time.Now()

	ctx := r.Context()
	specResolver := s.faultStore.SpecResolver(ctx)
	compResolver := s.faultStore.CompositionResolver(ctx)

	if err := model.ValidateComposition(&comp, specResolver, compResolver); err != nil {
		s.logger.Warn("composition validation failed", "id", comp.ID, "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.faultStore.CreateComposition(ctx, &comp); err != nil {
		s.logger.Error("create composition failed", "id", comp.ID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create composition")
		return
	}
	s.logger.Info("composition created", "id", comp.ID, "mode", comp.ExecutionMode, "members", len(comp.Members))
	writeJSON(w, http.StatusCreated, comp)
}

// handleListFaultCompositions returns all fault compositions.
//
// @Summary      List fault compositions
// @Description  Returns all fault compositions in creation order.
// @Tags         faults
// @Produce      json
// @Success      200  {array}   model.FaultComposition
// @Failure      500  {object}  api.ErrorResponse
// @Router       /faults/compositions [get]
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

// handleGetFaultComposition returns a single composition by id.
//
// @Summary      Get fault composition
// @Tags         faults
// @Produce      json
// @Param        id   path      string  true  "Fault composition ID"
// @Success      200  {object}  model.FaultComposition
// @Failure      404  {object}  api.ErrorResponse  "composition not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /faults/compositions/{id} [get]
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

// handleDeleteFaultComposition removes a fault composition.
//
// @Summary      Delete fault composition
// @Tags         faults
// @Param        id   path  string  true  "Fault composition ID"
// @Success      204  "composition deleted"
// @Failure      404  {object}  api.ErrorResponse  "composition not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /faults/compositions/{id} [delete]
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
