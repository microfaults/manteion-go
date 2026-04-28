package api

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreateRule creates a new fault injection rule.
//
// @Summary      Create rule
// @Description  Persist a new rule. The store layer validates the rule per
// @Description  model.Rule.Validate() (id/name/service required, exactly one of
// @Description  fault_spec_id or fault_composition_id, mode in {inline,background}).
// @Tags         rules
// @Accept       json
// @Produce      json
// @Param        rule  body      model.Rule  true  "Rule definition"
// @Success      201   {object}  model.Rule
// @Failure      400   {object}  api.ErrorResponse  "validation error or infrastructure failure (currently collapsed; see openapi-conventions.md known gaps)"
// @Router       /rules [post]
func (s *Server) handleCreateRule(w http.ResponseWriter, r *http.Request) {
	var rule model.Rule
	if err := readJSON(r, &rule); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	// Auto-generate ID and timestamps if not provided.
	if rule.ID == "" {
		rule.ID = generateID("rule")
	}
	now := time.Now()
	rule.CreatedAt = now
	rule.UpdatedAt = now

	if err := s.rules.Create(r.Context(), &rule); err != nil {
		s.logger.Error("create rule failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	s.logger.Info("rule created", "id", rule.ID, "service", rule.Service)
	writeJSON(w, http.StatusCreated, rule)
}

// handleListRules returns all rules.
//
// @Summary      List rules
// @Description  Returns all configured rules, ordered by priority descending.
// @Tags         rules
// @Produce      json
// @Success      200  {array}   model.Rule
// @Failure      500  {object}  api.ErrorResponse  "internal error"
// @Router       /rules [get]
func (s *Server) handleListRules(w http.ResponseWriter, r *http.Request) {
	rules, err := s.rules.List(r.Context())
	if err != nil {
		s.logger.Error("list rules failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list rules")
		return
	}
	if rules == nil {
		rules = []*model.Rule{}
	}
	writeJSON(w, http.StatusOK, rules)
}

// handleGetRule returns a single rule by ID.
//
// @Summary      Get rule
// @Tags         rules
// @Produce      json
// @Param        id   path      string  true  "Rule ID"
// @Success      200  {object}  model.Rule
// @Failure      404  {object}  api.ErrorResponse  "rule not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /rules/{id} [get]
func (s *Server) handleGetRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rule, err := s.rules.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "rule not found")
		return
	}
	if err != nil {
		s.logger.Error("get rule failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get rule")
		return
	}
	writeJSON(w, http.StatusOK, rule)
}

// handleUpdateRule updates an existing rule by ID.
//
// @Summary      Update rule
// @Description  Replace an existing rule. The id in the path overrides any id in the body.
// @Tags         rules
// @Accept       json
// @Produce      json
// @Param        id    path      string      true  "Rule ID"
// @Param        rule  body      model.Rule  true  "Updated rule"
// @Success      200   {object}  model.Rule
// @Failure      400   {object}  api.ErrorResponse  "invalid JSON or validation error"
// @Failure      404   {object}  api.ErrorResponse  "rule not found"
// @Router       /rules/{id} [put]
func (s *Server) handleUpdateRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var rule model.Rule
	if err := readJSON(r, &rule); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	rule.ID = id
	rule.UpdatedAt = time.Now()

	if err := s.rules.Update(r.Context(), &rule); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "rule not found")
			return
		}
		s.logger.Error("update rule failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	s.logger.Info("rule updated", "id", id)
	writeJSON(w, http.StatusOK, rule)
}

// handleDeleteRule deletes a rule by ID.
//
// @Summary      Delete rule
// @Tags         rules
// @Param        id   path  string  true  "Rule ID"
// @Success      204  "rule deleted"
// @Failure      404  {object}  api.ErrorResponse  "rule not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /rules/{id} [delete]
func (s *Server) handleDeleteRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if err := s.rules.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "rule not found")
			return
		}
		s.logger.Error("delete rule failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete rule")
		return
	}

	s.logger.Info("rule deleted", "id", id)
	w.WriteHeader(http.StatusNoContent)
}

// generateID creates a prefixed random hex ID.
func generateID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}
