package api

import (
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreatePolicy creates a new standing PolicyRule.
func (s *Server) handleCreatePolicy(w http.ResponseWriter, r *http.Request) {
	var rule model.PolicyRule
	if err := readJSON(r, &rule); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if rule.ID == "" {
		rule.ID = generateID("policy")
	}
	rule.CreatedAt = time.Now()

	if err := s.policies.Create(r.Context(), &rule); err != nil {
		s.logger.Error("create policy failed", "id", rule.ID, "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.logger.Info("policy created", "id", rule.ID, "name", rule.Name)
	writeJSON(w, http.StatusCreated, rule)
}

// handleListPolicies returns all policy rules.
func (s *Server) handleListPolicies(w http.ResponseWriter, r *http.Request) {
	rules, err := s.policies.List(r.Context())
	if err != nil {
		s.logger.Error("list policies failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list policies")
		return
	}
	if rules == nil {
		rules = []*model.PolicyRule{}
	}
	writeJSON(w, http.StatusOK, rules)
}

// handleGetPolicy returns a single policy rule by ID.
func (s *Server) handleGetPolicy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rule, err := s.policies.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "policy not found")
		return
	}
	if err != nil {
		s.logger.Error("get policy failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get policy")
		return
	}
	writeJSON(w, http.StatusOK, rule)
}

// handleDeletePolicy deletes a policy rule by ID.
func (s *Server) handleDeletePolicy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.policies.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "policy not found")
			return
		}
		s.logger.Error("delete policy failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete policy")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleEnablePolicy sets enabled=true on a policy rule.
func (s *Server) handleEnablePolicy(w http.ResponseWriter, r *http.Request) {
	s.setPolicyEnabled(w, r, true)
}

// handleDisablePolicy sets enabled=false on a policy rule.
func (s *Server) handleDisablePolicy(w http.ResponseWriter, r *http.Request) {
	s.setPolicyEnabled(w, r, false)
}

func (s *Server) setPolicyEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	id := r.PathValue("id")
	if err := s.policies.SetEnabled(r.Context(), id, enabled); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "policy not found")
			return
		}
		s.logger.Error("set policy enabled failed", "id", id, "enabled", enabled, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to update policy")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
