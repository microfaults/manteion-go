package api

import (
	"fmt"
	"net/http"
	"time"
)

// handleCreateRule creates a new fault injection rule.
func (s *Server) handleCreateRule(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	// Generate a stub ID and timestamps.
	body["id"] = fmt.Sprintf("rule-%d", time.Now().UnixNano())
	body["created_at"] = time.Now()
	body["updated_at"] = time.Now()

	s.logger.Info("rule created (stub)", "id", body["id"])

	// TODO: Validate with model.Rule.Validate(), persist via rule.Store.Add()
	writeJSON(w, http.StatusCreated, body)
}

// handleListRules returns all rules.
func (s *Server) handleListRules(w http.ResponseWriter, r *http.Request) {
	// TODO: Wire to rule.Store.List()
	writeJSON(w, http.StatusOK, []any{})
}

// handleGetRule returns a single rule by ID.
func (s *Server) handleGetRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.logger.Info("get rule (stub)", "id", id)

	// TODO: Wire to rule.Store.Get(id)
	writeError(w, http.StatusNotFound, fmt.Sprintf("rule %q not found", id))
}

// handleUpdateRule updates an existing rule by ID.
func (s *Server) handleUpdateRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.logger.Info("update rule (stub)", "id", id)

	// TODO: Wire to rule.Store.Update()
	writeError(w, http.StatusNotFound, fmt.Sprintf("rule %q not found", id))
}

// handleDeleteRule deletes a rule by ID.
func (s *Server) handleDeleteRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.logger.Info("delete rule (stub)", "id", id)

	// TODO: Wire to rule.Store.Delete(id)
	writeError(w, http.StatusNotFound, fmt.Sprintf("rule %q not found", id))
}
