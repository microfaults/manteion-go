package api

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreateRule creates a new fault injection rule.
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
	s.broadcastRulesChanged(r, rule.Service)
	writeJSON(w, http.StatusCreated, rule)
}

// handleListRules returns all rules.
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
	s.broadcastRulesChanged(r, rule.Service)
	writeJSON(w, http.StatusOK, rule)
}

// handleDeleteRule deletes a rule by ID.
func (s *Server) handleDeleteRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Fetch service before deleting so we can broadcast to the right subscribers.
	existing, err := s.rules.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "rule not found")
		return
	}
	if err != nil {
		s.logger.Error("get rule for delete failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete rule")
		return
	}
	service := existing.Service

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
	s.broadcastRulesChanged(r, service)
	w.WriteHeader(http.StatusNoContent)
}

// broadcastRulesChanged reads the current rule version and broadcasts a
// rules_changed SSE event to all subscribers for service. Best-effort: if
// Version() fails, no event is sent (the SDK will still catch up on next poll).
func (s *Server) broadcastRulesChanged(r *http.Request, service string) {
	if s.broker == nil {
		return
	}
	newVersion, err := s.rules.Version(r.Context())
	if err != nil {
		s.logger.Warn("broadcastRulesChanged: read version failed", "error", err)
		return
	}
	s.broker.Broadcast(service, Event{
		Type: "rules_changed",
		Data: fmt.Sprintf(`{"version":%d}`, newVersion),
	})
}

// generateID creates a prefixed random hex ID.
func generateID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}
