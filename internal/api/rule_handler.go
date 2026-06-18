package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreateRule creates a new rule (fault injection or cache-box).
//
// @Summary      Create rule
// @Description  Persist a new rule. Action.Type discriminates between fault_spec,
// @Description  fault_composition, and cachebox — exactly one payload must be set.
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
	s.broadcastRulesChanged(r.Context(), rule.Service)
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
	s.broadcastRulesChanged(r.Context(), rule.Service)
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
	s.broadcastRulesChanged(r.Context(), service)
	w.WriteHeader(http.StatusNoContent)
}

// broadcastRulesChanged reads the current rule version and pushes a
// rules_changed SSE event to every subscriber of each given service.
// Best-effort: if the broker is absent or Version() fails, no event is sent
// (SDKs still catch up on their next poll). This is the broadcast primitive
// shared by the rule handlers (which bump the version inside the repo) and the
// bumpAndBroadcast path (the explicit-bump callers).
func (s *Server) broadcastRulesChanged(ctx context.Context, services ...string) {
	if s.broker == nil {
		s.logger.Warn("broadcastRulesChanged: broker not initialized; rule-change events will not be broadcast")
		return
	}
	if len(services) == 0 {
		return
	}
	newVersion, err := s.rules.Version(ctx)
	if err != nil {
		s.logger.Warn("broadcastRulesChanged: read version failed", "error", err)
		return
	}
	data, err := json.Marshal(map[string]uint64{"version": newVersion})
	if err != nil {
		s.logger.Warn("broadcastRulesChanged: marshal event data failed", "error", err)
		return
	}
	for _, service := range services {
		if service == "" {
			continue
		}
		s.broker.Broadcast(service, Event{
			Type: "rules_changed",
			Data: string(data),
		})
	}
}

// bumpAndBroadcast increments the desired-state rule_version and then nudges
// each affected service over SSE. It is the single entry point for the
// desired-state mutations that bump the version OUTSIDE the rule repo —
// fault-config fire/cancel and the reaper — so they keep the SSE fast-path
// consistent with the poll path instead of bumping the version silently.
// (Rule create/update/delete bump inside the repo and then call
// broadcastRulesChanged directly; the orchestrator delivers via atrocontrol
// push fanout and never touches rule_version, so it is out of this path.)
// Best-effort: failures are logged, not fatal.
func (s *Server) bumpAndBroadcast(ctx context.Context, services ...string) {
	if err := s.rules.BumpVersion(ctx); err != nil {
		s.logger.Warn("bumpAndBroadcast: bump version failed", "error", err)
		return
	}
	s.broadcastRulesChanged(ctx, services...)
}

// generateID mints a "{prefix}-{uuidv7}" entity id (see internal/id).
func generateID(prefix string) string {
	return id.New(prefix)
}
