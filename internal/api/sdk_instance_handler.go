package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// Narrow views of the repos the SDK instance detail + kill-switch handlers
// depend on, so handler tests can inject DB-free fakes (the phaseReader /
// dbPing / rulever pattern). NewServer wires the real repos into them.

type sdkInstanceReader interface {
	Get(ctx context.Context, id string) (*model.SDKInstance, error)
}

type serviceRuleStore interface {
	EnabledForService(ctx context.Context, service string) ([]*model.Rule, error)
	Update(ctx context.Context, rule *model.Rule) error
}

type recentPhaseLister interface {
	RecentPhaseIDsForService(ctx context.Context, service string, limit int) ([]string, error)
}

// recentPhaseLimit caps recent_run_ids on the instance detail.
const recentPhaseLimit = 5

// SDKInstanceDetail is GET /api/v1/sdk/instances/{id}: the list row (same
// shape as the GET /sdk/instances items) plus the instance's rule and phase
// context.
//
// Not emitted, deliberately: last_error and last_rule_version_acked. Nothing
// tracks them — sdk_instances has no such columns and the poll's version
// query param is not persisted — and the UI treats both as optional, so they
// stay absent rather than zero-valued.
type SDKInstanceDetail struct {
	*model.SDKInstance
	// ActiveRuleIDs is every enabled rule targeting the instance's service,
	// priority order — the set the kill-switch disables. It is broader than
	// the poll set: a rule attached to a pending phase is listed here but is
	// only served once its phase runs (see RuleRepo.ForService).
	ActiveRuleIDs []string `json:"active_rule_ids"`
	// RecentRunIDs is the ids of the most recent phases (up to 5, newest-
	// started first) that froze this service or attached a rule targeting it.
	// A "run" is a phase (see /api/v1/phases).
	RecentRunIDs []string `json:"recent_run_ids"`
}

// KillSwitchResult is POST /api/v1/sdk/instances/{id}/kill-switch.
type KillSwitchResult struct {
	DisabledRuleIDs []string  `json:"disabled_rule_ids"`
	At              time.Time `json:"at"`
}

// handleGetInstance returns one registered SDK instance with its rule and
// phase context.
//
// @Summary      Get SDK instance
// @Description  Returns the instance's list row (status computed alive/stale/dead
// @Description  from last-poll-at) plus active_rule_ids — every enabled rule
// @Description  targeting the instance's service, the set the kill-switch disables —
// @Description  and recent_run_ids — the most recent phases (up to 5, newest first)
// @Description  that froze the service or attached a rule targeting it.
// @Tags         sdk
// @Produce      json
// @Param        id   path      string  true  "Instance ID"
// @Success      200  {object}  api.SDKInstanceDetail
// @Failure      404  {object}  api.ErrorResponse  "instance not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/instances/{id} [get]
func (s *Server) handleGetInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()

	inst, err := s.sdkInstances.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "instance not found")
		return
	}
	if err != nil {
		s.logger.Error("get instance failed", "instance_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get instance")
		return
	}

	rules, err := s.serviceRules.EnabledForService(ctx, inst.Service)
	if err != nil {
		s.logger.Error("get instance: list enabled rules failed", "instance_id", id, "service", inst.Service, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list rules for instance")
		return
	}
	ruleIDs := make([]string, 0, len(rules))
	for _, rule := range rules {
		ruleIDs = append(ruleIDs, rule.ID)
	}

	phaseIDs, err := s.phaseHistory.RecentPhaseIDsForService(ctx, inst.Service, recentPhaseLimit)
	if err != nil {
		s.logger.Error("get instance: list recent phases failed", "instance_id", id, "service", inst.Service, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list recent phases for instance")
		return
	}

	writeJSON(w, http.StatusOK, SDKInstanceDetail{
		SDKInstance:   inst,
		ActiveRuleIDs: ruleIDs,
		RecentRunIDs:  nilToEmpty(phaseIDs),
	})
}

// handleInstanceKillSwitch disables every enabled rule targeting the
// instance's service.
//
// @Summary      Kill-switch an SDK instance's rules
// @Description  Disables every currently-enabled rule whose service matches the
// @Description  instance's service, through the same full rule update as
// @Description  PUT /rules/{id} — the rule version bumps once per rule, so polling
// @Description  SDKs converge on their next poll (SSE subscribers are nudged too).
// @Description  Idempotent: a repeat call reports an empty disabled_rule_ids.
// @Tags         sdk
// @Produce      json
// @Param        id   path      string  true  "Instance ID"
// @Success      200  {object}  api.KillSwitchResult
// @Failure      404  {object}  api.ErrorResponse  "instance not found"
// @Failure      500  {object}  api.ErrorResponse  "lookup failed, or some rules could not be disabled — retry; rules already disabled stay disabled"
// @Router       /sdk/instances/{id}/kill-switch [post]
func (s *Server) handleInstanceKillSwitch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()

	inst, err := s.sdkInstances.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "instance not found")
		return
	}
	if err != nil {
		s.logger.Error("kill-switch: get instance failed", "instance_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get instance")
		return
	}

	rules, err := s.serviceRules.EnabledForService(ctx, inst.Service)
	if err != nil {
		s.logger.Error("kill-switch: list enabled rules failed", "instance_id", id, "service", inst.Service, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list rules for instance")
		return
	}

	// A kill-switch is maximally aggressive: every rule is attempted even if
	// one fails, and each write is the full-rule update PUT /rules/{id}
	// performs, so the version bump and SSE nudge are exactly what an
	// operator disabling the rules one by one would produce.
	now := time.Now().UTC()
	disabled := make([]string, 0, len(rules))
	failed := 0
	for _, rule := range rules {
		rule.Enabled = false
		rule.UpdatedAt = now
		switch err := s.serviceRules.Update(ctx, rule); {
		case errors.Is(err, store.ErrNotFound):
			continue // deleted since we listed it — nothing left to disable
		case err != nil:
			failed++
			s.logger.Error("kill-switch: disable rule failed",
				"instance_id", id, "service", inst.Service, "rule_id", rule.ID, "error", err)
			continue
		}
		disabled = append(disabled, rule.ID)
	}
	if len(disabled) > 0 {
		s.logger.Info("kill-switch: rules disabled",
			"instance_id", id, "service", inst.Service, "rule_ids", disabled)
		s.broadcastRulesChanged(ctx, inst.Service)
	}
	if failed > 0 {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("kill-switch: %d of %d rules could not be disabled; retry", failed, len(rules)))
		return
	}

	writeJSON(w, http.StatusOK, KillSwitchResult{DisabledRuleIDs: disabled, At: now})
}
