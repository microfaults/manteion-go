package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// Narrow views of the repos the SDK instance detail, kill-switch and
// desired-state (register) handlers depend on, so handler tests can inject
// DB-free fakes (the phaseReader / dbPing / rulever pattern). NewServer wires
// the real repos into them.

type sdkInstanceReader interface {
	Get(ctx context.Context, id string) (*model.SDKInstance, error)
}

// sdkRegistry is the sdk_instances table as the desired-state handlers use it:
// registration plus the observed state the instances report.
type sdkRegistry interface {
	sdkInstanceReader
	Register(ctx context.Context, inst *model.SDKInstance) error
	RecordPoll(ctx context.Context, id string, observedGeneration uint64) error
	RecordStatus(ctx context.Context, id string, st model.ObservedStatus) error
}

// desiredMaterializer builds an instance's desired-state document
// (*desired.Materializer).
type desiredMaterializer interface {
	Materialize(ctx context.Context, service string, inst *model.SDKInstance) (desired.Document, error)
}

type serviceRuleStore interface {
	EnabledForService(ctx context.Context, service string) ([]*model.Rule, error)
	Update(ctx context.Context, rule *model.Rule) error
}

type recentPhaseLister interface {
	RecentPhaseIDsForService(ctx context.Context, service string, limit int) ([]string, error)
}

// serviceFaultConfigStore is the slice of the fault-config repo the kill
// switch uses. ListActiveForService is the set the materializer serves as a
// poll's active_faults, so the kill switch cancels exactly the faults the
// service's documents carry.
type serviceFaultConfigStore interface {
	ListActiveForService(ctx context.Context, service string) ([]*model.FaultConfig, error)
	MarkCancelled(ctx context.Context, id string) error
}

// recentPhaseLimit caps recent_run_ids on the instance detail.
const recentPhaseLimit = 5

// SDKInstanceDetail is GET /api/v1/sdk/instances/{id}: the list row (same
// shape as the GET /sdk/instances items) plus the instance's rule and phase
// context.
//
// Not emitted, deliberately: last_error and last_rule_version_acked. Nothing
// tracks them under those names (the poll's version is persisted as
// observed_generation and the status report's errors as observed_errors, both
// on the list row), and the UI treats both as optional, so they stay absent
// rather than zero-valued.
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

// KillSwitchResult is POST /api/v1/sdk/instances/{id}/kill-switch: what the
// call changed, both lists empty (never null) when there was nothing left to
// change.
type KillSwitchResult struct {
	// DisabledRuleIDs is every rule the call disabled, priority order.
	DisabledRuleIDs []string `json:"disabled_rule_ids"`
	// CancelledFaultConfigIDs is every active fault config for the service the
	// call cancelled, oldest first.
	CancelledFaultConfigIDs []string  `json:"cancelled_fault_config_ids"`
	At                      time.Time `json:"at"`
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
// instance's service and cancels every active fault config for it: the
// service's desired state is left with no rules and no faults.
//
// @Summary      Kill-switch an SDK instance's rules and faults
// @Description  Disables every currently-enabled rule whose service matches the
// @Description  instance's service, through the same full rule update as
// @Description  PUT /rules/{id} (the rule version and the service generation bump
// @Description  once per rule), and cancels every active fault config for the service;
// @Description  bumps the service generation once after the last write, so polling SDKs
// @Description  converge on no rules and no faults on their next poll (held polls wake
// @Description  and SSE subscribers are nudged too).
// @Description  Idempotent: a repeat call reports empty disabled_rule_ids and
// @Description  cancelled_fault_config_ids and bumps nothing.
// @Tags         sdk
// @Produce      json
// @Param        id   path      string  true  "Instance ID"
// @Success      200  {object}  api.KillSwitchResult
// @Failure      404  {object}  api.ErrorResponse  "instance not found"
// @Failure      500  {object}  api.ErrorResponse  "lookup failed, or some rules could not be disabled or fault configs cancelled — retry; what was already disabled or cancelled stays so"
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

	// A kill-switch is maximally aggressive: every rule and every fault config
	// is attempted even if one fails. Each rule write is the full-rule update
	// PUT /rules/{id} performs, so the version bump and SSE nudge are exactly
	// what an operator disabling the rules one by one would produce.
	now := time.Now().UTC()
	disabled := make([]string, 0, len(rules))
	failedRules := 0
	for _, rule := range rules {
		rule.Enabled = false
		rule.UpdatedAt = now
		switch err := s.serviceRules.Update(ctx, rule); {
		case errors.Is(err, store.ErrNotFound):
			continue // deleted since we listed it — nothing left to disable
		case err != nil:
			failedRules++
			s.logger.Error("kill-switch: disable rule failed",
				"instance_id", id, "service", inst.Service, "rule_id", rule.ID, "error", err)
			continue
		}
		disabled = append(disabled, rule.ID)
	}
	if len(disabled) > 0 {
		s.logger.Info("kill-switch: rules disabled",
			"instance_id", id, "service", inst.Service, "rule_ids", disabled)
	}

	// Then the long-running faults: every fault config the service's polls
	// serve as active, cancelled as POST /faults/configs/{id}/cancel does.
	// A failed listing ends the call here, without the bump below: the rules
	// it disabled have bumped on their own.
	configs, err := s.serviceFaultConfigs.ListActiveForService(ctx, inst.Service)
	if err != nil {
		s.logger.Error("kill-switch: list active fault configs failed", "instance_id", id, "service", inst.Service, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list fault configs for instance")
		return
	}
	cancelled := make([]string, 0, len(configs))
	failedConfigs := 0
	for _, cfg := range configs {
		switch err := s.serviceFaultConfigs.MarkCancelled(ctx, cfg.ID); {
		case errors.Is(err, store.ErrNotFound):
			continue // deleted since we listed it — nothing left to cancel
		case err != nil:
			failedConfigs++
			s.logger.Error("kill-switch: cancel fault config failed",
				"instance_id", id, "service", inst.Service, "fault_config_id", cfg.ID, "error", err)
			continue
		}
		cancelled = append(cancelled, cfg.ID)
	}
	if len(cancelled) > 0 {
		s.logger.Info("kill-switch: fault configs cancelled",
			"instance_id", id, "service", inst.Service, "fault_config_ids", cancelled)
	}

	// One bump after the last write, even when some writes failed: each rule
	// update already bumped in its transaction, but a cancel does not, and this
	// bump wakes the held polls and nudges the SSE subscribers once everything
	// this call changed is in place. A call that changed nothing bumps nothing.
	if len(disabled)+len(cancelled) > 0 {
		s.bumpGeneration(ctx, inst.Service)
	}
	if failedRules+failedConfigs > 0 {
		writeError(w, http.StatusInternalServerError, killSwitchFailure(failedRules, len(rules), failedConfigs, len(configs)))
		return
	}

	writeJSON(w, http.StatusOK, KillSwitchResult{DisabledRuleIDs: disabled, CancelledFaultConfigIDs: cancelled, At: now})
}

// killSwitchFailure words a kill switch that left some of its writes undone.
func killSwitchFailure(failedRules, rules, failedConfigs, configs int) string {
	var undone []string
	if failedRules > 0 {
		undone = append(undone, fmt.Sprintf("%d of %d rules could not be disabled", failedRules, rules))
	}
	if failedConfigs > 0 {
		undone = append(undone, fmt.Sprintf("%d of %d fault configs could not be cancelled", failedConfigs, configs))
	}
	return "kill-switch: " + strings.Join(undone, ", ") + "; retry"
}
