package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
	"manteion-go/internal/model"
	"manteion-go/internal/ruleconv"
	"manteion-go/internal/store"
)

// activeFaultsForService returns the faults a service should currently apply,
// plus its freeze config — the desired-state set the SDK reconciles against on
// each poll. It unions manual long-running fault configs with any experiment-
// driven intent fault. Each entry carries a stable unique ID so the SDK keys
// its slots correctly.
func (s *Server) activeFaultsForService(ctx context.Context, service string) ([]atroposdk.FaultRequest, *atroposdk.DelayRequest) {
	var faults []atroposdk.FaultRequest

	if s.faultConfigs != nil {
		configs, err := s.faultConfigs.ListActiveForService(ctx, service)
		if err != nil {
			s.logger.Error("active faults: list manual failed", "service", service, "error", err)
		}
		for _, c := range configs {
			if len(c.Params) == 0 {
				continue // composition configs aren't deliverable yet
			}
			req := atroposdk.FaultRequest{
				ID:         c.ID,
				Category:   c.Category,
				FaultType:  c.FaultType,
				DurationMs: c.DurationMs,
				RampUpMs:   c.RampUpMs,
				RampDownMs: c.RampDownMs,
				Params:     c.Params,
			}
			if c.Network != nil {
				req.Network = &atroposdk.NetworkEnvelope{
					Target:    c.Network.Target,
					Direction: c.Network.Direction,
					Scope:     c.Network.Scope,
				}
			}
			faults = append(faults, req)
		}
	}

	var freeze *atroposdk.DelayRequest
	if s.intent != nil {
		if intent, ok := s.intent.Get(service); ok {
			if intent.ActiveFault != nil {
				faults = append(faults, *intent.ActiveFault)
			}
			freeze = intent.FreezeCfg
		}
	}
	return faults, freeze
}

// handleRegister registers (or re-registers) an SDK instance.
//
// @Summary      Register SDK instance
// @Description  Registers an atropos-go SDK process. The 201 response carries
// @Description  status="registered" and may include the initial desired state
// @Description  (rules, active_faults, freeze_cfg) so the SDK converges before
// @Description  its first poll.
// @Tags         sdk
// @Accept       json
// @Produce      json
// @Param        instance  body      model.SDKInstance  true  "SDK instance metadata"
// @Success      201       {object}  map[string]any     "registration ack with optional intent fields"
// @Failure      400       {object}  api.ErrorResponse  "invalid JSON or registration error"
// @Router       /sdk/register [post]
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var inst model.SDKInstance
	if err := readJSON(r, &inst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if err := s.sdk.Register(r.Context(), &inst); err != nil {
		s.logger.Error("sdk register failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	s.logger.Info("sdk registered", "id", inst.ID, "service", inst.Service)

	// Typed wire struct shared with the SDK (atroposdk.RegisterResponse
	// embeds RuleSync) — both ends marshal the same type, so the historical
	// active_fault-vs-active_faults key drift cannot recur. Deliver the
	// current desired state so a freshly-registered SDK converges before its
	// first poll.
	resp := atroposdk.RegisterResponse{Status: "registered"}
	if s.intent != nil {
		if intent, ok := s.intent.Get(inst.Service); ok && intent.Rules != nil {
			resp.Rules = intent.Rules
		}
	}
	faults, freeze := s.activeFaultsForService(r.Context(), inst.Service)
	if faults == nil {
		faults = []atroposdk.FaultRequest{}
	}
	resp.ActiveFaults = faults
	resp.FreezeCfg = freeze
	writeJSON(w, http.StatusCreated, resp)
}

// handleDeregister removes an SDK instance.
//
// @Summary      Deregister SDK instance
// @Tags         sdk
// @Param        id   path  string  true  "Instance ID"
// @Success      204  "instance deregistered"
// @Failure      404  {object}  api.ErrorResponse  "instance not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/register/{id} [delete]
func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if err := s.sdk.Deregister(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "instance not found")
			return
		}
		s.logger.Error("sdk deregister failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to deregister")
		return
	}

	s.logger.Info("sdk deregistered", "id", id)
	w.WriteHeader(http.StatusNoContent)
}

// handleListInstances returns all registered SDK instances.
//
// @Summary      List SDK instances
// @Description  Returns all currently registered SDK instances. Status is computed
// @Description  (alive/stale/dead) from last-poll-at.
// @Tags         sdk
// @Produce      json
// @Success      200  {array}   model.SDKInstance
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/instances [get]
func (s *Server) handleListInstances(w http.ResponseWriter, r *http.Request) {
	instances, err := s.sdk.List(r.Context())
	if err != nil {
		s.logger.Error("list instances failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list instances")
		return
	}
	if instances == nil {
		instances = []*model.SDKInstance{}
	}
	writeJSON(w, http.StatusOK, instances)
}

// handlePollRules is the SDK polling endpoint.
// Query params: ?service=X&version=N
// Returns 304 if store version == requested version, otherwise 200 with rules.
//
// @Summary      Poll for rule updates
// @Description  SDKs send their last-known rule version via the version query param.
// @Description  Returns 304 if unchanged, else 200 with the desired state the SDK
// @Description  reconciles against: {"version": uint64, "rules": []CompiledRule,
// @Description  "active_faults": []FaultRequest, "freeze_cfg": DelayRequest}.
// @Description  Manual fault add/remove bumps the version so it rides this path.
// @Description  Optional instance_id query param triggers a best-effort poll-timestamp touch.
// @Tags         sdk
// @Produce      json
// @Param        service      query     string  true   "service name"
// @Param        version      query     integer false  "last known rule version (uint64); omit on first poll"
// @Param        instance_id  query     string  false  "SDK instance ID for poll-timestamp tracking"
// @Success      200          {object}  map[string]any  "{version, rules, active_faults, freeze_cfg}"
// @Success      304          "no rule changes since requested version"
// @Failure      400          {object}  api.ErrorResponse  "service query parameter required"
// @Failure      500          {object}  api.ErrorResponse
// @Router       /sdk/rules [get]
func (s *Server) handlePollRules(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, "service query parameter required")
		return
	}

	versionStr := r.URL.Query().Get("version")
	requestedVersion, _ := strconv.ParseUint(versionStr, 10, 64)

	ctx := r.Context()

	// Touch poll timestamp for the instance (best-effort, don't fail the poll).
	// Must run BEFORE the 304 check — most polls are no-change, and last_poll_at
	// drives the computed liveness status.
	if instanceID := r.URL.Query().Get("instance_id"); instanceID != "" {
		if touchErr := s.sdk.TouchPoll(ctx, instanceID); touchErr != nil {
			s.logger.Warn("touch poll failed", "instance_id", instanceID, "error", touchErr)
		}
	}

	// Check current rule version.
	currentVersion, err := s.rules.Version(ctx)
	if err != nil {
		s.logger.Error("read rule version failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to read rule version")
		return
	}

	// 304 Not Modified — client already has the latest desired state. Fault
	// config changes bump the rule version, so add/remove of manual faults is
	// delivered through this same fast-path (a 200 follows the bump).
	if requestedVersion == currentVersion {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Desired fault set for this service (manual long-running + experiment) and
	// freeze config — the SDK reconciles its applied faults against this list.
	activeFaults, freezeCfg := s.activeFaultsForService(ctx, service)
	if activeFaults == nil {
		activeFaults = []atroposdk.FaultRequest{}
	}

	// Fetch rules for this service.
	rules, err := s.rules.ForService(ctx, service)
	if err != nil {
		s.logger.Error("fetch rules for service failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to fetch rules")
		return
	}

	if rules == nil {
		rules = []*model.Rule{}
	}

	specResolver := &ruleconv.FuncResolver{Fn: s.faults.SpecResolver(ctx)}
	compResolver := &ruleconv.FuncCompositionResolver{Fn: s.faults.CompositionResolver(ctx)}
	compiled, err := ruleconv.CompileRules(rules, specResolver, compResolver)
	if err != nil {
		// Do NOT fall back to raw model rules — that's a wire shape the SDK
		// can't decode. A 500 makes the SDK keep its stale rules and log
		// degraded, which is its designed failure mode.
		s.logger.Error("compile rules failed", "service", service, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to compile rules")
		return
	}

	writeJSON(w, http.StatusOK, atroposdk.RuleSync{
		Version:      currentVersion,
		Rules:        compiled,
		ActiveFaults: activeFaults,
		FreezeCfg:    freezeCfg,
	})
}

// handleInit is the startup readiness check for SDK initialization.
// Returns 200 when manteion is ready to serve rules.
//
// @Summary      SDK init readiness
// @Description  Returns 200 with status="ready" when manteion is ready to
// @Description  serve rules. Used by atropos-go SDKs as a startup gate.
// @Tags         sdk
// @Produce      json
// @Success      200  {object}  map[string]string  "{status: ready}"
// @Router       /sdk/init [get]
func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
	if err := s.dbPing.PingContext(r.Context()); err != nil {
		s.logger.Warn("sdk/init: db unreachable", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"reason": "database unreachable",
		})
		return
	}
	if _, err := s.rulever.Version(r.Context()); err != nil {
		s.logger.Warn("sdk/init: rule store not initialized", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"reason": "rule store not initialized",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
