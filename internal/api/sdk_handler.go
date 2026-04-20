package api

import (
	"errors"
	"net/http"
	"strconv"

	"manteion-go/internal/model"
	"manteion-go/internal/ruleconv"
	"manteion-go/internal/store"
)

// handleRegister registers (or re-registers) an SDK instance.
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

	resp := map[string]any{"status": "registered"}
	if s.intent != nil {
		if intent, ok := s.intent.Get(inst.Service); ok {
			if intent.Rules != nil {
				resp["rules"] = intent.Rules
			}
			if intent.ActiveFault != nil {
				resp["active_fault"] = intent.ActiveFault
			}
			if intent.FreezeCfg != nil {
				resp["freeze_cfg"] = intent.FreezeCfg
			}
		}
	}
	writeJSON(w, http.StatusCreated, resp)
}

// handleDeregister removes an SDK instance.
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
func (s *Server) handlePollRules(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, "service query parameter required")
		return
	}

	versionStr := r.URL.Query().Get("version")
	requestedVersion, _ := strconv.ParseUint(versionStr, 10, 64)

	ctx := r.Context()

	// Check current rule version.
	currentVersion, err := s.rules.Version(ctx)
	if err != nil {
		s.logger.Error("read rule version failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to read rule version")
		return
	}

	// 304 Not Modified — client already has the latest rules.
	if requestedVersion == currentVersion {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Fetch rules for this service.
	rules, err := s.rules.ForService(ctx, service)
	if err != nil {
		s.logger.Error("fetch rules for service failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to fetch rules")
		return
	}

	// Touch poll timestamp for the instance (best-effort, don't fail the poll).
	if instanceID := r.URL.Query().Get("instance_id"); instanceID != "" {
		if touchErr := s.sdk.TouchPoll(ctx, instanceID); touchErr != nil {
			s.logger.Warn("touch poll failed", "instance_id", instanceID, "error", touchErr)
		}
	}

	if rules == nil {
		rules = []*model.Rule{}
	}

	specResolver := &ruleconv.FuncResolver{Fn: s.faults.SpecResolver(ctx)}
	compResolver := &ruleconv.FuncCompositionResolver{Fn: s.faults.CompositionResolver(ctx)}
	compiled, err := ruleconv.CompileRules(rules, specResolver, compResolver)
	if err != nil {
		s.logger.Error("compile rules failed", "service", service, "error", err)
		writeJSON(w, http.StatusOK, map[string]any{
			"version": currentVersion,
			"rules":   rules,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"version": currentVersion,
		"rules":   compiled,
	})
}

// handleInit is the startup readiness check for SDK initialization.
// Returns 200 when manteion is ready to serve rules.
func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
