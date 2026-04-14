package api

import (
	"fmt"
	"net/http"
)

// handleRegister registers (or re-registers) an SDK instance.
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	id, _ := body["id"].(string)
	service, _ := body["service"].(string)
	s.logger.Info("sdk registered (stub)", "id", id, "service", service)

	// TODO: Wire to sdk.Registry.Register()
	writeJSON(w, http.StatusCreated, map[string]string{"status": "registered"})
}

// handleDeregister removes an SDK instance.
func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.logger.Info("sdk deregistered (stub)", "id", id)

	// TODO: Wire to sdk.Registry.Deregister(id)
	w.WriteHeader(http.StatusNoContent)
}

// handleListInstances returns all registered SDK instances.
func (s *Server) handleListInstances(w http.ResponseWriter, r *http.Request) {
	// TODO: Wire to sdk.Registry.List()
	writeJSON(w, http.StatusOK, []any{})
}

// handlePollRules is the SDK polling endpoint.
// Query params: ?service=X&version=N
// Returns 304 if store version == requested version, otherwise 200 with rules.
func (s *Server) handlePollRules(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	version := r.URL.Query().Get("version")

	s.logger.Info("sdk poll (stub)", "service", service, "version", version)

	// TODO: Wire to rule.Store.Version() and rule.Store.ForService(service)
	// If store version matches requested version, return 304.
	// Otherwise return current version + filtered rules.

	writeJSON(w, http.StatusOK, map[string]any{
		"version": 0,
		"rules":   []any{},
	})
}

// handleInit is the startup readiness check for SDK initialization.
// Returns 200 when manteion is ready to serve rules.
func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// sdkRulesResponse is the response shape for GET /api/v1/sdk/rules.
// Defined here for documentation; will be used when store is wired.
type sdkRulesResponse struct {
	Version uint64 `json:"version"`
	Rules   []any  `json:"rules"`
}

// registerRequest is the expected body for POST /api/v1/sdk/register.
// Defined here for documentation; will be used when store is wired.
type registerRequest struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Version string `json:"version"`
	Address string `json:"address"`
}

func init() {
	// Prevent unused type warnings. These types document the API contract
	// and will be used when the store layer is implemented.
	_ = sdkRulesResponse{}
	_ = registerRequest{}
}

// notImplemented is a helper for endpoints not yet wired.
func notImplemented(w http.ResponseWriter, what string) {
	writeError(w, http.StatusNotImplemented, fmt.Sprintf("%s: not yet implemented", what))
}
