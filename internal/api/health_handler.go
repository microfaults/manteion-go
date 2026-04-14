package api

import "net/http"

// handleHealthz is the liveness probe. Always returns 200.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is the readiness probe.
// For MVP this always returns 200. When stores are wired, it should
// check that initialization is complete.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// TODO: Check store initialization when store layer is implemented.
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleStatus returns an overview of the manteion state:
// rule count, instance count, and zeus reachability.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	// TODO: Wire to rule.Store.List(), sdk.Registry.Count(), zeus.Client.Healthy()
	status := map[string]any{
		"rules":          0,
		"instances":      0,
		"zeus_reachable": false,
	}
	writeJSON(w, http.StatusOK, status)
}
