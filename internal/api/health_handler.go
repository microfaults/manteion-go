package api

import "net/http"

// handleHealthz is the liveness probe. Always returns 200.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is the readiness probe.
// Returns 200 when the database is reachable, 503 otherwise.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.db.PingContext(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleStatus returns an overview of the manteion state:
// rule count, SDK instance count, and zeus reachability.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	ruleCount := 0
	instanceCount := 0

	if count, err := s.rules.Count(ctx); err == nil {
		ruleCount = count
	}
	if count, err := s.sdk.Count(ctx); err == nil {
		instanceCount = count
	}

	status := map[string]any{
		"rules":          ruleCount,
		"instances":      instanceCount,
		"zeus_reachable": s.zeus.Healthy(ctx),
	}
	writeJSON(w, http.StatusOK, status)
}
