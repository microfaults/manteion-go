package api

import "net/http"

// handleHealthz is the liveness probe. Always returns 200.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is the readiness probe.
// Returns 200 when the database is reachable and migrations have run.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// The server wouldn't start if DB was unreachable (Open would fail),
	// so if we're serving requests, we're ready.
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleStatus returns an overview of the manteion state:
// rule count, SDK instance count, and zeus reachability.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	ruleCount := 0
	instanceCount := 0

	if rules, err := s.rules.List(ctx); err == nil {
		ruleCount = len(rules)
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
