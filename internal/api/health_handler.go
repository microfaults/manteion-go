package api

import "net/http"

// handleHealthz is the liveness probe. Always returns 200.
//
// @Summary      Liveness probe
// @Description  Returns 200 if the process is running. No dependency checks.
// @Tags         health
// @Produce      json
// @Success      200  {object}  map[string]string  "{status: ok}"
// @Router       /healthz [get]
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is the readiness probe.
// Returns 200 when the database is reachable, 503 otherwise.
//
// @Summary      Readiness probe
// @Description  Returns 200 once the database is reachable, 503 otherwise.
// @Tags         health
// @Produce      json
// @Success      200  {object}  map[string]string  "{status: ok}"
// @Failure      503  {object}  api.ErrorResponse  "database unreachable"
// @Router       /readyz [get]
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.dbPing.PingContext(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleStatus returns an overview of the manteion state:
// rule count, SDK instance count, and zeus reachability.
//
// @Summary      Manteion status overview
// @Description  Returns a summary of internal counters and downstream reachability:
// @Description  rules (int), instances (int), zeus_reachable (bool).
// @Tags         health
// @Produce      json
// @Success      200  {object}  map[string]any  "{rules, instances, zeus_reachable}"
// @Router       /status [get]
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
		"db_healthy":     s.dbPing.PingContext(ctx) == nil,
		"rules":          ruleCount,
		"instances":      instanceCount,
		"zeus_reachable": s.zeus.Healthy(ctx),
	}
	writeJSON(w, http.StatusOK, status)
}
