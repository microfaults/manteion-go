package api

import (
	"net/http"
	"strings"
)

// handleZeusProxy is a generic pass-through handler for all zeus proxy routes.
// It strips the /api/v1/zeus prefix and would forward to the Archer API.
func (s *Server) handleZeusProxy(w http.ResponseWriter, r *http.Request) {
	// Strip the /api/v1/zeus prefix to get the Archer-relative path.
	archerPath := strings.TrimPrefix(r.URL.Path, "/api/v1/zeus")
	if archerPath == "" {
		archerPath = "/"
	}

	s.logger.Info("zeus proxy (stub)",
		"method", r.Method,
		"original_path", r.URL.Path,
		"archer_path", archerPath,
	)

	// TODO: Wire to zeus.Client.Do(r.Method, archerPath, r.Body)
	// On success: pipe resp.StatusCode + resp.Body back to caller.
	// On connection error: return 502.

	writeError(w, http.StatusBadGateway, "zeus proxy not wired")
}
