package api

import (
	"io"
	"net/http"
	"strings"
)

// handleZeusProxy is a generic pass-through handler for all zeus proxy routes.
// It strips the /api/v1/zeus prefix and forwards to the Archer API.
func (s *Server) handleZeusProxy(w http.ResponseWriter, r *http.Request) {
	// Strip the /api/v1/zeus prefix to get the Archer-relative path.
	archerPath := strings.TrimPrefix(r.URL.Path, "/api/v1/zeus")
	if archerPath == "" {
		archerPath = "/"
	}

	s.logger.Info("zeus proxy",
		"method", r.Method,
		"original_path", r.URL.Path,
		"archer_path", archerPath,
	)

	resp, err := s.zeus.Do(r.Context(), r.Method, archerPath, r.Body)
	if err != nil {
		s.logger.Error("zeus proxy failed", "error", err)
		writeError(w, http.StatusBadGateway, "zeus unreachable: "+err.Error())
		return
	}
	defer resp.Body.Close()

	// Copy response headers from Archer.
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	// Pipe status code and body back to the caller.
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
