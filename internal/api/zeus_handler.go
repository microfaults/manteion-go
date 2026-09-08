package api

import (
	"io"
	"net/http"
	"strings"
)

// zeusProxy forwards requests to the Archer (zeus-go) API. It strips the
// /api/v1/zeus prefix and pipes the request/response verbatim.
//
// Only Zeus-owned resources (workflows, runs, datasets) are proxied. Attacks
// are orchestrator-managed and not exposed here — operators interact with
// attacks through the experiment/run lifecycle endpoints.
func (s *Server) zeusProxy(w http.ResponseWriter, r *http.Request) {
	archerPath := strings.TrimPrefix(r.URL.Path, "/api/v1/zeus")
	if archerPath == "" {
		archerPath = "/"
	}
	// Carry the query string through. zeus filters its list endpoints by it
	// (GET /runs?experiment_id=&status=); forwarding only the path turned
	// every filtered read into "all runs". RawQuery goes through verbatim so
	// percent-encoding is neither decoded nor re-applied.
	if r.URL.RawQuery != "" {
		archerPath += "?" + r.URL.RawQuery
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

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
