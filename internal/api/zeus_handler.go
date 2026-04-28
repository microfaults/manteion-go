package api

import (
	"io"
	"net/http"
	"strings"
)

// zeusProxy is the shared passthrough body. All zeus-proxy handler shims
// delegate here. It strips the /api/v1/zeus prefix and forwards to the
// Archer (zeus-go) API.
//
// Per-route swag annotations live on the thin shims below; multiple router
// directives on one handler produce invalid OpenAPI when path templates
// (`/{id}` vs root) or accept/produce shapes differ across methods.
func (s *Server) zeusProxy(w http.ResponseWriter, r *http.Request) {
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

// handleZeusProxy is an unannotated alias kept so the (soon-to-be-deleted)
// /zeus/policies routes still compile and route until Task 12 removes them.
// It MUST stay annotation-free; adding swag router directives here would
// re-create the multi-route defect that motivated splitting the handler.
func (s *Server) handleZeusProxy(w http.ResponseWriter, r *http.Request) {
	s.zeusProxy(w, r)
}

// handleZeusWorkloadsCreate forwards POST /zeus/workloads to zeus-go.
//
// @Summary      Create workload (Zeus passthrough)
// @Description  Proxies the request to zeus-go's load generation API.
// @Description  Body and response shapes are defined by zeus-go; see its OpenAPI spec.
// @Description  The response status code is propagated from upstream.
// @Tags         zeus-proxy
// @Accept       json
// @Produce      json
// @Success      200      "passthrough — see zeus-go spec for response shape"
// @Failure      502      {object}  api.ErrorResponse  "zeus unreachable"
// @Failure      default  {object}  api.ErrorResponse  "upstream-propagated 4xx/5xx"
// @Router       /zeus/workloads [post]
func (s *Server) handleZeusWorkloadsCreate(w http.ResponseWriter, r *http.Request) {
	s.zeusProxy(w, r)
}

// handleZeusWorkloadsList forwards GET /zeus/workloads to zeus-go.
//
// @Summary      List workloads (Zeus passthrough)
// @Description  Proxies the request to zeus-go's load generation API.
// @Description  Response shape is defined by zeus-go; see its OpenAPI spec.
// @Tags         zeus-proxy
// @Produce      json
// @Success      200      "passthrough — see zeus-go spec for response shape"
// @Failure      502      {object}  api.ErrorResponse  "zeus unreachable"
// @Failure      default  {object}  api.ErrorResponse  "upstream-propagated 4xx/5xx"
// @Router       /zeus/workloads [get]
func (s *Server) handleZeusWorkloadsList(w http.ResponseWriter, r *http.Request) {
	s.zeusProxy(w, r)
}

// handleZeusWorkloadDelete forwards DELETE /zeus/workloads/{id} to zeus-go.
//
// @Summary      Delete workload (Zeus passthrough)
// @Description  Proxies the request to zeus-go's load generation API.
// @Tags         zeus-proxy
// @Param        id       path      string             true  "Workload ID (zeus-defined)"
// @Success      204      "passthrough — see zeus-go spec for response shape"
// @Failure      502      {object}  api.ErrorResponse  "zeus unreachable"
// @Failure      default  {object}  api.ErrorResponse  "upstream-propagated 4xx/5xx"
// @Router       /zeus/workloads/{id} [delete]
func (s *Server) handleZeusWorkloadDelete(w http.ResponseWriter, r *http.Request) {
	s.zeusProxy(w, r)
}

// handleZeusAttacksCreate forwards POST /zeus/attacks to zeus-go.
//
// @Summary      Create attack (Zeus passthrough)
// @Description  Proxies the request to zeus-go's load generation API.
// @Description  Body and response shapes are defined by zeus-go; see its OpenAPI spec.
// @Description  The response status code is propagated from upstream.
// @Tags         zeus-proxy
// @Accept       json
// @Produce      json
// @Success      200      "passthrough — see zeus-go spec for response shape"
// @Failure      502      {object}  api.ErrorResponse  "zeus unreachable"
// @Failure      default  {object}  api.ErrorResponse  "upstream-propagated 4xx/5xx"
// @Router       /zeus/attacks [post]
func (s *Server) handleZeusAttacksCreate(w http.ResponseWriter, r *http.Request) {
	s.zeusProxy(w, r)
}

// handleZeusAttackGet forwards GET /zeus/attacks/{id} to zeus-go.
//
// @Summary      Get attack (Zeus passthrough)
// @Description  Proxies the request to zeus-go's load generation API.
// @Description  Response shape is defined by zeus-go; see its OpenAPI spec.
// @Tags         zeus-proxy
// @Produce      json
// @Param        id       path      string             true  "Attack ID (zeus-defined)"
// @Success      200      "passthrough — see zeus-go spec for response shape"
// @Failure      502      {object}  api.ErrorResponse  "zeus unreachable"
// @Failure      default  {object}  api.ErrorResponse  "upstream-propagated 4xx/5xx"
// @Router       /zeus/attacks/{id} [get]
func (s *Server) handleZeusAttackGet(w http.ResponseWriter, r *http.Request) {
	s.zeusProxy(w, r)
}

// handleZeusAttackDelete forwards DELETE /zeus/attacks/{id} to zeus-go.
//
// @Summary      Delete attack (Zeus passthrough)
// @Description  Proxies the request to zeus-go's load generation API.
// @Tags         zeus-proxy
// @Param        id       path      string             true  "Attack ID (zeus-defined)"
// @Success      204      "passthrough — see zeus-go spec for response shape"
// @Failure      502      {object}  api.ErrorResponse  "zeus unreachable"
// @Failure      default  {object}  api.ErrorResponse  "upstream-propagated 4xx/5xx"
// @Router       /zeus/attacks/{id} [delete]
func (s *Server) handleZeusAttackDelete(w http.ResponseWriter, r *http.Request) {
	s.zeusProxy(w, r)
}
