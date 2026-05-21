package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// =========================================================================
// Workflow definitions (manteion-owned).
//
// Manteion holds the DEFINITION (the DSL spec); zeus holds EXECUTION
// (run state, attack lifecycle, validation results). The UI fans out
// two parallel queries for the workflow-detail view:
//
//   GET  /api/v1/workflows/{id}               manteion DB — definition
//   GET  /api/v1/zeus/runs?workflow_id=...    manteion → zeus — live state
//
// When a workflow is *started* or *validated*, the API layer inlines
// the manteion definition into the proxied call (POST .../runs or
// .../validate) so zeus never needs to call back for the spec.
// =========================================================================

// workflowListItemDTO is the slim row served on GET /workflows. The full
// `steps` / `thresholds` trees are omitted — the workflows-list page
// renders cards from this shape without paying for a per-row tree fetch.
// `request_node_count` is precomputed so the card subtitle stays cheap.
type workflowListItemDTO struct {
	ID                string   `json:"id"`
	Name              string   `json:"name"`
	Description       string   `json:"description,omitempty"`
	Targets           []string `json:"targets"`
	EstimatedRPSPerVU float64  `json:"estimated_rps_per_vu"`
	RequestNodeCount  int      `json:"request_node_count"`
	CreatedAt         string   `json:"created_at"`
	UpdatedAt         string   `json:"updated_at"`
}

// workflowDTO is the full payload served on GET /workflows/{id} and the
// 201 body for POST /workflows. Steps/thresholds are opaque JSON.
type workflowDTO struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	Description       string          `json:"description,omitempty"`
	Targets           []string        `json:"targets"`
	EstimatedRPSPerVU float64         `json:"estimated_rps_per_vu"`
	Steps             json.RawMessage `json:"steps"`
	Thresholds        json.RawMessage `json:"thresholds,omitempty"`
	CreatedAt         string          `json:"created_at"`
	UpdatedAt         string          `json:"updated_at"`
}

const timeRFC3339Nano = time.RFC3339Nano

func toWorkflowListItemDTO(w *model.Workflow) workflowListItemDTO {
	return workflowListItemDTO{
		ID:                w.ID,
		Name:              w.Name,
		Description:       w.Description,
		Targets:           defaultStringSlice(w.Targets),
		EstimatedRPSPerVU: w.EstimatedRPSPerVU,
		RequestNodeCount:  countRequestNodes(w.Steps),
		CreatedAt:         w.CreatedAt.UTC().Format(timeRFC3339Nano),
		UpdatedAt:         w.UpdatedAt.UTC().Format(timeRFC3339Nano),
	}
}

func toWorkflowDTO(w *model.Workflow) workflowDTO {
	return workflowDTO{
		ID:                w.ID,
		Name:              w.Name,
		Description:       w.Description,
		Targets:           defaultStringSlice(w.Targets),
		EstimatedRPSPerVU: w.EstimatedRPSPerVU,
		Steps:             w.Steps,
		Thresholds:        w.Thresholds,
		CreatedAt:         w.CreatedAt.UTC().Format(timeRFC3339Nano),
		UpdatedAt:         w.UpdatedAt.UTC().Format(timeRFC3339Nano),
	}
}

func defaultStringSlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// createWorkflowRequest is the body shape accepted by POST /workflows.
// `id` is optional — generated server-side if omitted.
type createWorkflowRequest struct {
	ID                string          `json:"id,omitempty"`
	Name              string          `json:"name"`
	Description       string          `json:"description,omitempty"`
	Targets           []string        `json:"targets"`
	EstimatedRPSPerVU float64         `json:"estimated_rps_per_vu"`
	Steps             json.RawMessage `json:"steps"`
	Thresholds        json.RawMessage `json:"thresholds,omitempty"`
}

func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var req createWorkflowRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	wf := &model.Workflow{
		ID:                req.ID,
		Name:              req.Name,
		Description:       req.Description,
		Targets:           req.Targets,
		EstimatedRPSPerVU: req.EstimatedRPSPerVU,
		Steps:             req.Steps,
		Thresholds:        req.Thresholds,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}
	if wf.ID == "" {
		wf.ID = generateID("wf")
	}
	if err := s.workflows.Create(r.Context(), wf); err != nil {
		s.logger.Error("create workflow failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, toWorkflowDTO(wf))
}

func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	rows, total, err := s.workflows.List(r.Context(),
		store.WorkflowFilter{}, store.Page{Limit: limit, Offset: offset})
	if err != nil {
		s.logger.Error("list workflows failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list workflows")
		return
	}
	items := make([]workflowListItemDTO, 0, len(rows))
	for _, wf := range rows {
		items = append(items, toWorkflowListItemDTO(wf))
	}
	writePage(w, http.StatusOK, items, total, limit, offset)
}

func (s *Server) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wf, err := s.workflows.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	if err != nil {
		s.logger.Error("get workflow failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get workflow")
		return
	}
	writeJSON(w, http.StatusOK, toWorkflowDTO(wf))
}

func (s *Server) handleUpdateWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req createWorkflowRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	wf := &model.Workflow{
		ID:                id,
		Name:              req.Name,
		Description:       req.Description,
		Targets:           req.Targets,
		EstimatedRPSPerVU: req.EstimatedRPSPerVU,
		Steps:             req.Steps,
		Thresholds:        req.Thresholds,
	}
	if err := s.workflows.Update(r.Context(), wf); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "workflow not found")
			return
		}
		s.logger.Error("update workflow failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Reload to get the canonical timestamps back.
	loaded, err := s.workflows.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "updated but reload failed")
		return
	}
	writeJSON(w, http.StatusOK, toWorkflowDTO(loaded))
}

func (s *Server) handleDeleteWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.workflows.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "workflow not found")
			return
		}
		s.logger.Error("delete workflow failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete workflow")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleValidateWorkflow loads the manteion definition, inlines it into
// a POST to zeus's validate endpoint, and returns the zeus response.
// Zeus is the source of truth for DSL semantics — manteion does not
// pretend to know whether a tree is valid.
func (s *Server) handleValidateWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wf, err := s.workflows.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load workflow")
		return
	}
	// Proxy to zeus with the manteion definition inlined as the request
	// body. The path rewriting layer already maps /api/v1/workflows/{id}
	// to /api/v1/zeus/workflows/{id} for proxy purposes — but to make
	// validate explicit (and avoid forcing zeus to round-trip back to
	// manteion for the spec), we POST the definition body directly.
	body, _ := json.Marshal(toWorkflowDTO(wf))
	s.proxyToZeusWithBody(w, r, "POST", "/api/v1/workflows/"+id+"/validate", body)
}

// proxyToZeusWithBody forwards a request to zeus with a manteion-built
// body. Used by /validate and /runs to inline the workflow definition.
func (s *Server) proxyToZeusWithBody(w http.ResponseWriter, r *http.Request, method, path string, body []byte) {
	s.logger.Info("zeus proxy (manteion-inlined body)",
		"method", method, "path", path, "body_bytes", len(body))
	resp, err := s.zeus.Do(r.Context(), method, path, bytes.NewReader(body))
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

// handleStartWorkflowRun loads the manteion definition and proxies a
// run-start request to zeus, inlining the definition so zeus does not
// need a credentialed callback to manteion.
func (s *Server) handleStartWorkflowRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wf, err := s.workflows.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load workflow")
		return
	}

	// The UI's run-start body carries vus/duration/dataset overrides.
	// We merge the definition into a single object and forward to zeus.
	var override map[string]json.RawMessage
	if err := readJSON(r, &override); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if override == nil {
		override = map[string]json.RawMessage{}
	}
	defJSON, _ := json.Marshal(toWorkflowDTO(wf))
	override["definition"] = defJSON

	body, err := json.Marshal(override)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode run body")
		return
	}
	s.proxyToZeusWithBody(w, r, "POST", "/api/v1/workflows/"+id+"/runs", body)
}
