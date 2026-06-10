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
// Manteion is the durable system of record for the DSL v2 DEFINITION;
// zeus is the validation + execution runtime:
//
//   - create/update: the document is validated against zeus's stateless
//     POST /api/v1/workflows/validate BEFORE persisting (fail closed —
//     zeus owns DSL semantics, manteion does not guess).
//   - run start: the definition is MATERIALIZED into zeus (register with
//     overwrite) and the run is triggered against the shared id. zeus's
//     in-memory store is a cache of manteion's workflows table.
//
// The UI still fans out two queries on the detail page: manteion for the
// definition, the zeus proxy for live run state.
// =========================================================================

// workflowListItemDTO is the slim row served on GET /workflows. The full
// DSL document is omitted — cards render from extracted metadata, and
// `request_node_count` is precomputed so the subtitle stays cheap.
type workflowListItemDTO struct {
	ID                string   `json:"id"`
	Name              string   `json:"name"`
	Version           string   `json:"version"`
	Description       string   `json:"description,omitempty"`
	Targets           []string `json:"targets"`
	EstimatedRPSPerVU float64  `json:"estimated_rps_per_vu"`
	RequestNodeCount  int      `json:"request_node_count"`
	CreatedAt         string   `json:"created_at"`
	UpdatedAt         string   `json:"updated_at"`
}

// workflowDTO is the full payload served on GET /workflows/{id} and the
// 201 body for POST /workflows. The DSL document is opaque to manteion.
type workflowDTO struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Version     string          `json:"version"`
	Description string          `json:"description,omitempty"`
	DSL         json.RawMessage `json:"dsl"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

const timeRFC3339Nano = time.RFC3339Nano

func toWorkflowListItemDTO(w *model.Workflow) workflowListItemDTO {
	meta := w.DSLMeta()
	return workflowListItemDTO{
		ID:                w.ID,
		Name:              w.Name,
		Version:           w.Version,
		Description:       w.Description,
		Targets:           meta.Targets,
		EstimatedRPSPerVU: meta.EstimatedRPSPerVU,
		RequestNodeCount:  countRequestNodes(meta.Root),
		CreatedAt:         w.CreatedAt.UTC().Format(timeRFC3339Nano),
		UpdatedAt:         w.UpdatedAt.UTC().Format(timeRFC3339Nano),
	}
}

func toWorkflowDTO(w *model.Workflow) workflowDTO {
	return workflowDTO{
		ID:          w.ID,
		Name:        w.Name,
		Version:     w.Version,
		Description: w.Description,
		DSL:         w.DSL,
		CreatedAt:   w.CreatedAt.UTC().Format(timeRFC3339Nano),
		UpdatedAt:   w.UpdatedAt.UTC().Format(timeRFC3339Nano),
	}
}

// createWorkflowRequest is the body shape accepted by POST /workflows and
// PUT /workflows/{id}. `dsl` is the full zeus DSL v2 document; `name`
// overrides the document's name when set (the handler injects the resolved
// id/name back into the document so manteion and zeus share identity).
type createWorkflowRequest struct {
	ID          string          `json:"id,omitempty"`
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	DSL         json.RawMessage `json:"dsl"`
}

// resolveWorkflowDoc merges the request into a workflow model: name falls
// back to the document's, then id/name/version are injected INTO the
// document so the materialized zeus copy carries the same identity.
func resolveWorkflowDoc(req createWorkflowRequest, wfID string) (*model.Workflow, error) {
	if len(req.DSL) == 0 || string(req.DSL) == "null" {
		return nil, errors.New("dsl document required")
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(req.DSL, &doc); err != nil {
		return nil, errors.New("dsl must be a JSON object: " + err.Error())
	}

	name := req.Name
	if name == "" {
		if raw, ok := doc["name"]; ok {
			_ = json.Unmarshal(raw, &name)
		}
	}
	if name == "" {
		return nil, errors.New("name required (in body or dsl.name)")
	}

	version := "2"
	if raw, ok := doc["version"]; ok {
		_ = json.Unmarshal(raw, &version)
	}

	idJSON, _ := json.Marshal(wfID)
	nameJSON, _ := json.Marshal(name)
	doc["id"] = idJSON
	doc["name"] = nameJSON

	dsl, err := json.Marshal(doc)
	if err != nil {
		return nil, errors.New("re-encode dsl: " + err.Error())
	}

	return &model.Workflow{
		ID:          wfID,
		Name:        name,
		Version:     version,
		Description: req.Description,
		DSL:         dsl,
	}, nil
}

// validateWithZeus runs the document through zeus's stateless validator,
// mapping outcomes onto HTTP statuses: nil = valid; 400 = zeus rejected the
// document; 502 = zeus unreachable (fail closed).
func (s *Server) validateWithZeus(w http.ResponseWriter, r *http.Request, dsl json.RawMessage) bool {
	err := s.zeus.ValidateWorkflowDoc(r.Context(), dsl)
	if err == nil {
		return true
	}
	var ve string = err.Error()
	if len(ve) >= 26 && ve[:26] == "workflow validation failed" {
		writeError(w, http.StatusBadRequest, ve)
		return false
	}
	s.logger.Error("zeus workflow validation unavailable", "error", err)
	writeError(w, http.StatusBadGateway, "workflow validation unavailable (zeus unreachable): "+ve)
	return false
}

func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var req createWorkflowRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	wfID := req.ID
	if wfID == "" {
		wfID = generateID("wf")
	}
	wf, err := resolveWorkflowDoc(req, wfID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	wf.CreatedAt = time.Now()
	wf.UpdatedAt = wf.CreatedAt

	if !s.validateWithZeus(w, r, wf.DSL) {
		return
	}

	if err := s.workflows.Create(r.Context(), wf); err != nil {
		if errors.Is(err, store.ErrDuplicateName) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
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
	wf, err := resolveWorkflowDoc(req, id)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if !s.validateWithZeus(w, r, wf.DSL) {
		return
	}

	if err := s.workflows.Update(r.Context(), wf); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "workflow not found")
			return
		}
		if errors.Is(err, store.ErrDuplicateName) {
			writeError(w, http.StatusConflict, err.Error())
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
		if errors.Is(err, store.ErrInUse) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		s.logger.Error("delete workflow failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete workflow")
		return
	}
	// Best-effort eviction of the zeus cache copy; zeus restarts empty and
	// re-materializes on the next run start, so failures are non-fatal.
	if err := s.zeus.DeleteWorkflow(r.Context(), id); err != nil {
		s.logger.Warn("zeus workflow cache eviction failed", "id", id, "error", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleValidateWorkflow re-runs the STORED definition through zeus's
// stateless validator. Zeus is the source of truth for DSL semantics —
// manteion does not pretend to know whether a tree is valid.
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
	if !s.validateWithZeus(w, r, wf.DSL) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "name": wf.Name})
}

// handleStartWorkflowRun materializes the stored definition into zeus and
// triggers a run against it. Materialize-before-run means zeus never needs
// the definition ahead of time — its in-memory store is a cache.
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

	if err := s.materializeWorkflow(r, wf); err != nil {
		s.logger.Error("materialize workflow failed", "id", id, "error", err)
		writeError(w, http.StatusBadGateway, "failed to materialize workflow into zeus: "+err.Error())
		return
	}

	// Forward the run-start body (vus/duration/dataset overrides) unchanged;
	// zeus resolves the workflow by the shared id we just materialized.
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	r.Body.Close()
	s.proxyToZeusWithBody(w, r, "POST", "/workflows/"+id+"/runs", body)
}

// materializeWorkflow pushes the definition into zeus: best-effort delete of
// any stale copy under the same id (covers renames, where overwrite-by-name
// would miss), then register with overwrite.
func (s *Server) materializeWorkflow(r *http.Request, wf *model.Workflow) error {
	if err := s.zeus.DeleteWorkflow(r.Context(), wf.ID); err != nil {
		s.logger.Warn("zeus stale workflow delete failed; proceeding to register",
			"id", wf.ID, "error", err)
	}
	return s.zeus.RegisterWorkflow(r.Context(), wf.DSL)
}

// proxyToZeusWithBody forwards a request to zeus with a manteion-built
// body (path is relative to zeus's /api/v1 root).
func (s *Server) proxyToZeusWithBody(w http.ResponseWriter, r *http.Request, method, path string, body []byte) {
	s.logger.Info("zeus proxy (manteion-built body)",
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
