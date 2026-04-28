// Package api provides the REST API server for manteion.
// It follows the zeus-go Archer pattern: a Server struct that owns
// dependencies and exposes Handler() returning a configured http.Handler.
package api

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// Server holds all dependencies for the manteion API.
type Server struct {
	logger      *slog.Logger
	db          *sql.DB
	rules       *store.RuleRepo
	faults      *store.FaultRepo
	faultStore  FaultStore
	sdk         *store.SDKRepo
	experiments *store.ExperimentRepo
	workloads   *store.WorkloadRepo
	autoRules   *store.AutoRuleRepo
	traces      *store.TraceRepo
	zeus        *zeus.Client
	intent      atrocontrol.IntentReader
}

// NewServer creates a new API server with all repository and client dependencies.
func NewServer(
	logger *slog.Logger,
	db *sql.DB,
	rules *store.RuleRepo,
	faults *store.FaultRepo,
	faultStore FaultStore,
	sdk *store.SDKRepo,
	experiments *store.ExperimentRepo,
	workloads *store.WorkloadRepo,
	autoRules *store.AutoRuleRepo,
	traces *store.TraceRepo,
	zeusClient *zeus.Client,
	intent atrocontrol.IntentReader,
) *Server {
	return &Server{
		logger:      logger,
		db:          db,
		rules:       rules,
		faults:      faults,
		faultStore:  faultStore,
		sdk:         sdk,
		experiments: experiments,
		workloads:   workloads,
		autoRules:   autoRules,
		traces:      traces,
		zeus:        zeusClient,
		intent:      intent,
	}
}

// Handler returns an http.Handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.routes(mux)
	return mux
}

// routes registers all API routes using Go 1.22+ method-path patterns.
func (s *Server) routes(mux *http.ServeMux) {
	// Health. Dual-mounted: root for direct-probe consumers (k8s liveness/readiness),
	// /api/v1/... for spec-driven clients that compose from the server URL — the
	// OpenAPI spec's `servers[0].url` ends in `/api/v1`, so a bare `/healthz`
	// path key would resolve to `/api/v1/healthz` on consumers, which would 404
	// if we only mounted at root. Both mounts share handlers.
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /api/v1/healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/v1/readyz", s.handleReadyz)
	mux.HandleFunc("GET /api/v1/status", s.handleStatus)

	// Rule CRUD
	mux.HandleFunc("POST /api/v1/rules", s.handleCreateRule)
	mux.HandleFunc("GET /api/v1/rules", s.handleListRules)
	mux.HandleFunc("GET /api/v1/rules/{id}", s.handleGetRule)
	mux.HandleFunc("PUT /api/v1/rules/{id}", s.handleUpdateRule)
	mux.HandleFunc("DELETE /api/v1/rules/{id}", s.handleDeleteRule)

	// Fault spec CRUD
	mux.HandleFunc("POST /api/v1/faults/specs", s.handleCreateFaultSpec)
	mux.HandleFunc("GET /api/v1/faults/specs", s.handleListFaultSpecs)
	mux.HandleFunc("GET /api/v1/faults/specs/{id}", s.handleGetFaultSpec)
	mux.HandleFunc("DELETE /api/v1/faults/specs/{id}", s.handleDeleteFaultSpec)

	// Fault composition CRUD (validates depth, directions, incompatibilities
	// via model.ValidateComposition before persisting).
	mux.HandleFunc("POST /api/v1/faults/compositions", s.handleCreateFaultComposition)
	mux.HandleFunc("GET /api/v1/faults/compositions", s.handleListFaultCompositions)
	mux.HandleFunc("GET /api/v1/faults/compositions/{id}", s.handleGetFaultComposition)
	mux.HandleFunc("DELETE /api/v1/faults/compositions/{id}", s.handleDeleteFaultComposition)

	// SDK registration & polling
	mux.HandleFunc("POST /api/v1/sdk/register", s.handleRegister)
	mux.HandleFunc("DELETE /api/v1/sdk/register/{id}", s.handleDeregister)
	mux.HandleFunc("GET /api/v1/sdk/instances", s.handleListInstances)
	mux.HandleFunc("GET /api/v1/sdk/rules", s.handlePollRules)
	mux.HandleFunc("GET /api/v1/sdk/init", s.handleInit)

	// Zeus proxy (pass-through to Archer). Each method/path gets its own shim
	// so swag emits a single, valid router directive per operation; the shims
	// share behavior via the private (*Server).zeusProxy helper.
	mux.HandleFunc("POST /api/v1/zeus/workloads", s.handleZeusWorkloadsCreate)
	mux.HandleFunc("GET /api/v1/zeus/workloads", s.handleZeusWorkloadsList)
	mux.HandleFunc("DELETE /api/v1/zeus/workloads/{id}", s.handleZeusWorkloadDelete)
	mux.HandleFunc("POST /api/v1/zeus/attacks", s.handleZeusAttacksCreate)
	mux.HandleFunc("GET /api/v1/zeus/attacks/{id}", s.handleZeusAttackGet)
	mux.HandleFunc("DELETE /api/v1/zeus/attacks/{id}", s.handleZeusAttackDelete)
	// /zeus/policies routes are intentionally un-annotated — Task 12 deletes
	// them. They keep using the unannotated handleZeusProxy alias.
	mux.HandleFunc("POST /api/v1/zeus/policies", s.handleZeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/policies", s.handleZeusProxy)
	mux.HandleFunc("DELETE /api/v1/zeus/policies/{id}", s.handleZeusProxy)
}

// --- JSON helpers ---

// writeJSON serializes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Best-effort — headers already sent.
		slog.Error("writeJSON encode error", "error", err)
	}
}

// writeError writes a JSON error response using the standard envelope.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

// readJSON decodes the request body into v.
func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
