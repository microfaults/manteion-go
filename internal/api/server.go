// Package api provides the REST API server for manteion.
// It follows the zeus-go Archer pattern: a Server struct that owns
// dependencies and exposes Handler() returning a configured http.Handler.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"

	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// dbPinger abstracts database ping for health checks and testing.
type dbPinger interface {
	PingContext(ctx context.Context) error
}

// ruleVersioner abstracts rule-version reads for health checks and testing.
type ruleVersioner interface {
	Version(ctx context.Context) (uint64, error)
}

// Server holds all dependencies for the manteion API.
type Server struct {
	logger           *slog.Logger
	db               *sql.DB
	dbPing           dbPinger // same as db; separate field so tests can inject a fake
	rules            *store.RuleRepo
	rulever          ruleVersioner // same as rules; separate field so tests can inject a fake
	faults           *store.FaultRepo
	faultStore       FaultStore
	faultConfigs     *store.FaultConfigRepo
	sdk              *store.SDKRepo
	sdkInstances     sdkInstanceReader // same as sdk; separate field so tests can inject a fake
	serviceRules     serviceRuleStore  // same as rules; separate field so tests can inject a fake
	experiments      *store.ExperimentRepo
	phaseHistory     recentPhaseLister // same as experiments; separate field so tests can inject a fake
	phaseReader      phaseReader       // same as experiments; separate field so ingest tests can inject a fake
	phaseFaultEvents *store.PhaseFaultEventRepo
	workflows        *store.WorkflowRepo
	workloads        *store.WorkloadRepo
	policies         *store.PolicyRepo
	traces           *store.TraceRepo
	zeus             *zeus.Client
	intent           atrocontrol.IntentReader
	orch             *orchestrator.Orchestrator
	cacheStore       *cachestore.Store
	broker           *EventBroker
}

// NewServer creates a new API server with all repository and client dependencies.
func NewServer(
	logger *slog.Logger,
	db *sql.DB,
	rules *store.RuleRepo,
	faults *store.FaultRepo,
	faultStore FaultStore,
	faultConfigs *store.FaultConfigRepo,
	sdk *store.SDKRepo,
	experiments *store.ExperimentRepo,
	phaseFaultEvents *store.PhaseFaultEventRepo,
	workflows *store.WorkflowRepo,
	workloads *store.WorkloadRepo,
	traces *store.TraceRepo,
	zeusClient *zeus.Client,
	intent atrocontrol.IntentReader,
	orch *orchestrator.Orchestrator,
	cs *cachestore.Store,
	policies *store.PolicyRepo,
) *Server {
	return &Server{
		logger:           logger,
		db:               db,
		dbPing:           db,
		rules:            rules,
		rulever:          rules,
		faults:           faults,
		faultStore:       faultStore,
		faultConfigs:     faultConfigs,
		sdk:              sdk,
		sdkInstances:     sdk,
		serviceRules:     rules,
		experiments:      experiments,
		phaseHistory:     experiments,
		phaseReader:      experiments,
		phaseFaultEvents: phaseFaultEvents,
		workflows:        workflows,
		workloads:        workloads,
		policies:         policies,
		traces:           traces,
		zeus:             zeusClient,
		intent:           intent,
		orch:             orch,
		cacheStore:       cs,
		broker:           NewEventBroker(),
	}
}

// Handler returns an http.Handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.routes(mux)

	allowed_origins := os.Getenv("CORS_ALLOWED_ORIGINS")
	if allowed_origins == "" {
		allowed_origins = "*"
	}

	// Simple CORS middleware for local development.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", allowed_origins)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Faults-Lab-Environment")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		mux.ServeHTTP(w, r)
	})
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
	mux.HandleFunc("PUT /api/v1/faults/specs/{id}", s.handleUpdateFaultSpec)
	mux.HandleFunc("DELETE /api/v1/faults/specs/{id}", s.handleDeleteFaultSpec)

	// Fault composition CRUD (validates depth, directions, incompatibilities
	// via model.ValidateComposition before persisting).
	mux.HandleFunc("POST /api/v1/faults/compositions", s.handleCreateFaultComposition)
	mux.HandleFunc("GET /api/v1/faults/compositions", s.handleListFaultCompositions)
	mux.HandleFunc("GET /api/v1/faults/compositions/{id}", s.handleGetFaultComposition)
	mux.HandleFunc("DELETE /api/v1/faults/compositions/{id}", s.handleDeleteFaultComposition)

	// Supported-fault catalogue (vocabulary + params field metadata) for
	// UI form rendering; backed by atropos-go/faultparams.
	mux.HandleFunc("GET /api/v1/faults/catalog", s.handleFaultCatalog)

	// Long-running manual faults (side channel to rule-attached faults).
	// Fired explicitly, delivered to SDKs via the poll active_faults set,
	// reconciled and watchdog-reaped SDK-side.
	mux.HandleFunc("POST /api/v1/faults/configs", s.handleCreateFaultConfig)
	mux.HandleFunc("GET /api/v1/faults/configs", s.handleListFaultConfigs)
	mux.HandleFunc("DELETE /api/v1/faults/configs/{id}", s.handleDeleteFaultConfig)
	mux.HandleFunc("POST /api/v1/faults/configs/{id}/fire", s.handleFireFaultConfig)
	mux.HandleFunc("POST /api/v1/faults/configs/{id}/cancel", s.handleCancelFaultConfig)

	// Workflow definitions (manteion-owned). Zeus owns execution; the
	// UI fans out two parallel queries on the detail page (manteion DB
	// for the definition, zeus proxy for live run state). Validate and
	// run-start inline the definition into the proxied call.
	mux.HandleFunc("POST /api/v1/workflows", s.handleCreateWorkflow)
	mux.HandleFunc("GET /api/v1/workflows", s.handleListWorkflows)
	mux.HandleFunc("GET /api/v1/workflows/{id}", s.handleGetWorkflow)
	mux.HandleFunc("PUT /api/v1/workflows/{id}", s.handleUpdateWorkflow)
	mux.HandleFunc("DELETE /api/v1/workflows/{id}", s.handleDeleteWorkflow)
	mux.HandleFunc("POST /api/v1/workflows/{id}/validate", s.handleValidateWorkflow)
	mux.HandleFunc("POST /api/v1/workflows/{id}/runs", s.handleStartWorkflowRun)

	// Workflow-builder catalog: live SDK route inventory aggregated
	// from sdk_instances.routes. 30-second handler cache; 2-minute
	// liveness window. No curated demo fallback — empty SDK fleet
	// returns an empty list with a hint.
	mux.HandleFunc("GET /api/v1/catalog/endpoints", s.handleListCatalogEndpoints)

	// Experiment CRUD + lifecycle (phase-first model).
	// /runs and /contributions endpoints were retired; phases are the run
	// unit. Pause is phase-level state: the experiment row stays 'running'
	// while its current phase is 'paused' (the experiment_status enum has no
	// paused label) — /pause, /resume, and /cancel drive the orchestrator FSM.
	// PUT edits the plan only while it is still one: the experiment must be
	// planned (and, for a phase, the phase pending); anything later is 409.
	mux.HandleFunc("POST /api/v1/experiments", s.handleCreateExperiment)
	mux.HandleFunc("GET /api/v1/experiments", s.handleListExperiments)
	mux.HandleFunc("GET /api/v1/experiments/{id}", s.handleGetExperiment)
	mux.HandleFunc("PUT /api/v1/experiments/{id}", s.handleUpdateExperiment)
	mux.HandleFunc("DELETE /api/v1/experiments/{id}", s.handleDeleteExperiment)
	mux.HandleFunc("POST /api/v1/experiments/{id}/start", s.handleStartExperiment)
	mux.HandleFunc("POST /api/v1/experiments/{id}/pause", s.handlePauseExperiment)
	mux.HandleFunc("POST /api/v1/experiments/{id}/resume", s.handleResumeExperiment)
	mux.HandleFunc("POST /api/v1/experiments/{id}/cancel", s.handleCancelExperiment)
	mux.HandleFunc("POST /api/v1/experiments/{id}/stop", s.handleStopExperiment)
	mux.HandleFunc("GET /api/v1/experiments/{id}/results", s.handleExperimentResults)

	// Phase-native run-details surface (a "run" = a phase; phaseId is globally
	// unique so no experiment segment is needed). Live panels (steps/events/
	// resources) are the v2 follow-on. See docs/specs/2026-06-18-run-details-*.
	mux.HandleFunc("GET /api/v1/phases", s.handleListPhases)
	mux.HandleFunc("GET /api/v1/phases/{phaseId}", s.handleGetPhaseDetail)
	mux.HandleFunc("GET /api/v1/phases/{phaseId}/faults", s.handleGetPhaseFaults)
	mux.HandleFunc("POST /api/v1/phases/{phaseId}/pause", s.handlePausePhaseFlat)
	mux.HandleFunc("POST /api/v1/phases/{phaseId}/resume", s.handleResumePhaseFlat)
	mux.HandleFunc("POST /api/v1/phases/{phaseId}/stop", s.handleStopPhaseFlat)

	// Phase CRUD + lifecycle.
	mux.HandleFunc("POST /api/v1/experiments/{id}/phases", s.handleCreatePhase)
	mux.HandleFunc("GET /api/v1/experiments/{id}/phases/{phaseId}", s.handleGetPhase)
	mux.HandleFunc("PUT /api/v1/experiments/{id}/phases/{phaseId}", s.handleUpdatePhase)
	mux.HandleFunc("DELETE /api/v1/experiments/{id}/phases/{phaseId}", s.handleDeletePhase)
	mux.HandleFunc("POST /api/v1/experiments/{id}/phases/{phaseId}/start", s.handleStartPhase)
	mux.HandleFunc("POST /api/v1/experiments/{id}/phases/{phaseId}/stop", s.handleStopPhase)
	mux.HandleFunc("GET /api/v1/experiments/{id}/phases/{phaseId}/results", s.handlePhaseResults)

	// Policy CRUD + enable/disable
	mux.HandleFunc("POST /api/v1/policies", s.handleCreatePolicy)
	mux.HandleFunc("GET /api/v1/policies", s.handleListPolicies)
	mux.HandleFunc("GET /api/v1/policies/{id}", s.handleGetPolicy)
	mux.HandleFunc("DELETE /api/v1/policies/{id}", s.handleDeletePolicy)
	mux.HandleFunc("PATCH /api/v1/policies/{id}/enable", s.handleEnablePolicy)
	mux.HandleFunc("PATCH /api/v1/policies/{id}/disable", s.handleDisablePolicy)

	// SDK registration & polling
	mux.HandleFunc("POST /api/v1/sdk/register", s.handleRegister)
	mux.HandleFunc("DELETE /api/v1/sdk/register/{id}", s.handleDeregister)
	mux.HandleFunc("GET /api/v1/sdk/instances", s.handleListInstances)
	mux.HandleFunc("GET /api/v1/sdk/instances/{id}", s.handleGetInstance)
	mux.HandleFunc("POST /api/v1/sdk/instances/{id}/kill-switch", s.handleInstanceKillSwitch)
	mux.HandleFunc("GET /api/v1/sdk/rules", s.handlePollRules)
	mux.HandleFunc("GET /api/v1/sdk/init", s.handleInit)
	mux.HandleFunc("GET /api/v1/sdk/events", s.handleSSEEvents)

	// Cache ingest + serve
	mux.HandleFunc("POST /api/v1/cache/ingest", s.handleCacheIngest)
	mux.HandleFunc("GET /api/v1/cache/entries", s.handleCacheEntries)
	mux.HandleFunc("POST /api/v1/sdk/cachebox/drain", s.handleCacheDrain)

	// Zeus proxy — workflow lifecycle (register, validate, trigger runs, view status).
	// Manteion owns experiment orchestration; zeus owns workflow execution.
	// Attacks are NOT proxied — they are orchestrator-managed via typed client
	// methods (StartAttack/StopAttack). Direct attack proxy would bypass
	// orchestrator bookkeeping (poller state, result harvesting, run FSM).
	mux.HandleFunc("POST /api/v1/zeus/workflows", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/workflows", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/workflows/{id}", s.zeusProxy)
	mux.HandleFunc("DELETE /api/v1/zeus/workflows/{id}", s.zeusProxy)
	mux.HandleFunc("POST /api/v1/zeus/workflows/{id}/validate", s.zeusProxy)
	mux.HandleFunc("POST /api/v1/zeus/workflows/{id}/runs", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/workflows/{id}/runs", s.zeusProxy)

	// Zeus proxy — run status (cross-workflow).
	mux.HandleFunc("GET /api/v1/zeus/runs", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/runs/{run_id}", s.zeusProxy)
	mux.HandleFunc("DELETE /api/v1/zeus/runs/{run_id}", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/runs/{run_id}/events", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/runs/{run_id}/stats", s.zeusProxy)

	// Zeus proxy — datasets.
	mux.HandleFunc("POST /api/v1/zeus/datasets", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/datasets", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/datasets/{id}", s.zeusProxy)
	mux.HandleFunc("POST /api/v1/zeus/datasets/{id}/upload", s.zeusProxy)
	mux.HandleFunc("GET /api/v1/zeus/datasets/{id}/sample", s.zeusProxy)
	mux.HandleFunc("DELETE /api/v1/zeus/datasets/{id}", s.zeusProxy)
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

const maxBodySize = 1 << 20 // 1 MiB

// readJSON decodes the request body into v.
func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(io.LimitReader(r.Body, maxBodySize)).Decode(v)
}
