// Command manteion is the central coordination controller for the atropos
// ecosystem. It provides centralized rule management, zeus-go workflow
// orchestration, and SDK startup dependency enforcement.
//
// All state is persisted in PostgreSQL. Connection configured via
// MANTEION_DATABASE_URL environment variable.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"manteion-go/internal/api"
	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/atropos"
	"manteion-go/internal/cachestore"
	"manteion-go/internal/db"
	"manteion-go/internal/desired"
	"manteion-go/internal/logql"
	"manteion-go/internal/model"
	"manteion-go/internal/opaharness"
	"manteion-go/internal/orchestrator"
	"manteion-go/internal/policy"
	"manteion-go/internal/promql"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// @title           Manteion Control-Plane API
// @version         1.0
// @description     Central coordination controller for the atropos ecosystem.
// @description     Manages rules, faults, experiments, workflows, and SDK lifecycle.
// @servers.url            http://localhost:9090/api/v1
// @servers.description    Local dev (HTTP)
// @servers.url            https://localhost:9090/api/v1
// @servers.description    Local dev (HTTPS)
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Configuration from environment variables.
	addr := envOr("MANTEION_ADDR", ":9090")
	dsn := envOr("MANTEION_DATABASE_URL",
		"postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable")
	zeusURL := envOr("ZEUS_URL", "http://archer:8080")
	prometheusURL := envOr("PROMETHEUS_URL", "http://prometheus:9090")
	lokiURL := envOr("MANTEION_LOKI_URL", "http://loki:3100")
	cacheDir := envOr("CACHE_DIR", "/var/cache/manteion")
	// The convergence and drain barrier bounds (orchestrator setup below), which
	// also size the HTTP write timeout: startup refuses a value that is not a
	// positive duration of at most an hour (barrierTimeouts).
	convergeTimeout, drainTimeout, err := barrierTimeouts()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	// Connect to PostgreSQL and run migrations.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := db.Open(ctx, dsn)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close(database)

	// Create repositories. Generations is the desired-state change fabric: the
	// rule repo bumps it in-transaction, the API bumps it for fault configs, the
	// orchestrator bumps it on phase transitions, and every bump wakes the same
	// long-polls and SSE subscribers.
	gens := desired.NewGenerations(store.NewGenerationRepo(database), desired.NewBroker())
	ruleRepo := store.NewRuleRepo(database, gens)
	faultRepo := store.NewFaultRepo(database)
	faultConfigRepo := store.NewFaultConfigRepo(database)
	sdkRepo := store.NewSDKRepo(database)
	experimentRepo := store.NewExperimentRepo(database)
	workflowRepo := store.NewWorkflowRepo(database)
	workloadRepo := store.NewWorkloadRepo(database)
	traceRepo := store.NewTraceRepo(database)
	artifactRepo := store.NewReplayArtifactRepo(database)

	// Create zeus client.
	zeusClient := zeus.NewClient(zeusURL)

	// Create the atropos transport + fleet controller: instance status,
	// fidelity pulls and the live-instance resolver. Nothing is pushed to an
	// instance; rules, faults, the freeze and replay sets reach it through
	// the desired state it polls (replay sets are pulled by the instance).
	txClient := atropos.NewClient(atropos.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}))
	resolver := &atrocontrol.RepoResolver{Repo: sdkRepo}
	controller := atrocontrol.New(txClient, resolver,
		atrocontrol.WithDefaultTimeout(2*time.Second),
		atrocontrol.WithDefaultConcurrency(16),
		atrocontrol.WithLogger(logger),
	)

	policyRepo := store.NewPolicyRepo(database)

	// Create promql client, cache store, orchestrator, and policy engine.
	promClient := promql.NewClient(prometheusURL)
	cs := cachestore.New(cacheDir)
	if err := cs.EnsureRoot(); err != nil {
		logger.Error("cache store root not writable", "dir", cacheDir, "error", err)
		os.Exit(1)
	}
	phaseFaultEventRepo := store.NewPhaseFaultEventRepo(database)
	orch := orchestrator.New(experimentRepo, ruleRepo, faultRepo, workloadRepo, workflowRepo, controller, promClient, zeusClient, cs, phaseFaultEventRepo, artifactRepo, gens, logger)
	// Convergence barrier (desired-state contract §4): a phase entry or resume
	// bumps its services and waits up to MANTEION_CONVERGE_TIMEOUT for every
	// alive instance to observe the bump (and, on a frozen service, to hold the
	// replay set committed), failing the phase on timeout; an exit or drain
	// waits as long and only logs.
	orch.WithConvergeTimeout(convergeTimeout)
	// Drain barrier (MANT-2): MANTEION_DRAIN_TIMEOUT must be ≥ 3× the SDK poll
	// interval + flush time (default 30s); MANTEION_ALLOW_DEGRADED_BASELINE lets
	// isolation phases start from a degraded recording.
	orch.WithDrainTimeout(drainTimeout)
	orch.WithAllowDegradedBaseline(envOr("MANTEION_ALLOW_DEGRADED_BASELINE", "false") == "true")
	// MANTEION_ZEUS_DATASET_ID is the fallback zeus dataset for phase workflow
	// rows that carry no dataset_id of their own. Without either, zeus stages
	// an empty pool map, so a data_schema workflow drives unresolved
	// {{data.*}} traffic rather than failing.
	orch.WithZeusDatasetID(envOr("MANTEION_ZEUS_DATASET_ID", ""))
	policyEngine := policy.New(policyRepo, ruleRepo, promClient, logger)

	// OPA harness (docs/opa-harness.md): Rego policies over Prometheus/Loki
	// inputs whose outputs are rule mutations applied through ruleRepo, so
	// they reach instances by the same path as PUT /api/v1/rules/{id}. Always
	// constructed (the API's dry-run evaluate uses it); only Run is gated.
	opaPolicyRepo := store.NewOPAPolicyRepo(database)
	policyDecisionRepo := store.NewPolicyDecisionRepo(database)
	harness := opaharness.New(opaharness.Deps{
		Policies:  opaPolicyRepo,
		Decisions: policyDecisionRepo,
		Rules:     ruleRepo,
		Instances: sdkRepo,
		Providers: opaharness.Providers{
			model.InputSourcePrometheus: opaharness.VectorProvider{Querier: promClient},
			model.InputSourceLoki:       opaharness.VectorProvider{Querier: logql.NewClient(lokiURL)},
		},
		Mutator: opaharness.NewRuleMutator(ruleRepo, faultRepo),
		Logger:  logger,
	})

	// Restore in-flight runs from the DB. Must run before the API server
	// accepts requests, so callers see consistent state.
	if err := orch.Recover(ctx); err != nil {
		logger.Error("orchestrator recover failed", "error", err)
		os.Exit(1)
	}

	// The desired-state materializer: a function of the database that the SDK
	// handlers serve per instance, filtered by its capabilities. freeze_cfg and
	// replay_set come from the running phase that freezes the service and the
	// artifact assigned to it; one PhaseSources value fills both seams, so they
	// always name the same phase.
	phaseSources := &desired.PhaseSources{Phases: experimentRepo, Artifacts: artifactRepo}
	mat := &desired.Materializer{
		Generations: gens, Rules: ruleRepo, FaultConfigs: faultConfigRepo,
		CacheBox: experimentRepo, Specs: faultRepo,
		Freeze: phaseSources, ReplaySets: phaseSources,
	}

	// Create the API server with all dependencies.
	srv := api.NewServer(logger, database,
		ruleRepo, faultRepo, faultRepo, faultConfigRepo, sdkRepo,
		experimentRepo, phaseFaultEventRepo, workflowRepo, workloadRepo, traceRepo,
		zeusClient, gens, mat, orch, cs, artifactRepo, policyRepo,
		opaPolicyRepo, policyDecisionRepo, harness,
	)

	httpServer := &http.Server{
		Addr:        addr,
		Handler:     srv.Handler(),
		ReadTimeout: 10 * time.Second,
		// The lifecycle handlers answer only once the orchestrator is done,
		// and a stop can wait out a drain barrier and then an exit bump's
		// convergence wait (httpWriteTimeout): a shorter write deadline cuts
		// off the 202 of a stop that succeeded.
		WriteTimeout: httpWriteTimeout(convergeTimeout, drainTimeout),
		IdleTimeout:  60 * time.Second,
	}
	// Shutdown waits for in-flight requests, and a held long-poll (up to 60 s)
	// or an SDK's SSE stream would outlast its 10 s budget below: on Shutdown,
	// held polls answer at once as if their wait had elapsed and streams end.
	httpServer.RegisterOnShutdown(srv.BeginShutdown)

	// Policy automation. "rego" (or "on") starts the OPA harness
	// (docs/opa-harness.md); "threshold" starts the frozen PromQL engine
	// (docs/decisions/2026-06-policy-engine-freeze.md); "off" starts neither.
	switch mode := envOr("MANTEION_POLICY_ENGINE", "off"); mode {
	case "rego", "on":
		go harness.Run(ctx)
	case "threshold":
		go policyEngine.Run(ctx)
	case "off":
		logger.Info("policy automation disabled (MANTEION_POLICY_ENGINE=off; rego|on starts the OPA harness, threshold the frozen PromQL engine)")
	default:
		logger.Warn("unknown MANTEION_POLICY_ENGINE value; policy automation disabled", "value", mode)
	}

	// Complete expired long-running fault configs (status bookkeeping +
	// generation bump; the SDK reconciles them away).
	srv.StartFaultConfigReaper(ctx, 30*time.Second)

	// Start SDK reaper — purges instances dead for > 10 minutes. Opt-in via
	// MANTEION_SDK_PURGE_ENABLED (default false) so dead pods can linger on the
	// dashboard for postmortem unless an operator wants them auto-cleaned.
	if envOr("MANTEION_SDK_PURGE_ENABLED", "false") == "true" {
		go func() {
			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					n, err := sdkRepo.PurgeDead(ctx, 10*time.Minute)
					if err != nil {
						logger.Error("sdk reaper: purge failed", "error", err)
					} else if n > 0 {
						logger.Info("sdk reaper: purged dead instances", "count", n)
					}
				}
			}
		}()
	}

	// Start server in a goroutine.
	go func() {
		displayAddr := addr
		if displayAddr[0] == ':' {
			displayAddr = "localhost" + displayAddr
		}
		logger.Info("manteion starting",
			"url", "http://"+displayAddr,
			"zeus_url", zeusURL,
			"loki_url", lokiURL,
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Block until signal.
	<-ctx.Done()
	logger.Info("shutdown signal received")

	// Give in-flight requests 10 seconds to complete.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
		os.Exit(1)
	}

	logger.Info("manteion stopped")
}

// envOr returns the value of the environment variable key, or fallback if unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// maxBarrierTimeout caps MANTEION_CONVERGE_TIMEOUT and MANTEION_DRAIN_TIMEOUT.
// At the cap httpWriteTimeout is about three hours; without one, a large value
// of either would overflow it to a negative duration (no write deadline).
const maxBarrierTimeout = time.Hour

// barrierTimeouts reads the orchestrator's two barrier bounds:
// MANTEION_CONVERGE_TIMEOUT (the convergence barrier) and
// MANTEION_DRAIN_TIMEOUT (the drain barrier's report gate), 30 s each when
// unset. Both must be positive durations of at most maxBarrierTimeout: a zero
// or negative converge bound would fail every phase entry at its first look, a
// zero drain bound would end every drain before an instance could report, and
// both size httpWriteTimeout, which a negative drain bound shrinks (far enough
// below zero, to no write deadline at all) and a huge one overflows. A typo
// would silently run on the default.
func barrierTimeouts() (converge, drain time.Duration, err error) {
	if converge, err = boundedDurationEnv("MANTEION_CONVERGE_TIMEOUT", 30*time.Second, maxBarrierTimeout); err != nil {
		return 0, 0, err
	}
	if drain, err = boundedDurationEnv("MANTEION_DRAIN_TIMEOUT", 30*time.Second, maxBarrierTimeout); err != nil {
		return 0, 0, err
	}
	return converge, drain, nil
}

// boundedDurationEnv is positiveDurationEnv with an upper limit: a value above
// limit is an error naming key.
func boundedDurationEnv(key string, fallback, limit time.Duration) (time.Duration, error) {
	d, err := positiveDurationEnv(key, fallback)
	if err != nil {
		return 0, err
	}
	if d > limit {
		return 0, fmt.Errorf("%s=%q must be at most %s", key, os.Getenv(key), limit)
	}
	return d, nil
}

// positiveDurationEnv reads the environment variable key as a Go duration
// that must be positive; unset is fallback. A value that is not a duration,
// or is zero or negative, is an error naming key.
func positiveDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a duration (want e.g. \"30s\"): %w", key, v, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s=%q must be positive", key, v)
	}
	return d, nil
}

// httpWriteTimeout is the HTTP server's write deadline. A stop answers only
// once the phase has ended, and on the way it can wait out a recording
// phase's drain barrier — the convergence wait after its bump (up to
// convergeTimeout) and the report gate (up to drainTimeout) — and then the
// exit bump's convergence wait (up to convergeTimeout again); a start or a
// resume waits out one convergence. The extra 30 s covers the rest of the work
// (zeus calls, fidelity pulls, harvest). The long-poll and SSE handlers set
// their own deadlines.
func httpWriteTimeout(convergeTimeout, drainTimeout time.Duration) time.Duration {
	return 2*convergeTimeout + drainTimeout + 30*time.Second
}
