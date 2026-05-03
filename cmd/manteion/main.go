// Command manteion is the central coordination controller for the atropos
// ecosystem. It provides centralized rule management, zeus-go workflow
// orchestration, and SDK startup dependency enforcement.
//
// All state is persisted in PostgreSQL. Connection configured via
// MANTEION_DATABASE_URL environment variable.
package main

import (
	"context"
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
// @servers.url            http://localhost:8080/api/v1
// @servers.description    Local dev (HTTP)
// @servers.url            https://localhost:8080/api/v1
// @servers.description    Local dev (HTTPS)
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Configuration from environment variables.
	addr := envOr("MANTEION_ADDR", ":8080")
	dsn := envOr("MANTEION_DATABASE_URL",
		"postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable")
	zeusURL := envOr("ZEUS_URL", "http://archer:8080")
	prometheusURL := envOr("PROMETHEUS_URL", "http://prometheus:9090")
	cacheDir := envOr("CACHE_DIR", "/var/cache/manteion")

	// Connect to PostgreSQL and run migrations.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := db.Open(ctx, dsn)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close(database)

	// Create repositories.
	ruleRepo := store.NewRuleRepo(database)
	faultRepo := store.NewFaultRepo(database)
	sdkRepo := store.NewSDKRepo(database)
	experimentRepo := store.NewExperimentRepo(database)
	workloadRepo := store.NewWorkloadRepo(database)
	traceRepo := store.NewTraceRepo(database)

	// Create zeus client.
	zeusClient := zeus.NewClient(zeusURL)

	// Create atropos transport + orchestration controller.
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
	orch := orchestrator.New(experimentRepo, ruleRepo, faultRepo, workloadRepo, controller, promClient, zeusClient, cs, logger)
	policyEngine := policy.New(policyRepo, ruleRepo, faultRepo, controller, promClient, logger)

	// Restore in-flight runs from the DB. Must run before the API server
	// accepts requests, so callers see consistent state.
	if err := orch.Recover(ctx); err != nil {
		logger.Error("orchestrator recover failed", "error", err)
		os.Exit(1)
	}

	// Create the API server with all dependencies.
	srv := api.NewServer(logger, database,
		ruleRepo, faultRepo, faultRepo, sdkRepo,
		experimentRepo, workloadRepo, traceRepo,
		zeusClient, controller.IntentReader(), orch, cs, policyRepo,
	)

	httpServer := &http.Server{
		Addr:         addr,
		Handler:      srv.Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start policy engine as background goroutine.
	go policyEngine.Run(ctx)

	// Start server in a goroutine.
	go func() {
		logger.Info("manteion starting",
			"addr", addr,
			"zeus_url", zeusURL,
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
