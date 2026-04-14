// Command manteion is the central coordination controller for the atropos
// ecosystem. It provides centralized rule management, zeus-go workflow
// orchestration, and SDK startup dependency enforcement.
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
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Configuration from environment variables.
	addr := envOr("MANTEION_ADDR", ":8080")

	// TODO: Wire stores and zeus client when store layer is implemented.
	// ruleStore := rule.NewStore()
	// sdkRegistry := sdk.NewRegistry()
	// zeusClient := zeus.NewClient(envOr("ZEUS_URL", "http://archer:8080"))

	srv := api.NewServer(logger)

	httpServer := &http.Server{
		Addr:         addr,
		Handler:      srv.Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown: listen for SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start server in a goroutine.
	go func() {
		logger.Info("manteion starting", "addr", addr)
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
