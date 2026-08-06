// Package atropostest stands up real atropos-go SDK admin surfaces for
// manteion tests. The SDK internalized its granular wiring (2026-07-14,
// atropos-go d810a0a): the hand-assembled fake muxes this replaces cannot be
// built anymore, and atropos.Serve in offline mode serves the exact control
// surface a deployed SDK mounts — strictly better contract fidelity. State
// that used to be poked through white-box handles (evaluator, cache-box) is
// asserted through the HTTP surface instead.
package atropostest

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
	"go.opentelemetry.io/otel/trace/noop"
)

// NewHandler builds the SDK control surface via atropos.Serve in offline
// mode and registers its shutdown on t.Cleanup. Each call is an independent
// SDK instance: its own evaluator, fault slots, and cache-box.
func NewHandler(t *testing.T) http.Handler {
	t.Helper()
	// An empty Config.ManteionURL falls back to the MANTEION_URL env var;
	// clear it so tests stay offline regardless of the developer's shell.
	t.Setenv("MANTEION_URL", "")
	h, shutdown, err := atroposdk.Serve(context.Background(), atroposdk.Config{
		Service:        "atropostest",
		TracerProvider: noop.NewTracerProvider(), // hermetic: no OTLP exporter
		Logger:         slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("atropos.Serve: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			t.Errorf("atropos shutdown: %v", err)
		}
	})
	return h
}

// NewServer wraps NewHandler in an httptest.Server closed on t.Cleanup.
func NewServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(NewHandler(t))
	t.Cleanup(srv.Close)
	return srv
}
