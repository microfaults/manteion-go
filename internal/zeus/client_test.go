package zeus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Pins the run wire contract the poller's load-health warning depends on:
// zeus stamps reason="thresholds breached" on a COMPLETED run when k6 exits
// 99 (client-side gates crossed). Status stays completed — a breach is a
// health annotation, never a run failure.
func TestRunInfoDecodesBreachReason(t *testing.T) {
	var info RunInfo
	raw := `{"id":"run-1","status":"completed","reason":"thresholds breached","extra":"ignored"}`
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if info.Status != "completed" || info.Reason != "thresholds breached" {
		t.Fatalf("got status=%q reason=%q", info.Status, info.Reason)
	}
}

// Pins GET /api/v1/runs/{id}/stats against zeus-go's stats.RunStats wire
// shape: duration and the latency percentiles are Go time.Duration values
// serialised as integer NANOSECONDS; an in-flight run answers 200 with the
// placeholder envelope {"run_id", "status": "no stats available yet"} (not a
// 404); a run zeus no longer knows (in-memory state lost on restart) is 404.
func TestGetRunStats(t *testing.T) {
	var gotPath, gotMethod string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/runs/{run_id}/stats", func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.Header().Set("Content-Type", "application/json")
		switch r.PathValue("run_id") {
		case "run-ok":
			_, _ = w.Write([]byte(`{
				"run_id": "run-ok", "workflow_id": "wf-1", "status": "completed",
				"duration": 10000000000, "iterations": 50,
				"requests_sent": 250, "requests_ok": 240, "requests_dropped": 10,
				"latency_p50": 1500000, "latency_p95": 4000000, "latency_p99": 12345678,
				"variant_distribution": {"a": 30, "b": 20},
				"finalized_at": "2026-09-08T12:00:00Z"
			}`))
		case "run-pending":
			_, _ = w.Write([]byte(`{"run_id": "run-pending", "status": "no stats available yet"}`))
		case "run-gone":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error": "run not found: run-gone"}`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error": "boom"}`))
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := NewClient(srv.URL)
	ctx := context.Background()

	t.Run("finalized stats decode nanoseconds verbatim", func(t *testing.T) {
		rs, err := c.GetRunStats(ctx, "run-ok")
		if err != nil {
			t.Fatalf("GetRunStats: %v", err)
		}
		if gotMethod != http.MethodGet || gotPath != "/api/v1/runs/run-ok/stats" {
			t.Fatalf("request = %s %s, want GET /api/v1/runs/run-ok/stats", gotMethod, gotPath)
		}
		if rs.RunID != "run-ok" || rs.WorkflowID != "wf-1" || rs.Status != "completed" {
			t.Errorf("identity: %+v", rs)
		}
		if rs.Duration != 10_000_000_000 || rs.Iterations != 50 {
			t.Errorf("duration=%d iterations=%d, want 10000000000/50", rs.Duration, rs.Iterations)
		}
		if rs.RequestsSent != 250 || rs.RequestsOK != 240 || rs.RequestsDropped != 10 {
			t.Errorf("counts: sent=%d ok=%d dropped=%d, want 250/240/10", rs.RequestsSent, rs.RequestsOK, rs.RequestsDropped)
		}
		if rs.LatencyP50 != 1_500_000 || rs.LatencyP95 != 4_000_000 || rs.LatencyP99 != 12_345_678 {
			t.Errorf("latencies (ns): p50=%d p95=%d p99=%d, want 1500000/4000000/12345678", rs.LatencyP50, rs.LatencyP95, rs.LatencyP99)
		}
		if rs.VariantDistribution["a"] != 30 || rs.VariantDistribution["b"] != 20 {
			t.Errorf("variant_distribution = %v", rs.VariantDistribution)
		}
		if want := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC); !rs.FinalizedAt.Equal(want) {
			t.Errorf("finalized_at = %v, want %v", rs.FinalizedAt, want)
		}
	})

	t.Run("placeholder envelope is ErrRunStatsNotReady", func(t *testing.T) {
		rs, err := c.GetRunStats(ctx, "run-pending")
		if !errors.Is(err, ErrRunStatsNotReady) {
			t.Fatalf("err = %v (stats=%+v), want ErrRunStatsNotReady", err, rs)
		}
		if rs != nil {
			t.Fatalf("stats = %+v, want nil on not-ready", rs)
		}
	})

	t.Run("404 is ErrRunNotFound", func(t *testing.T) {
		_, err := c.GetRunStats(ctx, "run-gone")
		if !errors.Is(err, ErrRunNotFound) {
			t.Fatalf("err = %v, want ErrRunNotFound", err)
		}
		if errors.Is(err, ErrRunStatsNotReady) {
			t.Fatal("404 must not read as not-ready (a not-ready is retried; a 404 is final)")
		}
	})

	t.Run("other statuses are plain errors", func(t *testing.T) {
		_, err := c.GetRunStats(ctx, "run-boom")
		if err == nil || errors.Is(err, ErrRunNotFound) || errors.Is(err, ErrRunStatsNotReady) {
			t.Fatalf("err = %v, want a non-sentinel error", err)
		}
	})
}
