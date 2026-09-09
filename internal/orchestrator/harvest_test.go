package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/zeus"
)

// TestFinishPhase_HarvestUsesFidelitySnapshot pins the phase_service_cache
// harvest to the same W6 fidelity snapshots the verdict consumes: an isolation
// phase whose verdict saw 100 replay hits must persist hit_rate=1.0 and
// request_count=100. Harvest previously read the SDK's /admin/cachebox Store
// counters, which the replay path stopped incrementing when the record/replay
// split landed (replay consults the ReplaySet; hits count in the
// FidelityRegistry) — the exp2 "hit_rate=0 while the verdict saw 389 hits" bug.
func TestFinishPhase_HarvestUsesFidelitySnapshot(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")
	exp := mkExp(t)
	iso := mkRunningIsolation(t, exp.ID, "frontend", 0)

	finishIsolationVerdict(t, o, exp.ID, iso.ID, cleanSnapshot()) // 100 hits / 0 misses, mean age 30000 ms

	rows, err := testExpRepo.ListServiceCacheForPhase(ctx, iso.ID)
	if err != nil {
		t.Fatalf("list service cache: %v", err)
	}
	if len(rows) != 1 || rows[0].Service != "frontend" {
		t.Fatalf("service_cache rows = %+v, want exactly one frontend row", rows)
	}
	r := rows[0]
	if r.CacheHitRate != 1.0 || r.RequestCount != 100 {
		t.Fatalf("hit_rate=%v request_count=%d, want 1.0/100 (from the fidelity snapshot the verdict used)",
			r.CacheHitRate, r.RequestCount)
	}
	if r.CacheStaleness != 30000 {
		t.Fatalf("staleness=%v, want 30000 (snapshot mean replay age)", r.CacheStaleness)
	}
}

// ----------------------------------------------------------------------------
// k6 run stats → phase_workflow_results
// ----------------------------------------------------------------------------

// mkRunningPlain creates a running phase with no cache-box role (no
// persist_cache, no frozen services) and marks its experiment running — the
// shape of every expctl example phase, whose only load driver is a k6 run.
func mkRunningPlain(t *testing.T, expID string, pos int) *model.ExperimentPhase {
	t.Helper()
	ctx := context.Background()
	p := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: expID, Name: fmt.Sprintf("plain-%d", pos), Position: pos, Status: "pending"}
	if err := testExpRepo.CreatePhase(ctx, p); err != nil {
		t.Fatalf("create phase: %v", err)
	}
	if err := testExpRepo.UpdatePhaseStatus(ctx, p.ID, "running"); err != nil {
		t.Fatalf("run phase: %v", err)
	}
	if err := testExpRepo.UpdateStatus(ctx, expID, "running"); err != nil {
		t.Fatalf("run experiment: %v", err)
	}
	return p
}

// k6RunStats is a finalized zeus snapshot with values chosen so every
// conversion is checkable by hand: 250 requests over 10 s (25 rps), 10 dropped
// (4 %), and a p99 whose nanoseconds do not divide evenly into microseconds
// (12,345,678 ns → 12,345 µs: integer truncation, not rounding).
func k6RunStats(workflowID string) zeus.RunStats {
	return zeus.RunStats{
		WorkflowID: workflowID, Status: "completed",
		Duration: 10_000_000_000, Iterations: 50,
		RequestsSent: 250, RequestsOK: 240, RequestsDropped: 10,
		LatencyP50: 1_500_000, LatencyP95: 4_000_000, LatencyP99: 12_345_678,
		VariantDistribution: map[string]int64{"a": 30, "b": 20},
		FinalizedAt:         time.Now().UTC().Truncate(time.Second),
	}
}

// logBuffer is a mutex-guarded slog sink: finishPhase re-walks the scheduler
// on a goroutine, so log writes can outlive the call under test.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestHarvest_RunOnlyRowPersistsRunStats pins the k6 harvest path: a phase
// workflow that ran only as a zeus workflow run (zeus_run_id set, no attack —
// every expctl example phase) must persist a phase_workflow_results row from
// GET /runs/{id}/stats. Harvest used to skip every row without an attack id,
// so such a phase completed with zero results.
func TestHarvest_RunOnlyRowPersistsRunStats(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	exp := mkExp(t)
	p := mkRunningPlain(t, exp.ID, 0)
	runID := "run-" + p.ID
	wfID := attachAttackWorkflow(t, p.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 10, ZeusRunID: runID})
	fz.setRunStats(runID, k6RunStats(wfID), 0)

	if !o.finishPhase(ctx, p.ID, "completed", "running") {
		t.Fatal("finishPhase returned false, want true")
	}

	rows, err := testExpRepo.ListWorkflowResultsForPhase(ctx, p.ID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("workflow results: %d rows, err=%v; want exactly 1 for the run-only row", len(rows), err)
	}
	got := rows[0]
	if got.WorkflowID != wfID {
		t.Errorf("workflow_id = %q, want %q", got.WorkflowID, wfID)
	}
	// request_count = requests_sent, error_count = requests_dropped.
	if got.RequestCount != 250 || got.ErrorCount != 10 {
		t.Errorf("request_count=%d error_count=%d, want 250/10", got.RequestCount, got.ErrorCount)
	}
	// error_rate = dropped / sent; throughput_rps = sent / duration_seconds.
	if math.Abs(got.ErrorRate-0.04) > 1e-12 {
		t.Errorf("error_rate = %v, want 0.04 (10/250)", got.ErrorRate)
	}
	if math.Abs(got.ThroughputRPS-25.0) > 1e-9 {
		t.Errorf("throughput_rps = %v, want 25 (250 req / 10 s)", got.ThroughputRPS)
	}
	// ns → µs by integer truncation; p999 = p99 (zeus reports no p999).
	if got.LatencyP50Us != 1500 || got.LatencyP95Us != 4000 || got.LatencyP99Us != 12345 {
		t.Errorf("latency µs: p50=%d p95=%d p99=%d, want 1500/4000/12345",
			got.LatencyP50Us, got.LatencyP95Us, got.LatencyP99Us)
	}
	if got.LatencyP999Us != 12345 {
		t.Errorf("latency_p999_us = %d, want 12345 (= p99; zeus reports no p999)", got.LatencyP999Us)
	}
	// raw_metrics carries the RunStats snapshot itself, nanoseconds untouched.
	var raw zeus.RunStats
	if err := json.Unmarshal(got.RawMetrics, &raw); err != nil {
		t.Fatalf("raw_metrics is not a RunStats document: %v (%s)", err, got.RawMetrics)
	}
	if raw.RunID != runID || raw.RequestsSent != 250 || raw.Duration != 10_000_000_000 ||
		raw.LatencyP99 != 12_345_678 || raw.VariantDistribution["a"] != 30 {
		t.Errorf("raw_metrics = %s", got.RawMetrics)
	}
	if n := fz.runStatsCalls(runID); n != 1 {
		t.Errorf("stats endpoint hit %d times, want 1 (finalized on the first ask)", n)
	}
	if st := getPhase(t, p.ID).Status; st != "completed" {
		t.Errorf("phase status = %q, want completed", st)
	}
}

// TestHarvest_RunStatsNotReadyBacksOffThenPersists pins the retry contract:
// zeus answers the placeholder envelope while k6 is still flushing, and
// harvest waits it out on the same 1/2/4 s schedule the attack path uses
// instead of giving up on the first "not ready".
func TestHarvest_RunStatsNotReadyBacksOffThenPersists(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	exp := mkExp(t)
	p := mkRunningPlain(t, exp.ID, 0)
	runID := "run-" + p.ID
	wfID := attachAttackWorkflow(t, p.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 10, ZeusRunID: runID})
	fz.setRunStats(runID, k6RunStats(wfID), 2) // placeholder twice, then the snapshot

	start := time.Now()
	if !o.finishPhase(ctx, p.ID, "completed", "running") {
		t.Fatal("finishPhase returned false, want true")
	}
	elapsed := time.Since(start)

	rows, err := testExpRepo.ListWorkflowResultsForPhase(ctx, p.ID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("workflow results: %d rows, err=%v; want 1 after the placeholder clears", len(rows), err)
	}
	if rows[0].RequestCount != 250 || rows[0].LatencyP99Us != 12345 {
		t.Errorf("row = %+v, want the finalized snapshot (250 req, p99 12345 µs)", rows[0])
	}
	if n := fz.runStatsCalls(runID); n != 3 {
		t.Errorf("stats endpoint hit %d times, want 3 (placeholder, placeholder, snapshot)", n)
	}
	if elapsed < 3*time.Second {
		t.Errorf("harvest took %v, want ≥ 3 s (1 s + 2 s backoff before the third ask)", elapsed)
	}
}

// TestHarvest_RunStatsGoneTolerated pins the zeus-restart case: run state is
// in-memory on zeus, so a restart between run completion and harvest turns
// GET /runs/{id}/stats into a 404. That must not fail the phase — it completes
// with no result row for that workflow, no retries (a 404 is final, unlike the
// placeholder), and a structured stats_unavailable warning carrying the ids an
// operator needs to find the gap.
func TestHarvest_RunStatsGoneTolerated(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t) // knows no runs → the stats endpoint 404s
	o := newOrch(t, fz.srv.URL)
	logs := &logBuffer{}
	o.logger = slog.New(slog.NewJSONHandler(logs, nil))
	exp := mkExp(t)
	p := mkRunningPlain(t, exp.ID, 0)
	runID := "run-" + p.ID
	wfID := attachAttackWorkflow(t, p.ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1, ZeusRunID: runID})

	if !o.finishPhase(ctx, p.ID, "completed", "running") {
		t.Fatal("finishPhase returned false, want true")
	}

	rows, err := testExpRepo.ListWorkflowResultsForPhase(ctx, p.ID)
	if err != nil || len(rows) != 0 {
		t.Fatalf("workflow results: %d rows, err=%v; want none (stats gone, nothing true to write)", len(rows), err)
	}
	if st := getPhase(t, p.ID).Status; st != "completed" {
		t.Errorf("phase status = %q, want completed (missing stats must not fail the phase)", st)
	}
	if st := getExp(t, exp.ID).Status; st == "failed" {
		t.Errorf("experiment status = %q, want not failed", st)
	}
	if n := fz.runStatsMisses(runID); n != 1 {
		t.Errorf("stats endpoint asked %d times, want 1 (a 404 is final — no backoff)", n)
	}

	var warning string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "stats_unavailable") {
			warning = line
			break
		}
	}
	if warning == "" {
		t.Fatalf("no stats_unavailable log line; logs:\n%s", logs.String())
	}
	for _, want := range []string{
		`"level":"WARN"`,
		`"phase_id":"` + p.ID + `"`,
		`"workflow_id":"` + wfID + `"`,
		`"zeus_run_id":"` + runID + `"`,
		`"reason":"stats_unavailable"`,
	} {
		if !strings.Contains(warning, want) {
			t.Errorf("warning lacks %s: %s", want, warning)
		}
	}
}

// TestHarvest_RowWithBothIdsKeepsAttackResult pins the no-double-write rule:
// a phase workflow that drove BOTH an additive attack and a k6 run keeps the
// attack-based result exactly as before — the run stats are never fetched and
// cannot overwrite the row.
func TestHarvest_RowWithBothIdsKeepsAttackResult(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	exp := mkExp(t)
	p := mkRunningPlain(t, exp.ID, 0)
	runID, atkID := "run-"+p.ID, "atk-"+p.ID
	wfID := attachAttackWorkflow(t, p.ID, model.PhaseWorkflow{
		VUs: 1, DurationSec: 1, TargetURL: "http://frontend:8080/", TargetMethod: "GET",
		ZeusAttackID: atkID, ZeusRunID: runID,
	})
	fz.seedAttack(atkID, "completed", &zeus.AttackResultInfo{
		Service: "frontend", TotalRequests: 100, DurationMs: 1000, SuccessRate: 0.95,
		LatencyP50Us: 1000, LatencyP95Us: 2000, LatencyP99Us: 3000, ThroughputRPS: 100,
	})
	fz.setRunStats(runID, k6RunStats(wfID), 0) // 250 requests — must NOT win

	if !o.finishPhase(ctx, p.ID, "completed", "running") {
		t.Fatal("finishPhase returned false, want true")
	}

	rows, err := testExpRepo.ListWorkflowResultsForPhase(ctx, p.ID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("workflow results: %d rows, err=%v; want 1", len(rows), err)
	}
	got := rows[0]
	if got.WorkflowID != wfID || got.RequestCount != 100 || got.ErrorCount != 5 || got.LatencyP99Us != 3000 {
		t.Errorf("row = %+v, want the attack result (100 req, 5 errors, p99 3000 µs)", got)
	}
	if n := fz.runStatsCalls(runID); n != 0 {
		t.Errorf("stats endpoint hit %d times for a row that also has an attack, want 0", n)
	}
}

// TestRunStatsToResult_ZeroGuards pins the division guards: a run that sent
// nothing has error_rate 0 (not NaN) and a snapshot with no duration has
// throughput 0 (not +Inf) — either would poison the row's insert and the
// experiment rollup. Sub-microsecond latencies truncate to 0.
func TestRunStatsToResult_ZeroGuards(t *testing.T) {
	empty := runStatsToResult("phase-1", "wf-1", &zeus.RunStats{RunID: "run-1"})
	if empty.RequestCount != 0 || empty.ErrorCount != 0 || empty.ErrorRate != 0 || empty.ThroughputRPS != 0 {
		t.Errorf("empty run → %+v, want all-zero counters", empty)
	}
	noDuration := runStatsToResult("phase-1", "wf-1", &zeus.RunStats{RequestsSent: 10, RequestsDropped: 10, LatencyP99: 999})
	if noDuration.ErrorRate != 1 || noDuration.ThroughputRPS != 0 {
		t.Errorf("zero-duration run → error_rate=%v throughput=%v, want 1/0", noDuration.ErrorRate, noDuration.ThroughputRPS)
	}
	if noDuration.LatencyP99Us != 0 || noDuration.LatencyP999Us != 0 {
		t.Errorf("999 ns → p99=%d p999=%d µs, want 0/0 (sub-µs truncates)", noDuration.LatencyP99Us, noDuration.LatencyP999Us)
	}
	for _, r := range []*model.PhaseWorkflowResult{empty, noDuration} {
		if err := r.Validate(); err != nil {
			t.Errorf("%+v fails Validate: %v", r, err)
		}
	}
}
