package orchestrator

import (
	"context"
	"testing"
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
