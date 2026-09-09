package api

import (
	"reflect"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/zeus"
)

func pw(workflowID, datasetID string, durationSec int) model.PhaseWorkflow {
	return model.PhaseWorkflow{WorkflowID: workflowID, VUs: 1, DurationSec: durationSec, DatasetID: datasetID}
}

// TestDatasetUnion pins the derived read-only phase field: the set-union of
// the phase's non-empty workflow dataset ids in first-seen order, never nil
// (the JSON must be [] rather than null).
func TestDatasetUnion(t *testing.T) {
	cases := []struct {
		name string
		pws  []model.PhaseWorkflow
		want []string
	}{
		{"nil rows", nil, []string{}},
		{"no datasets", []model.PhaseWorkflow{pw("wf-a", "", 1)}, []string{}},
		{"dedup keeps first-seen order", []model.PhaseWorkflow{
			pw("wf-a", "ds-b", 1), pw("wf-b", "ds-a", 1), pw("wf-c", "ds-b", 1), pw("wf-d", "", 1),
		}, []string{"ds-b", "ds-a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := datasetUnion(tc.pws)
			if got == nil {
				t.Fatal("datasetUnion returned nil; must be a non-nil slice so JSON is []")
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPlanLength pins the horizon a dataset must outlive: phases run
// sequentially and a phase's workflows concurrently, so the plan needs
// Σ over phases of max(duration_sec), plus a 5-minute slack.
func TestPlanLength(t *testing.T) {
	plan := [][]model.PhaseWorkflow{
		{pw("wf-a", "", 60), pw("wf-b", "", 120)}, // max 120
		{pw("wf-a", "", 30)},                      // 30
		{},                                        // driver-less phase adds nothing
	}
	if got, want := planLength(plan), 150*time.Second+5*time.Minute; got != want {
		t.Fatalf("planLength = %v, want %v", got, want)
	}
	if got, want := planLength(nil), 5*time.Minute; got != want {
		t.Fatalf("planLength(nil) = %v, want just the slack %v", got, want)
	}
}

// TestCheckDatasets is the union/TTL validator table: every referenced id
// must exist in zeus, and each must be kept past now + plan length unless
// ttl_s == 0 (never expires).
func TestCheckDatasets(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	ds := func(id string, ttlS int, createdAt time.Time) zeus.Dataset {
		return zeus.Dataset{ID: id, Name: id, Source: "upload", TTLS: ttlS, CreatedAt: createdAt}
	}
	cases := []struct {
		name                   string
		plan                   [][]model.PhaseWorkflow
		known                  []zeus.Dataset
		wantMissing, wantExpir []string
	}{
		{
			name:  "none referenced",
			plan:  [][]model.PhaseWorkflow{{pw("wf-a", "", 60)}, {}},
			known: nil, // zeus is never consulted; nothing to report either way
		},
		{
			name:        "missing",
			plan:        [][]model.PhaseWorkflow{{pw("wf-a", "ds-a", 60), pw("wf-b", "ds-b", 60)}},
			known:       []zeus.Dataset{ds("ds-a", 86400, now)},
			wantMissing: []string{"ds-b"},
		},
		{
			name:      "expiring: ttl shorter than the plan + slack",
			plan:      [][]model.PhaseWorkflow{{pw("wf-a", "ds-short", 60)}},
			known:     []zeus.Dataset{ds("ds-short", 60, now)}, // expires at now+60s < now+60s+5m
			wantExpir: []string{"ds-short"},
		},
		{
			name:  "ttl 0 never expires",
			plan:  [][]model.PhaseWorkflow{{pw("wf-a", "ds-forever", 36000)}},
			known: []zeus.Dataset{ds("ds-forever", 0, now.Add(-30*24*time.Hour))},
		},
		{
			name:  "ok: expires after the plan",
			plan:  [][]model.PhaseWorkflow{{pw("wf-a", "ds-a", 60)}},
			known: []zeus.Dataset{ds("ds-a", 3600, now)},
		},
		{
			name: "boundary: expiry exactly at now+plan is expiring (must be strictly later)",
			plan: [][]model.PhaseWorkflow{{pw("wf-a", "ds-edge", 60)}},
			// plan = 60s + 5m = 360s
			known:     []zeus.Dataset{ds("ds-edge", 360, now)},
			wantExpir: []string{"ds-edge"},
		},
		{
			name: "horizon sums the per-phase max across phases",
			plan: [][]model.PhaseWorkflow{
				{pw("wf-a", "ds-a", 60), pw("wf-b", "ds-b", 120)}, // 120
				{pw("wf-a", "ds-a", 30)},                          // 30 → 150s + 300s = 450s
			},
			known: []zeus.Dataset{
				ds("ds-a", 451, now), // survives by one second
				ds("ds-b", 450, now), // exactly the horizon → expiring
			},
			wantExpir: []string{"ds-b"},
		},
		{
			name: "created_at in the past counts against the ttl",
			plan: [][]model.PhaseWorkflow{{pw("wf-a", "ds-old", 60)}},
			// 24h ttl minted 23h59m ago: ~1 minute left, plan needs 6.
			known:     []zeus.Dataset{ds("ds-old", 86400, now.Add(-(24*time.Hour - time.Minute)))},
			wantExpir: []string{"ds-old"},
		},
		{
			name: "missing and expiring reported separately, first-seen order, deduped",
			plan: [][]model.PhaseWorkflow{
				{pw("wf-a", "ds-gone2", 10), pw("wf-b", "ds-short", 10)},
				{pw("wf-a", "ds-gone1", 10), pw("wf-b", "ds-gone2", 10)},
			},
			known:       []zeus.Dataset{ds("ds-short", 10, now)},
			wantMissing: []string{"ds-gone2", "ds-gone1"},
			wantExpir:   []string{"ds-short"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			missing, expiring := checkDatasets(now, tc.plan, tc.known)
			if !sameIDs(missing, tc.wantMissing) {
				t.Errorf("missing = %v, want %v", missing, tc.wantMissing)
			}
			if !sameIDs(expiring, tc.wantExpir) {
				t.Errorf("expiring = %v, want %v", expiring, tc.wantExpir)
			}
		})
	}
}

// sameIDs treats nil and empty as equal (the JSON-facing slices are
// normalized by the handler, not the validator).
func sameIDs(got, want []string) bool {
	if len(got) == 0 && len(want) == 0 {
		return true
	}
	return reflect.DeepEqual(got, want)
}
