package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

func TestListPhasesPaged(t *testing.T) {
	if testDB == nil {
		t.Skip("no test DB")
	}
	ctx := context.Background()
	expRepo := NewExperimentRepo(testDB)
	exp := &model.Experiment{ID: id.New("exp"), Name: "lp-test", Status: "planned", CreatedAt: time.Now()}
	if err := expRepo.Create(ctx, exp); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	ph := &model.ExperimentPhase{
		ID: id.New("phase"), ExperimentID: exp.ID, Name: "iso", Position: 0, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: "frontend", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
	}
	if err := expRepo.CreatePhase(ctx, ph); err != nil {
		t.Fatalf("create phase: %v", err)
	}

	// Page until the created phase appears rather than asserting it lands on
	// page 1: the shared test DB carries rows from other suites and earlier
	// runs, and a fresh pending phase (NULL started_at) sorts after every
	// started row — with enough leftovers it legitimately lives on a later
	// page. Bounded by the reported total, so a genuinely missing row still
	// fails instead of looping.
	const pageSize = 50
	var (
		found *model.PhaseListItem
		total int
	)
	for offset := 0; ; offset += pageSize {
		items, tot, err := expRepo.ListPhasesPaged(ctx, Page{Limit: pageSize, Offset: offset})
		if err != nil {
			t.Fatalf("list phases paged (offset %d): %v", offset, err)
		}
		total = tot
		for _, it := range items {
			if it.ID == ph.ID {
				found = it
				break
			}
		}
		if found != nil || len(items) == 0 || offset+pageSize >= total {
			break
		}
	}
	if total < 1 {
		t.Fatalf("total=%d, want >=1", total)
	}
	if found == nil {
		t.Fatalf("created phase not in any page (total=%d)", total)
	}
	if found.ExperimentName != "lp-test" {
		t.Errorf("experiment_name=%q, want lp-test", found.ExperimentName)
	}
	if found.FrozenServiceCount != 1 {
		t.Errorf("frozen_service_count=%d, want 1", found.FrozenServiceCount)
	}
}

func TestPhaseFaultEventRepo(t *testing.T) {
	if testDB == nil {
		t.Skip("no test DB")
	}
	ctx := context.Background()
	expRepo := NewExperimentRepo(testDB)
	repo := NewPhaseFaultEventRepo(testDB)

	// Fixtures: an experiment + phase to satisfy the FK.
	exp := &model.Experiment{ID: id.New("exp"), Name: "fe-test", Status: "planned", CreatedAt: time.Now()}
	if err := expRepo.Create(ctx, exp); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	ph := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "p0", Position: 0, Status: "pending"}
	if err := expRepo.CreatePhase(ctx, ph); err != nil {
		t.Fatalf("create phase: %v", err)
	}

	open := &model.PhaseFaultEvent{
		ID: id.New("fevt"), PhaseID: ph.ID, Source: "cachebox", Service: "frontend",
		Kind: "cachebox:replay", Detail: json.RawMessage(`{"key_strategy":"exact"}`), StartedAt: time.Now(),
	}
	if err := repo.Create(ctx, open); err != nil {
		t.Fatalf("create event: %v", err)
	}

	got, err := repo.ListForPhase(ctx, ph.ID)
	if err != nil || len(got) != 1 {
		t.Fatalf("list: %d rows, err=%v; want 1", len(got), err)
	}
	if got[0].EndedAt != nil {
		t.Errorf("event should be open, got ended_at=%v", got[0].EndedAt)
	}

	if err := repo.EndOpenForPhase(ctx, ph.ID, time.Now()); err != nil {
		t.Fatalf("end open: %v", err)
	}
	got, _ = repo.ListForPhase(ctx, ph.ID)
	if got[0].EndedAt == nil {
		t.Error("event should be closed after EndOpenForPhase")
	}
}
