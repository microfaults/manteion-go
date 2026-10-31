package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/testutil"
)

func TestReplayArtifactRepo_UpsertGetForPhase(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	exp := NewExperimentRepo(conn)
	repo := NewReplayArtifactRepo(conn)

	e := &model.Experiment{ID: id.New("exp"), Name: "art", Status: "planned", CreatedAt: time.Now()}
	if err := exp.Create(ctx, e); err != nil {
		t.Fatal(err)
	}
	p := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: "iso", Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: "frontend", Mode: "replay", MutationPolicy: "deny"}}}
	if err := exp.CreatePhase(ctx, p); err != nil {
		t.Fatal(err)
	}

	if got, err := repo.ForPhaseService(ctx, p.ID, "frontend"); err != nil || got != nil {
		t.Fatalf("ForPhaseService before upsert = %v, %v; want nil, nil", got, err)
	}
	a := &model.ReplayArtifact{ID: "sha256:" + id.New("h"), ExperimentID: e.ID, PhaseID: p.ID, Service: "frontend",
		EntryCount: 3, W5SHA256: "w5", SizeBytes: 120, Path: "/tmp/x.ndjson"}
	if err := repo.Upsert(ctx, a); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := repo.GetByID(ctx, a.ID)
	if err != nil || got.PhaseID != p.ID || got.EntryCount != 3 || got.Path != "/tmp/x.ndjson" || got.CreatedAt.IsZero() {
		t.Fatalf("GetByID = %+v, %v", got, err)
	}
	a.ID, a.EntryCount = "sha256:"+id.New("h"), 4 // re-assign the pair to a new build
	if err := repo.Upsert(ctx, a); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	if got, _ := repo.ForPhaseService(ctx, p.ID, "frontend"); got == nil || got.ID != a.ID || got.EntryCount != 4 {
		t.Fatalf("ForPhaseService after re-assign = %+v", got)
	}
	if _, err := repo.GetByID(ctx, "sha256:nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID unknown = %v, want ErrNotFound", err)
	}
}
