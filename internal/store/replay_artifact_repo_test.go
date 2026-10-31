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
	if got, err := repo.ForPhaseService(ctx, p.ID, "frontend"); err != nil || got == nil || got.ID != a.ID || got.EntryCount != 4 {
		t.Fatalf("ForPhaseService after re-assign = %+v, %v", got, err)
	}
	if _, err := repo.GetByID(ctx, "sha256:nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID unknown = %v, want ErrNotFound", err)
	}
}

// TestReplayArtifactRepo_ReassignShareAndCascade pins what the test above
// leaves open. Upsert reads created_at back. A re-assign replaces every column
// of the pair's row, restamps created_at, and the old id stops resolving (an
// artifact id resolves only while some pair holds it). One id can be assigned
// to several pairs (two phases replaying the same recording share the
// content-addressed file). Deleting a phase removes its rows; deleting the
// experiment removes the rest.
func TestReplayArtifactRepo_ReassignShareAndCascade(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	exp := NewExperimentRepo(conn)
	repo := NewReplayArtifactRepo(conn)

	e := &model.Experiment{ID: id.New("exp"), Name: "art-cascade", Status: "planned", CreatedAt: time.Now()}
	if err := exp.Create(ctx, e); err != nil {
		t.Fatal(err)
	}
	mkPhase := func(name string, pos int) *model.ExperimentPhase {
		t.Helper()
		p := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: e.ID, Name: name, Position: pos, Status: "pending",
			FrozenServices: []model.CacheBoxConfig{{Service: "frontend", Mode: "replay", MutationPolicy: "deny"}}}
		if err := exp.CreatePhase(ctx, p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	p1, p2 := mkPhase("iso-1", 0), mkPhase("iso-2", 1)
	same := func(what string, got *model.ReplayArtifact, want model.ReplayArtifact) {
		t.Helper()
		if got == nil {
			t.Fatalf("%s = nil, want %+v", what, want)
		}
		createdAt := got.CreatedAt
		g := *got
		g.CreatedAt, want.CreatedAt = time.Time{}, time.Time{}
		if g != want || createdAt.IsZero() {
			t.Fatalf("%s = %+v (created_at %v), want %+v", what, *got, createdAt, want)
		}
	}

	first := &model.ReplayArtifact{ID: "sha256:" + id.New("h"), ExperimentID: e.ID, PhaseID: p1.ID, Service: "frontend",
		EntryCount: 3, W5SHA256: "w5-a", SizeBytes: 120, Path: "/tmp/a.ndjson"}
	if err := repo.Upsert(ctx, first); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if first.CreatedAt.IsZero() {
		t.Fatal("Upsert left CreatedAt zero, want the row's created_at")
	}
	stored, err := repo.ForPhaseService(ctx, p1.ID, "frontend")
	if err != nil {
		t.Fatal(err)
	}
	if !stored.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("stored created_at %v, Upsert read back %v", stored.CreatedAt, first.CreatedAt)
	}

	// Re-assign the pair to a new build: every column is replaced.
	second := &model.ReplayArtifact{ID: "sha256:" + id.New("h"), ExperimentID: e.ID, PhaseID: p1.ID, Service: "frontend",
		EntryCount: 7, W5SHA256: "w5-b", SizeBytes: 280, Path: "/tmp/b.ndjson"}
	if err := repo.Upsert(ctx, second); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	if second.CreatedAt.Before(first.CreatedAt) {
		t.Fatalf("re-assign created_at %v is before the first %v: want it restamped", second.CreatedAt, first.CreatedAt)
	}
	got, err := repo.ForPhaseService(ctx, p1.ID, "frontend")
	if err != nil {
		t.Fatal(err)
	}
	same("ForPhaseService after re-assign", got, *second)
	if !got.CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("stored created_at %v, Upsert read back %v", got.CreatedAt, second.CreatedAt)
	}
	if _, err := repo.GetByID(ctx, first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID(replaced id) = %v, want ErrNotFound", err)
	}

	// The same artifact assigned to a second phase: both pairs hold it, and
	// the id resolves to the newest row.
	shared := *second
	shared.PhaseID = p2.ID
	if err := repo.Upsert(ctx, &shared); err != nil {
		t.Fatalf("upsert shared: %v", err)
	}
	for _, p := range []*model.ExperimentPhase{p1, p2} {
		if got, err := repo.ForPhaseService(ctx, p.ID, "frontend"); err != nil || got == nil || got.ID != second.ID {
			t.Fatalf("ForPhaseService(%s) = %+v, %v; want the shared id %s", p.Name, got, err, second.ID)
		}
	}
	if got, err := repo.GetByID(ctx, second.ID); err != nil || got.PhaseID != p2.ID {
		t.Fatalf("GetByID(shared) = %+v, %v; want the newest row (phase %s)", got, err, p2.ID)
	}

	// Deleting a phase removes its rows only.
	if err := exp.DeletePhase(ctx, p1.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := repo.ForPhaseService(ctx, p1.ID, "frontend"); err != nil || got != nil {
		t.Fatalf("ForPhaseService(deleted phase) = %+v, %v; want nil, nil", got, err)
	}
	if got, err := repo.GetByID(ctx, second.ID); err != nil || got.PhaseID != p2.ID {
		t.Fatalf("GetByID after the phase delete = %+v, %v; want the remaining pair", got, err)
	}

	// Deleting the experiment removes the rest.
	if err := exp.Delete(ctx, e.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := repo.ForPhaseService(ctx, p2.ID, "frontend"); err != nil || got != nil {
		t.Fatalf("ForPhaseService after the experiment delete = %+v, %v; want nil, nil", got, err)
	}
	if _, err := repo.GetByID(ctx, second.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID after the experiment delete = %v, want ErrNotFound", err)
	}
}
