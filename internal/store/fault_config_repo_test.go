package store

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// TestFaultConfigRepo_Lifecycle exercises create → fire → active/expired →
// terminal transitions, focusing on the duration-interval SQL that drives the
// poll set (ListActiveForService) and the reaper (ListExpired).
func TestFaultConfigRepo_Lifecycle(t *testing.T) {
	ctx := context.Background()
	repo := NewFaultConfigRepo(testDB)

	tag := fmt.Sprintf("%d", time.Now().UnixNano())
	svc := "svc-" + tag

	mk := func(id string, dur int64) *model.FaultConfig {
		return &model.FaultConfig{
			ID:         id,
			Name:       "lr-" + id,
			Service:    svc,
			Category:   "inline",
			FaultType:  "latency",
			Params:     json.RawMessage(`{"delay":"100ms"}`),
			DurationMs: dur,
			Status:     model.FaultConfigReady,
		}
	}

	infinite := mk("fc-inf-"+tag, 0)
	finite := mk("fc-fin-"+tag, 1) // expires ~immediately after firing
	for _, f := range []*model.FaultConfig{infinite, finite} {
		if err := repo.Create(ctx, f); err != nil {
			t.Fatalf("create %s: %v", f.ID, err)
		}
		t.Cleanup(func() { _ = repo.Delete(context.Background(), f.ID) })
	}

	// Ready configs are not yet part of the poll set.
	if got, err := repo.ListActiveForService(ctx, svc); err != nil || len(got) != 0 {
		t.Fatalf("expected no active before fire, got %d (err %v)", len(got), err)
	}

	// Fire both.
	for _, f := range []*model.FaultConfig{infinite, finite} {
		if err := repo.MarkFired(ctx, f.ID); err != nil {
			t.Fatalf("fire %s: %v", f.ID, err)
		}
	}

	// Let the 1ms finite fault elapse.
	time.Sleep(30 * time.Millisecond)

	// Poll set: only the infinite fault (finite has expired).
	active, err := repo.ListActiveForService(ctx, svc)
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(active) != 1 || active[0].ID != infinite.ID {
		t.Fatalf("expected only %s active, got %v", infinite.ID, ids(active))
	}

	// Reaper set: only the finite fault.
	expired, err := repo.ListExpired(ctx)
	if err != nil {
		t.Fatalf("list expired: %v", err)
	}
	foundFinite := false
	for _, f := range expired {
		if f.ID == finite.ID {
			foundFinite = true
		}
		if f.ID == infinite.ID {
			t.Fatal("infinite fault must never be expired")
		}
	}
	if !foundFinite {
		t.Fatalf("expected %s in expired set, got %v", finite.ID, ids(expired))
	}

	// Reaper marks finite completed; cancel the infinite one.
	if err := repo.MarkCompleted(ctx, finite.ID); err != nil {
		t.Fatalf("mark completed: %v", err)
	}
	if err := repo.MarkCancelled(ctx, infinite.ID); err != nil {
		t.Fatalf("mark cancelled: %v", err)
	}

	// Nothing active for the service now.
	if got, _ := repo.ListActiveForService(ctx, svc); len(got) != 0 {
		t.Fatalf("expected no active after terminal, got %v", ids(got))
	}

	gotInf, err := repo.Get(ctx, infinite.ID)
	if err != nil {
		t.Fatalf("get infinite: %v", err)
	}
	if gotInf.Status != model.FaultConfigCancelled || gotInf.CompletedAt == nil {
		t.Fatalf("expected cancelled+completed_at, got status=%s completed_at=%v", gotInf.Status, gotInf.CompletedAt)
	}

	if err := repo.Delete(ctx, "fc-missing-"+tag); err != ErrNotFound {
		t.Fatalf("delete missing: expected ErrNotFound, got %v", err)
	}
}

// TestFaultConfigRepo_TiesOnCreationTimeBreakByID pins the poll set's order as
// a function of the stored configs: oldest first, and configs created in the
// same instant by id, however the heap lays them out. The order is part of the
// materialized document, so it feeds the ETag the instances converge on.
func TestFaultConfigRepo_TiesOnCreationTimeBreakByID(t *testing.T) {
	ctx := context.Background()
	repo := NewFaultConfigRepo(testDB)
	tag := fmt.Sprintf("%d", time.Now().UnixNano())
	svc := "svc-" + tag
	zID, aID := "fc-z-"+tag, "fc-a-"+tag

	// Created and fired z first, so the scan order is the reverse of the id order.
	for _, id := range []string{zID, aID} {
		f := &model.FaultConfig{ID: id, Name: "tie-" + id, Service: svc, Category: "inline", FaultType: "latency",
			Params: json.RawMessage(`{"delay":"100ms"}`), Status: model.FaultConfigReady}
		if err := repo.Create(ctx, f); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		t.Cleanup(func() { _ = repo.Delete(context.Background(), id) })
		if err := repo.MarkFired(ctx, id); err != nil {
			t.Fatalf("fire %s: %v", id, err)
		}
	}
	// created_at is now() per insert; pin both configs to one instant.
	if _, err := testDB.ExecContext(ctx,
		`UPDATE fault_configs SET created_at = $1 WHERE service = $2`, time.Now().Add(-time.Hour), svc); err != nil {
		t.Fatalf("pin created_at: %v", err)
	}

	want := []string{aID, zID}
	active, err := repo.ListActiveForService(ctx, svc)
	if err != nil {
		t.Fatalf("ListActiveForService: %v", err)
	}
	if !slices.Equal(ids(active), want) {
		t.Errorf("ListActiveForService = %v, want %v", ids(active), want)
	}
	listed, err := repo.List(ctx, svc, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !slices.Equal(ids(listed), want) {
		t.Errorf("List = %v, want %v", ids(listed), want)
	}
}

func ids(fs []*model.FaultConfig) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.ID
	}
	return out
}
