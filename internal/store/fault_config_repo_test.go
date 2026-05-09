package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/testutil"
)

func TestFaultConfigRepo_CRUD(t *testing.T) {
	db := testutil.TestDB(t)
	repo := store.NewFaultConfigRepo(db)
	ctx := context.Background()

	cfg := &model.FaultConfig{
		ID:         "fc-test-1",
		Name:       "Test Fault",
		Service:    "productcatalog",
		Category:   "inline",
		FaultType:  "latency",
		FaultReq:   json.RawMessage(`{"delay":"100ms"}`),
		DurationMs: 0,
		Status:     model.FaultConfigReady,
	}

	// Create
	if err := repo.Create(ctx, cfg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Get
	got, err := repo.Get(ctx, cfg.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != cfg.Name {
		t.Errorf("got name %q, want %q", got.Name, cfg.Name)
	}

	// Update
	got.Description = "Updated description"
	if err := repo.Update(ctx, got); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// List
	list, err := repo.List(ctx, store.ListFaultConfigFilters{Service: "productcatalog"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List length = %d, want 1", len(list))
	}
	if list[0].Description != "Updated description" {
		t.Errorf("List desc = %q, want updated", list[0].Description)
	}

	// MarkFired
	if err := repo.MarkFired(ctx, cfg.ID); err != nil {
		t.Fatalf("MarkFired: %v", err)
	}

	// Cannot delete active
	if err := repo.Delete(ctx, cfg.ID); err != store.ErrConflict {
		t.Errorf("Delete active should return ErrConflict, got %v", err)
	}

	// Cannot update active
	got.Description = "Fail update"
	if err := repo.Update(ctx, got); err != store.ErrConflict {
		t.Errorf("Update active should return ErrConflict, got %v", err)
	}

	// MarkCompleted
	if err := repo.MarkCompleted(ctx, cfg.ID); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	// Delete
	if err := repo.Delete(ctx, cfg.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := repo.Get(ctx, cfg.ID); err != store.ErrNotFound {
		t.Errorf("Get after delete should return ErrNotFound, got %v", err)
	}
}

func TestFaultConfigRepo_Expired(t *testing.T) {
	db := testutil.TestDB(t)
	repo := store.NewFaultConfigRepo(db)
	ctx := context.Background()

	cfg := &model.FaultConfig{
		ID:         "fc-exp-1",
		Name:       "Expiring Fault",
		Service:    "productcatalog",
		Category:   "inline",
		FaultType:  "latency",
		FaultReq:   json.RawMessage(`{"delay":"100ms"}`),
		DurationMs: 1, // 1ms
		Status:     model.FaultConfigReady,
	}

	if err := repo.Create(ctx, cfg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := repo.MarkFired(ctx, cfg.ID); err != nil {
		t.Fatalf("MarkFired: %v", err)
	}

	// wait 10ms to ensure it expires
	time.Sleep(10 * time.Millisecond)

	expired, err := repo.ListExpired(ctx)
	if err != nil {
		t.Fatalf("ListExpired: %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired fault, got %d", len(expired))
	}
	if expired[0].ID != cfg.ID {
		t.Errorf("got %q, want %q", expired[0].ID, cfg.ID)
	}
}
