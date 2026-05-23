package store

import (
	"context"
	"encoding/json"
	"fmt"
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
			FaultReq:   json.RawMessage(`{"delay":"100ms"}`),
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

func ids(fs []*model.FaultConfig) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.ID
	}
	return out
}
