package desired_test

import (
	"context"
	"testing"

	"manteion-go/internal/desired"
	"manteion-go/internal/store"
	"manteion-go/internal/testutil"
)

// TestGenerationRepo_BumpAndGet pins the service_generations SQL: absent = 1,
// first bump = 2, a bump inside a rolled-back transaction leaves no trace, and
// desired.Generations drives it unchanged.
func TestGenerationRepo_BumpAndGet(t *testing.T) {
	conn := testutil.TestDB(t)
	ctx := context.Background()
	repo := store.NewGenerationRepo(conn)
	g := desired.NewGenerations(repo, desired.NewBroker())

	if gen, err := g.Get(ctx, "cart"); err != nil || gen != 1 {
		t.Fatalf("Get absent = %d, %v; want 1", gen, err)
	}
	if gen, err := g.Bump(ctx, "cart"); err != nil || gen != 2 {
		t.Fatalf("Bump = %d, %v; want 2", gen, err)
	}
	tx, _ := conn.BeginTx(ctx, nil)
	if gen, err := g.BumpTx(ctx, tx, "cart"); err != nil || gen != 3 {
		t.Fatalf("BumpTx = %d, %v; want 3", gen, err)
	}
	_ = tx.Rollback()
	if gen, _ := g.Get(ctx, "cart"); gen != 2 {
		t.Fatalf("Get after rollback = %d, want 2", gen)
	}
}
