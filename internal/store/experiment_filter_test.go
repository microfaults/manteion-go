package store

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// TestExperimentList_FilterCreatedBy pins ExperimentFilter.CreatedBy: List keeps
// only the experiments that creator made, counts only those in the total, and
// ANDs the filter with Status. The demo lists its own runs this way.
func TestExperimentList_FilterCreatedBy(t *testing.T) {
	ctx := context.Background()
	repo := NewExperimentRepo(testDB)
	const demo = "manteion-demo"

	// The totals below are exact, so no demo experiment from an earlier run
	// may linger in the test database.
	if _, err := testDB.ExecContext(ctx, `DELETE FROM experiments WHERE created_by = $1`, demo); err != nil {
		t.Fatalf("clear earlier demo experiments: %v", err)
	}

	tag := fmt.Sprintf("filter-createdby-%d", time.Now().UnixNano())
	create := func(suffix, createdBy, status string) *model.Experiment {
		t.Helper()
		exp := &model.Experiment{ID: "exp-" + tag + "-" + suffix, Name: tag + "-" + suffix,
			Status: status, CreatedBy: createdBy, CreatedAt: time.Now()}
		if err := repo.Create(ctx, exp); err != nil {
			t.Fatalf("create experiment %s: %v", suffix, err)
		}
		t.Cleanup(func() { _, _ = testDB.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
		return exp
	}
	planned := create("a", demo, "planned")
	finished := create("b", demo, "completed")
	create("c", "lab-smoke", "planned")

	ids := func(exps []*model.Experiment) []string {
		out := make([]string, 0, len(exps))
		for _, e := range exps {
			out = append(out, e.ID)
		}
		slices.Sort(out)
		return out
	}

	got, total, err := repo.List(ctx, ExperimentFilter{CreatedBy: demo}, Page{Limit: 200})
	if err != nil {
		t.Fatalf("list by creator: %v", err)
	}
	want := []string{planned.ID, finished.ID}
	slices.Sort(want)
	if !slices.Equal(ids(got), want) || total != 2 {
		t.Fatalf("List(CreatedBy=%q) = %v, total %d; want %v, total 2", demo, ids(got), total, want)
	}
	for _, e := range got {
		if e.CreatedBy != demo {
			t.Errorf("experiment %s created_by = %q, want %q", e.ID, e.CreatedBy, demo)
		}
	}

	got, total, err = repo.List(ctx, ExperimentFilter{CreatedBy: demo, Status: "planned"}, Page{Limit: 200})
	if err != nil {
		t.Fatalf("list by creator and status: %v", err)
	}
	if !slices.Equal(ids(got), []string{planned.ID}) || total != 1 {
		t.Fatalf("List(CreatedBy=%q, Status=planned) = %v, total %d; want [%s], total 1", demo, ids(got), total, planned.ID)
	}

	got, total, err = repo.List(ctx, ExperimentFilter{CreatedBy: "nobody-" + tag}, Page{Limit: 200})
	if err != nil {
		t.Fatalf("list by unknown creator: %v", err)
	}
	if len(got) != 0 || total != 0 {
		t.Fatalf("List(CreatedBy=unknown) = %v, total %d; want none", ids(got), total)
	}
}
