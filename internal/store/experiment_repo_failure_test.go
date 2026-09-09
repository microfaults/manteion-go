package store

import (
	"context"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// findPhaseItem pages through ListPhasesPaged until it finds phaseID (the
// shared test DB carries other suites' rows, so the item may sit on any page).
func findPhaseItem(t *testing.T, repo *ExperimentRepo, phaseID string) *model.PhaseListItem {
	t.Helper()
	ctx := context.Background()
	for offset := 0; ; offset += 200 {
		items, total, err := repo.ListPhasesPaged(ctx, Page{Limit: 200, Offset: offset})
		if err != nil {
			t.Fatalf("list phases paged: %v", err)
		}
		for _, it := range items {
			if it.ID == phaseID {
				return it
			}
		}
		if offset+200 >= total {
			t.Fatalf("phase %s not found in ListPhasesPaged (total=%d)", phaseID, total)
		}
	}
}

// findExperiment pages through List(status) until it finds expID.
func findExperiment(t *testing.T, repo *ExperimentRepo, status, expID string) *model.Experiment {
	t.Helper()
	ctx := context.Background()
	for offset := 0; ; offset += 200 {
		exps, total, err := repo.List(ctx, ExperimentFilter{Status: status}, Page{Limit: 200, Offset: offset})
		if err != nil {
			t.Fatalf("list experiments: %v", err)
		}
		for _, e := range exps {
			if e.ID == expID {
				return e
			}
		}
		if offset+200 >= total {
			t.Fatalf("experiment %s not found in List(status=%s) (total=%d)", expID, status, total)
		}
	}
}

// TestExperimentRepo_FailureReasonRoundTrip pins migration 11 and the Fail*
// CAS writes: the reason lands in the same statement as the failed
// transition and comes back on every SELECT that scans the row (GetPhase,
// ListPhasesForExperiment, ListPhasesPaged; Get, List), a healthy row reads
// back empty, and a lost CAS writes neither status nor reason.
func TestExperimentRepo_FailureReasonRoundTrip(t *testing.T) {
	ctx := context.Background()
	repo := NewExperimentRepo(testDB)

	exp := &model.Experiment{ID: id.New("exp"), Name: "fr-" + id.New("n"), Status: "planned", CreatedAt: time.Now()}
	if err := repo.Create(ctx, exp); err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	t.Cleanup(func() { _, _ = testDB.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
	if err := repo.UpdateStatus(ctx, exp.ID, "running"); err != nil {
		t.Fatalf("run experiment: %v", err)
	}
	failing := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "isolation-cartservice", Position: 0, Status: "pending"}
	healthy := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "baseline", Position: 1, Status: "pending"}
	for _, p := range []*model.ExperimentPhase{failing, healthy} {
		if err := repo.CreatePhase(ctx, p); err != nil {
			t.Fatalf("create phase %s: %v", p.Name, err)
		}
	}
	if err := repo.UpdatePhaseStatus(ctx, failing.ID, "running"); err != nil {
		t.Fatalf("run phase: %v", err)
	}

	// A lost CAS (wrong from-set) writes neither the status nor the reason.
	if ok, err := repo.FailPhase(ctx, failing.ID, "must not land", "paused"); err != nil || ok {
		t.Fatalf("FailPhase from wrong state: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if got, err := repo.GetPhase(ctx, failing.ID); err != nil || got.Status != "running" || got.FailureReason != "" {
		t.Fatalf("after lost CAS: %+v err=%v, want running with no reason", got, err)
	}

	const reason = `zeus rejected run run-1 for workflow wf-checkout: status 422: {"error":"dataset schema mismatch"}`
	if ok, err := repo.FailPhase(ctx, failing.ID, reason, "running"); err != nil || !ok {
		t.Fatalf("FailPhase: ok=%v err=%v, want ok=true", ok, err)
	}
	got, err := repo.GetPhase(ctx, failing.ID)
	if err != nil {
		t.Fatalf("get phase: %v", err)
	}
	if got.Status != "failed" || got.FailureReason != reason || got.CompletedAt == nil {
		t.Errorf("GetPhase after FailPhase: status=%q reason=%q completed_at=%v, want failed/%q/stamped",
			got.Status, got.FailureReason, got.CompletedAt, reason)
	}
	// The second caller loses and the winner's reason stays.
	if ok, err := repo.FailPhase(ctx, failing.ID, "second caller", "running"); err != nil || ok {
		t.Fatalf("second FailPhase: ok=%v err=%v, want ok=false", ok, err)
	}
	if got, _ := repo.GetPhase(ctx, failing.ID); got.FailureReason != reason {
		t.Errorf("reason after lost second CAS = %q, want the first winner's", got.FailureReason)
	}

	byID := map[string]*model.ExperimentPhase{}
	phases, err := repo.ListPhasesForExperiment(ctx, exp.ID)
	if err != nil {
		t.Fatalf("list phases: %v", err)
	}
	for _, p := range phases {
		byID[p.ID] = p
	}
	if p := byID[failing.ID]; p == nil || p.FailureReason != reason {
		t.Errorf("ListPhasesForExperiment failed phase = %+v, want reason %q", p, reason)
	}
	if p := byID[healthy.ID]; p == nil || p.FailureReason != "" || p.Status != "pending" {
		t.Errorf("ListPhasesForExperiment healthy phase = %+v, want pending with no reason", p)
	}
	if it := findPhaseItem(t, repo, failing.ID); it.FailureReason != reason || it.Status != "failed" {
		t.Errorf("ListPhasesPaged failed item = %+v, want failed with reason %q", it, reason)
	}
	if it := findPhaseItem(t, repo, healthy.ID); it.FailureReason != "" {
		t.Errorf("ListPhasesPaged healthy item carries reason %q", it.FailureReason)
	}

	// Experiment: same contract, naming the phase.
	expReason := "phase isolation-cartservice failed: " + reason
	if ok, err := repo.FailExperiment(ctx, exp.ID, expReason, "planned"); err != nil || ok {
		t.Fatalf("FailExperiment from wrong state: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if e, _ := repo.Get(ctx, exp.ID); e.FailureReason != "" || e.Status != "running" {
		t.Fatalf("after lost experiment CAS: %+v, want running with no reason", e)
	}
	if ok, err := repo.FailExperiment(ctx, exp.ID, expReason, "running"); err != nil || !ok {
		t.Fatalf("FailExperiment: ok=%v err=%v, want ok=true", ok, err)
	}
	e, err := repo.Get(ctx, exp.ID)
	if err != nil {
		t.Fatalf("get experiment: %v", err)
	}
	if e.Status != "failed" || e.FailureReason != expReason || e.CompletedAt == nil {
		t.Errorf("Get after FailExperiment: status=%q reason=%q completed_at=%v, want failed/%q/stamped",
			e.Status, e.FailureReason, e.CompletedAt, expReason)
	}
	if ok, err := repo.FailExperiment(ctx, exp.ID, "second caller", "running"); err != nil || ok {
		t.Fatalf("second FailExperiment: ok=%v err=%v, want ok=false", ok, err)
	}
	if got := findExperiment(t, repo, "failed", exp.ID); got.FailureReason != expReason {
		t.Errorf("List failed experiment reason = %q, want %q", got.FailureReason, expReason)
	}
}
